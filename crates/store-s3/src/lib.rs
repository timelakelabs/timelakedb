//! S3 backend for the [`Store`] chokepoint + AWS KMS backend for the
//! [`Kms`] seam (ARCHITECTURE §12.1/§12.2).
//!
//! Everything async lives on an OWNED runtime ([`AwsContext`]): engine
//! threads call the store from blocking pools AND from async contexts
//! (`Engine::open` runs inside the server's runtime), and `block_on`
//! from an async context panics — so calls are `spawn`ed onto the owned
//! runtime and awaited over a channel, which is safe from any thread.
//!
//! Build-time deviation from §12.1 recorded here: aws-sdk-s3 directly,
//! not the `object_store` crate — the SDK exposes exact control of
//! SSE-KMS, `bucket-key-enabled`, and `If-None-Match` (the CAS
//! primitive), which is the point of the exercise. The abstraction that
//! matters is our own `Store` trait, and it is unchanged.

use std::io::{Error, ErrorKind, Result};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::ServerSideEncryption;
use timelord_store::{Kms, Store};

/// One runtime + one credential/config resolution, shared by the S3 and
/// KMS clients (same region, same endpoint override, same chain —
/// `AWS_ENDPOINT_URL` pointed at LocalStack switches everything at once).
pub struct AwsContext {
    rt: tokio::runtime::Runtime,
    config: aws_config::SdkConfig,
}

impl AwsContext {
    pub fn new() -> Result<Arc<AwsContext>> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("timelord-aws")
            .enable_all()
            .build()
            .map_err(|e| Error::other(format!("aws runtime: {e}")))?;
        let config = {
            let (tx, rx) = std::sync::mpsc::channel();
            rt.spawn(async move {
                let cfg = aws_config::defaults(aws_config::BehaviorVersion::latest())
                    .load()
                    .await;
                let _ = tx.send(cfg);
            });
            rx.recv()
                .map_err(|_| Error::other("aws config load: runtime died"))?
        };
        Ok(Arc::new(AwsContext { rt, config }))
    }

    /// Run `fut` on the owned runtime, block the CALLING thread (never a
    /// runtime worker of ours) until it completes.
    fn wait<T: Send + 'static>(
        &self,
        fut: impl std::future::Future<Output = T> + Send + 'static,
    ) -> Result<T> {
        let (tx, rx) = std::sync::mpsc::channel();
        self.rt.spawn(async move {
            let _ = tx.send(fut.await);
        });
        rx.recv()
            .map_err(|_| Error::other("aws task dropped: runtime died"))
    }
}

/// Per-op request counters for /metrics — the drill's raw material.
#[derive(Default)]
pub struct S3Stats {
    pub get_total: AtomicU64,
    pub put_total: AtomicU64,
    pub head_total: AtomicU64,
    pub list_total: AtomicU64,
    pub delete_total: AtomicU64,
    pub read_bytes_total: AtomicU64,
    pub write_bytes_total: AtomicU64,
}

pub struct S3Store {
    ctx: Arc<AwsContext>,
    client: aws_sdk_s3::Client,
    bucket: String,
    /// Empty, or "prefix/" — always slash-terminated when non-empty.
    prefix: String,
    /// SSE-KMS key for server-side encryption; None = rely on the
    /// bucket's default encryption configuration.
    sse_key_id: Option<String>,
    stats: Arc<S3Stats>,
}

impl S3Store {
    /// `url` is `s3://bucket` or `s3://bucket/prefix`. Path-style
    /// addressing is forced automatically under an endpoint override
    /// (LocalStack), or explicitly via `TIMELORD_S3_FORCE_PATH_STYLE=1`.
    pub fn new(ctx: Arc<AwsContext>, url: &str, sse_key_id: Option<String>) -> Result<S3Store> {
        let (bucket, prefix) = parse_s3_url(url)?;

        let endpoint_override = std::env::var("AWS_ENDPOINT_URL").is_ok();
        let force_path = endpoint_override
            || std::env::var("TIMELORD_S3_FORCE_PATH_STYLE").as_deref() == Ok("1");
        let s3cfg = aws_sdk_s3::config::Builder::from(&ctx.config)
            .force_path_style(force_path)
            .build();
        Ok(S3Store {
            client: aws_sdk_s3::Client::from_conf(s3cfg),
            ctx,
            bucket,
            prefix,
            sse_key_id,
            stats: Arc::new(S3Stats::default()),
        })
    }

    pub fn stats(&self) -> Arc<S3Stats> {
        self.stats.clone()
    }

    fn key(&self, path: &str) -> String {
        format!("{}{}", self.prefix, path.trim_matches('/'))
    }

    fn sse(
        &self,
        req: aws_sdk_s3::operation::put_object::builders::PutObjectFluentBuilder,
    ) -> aws_sdk_s3::operation::put_object::builders::PutObjectFluentBuilder {
        match &self.sse_key_id {
            Some(id) => req
                .server_side_encryption(ServerSideEncryption::AwsKms)
                .ssekms_key_id(id)
                // S3's own key cache — without it SSE-KMS pays one KMS
                // call per object and no client code can help (§12.2)
                .bucket_key_enabled(true),
            None => req,
        }
    }
}

fn io_err(op: &str, path: &str, e: impl std::fmt::Display) -> Error {
    Error::other(format!("s3 {op} {path}: {e}"))
}

/// `s3://bucket` or `s3://bucket/some/prefix` → (bucket, "" | "prefix/").
fn parse_s3_url(url: &str) -> Result<(String, String)> {
    let rest = url.strip_prefix("s3://").ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidInput,
            format!("TIMELORD_OBJECT_STORE must be s3://bucket[/prefix], got {url:?}"),
        )
    })?;
    let (bucket, prefix) = match rest.split_once('/') {
        None => (rest.to_string(), String::new()),
        Some((b, p)) => {
            let p = p.trim_matches('/');
            (
                b.to_string(),
                if p.is_empty() {
                    String::new()
                } else {
                    format!("{p}/")
                },
            )
        }
    };
    if bucket.is_empty() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "s3:// url has no bucket",
        ));
    }
    Ok((bucket, prefix))
}

impl Store for S3Store {
    fn put(&self, path: &str, bytes: &[u8]) -> Result<()> {
        self.stats.put_total.fetch_add(1, Ordering::Relaxed);
        self.stats
            .write_bytes_total
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        let req = self
            .sse(self.client.put_object())
            .bucket(&self.bucket)
            .key(self.key(path))
            .body(ByteStream::from(bytes.to_vec()));
        let path = path.to_string();
        self.ctx
            .wait(async move { req.send().await })?
            .map(|_| ())
            .map_err(|e| io_err("put", &path, aws_sdk_s3::error::DisplayErrorContext(e)))
    }

    fn put_if_absent(&self, path: &str, bytes: &[u8]) -> Result<bool> {
        // If-None-Match: * — the CAS primitive (§12.3). 412 = the other
        // writer won. 409 = another conditional write was mid-flight and
        // S3 asks us to retry; bounded here because the caller's commit
        // loop treats Ok(false) as "re-read and re-propose", not retry.
        for attempt in 0..5 {
            self.stats.put_total.fetch_add(1, Ordering::Relaxed);
            let req = self
                .sse(self.client.put_object())
                .bucket(&self.bucket)
                .key(self.key(path))
                .if_none_match("*")
                .body(ByteStream::from(bytes.to_vec()));
            let res = self.ctx.wait(async move { req.send().await })?;
            match res {
                Ok(_) => {
                    self.stats
                        .write_bytes_total
                        .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                    return Ok(true);
                }
                Err(e) => {
                    let status = e.raw_response().map(|r| r.status().as_u16()).unwrap_or(0);
                    match status {
                        412 => return Ok(false),
                        409 if attempt < 4 => {
                            tracing::warn!(
                                path,
                                attempt,
                                "s3 conditional write conflict (409); retrying"
                            );
                            std::thread::sleep(std::time::Duration::from_millis(20 << attempt));
                            continue;
                        }
                        _ => {
                            return Err(io_err(
                                "put-if-absent",
                                path,
                                aws_sdk_s3::error::DisplayErrorContext(e),
                            ));
                        }
                    }
                }
            }
        }
        unreachable!("bounded retry loop returns");
    }

    fn get(&self, path: &str) -> Result<Vec<u8>> {
        self.stats.get_total.fetch_add(1, Ordering::Relaxed);
        let req = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(self.key(path));
        let out = self.ctx.wait(async move {
            match req.send().await {
                Ok(o) => match o.body.collect().await {
                    Ok(b) => Ok(b.into_bytes().to_vec()),
                    Err(e) => Err(format!("body: {e}")),
                },
                Err(e)
                    if matches!(&e,
                    aws_sdk_s3::error::SdkError::ServiceError(se)
                        if se.err().is_no_such_key()) =>
                {
                    Err("NOT_FOUND".to_string())
                }
                Err(e) => Err(aws_sdk_s3::error::DisplayErrorContext(e).to_string()),
            }
        })?;
        match out {
            Ok(v) => {
                self.stats
                    .read_bytes_total
                    .fetch_add(v.len() as u64, Ordering::Relaxed);
                Ok(v)
            }
            Err(m) if m == "NOT_FOUND" => Err(Error::new(
                ErrorKind::NotFound,
                format!("s3 get {path}: no such key"),
            )),
            Err(m) => Err(io_err("get", path, m)),
        }
    }

    fn get_range(&self, path: &str, offset: u64, len: usize) -> Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        self.stats.get_total.fetch_add(1, Ordering::Relaxed);
        let range = format!("bytes={offset}-{}", offset + len as u64 - 1);
        let req = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(self.key(path))
            .range(range);
        let out = self.ctx.wait(async move {
            match req.send().await {
                Ok(o) => match o.body.collect().await {
                    Ok(b) => Ok(b.into_bytes().to_vec()),
                    Err(e) => Err(format!("body: {e}")),
                },
                // a start past EOF is 416 InvalidRange: the local-store
                // contract says short read, not error
                Err(e)
                    if matches!(&e,
                    aws_sdk_s3::error::SdkError::ServiceError(se)
                        if se.raw().status().as_u16() == 416) =>
                {
                    Ok(Vec::new())
                }
                Err(e) => Err(aws_sdk_s3::error::DisplayErrorContext(e).to_string()),
            }
        })?;
        match out {
            Ok(v) => {
                self.stats
                    .read_bytes_total
                    .fetch_add(v.len() as u64, Ordering::Relaxed);
                Ok(v)
            }
            Err(m) => Err(io_err("get-range", path, m)),
        }
    }

    fn size(&self, path: &str) -> Result<u64> {
        self.stats.head_total.fetch_add(1, Ordering::Relaxed);
        let req = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(self.key(path));
        let out = self.ctx.wait(async move {
            req.send()
                .await
                .map_err(|e| aws_sdk_s3::error::DisplayErrorContext(e).to_string())
        })?;
        let head = out.map_err(|m| io_err("head", path, m))?;
        head.content_length()
            .map(|l| l as u64)
            .ok_or_else(|| io_err("head", path, "no content-length"))
    }

    fn delete(&self, path: &str) -> Result<()> {
        self.stats.delete_total.fetch_add(1, Ordering::Relaxed);
        let req = self
            .client
            .delete_object()
            .bucket(&self.bucket)
            .key(self.key(path));
        let path = path.to_string();
        self.ctx
            .wait(async move { req.send().await })?
            .map(|_| ()) // S3 DeleteObject on a missing key is a no-op 204, like LocalStore
            .map_err(|e| io_err("delete", &path, aws_sdk_s3::error::DisplayErrorContext(e)))
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        // LocalStore lists a DIRECTORY, so the S3 prefix is
        // slash-terminated — "catalog" must not match "catalog2/".
        let dir = prefix.trim_matches('/');
        let key_prefix = if dir.is_empty() {
            self.prefix.clone()
        } else {
            format!("{}{dir}/", self.prefix)
        };
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            self.stats.list_total.fetch_add(1, Ordering::Relaxed);
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&key_prefix);
            if let Some(t) = &token {
                req = req.continuation_token(t);
            }
            let page = self
                .ctx
                .wait(async move { req.send().await })?
                .map_err(|e| io_err("list", prefix, aws_sdk_s3::error::DisplayErrorContext(e)))?;
            for obj in page.contents() {
                if let Some(key) = obj.key()
                    && let Some(rel) = key.strip_prefix(self.prefix.as_str())
                {
                    out.push(rel.to_string());
                }
            }
            match page.next_continuation_token() {
                Some(t) => token = Some(t.to_string()),
                None => break,
            }
        }
        // S3 returns keys in lexicographic order and the shared root
        // prefix preserves it — but the manifest replay contract is
        // load-bearing, so sort anyway rather than trust it.
        out.sort();
        Ok(out)
    }
}

/// [`Kms`] over AWS KMS. `generate` maps 1:1 to GenerateDataKey (one
/// call returns the plaintext/wrapped pair); Decrypt infers the CMK from
/// the ciphertext blob. Wrap behind [`timelord_store::CachingKms`] in
/// production — that is where "thousands of calls" becomes a handful.
pub struct AwsKms {
    ctx: Arc<AwsContext>,
    client: aws_sdk_kms::Client,
    /// Key id, ARN, or alias (`alias/timelord`).
    key_id: String,
}

impl AwsKms {
    pub fn new(ctx: Arc<AwsContext>, key_id: String) -> AwsKms {
        AwsKms {
            client: aws_sdk_kms::Client::new(&ctx.config),
            ctx,
            key_id,
        }
    }
}

impl Kms for AwsKms {
    fn wrap(&self, dek: &[u8; 32]) -> Result<Vec<u8>> {
        let req = self
            .client
            .encrypt()
            .key_id(&self.key_id)
            .plaintext(aws_sdk_kms::primitives::Blob::new(dek.to_vec()));
        let out = self
            .ctx
            .wait(async move {
                req.send()
                    .await
                    .map_err(|e| aws_sdk_kms::error::DisplayErrorContext(e).to_string())
            })?
            .map_err(Error::other)?;
        out.ciphertext_blob()
            .map(|b| b.as_ref().to_vec())
            .ok_or_else(|| Error::other("kms encrypt returned no ciphertext"))
    }

    fn unwrap(&self, wrapped: &[u8]) -> Result<[u8; 32]> {
        let req = self
            .client
            .decrypt()
            .ciphertext_blob(aws_sdk_kms::primitives::Blob::new(wrapped.to_vec()));
        let out = self
            .ctx
            .wait(async move {
                req.send()
                    .await
                    .map_err(|e| aws_sdk_kms::error::DisplayErrorContext(e).to_string())
            })?
            .map_err(|m| Error::new(ErrorKind::InvalidData, format!("kms decrypt failed: {m}")))?;
        let pt = out
            .plaintext()
            .ok_or_else(|| Error::other("kms decrypt returned no plaintext"))?;
        let bytes: &[u8] = pt.as_ref();
        bytes.try_into().map_err(|_| {
            Error::new(
                ErrorKind::InvalidData,
                format!("kms decrypt returned {} bytes, wanted 32", bytes.len()),
            )
        })
    }

    fn generate(&self) -> Result<([u8; 32], Vec<u8>)> {
        let req = self
            .client
            .generate_data_key()
            .key_id(&self.key_id)
            .key_spec(aws_sdk_kms::types::DataKeySpec::Aes256);
        let out = self
            .ctx
            .wait(async move {
                req.send()
                    .await
                    .map_err(|e| aws_sdk_kms::error::DisplayErrorContext(e).to_string())
            })?
            .map_err(Error::other)?;
        let pt: [u8; 32] = out
            .plaintext()
            .map(|b| b.as_ref().to_vec())
            .ok_or_else(|| Error::other("GenerateDataKey returned no plaintext"))?
            .try_into()
            .map_err(|_| Error::other("GenerateDataKey plaintext is not 32 bytes"))?;
        let wrapped = out
            .ciphertext_blob()
            .map(|b| b.as_ref().to_vec())
            .ok_or_else(|| Error::other("GenerateDataKey returned no ciphertext"))?;
        Ok((pt, wrapped))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_parsing() {
        assert_eq!(parse_s3_url("s3://b").unwrap(), ("b".into(), String::new()));
        assert_eq!(
            parse_s3_url("s3://b/").unwrap(),
            ("b".into(), String::new())
        );
        assert_eq!(
            parse_s3_url("s3://bucket/a/b/").unwrap(),
            ("bucket".into(), "a/b/".into())
        );
        assert!(parse_s3_url("file:///nope").is_err());
        assert!(parse_s3_url("s3://").is_err());
    }

    /// Everything below needs LocalStack (or real AWS) and is ignored by
    /// default. The C0 drill runs it inside the compose network:
    ///   AWS_ENDPOINT_URL=http://localstack:4566 AWS_REGION=us-east-1 \
    ///   AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
    ///   TLDB_S3_TEST_BUCKET=timelord-it TLDB_KMS_TEST_KEY=alias/timelord \
    ///   cargo test -p timelord-store-s3 -- --ignored
    fn it_env() -> Option<(Arc<AwsContext>, String, String)> {
        let bucket = std::env::var("TLDB_S3_TEST_BUCKET").ok()?;
        let key = std::env::var("TLDB_KMS_TEST_KEY").ok()?;
        Some((AwsContext::new().unwrap(), bucket, key))
    }

    #[test]
    #[ignore = "needs LocalStack/AWS: see it_env()"]
    fn s3_store_contract() {
        let (ctx, bucket, _) = it_env().expect("TLDB_S3_TEST_BUCKET not set");
        let s = S3Store::new(ctx, &format!("s3://{bucket}/contract"), None).unwrap();

        let body: Vec<u8> = (0..200_000u32).flat_map(|i| i.to_le_bytes()).collect();
        s.put("db/t/data/2026080900/a.parquet", &body).unwrap();
        assert_eq!(s.get("db/t/data/2026080900/a.parquet").unwrap(), body);
        assert_eq!(
            s.size("db/t/data/2026080900/a.parquet").unwrap(),
            body.len() as u64
        );

        // range semantics must match LocalStore exactly
        assert_eq!(
            s.get_range("db/t/data/2026080900/a.parquet", 4, 8).unwrap(),
            &body[4..12]
        );
        let tail = s
            .get_range("db/t/data/2026080900/a.parquet", body.len() as u64 - 5, 100)
            .unwrap();
        assert_eq!(tail, &body[body.len() - 5..]);
        assert_eq!(
            s.get_range("db/t/data/2026080900/a.parquet", body.len() as u64 + 10, 4)
                .unwrap(),
            Vec::<u8>::new()
        );

        // list is directory-scoped and sorted
        s.put("db/t/data/2026080901/b.parquet", b"B").unwrap();
        let listed = s.list("db/t").unwrap();
        assert_eq!(listed.len(), 2);
        assert!(listed[0] < listed[1]);
        assert!(s.list("db/t2").unwrap().is_empty(), "no prefix bleed");

        // the CAS primitive (LocalStack fidelity check, §12.6)
        assert!(s.put_if_absent("cas/000001.json", b"WINNER").unwrap());
        assert!(
            !s.put_if_absent("cas/000001.json", b"loser").unwrap(),
            "LocalStack must honor If-None-Match — if this fails, the CAS \
             drill moves to a real S3 sandbox (ARCHITECTURE §16 risk 6)"
        );
        assert_eq!(s.get("cas/000001.json").unwrap(), b"WINNER");

        s.delete("db/t/data/2026080900/a.parquet").unwrap();
        s.delete("db/t/data/2026080900/a.parquet").unwrap(); // idempotent
        s.delete("db/t/data/2026080901/b.parquet").unwrap();
        s.delete("cas/000001.json").unwrap();
    }

    #[test]
    #[ignore = "needs LocalStack/AWS: see it_env()"]
    fn kms_roundtrip_and_encrypted_store_over_s3() {
        use timelord_store::{CachingKms, EncryptingStore};
        let (ctx, bucket, key_id) = it_env().expect("TLDB env not set");

        let kms = AwsKms::new(ctx.clone(), key_id);
        let (dek, wrapped) = kms.generate().unwrap();
        assert_eq!(kms.unwrap(&wrapped).unwrap(), dek);

        // the full production stack: EncryptingStore(CachingKms(AwsKms), S3Store)
        let cached = CachingKms::new(
            AwsKms::new(ctx.clone(), std::env::var("TLDB_KMS_TEST_KEY").unwrap()),
            std::time::Duration::from_secs(300),
            1000,
        );
        let stats = cached.stats();
        let s3 = S3Store::new(ctx, &format!("s3://{bucket}/enc"), None).unwrap();
        let enc = EncryptingStore::new(s3, Arc::new(cached));

        for i in 0..20 {
            enc.put(&format!("obj/{i:04}"), format!("payload {i}").as_bytes())
                .unwrap();
        }
        for i in 0..20 {
            assert_eq!(
                enc.get(&format!("obj/{i:04}")).unwrap(),
                format!("payload {i}").as_bytes()
            );
        }
        // 20 objects, ONE GenerateDataKey, zero Decrypts (self-seeded)
        assert_eq!(stats.generate_calls.load(Ordering::Relaxed), 1);
        assert_eq!(stats.generate_hits.load(Ordering::Relaxed), 19);
        assert_eq!(stats.decrypt_calls.load(Ordering::Relaxed), 0);

        for i in 0..20 {
            enc.delete(&format!("obj/{i:04}")).unwrap();
        }
    }
}
