# mTLS client-authorization: not wired end to end (an operational gap)

**Noted 2026-08-14** by Riverkeeper, building R3's client-certificate
narrowing control; **wired the same day**. This is deliberately NOT filed
as a `FINDING_*` — it was never a breach. A client certificate could not
WIDEN what its holder saw; it simply failed to NARROW, leaving a cert
holder at the same self-asserted, honor-system baseline that SECURITY.md
exposure 7 already documents as open. So this is an mTLS completeness gap
— the client-authorization plane was not plumbed through — not a security
finding. Recorded because the document described the capability as
current; the fix made the document true. Confirmed over the wire against
the `tls` topology, and traced to two specific gaps in the query path.

---

## The documented claim

SECURITY.md lists, under "Known exposures" (whose preamble says these are
"verified properties of the current build, not hypotheticals"):

> **7.** ... Two credentials now change this *for callers that present
> one*: a verified client certificate (exposure 9), and ... a data-plane
> token whose grants intersect the caller's claims. Both only *narrow*
> what a caller sees ...

> **9.** ... a verified identity's SEC-2 claims are intersected with its
> grants.

So the document states, as current behaviour, that a verified client
certificate narrows a caller's SEC-2 visibility. The **token** half of
that sentence is real and Riverkeeper verifies it end to end
(`sec2-grants-only-narrow`). The **certificate** half is not reachable.

## What actually happens

A trusted client certificate presenting `X-TimeLake-Authorizations:
alpha,beta,gamma` over Flight SQL returns the identical row set to an
anonymous caller presenting the same claims — no narrowing:

```
no-cert     + claims(alpha,beta,gamma):  idx [0,1,2,3,4,5]
trusted-cert + claims(alpha,beta,gamma): idx [0,1,2,3,4,5]
```

## Why — two gaps, either sufficient

1. **The identity never reaches the query session.** The verified
   certificate identity is captured at the Flight connection
   (`PeerIdentity`, `flight/src/lib.rs`) and counted for the
   authenticated/anonymous metric, but the request handlers resolve
   authorizations through `resolve_auths` → `authenticate_read`, which
   folds in **token** grants only. `Engine::sql_batches` then builds the
   session with `QuerySession::with_authorizations(auths)` — `identity:
   None` — and never calls `.resolve(granted)`. So the narrowing method
   that *does* exist (`QuerySession::resolve`, and its unit tests
   `a_verified_identity_narrows_its_claims_to_its_grants` /
   `an_identity_with_no_grants_recorded_keeps_its_claims`) is dead code in
   the request path.

2. **There is no surface to grant a certificate anything.** Token grants
   are set at issue time (`POST /admin/tokens`, `authorizations`). There
   is no equivalent for a certificate identity — no store, no admin route
   mapping a CN to a set of authorizations. So even with gap 1 fixed,
   every certificate would be the "no grants recorded" case, which
   `resolve` correctly passes through unchanged. The narrowing condition
   `claims ∩ grants ⊊ claims` cannot be constructed for a certificate.

The mechanism is correct and unit-tested in isolation; it is simply not
wired to anything a deployment can drive.

## Why this is an operational gap, not a finding

The certificate does not WIDEN visibility, and the anonymous
honor-system path is exactly as (un)restricted as documented. Nobody
sees anything they should not — there is no breach and no
over-permission. The missing behaviour was an OPTIONAL tightening (hold
a cert holder to less than it claims), and its absence leaves that
holder at the self-asserted baseline exposure 7 already documents as
open. So an operator relying on the documented posture is not misled
about their actual exposure; they simply do not get an additional
restriction the document implied was available. That is mTLS
client-authorization being incomplete — a maturity/plumbing gap — rather
than a security defect. It is recorded because the document described the
capability as a current property, and closing the gap made that true.

## The options (a posture/product decision, not Riverkeeper's)

Riverkeeper does not set posture. Both directions were legitimate:

- **Soften the document.** Mark the certificate-narrowing path in
  exposures 7 and 9 as a mechanism present in the query layer but not yet
  operational (no identity plumbing, no grant surface), the way other
  phased items are described. Riverkeeper then verifies the certificate's
  *actual* current property — "grants nothing on its own": a certificate
  neither widens nor restricts visibility — which is testable and
  falsifiable.
- **Wire the feature.** Thread `PeerIdentity` into `sql_batches`'
  `QuerySession` and add an admin surface that grants a certificate
  identity a set of authorizations. Then the full narrowing property
  becomes verifiable end to end, and Riverkeeper's control reuses the
  `sec2-grants-only-narrow` oracle over the certificate path.

## Status

**Fixed 2026-08-14** — the feature was wired (operator decision, over
softening the document). Both gaps are closed:

1. `PeerIdentity` now threads from the Flight connection through
   `SqlBackend::query_batches` into `Engine::sql_batches`, which sets
   `QuerySession.identity` and calls `.resolve(granted)`. The cert
   identity reaches the query.
2. `Auth` gained a persisted cert-grants store (`cert-grants.json`,
   beside the tokens) and an admin surface: `GET /admin/cert-grants`,
   `PUT /admin/cert-grants/{cn}`, `DELETE /admin/cert-grants/{cn}`. An
   operator can now grant a certificate identity a set of authorizations.

So SECURITY.md exposures 7 and 9 are now accurate as written — the
implementation caught up to the document rather than the other way
around. Riverkeeper's `flight-cert-grants-narrow` verifies it end to end
over the `tls` topology, reusing the token control's SEC-2 oracle: a
trusted certificate granted G, presenting claims C, sees exactly C∩G and
never more. The control's probe mutation doubles as the regression guard
— before this fix a certificate did not narrow, so the "grants ignored"
assertion would have HELD (NOT-FALSIFIABLE); it now deviates, PROVEN.

Rust regression coverage:
`crates/auth/src/lib.rs::cert_grants_are_set_read_removed_and_survive_reopen`
plus the pre-existing `QuerySession` intersection tests.
