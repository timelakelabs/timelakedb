#!/bin/sh
# TimeLakeDB backup and restore (AT-5).
#
# Everything runs through a throwaway helper container, so the only host
# prerequisite is Docker: no tar, no gzip, no host bind mounts (which are
# what break this on Windows and macOS), and no dependency on the
# TimeLakeDB image itself.
#
#   ./ops/tldb-backup.sh backup                       # live, no downtime
#   ./ops/tldb-backup.sh verify  -f FILE
#   ./ops/tldb-backup.sh restore -f FILE --recreate
#
# See docs/BACKUP_RESTORE.md for the runbook and the consistency argument.
set -eu

# Git Bash / MSYS rewrites bare "/data" style arguments into Windows paths
# before docker ever sees them. Harmless everywhere else.
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

VOLUME="${TLDB_VOLUME:-bench-timelakedb_timelake-data}"
HELPER="${TLDB_HELPER_IMAGE:-alpine:3}"
COMPRESS=1
RECREATE=0
ASSUME_YES=0
STOP_CONTAINER=""
FILE=""
OUT=""

die() { printf 'error: %s\n' "$*" >&2; exit 1; }
note() { printf '%s\n' "$*" >&2; }

usage() {
  cat <<'EOF'
usage: tldb-backup.sh <command> [options]

commands:
  backup            archive a data volume to a file on this host
  verify   -f FILE  check an archive is structurally complete
  restore  -f FILE  restore an archive into a data volume

options:
  -v VOLUME         Docker volume holding TIMELAKE_DATA_DIR
                    (default: $TLDB_VOLUME or bench-timelakedb_timelake-data)
  -o FILE           backup output path (default: ./timelake-backup-<UTC>.tgz)
  -f FILE           archive to verify or restore
  --no-compress     write a plain .tar; Parquet is already compressed, so
                    this is usually much faster and barely larger
  --stop NAME       stop container NAME for the duration of the backup and
                    start it again afterwards (a quiesced backup; not
                    required for correctness)
  --recreate        restore only: delete and recreate the target volume
                    first, so the restore lands on empty storage
  -y                do not prompt before destructive steps
  -h, --help        this text

environment:
  TLDB_VOLUME         default volume name
  TLDB_HELPER_IMAGE   helper image for tar (default alpine:3)
EOF
}

need_docker() {
  command -v docker >/dev/null 2>&1 || die "docker is not on PATH"
  docker info >/dev/null 2>&1 || die "cannot talk to the Docker daemon"
}

volume_exists() { docker volume inspect "$1" >/dev/null 2>&1; }

# Containers (running or not) that mount the volume.
users_of_volume() {
  docker ps -a --filter "volume=$1" --format '{{.Names}} ({{.State}})' 2>/dev/null
}

running_users_of_volume() {
  docker ps --filter "volume=$1" --format '{{.Names}}' 2>/dev/null
}

# Gzip magic (1f 8b) — cheaper and more honest than trusting the suffix.
is_gzip() {
  magic=$(od -An -tx1 -N2 < "$1" 2>/dev/null | tr -d ' \n')
  [ "$magic" = "1f8b" ]
}

confirm() {
  [ "$ASSUME_YES" -eq 1 ] && return 0
  printf '%s [y/N] ' "$1" >&2
  read -r reply
  case "$reply" in y|Y|yes|YES) return 0 ;; *) die "aborted" ;; esac
}

cmd=""
case "${1:-}" in
  backup|verify|restore) cmd="$1"; shift ;;
  -h|--help|"") usage; exit 0 ;;
  *) die "unknown command '$1' (try --help)" ;;
esac

while [ $# -gt 0 ]; do
  case "$1" in
    -v) VOLUME="${2:?-v needs a volume name}"; shift 2 ;;
    -o) OUT="${2:?-o needs a path}"; shift 2 ;;
    -f) FILE="${2:?-f needs a path}"; shift 2 ;;
    --no-compress) COMPRESS=0; shift ;;
    --stop) STOP_CONTAINER="${2:?--stop needs a container name}"; shift 2 ;;
    --recreate) RECREATE=1; shift ;;
    -y) ASSUME_YES=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown option '$1' (try --help)" ;;
  esac
done

need_docker

case "$cmd" in

backup)
  volume_exists "$VOLUME" || die "no such Docker volume: $VOLUME"

  if [ -z "$OUT" ]; then
    stamp=$(date -u +%Y%m%d-%H%M%S)
    if [ "$COMPRESS" -eq 1 ]; then OUT="timelake-backup-$stamp.tgz"
    else OUT="timelake-backup-$stamp.tar"; fi
  fi
  [ -e "$OUT" ] && die "$OUT already exists"

  if [ -n "$STOP_CONTAINER" ]; then
    note "stopping $STOP_CONTAINER for a quiesced backup"
    docker stop "$STOP_CONTAINER" >/dev/null
  fi

  note "archiving volume $VOLUME -> $OUT"
  # Read-only mount: a live backup can never modify the source. The archive
  # streams over stdout, so no host directory is mounted anywhere.
  if [ "$COMPRESS" -eq 1 ]; then
    docker run --rm -v "$VOLUME":/data:ro "$HELPER" \
      tar czf - -C /data . > "$OUT"
  else
    docker run --rm -v "$VOLUME":/data:ro "$HELPER" \
      tar cf - -C /data . > "$OUT"
  fi

  if [ -n "$STOP_CONTAINER" ]; then
    note "starting $STOP_CONTAINER"
    docker start "$STOP_CONTAINER" >/dev/null
  fi

  size=$(wc -c < "$OUT" | tr -d ' ')
  note "wrote $OUT ($size bytes)"
  note "verify it now:  $0 verify -f $OUT"
  ;;

verify)
  [ -n "$FILE" ] || die "verify needs -f FILE"
  [ -f "$FILE" ] || die "no such file: $FILE"

  note "reading $FILE"
  if is_gzip "$FILE"; then
    listing=$(docker run --rm -i "$HELPER" tar tzf - < "$FILE")
  else
    listing=$(docker run --rm -i "$HELPER" tar tf - < "$FILE")
  fi
  [ -n "$listing" ] || die "archive is empty or unreadable"

  manifests=$(printf '%s\n' "$listing" | grep -c 'objects/catalog/manifest/.*\.json$' || true)
  parquet=$(printf '%s\n'  "$listing" | grep -c '\.parquet$' || true)
  wal=$(printf '%s\n'      "$listing" | grep -c 'wal/wal\..*\.log$' || true)
  partial=$(printf '%s\n'  "$listing" | grep -c '\.tmp-write$' || true)
  entries=$(printf '%s\n'  "$listing" | wc -l | tr -d ' ')

  note "entries:        $entries"
  note "manifest log:   $manifests"
  note "parquet files:  $parquet"
  note "WAL segments:   $wal"

  [ "$manifests" -gt 0 ] || die "no catalog manifests — this is not a TimeLakeDB data directory"

  # Objects are written temp-then-rename, so a live backup can catch a
  # half-written .tmp-write file. It is never referenced by the catalog, but
  # one inside catalog/manifest/ would fail manifest replay at boot — restore
  # removes them.
  if [ "$partial" -gt 0 ]; then
    note "note:           $partial in-flight .tmp-write file(s) captured; restore will drop them"
  fi
  note "OK: archive contains a replayable catalog"
  ;;

restore)
  [ -n "$FILE" ] || die "restore needs -f FILE"
  [ -f "$FILE" ] || die "no such file: $FILE"

  running=$(running_users_of_volume "$VOLUME" || true)
  if [ -n "$running" ]; then
    die "these containers are running on $VOLUME: $running
stop them first — restoring under a live server corrupts the catalog"
  fi

  if [ "$RECREATE" -eq 1 ]; then
    if volume_exists "$VOLUME"; then
      holders=$(users_of_volume "$VOLUME" || true)
      [ -n "$holders" ] && note "note: stopped containers still reference it: $holders"
      confirm "delete and recreate volume $VOLUME (all current data is lost)?"
      # Any stopped container attached to the volume must be removed first;
      # Docker refuses to delete a volume that is still referenced.
      docker volume rm "$VOLUME" >/dev/null
    fi
    docker volume create "$VOLUME" >/dev/null
    note "recreated volume $VOLUME"
  else
    volume_exists "$VOLUME" || docker volume create "$VOLUME" >/dev/null
    note "restoring into existing volume $VOLUME (use --recreate for a clean restore)"
  fi

  note "restoring $FILE -> $VOLUME"
  if is_gzip "$FILE"; then
    docker run --rm -i -v "$VOLUME":/data "$HELPER" tar xzf - -C /data < "$FILE"
  else
    docker run --rm -i -v "$VOLUME":/data "$HELPER" tar xf - -C /data < "$FILE"
  fi

  # Drop any half-written object the live backup caught mid-rename. These are
  # unreferenced by definition, but one landing in catalog/manifest/ would
  # abort manifest replay at boot.
  dropped=$(docker run --rm -v "$VOLUME":/data "$HELPER" sh -c \
    'n=$(find /data -name "*.tmp-write" | wc -l); find /data -name "*.tmp-write" -delete; echo "$n"' | tr -d ' ')
  if [ "${dropped:-0}" -gt 0 ]; then
    note "dropped $dropped in-flight .tmp-write file(s)"
  fi

  docker run --rm -v "$VOLUME":/data:ro "$HELPER" \
    sh -c 'echo "restored:"; ls /data; echo "manifests: $(ls /data/objects/catalog/manifest 2>/dev/null | wc -l)"' >&2

  note "done — start the server and check /health, then count rows"
  ;;

esac
