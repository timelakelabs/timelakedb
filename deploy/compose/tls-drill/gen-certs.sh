#!/bin/sh
# AT-7 drill certs: a throwaway CA plus a short-TTL (24 h) server cert —
# the daily-renewal shape SEC-3 is designed for. Usage:
#   ./gen-certs.sh initial   # CA + server.crt/key (the pair the server boots with)
#   ./gen-certs.sh renewal   # renewal.crt/key: fresh 24 h pair from the same CA
# SANs cover host-side clients (localhost/127.0.0.1) and the in-compose
# hostname Telegraf dials (timelakedb-tls).
set -e
cd "$(dirname "$0")"
# Git-Bash on Windows rewrites "/CN=..." into a filesystem path without this
export MSYS_NO_PATHCONV=1
mkdir -p certs
SAN="subjectAltName=DNS:localhost,DNS:timelakedb-tls,IP:127.0.0.1"

if [ "$1" = "initial" ]; then
  openssl ecparam -name prime256v1 -genkey -noout -out certs/ca.key
  # keyUsage/basicConstraints explicit: OpenSSL 3.x clients (Python ssl)
  # reject a CA whose keyUsage extension is absent
  openssl req -new -x509 -key certs/ca.key -out certs/ca.crt -days 10 \
    -subj "/CN=timelake-drill-ca" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign"
  openssl ecparam -name prime256v1 -genkey -noout -out certs/server.key
  openssl req -new -key certs/server.key -out certs/server.csr \
    -subj "/CN=timelakedb" -addext "$SAN" \
    -addext "keyUsage=critical,digitalSignature" \
    -addext "extendedKeyUsage=serverAuth"
  openssl x509 -req -in certs/server.csr -CA certs/ca.crt -CAkey certs/ca.key \
    -CAcreateserial -out certs/server.crt -days 1 -copy_extensions copy
  rm -f certs/server.csr
  echo "initial: $(openssl x509 -in certs/server.crt -noout -serial -enddate)"
elif [ "$1" = "renewal" ]; then
  openssl ecparam -name prime256v1 -genkey -noout -out certs/renewal.key
  openssl req -new -key certs/renewal.key -out certs/renewal.csr \
    -subj "/CN=timelakedb" -addext "$SAN" \
    -addext "keyUsage=critical,digitalSignature" \
    -addext "extendedKeyUsage=serverAuth"
  openssl x509 -req -in certs/renewal.csr -CA certs/ca.crt -CAkey certs/ca.key \
    -CAcreateserial -out certs/renewal.crt -days 1 -copy_extensions copy
  rm -f certs/renewal.csr
  echo "renewal: $(openssl x509 -in certs/renewal.crt -noout -serial -enddate)"
elif [ "$1" = "client" ]; then
  # A CLIENT certificate from the same CA (SEC-3 want mode). Its CN is
  # the identity the server reads out of the verified chain, and what a
  # principal's grants are matched on.
  CN="${2:-tributary-node-1}"
  openssl ecparam -name prime256v1 -genkey -noout -out "certs/client-$CN.key"
  openssl req -new -key "certs/client-$CN.key" -out "certs/client-$CN.csr" \
    -subj "/CN=$CN"
  printf 'extendedKeyUsage=clientAuth\nkeyUsage=digitalSignature\n' > certs/client.ext
  openssl x509 -req -in "certs/client-$CN.csr" -CA certs/ca.crt -CAkey certs/ca.key \
    -CAcreateserial -out "certs/client-$CN.crt" -days 1 \
    -extfile certs/client.ext
  rm -f "certs/client-$CN.csr" certs/client.ext
  echo "client: $(openssl x509 -in "certs/client-$CN.crt" -noout -subject -enddate)"
else
  echo "usage: $0 initial|renewal|client [CN]" >&2
  exit 2
fi
