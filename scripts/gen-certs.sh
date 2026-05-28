#!/bin/bash
set -e

CERT_DIR="./certs"
mkdir -p "$CERT_DIR"

echo "Generating Certificate Authority (CA) key and certificate..."
openssl genrsa -out "$CERT_DIR/ca-key.pem" 2048
openssl req -x509 -new -nodes -key "$CERT_DIR/ca-key.pem" -sha256 -days 3650 -out "$CERT_DIR/ca.pem" -subj "/CN=kaze-ca"

echo "Generating Master key and certificate..."
openssl genrsa -out "$CERT_DIR/master-key.pem" 2048
openssl req -new -key "$CERT_DIR/master-key.pem" -out "$CERT_DIR/master.csr" -subj "/CN=localhost"

# Create extension file for Master SAN
cat > "$CERT_DIR/master.ext" <<EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
IP.1 = 127.0.0.1
EOF

openssl x509 -req -in "$CERT_DIR/master.csr" -CA "$CERT_DIR/ca.pem" -CAkey "$CERT_DIR/ca-key.pem" -CAcreateserial -out "$CERT_DIR/master.pem" -days 365 -sha256 -extfile "$CERT_DIR/master.ext"

echo "Generating Worker key and certificate (CN=worker)..."
openssl genrsa -out "$CERT_DIR/worker-key.pem" 2048
openssl req -new -key "$CERT_DIR/worker-key.pem" -out "$CERT_DIR/worker.csr" -subj "/CN=worker"
openssl x509 -req -in "$CERT_DIR/worker.csr" -CA "$CERT_DIR/ca.pem" -CAkey "$CERT_DIR/ca-key.pem" -CAcreateserial -out "$CERT_DIR/worker.pem" -days 365 -sha256

echo "Generating CLI Client key and certificate (CN=client)..."
openssl genrsa -out "$CERT_DIR/client-key.pem" 2048
openssl req -new -key "$CERT_DIR/client-key.pem" -out "$CERT_DIR/client.csr" -subj "/CN=client"
openssl x509 -req -in "$CERT_DIR/client.csr" -CA "$CERT_DIR/ca.pem" -CAkey "$CERT_DIR/ca-key.pem" -CAcreateserial -out "$CERT_DIR/client.pem" -days 365 -sha256

# Cleanup CSRs and ext files
rm -f "$CERT_DIR"/*.csr
rm -f "$CERT_DIR"/*.ext
rm -f "$CERT_DIR"/*.srl

echo "Certificates generated in $CERT_DIR"
