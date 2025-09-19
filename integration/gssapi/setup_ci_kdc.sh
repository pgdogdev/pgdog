#!/bin/bash

# Setup a minimal KDC for CI testing
# This script is designed to run in GitHub Actions Ubuntu environment

set -e

echo "========================================="
echo "Setting up Kerberos KDC for CI"
echo "========================================="

# Install Kerberos KDC packages
echo "Installing Kerberos KDC packages..."
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
    krb5-kdc \
    krb5-admin-server \
    krb5-config \
    krb5-user \
    libkrb5-dev

# Define realm
REALM="PGDOG.LOCAL"
DOMAIN="pgdog.local"
KDC_PASSWORD="admin123"

# Create krb5.conf
echo "Creating /etc/krb5.conf..."
sudo tee /etc/krb5.conf > /dev/null << EOF
[libdefaults]
    default_realm = $REALM
    dns_lookup_realm = false
    dns_lookup_kdc = false
    ticket_lifetime = 24h
    renew_lifetime = 7d
    forwardable = true

[realms]
    $REALM = {
        kdc = localhost
        admin_server = localhost
        default_domain = $DOMAIN
    }

[domain_realm]
    .$DOMAIN = $REALM
    $DOMAIN = $REALM
    localhost = $REALM

[logging]
    kdc = FILE:/var/log/krb5kdc.log
    admin_server = FILE:/var/log/kadmin.log
    default = FILE:/var/log/krb5lib.log
EOF

# Create kdc.conf
echo "Creating /etc/krb5kdc/kdc.conf..."
sudo mkdir -p /etc/krb5kdc
sudo tee /etc/krb5kdc/kdc.conf > /dev/null << EOF
[kdcdefaults]
    kdc_ports = 88
    kdc_tcp_ports = 88

[realms]
    $REALM = {
        acl_file = /etc/krb5kdc/kadm5.acl
        database_name = /var/lib/krb5kdc/principal
        key_stash_file = /etc/krb5kdc/.k5.$REALM
        max_renewable_life = 7d 0h 0m 0s
        max_life = 1d 0h 0m 0s
        master_key_type = aes256-cts-hmac-sha1-96
        supported_enctypes = aes256-cts-hmac-sha1-96:normal aes128-cts-hmac-sha1-96:normal
        default_principal_flags = +renewable, +forwardable
    }

[logging]
    kdc = FILE:/var/log/krb5kdc.log
    admin_server = FILE:/var/log/kadmin.log
EOF

# Create ACL file
echo "Creating /etc/krb5kdc/kadm5.acl..."
sudo tee /etc/krb5kdc/kadm5.acl > /dev/null << EOF
*/admin@$REALM *
EOF

# Initialize the database
echo "Initializing Kerberos database..."
sudo kdb5_util create -s -r $REALM -P $KDC_PASSWORD

# Start the KDC
echo "Starting KDC..."
sudo systemctl start krb5-kdc || sudo krb5kdc
sudo systemctl start krb5-admin-server || true

# Create admin principal
echo "Creating admin principal..."
echo -e "$KDC_PASSWORD\n$KDC_PASSWORD" | sudo kadmin.local -q "addprinc admin/admin"

# Verify KDC is running
echo "Verifying KDC..."
if sudo kadmin.local -q "listprincs" | grep -q "krbtgt/$REALM@$REALM"; then
    echo "✓ KDC is running and accessible"
else
    echo "✗ KDC setup failed"
    exit 1
fi

echo "========================================="
echo "KDC setup complete!"
echo "Realm: $REALM"
echo "========================================="