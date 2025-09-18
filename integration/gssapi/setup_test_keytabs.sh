#!/bin/bash

# Setup script for GSSAPI test keytabs
# This creates mock keytab files for testing purposes

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
KEYTAB_DIR="${SCRIPT_DIR}/keytabs"

echo "Setting up GSSAPI test keytabs in ${KEYTAB_DIR}"

# Create keytab directory
mkdir -p "${KEYTAB_DIR}"

# Check if we have kadmin.local available (for real keytab creation)
if command -v kadmin.local &> /dev/null || command -v /opt/homebrew/opt/krb5/sbin/kadmin.local &> /dev/null; then
    echo "Kerberos admin tools found, attempting to create real keytabs..."

    # Try to find kadmin.local
    KADMIN_LOCAL=""
    if command -v kadmin.local &> /dev/null; then
        KADMIN_LOCAL="kadmin.local"
    elif [ -x "/opt/homebrew/opt/krb5/sbin/kadmin.local" ]; then
        KADMIN_LOCAL="/opt/homebrew/opt/krb5/sbin/kadmin.local"
    elif [ -x "/usr/sbin/kadmin.local" ]; then
        KADMIN_LOCAL="/usr/sbin/kadmin.local"
    fi

    if [ -n "$KADMIN_LOCAL" ] && [ -f "/opt/homebrew/etc/krb5kdc/kdc.conf" -o -f "/etc/krb5kdc/kdc.conf" ]; then
        # Try to create test principals and keytabs
        echo "Creating test principals and keytabs (this may fail if KDC is not configured)..."

        # Export keytabs for test principals (these may fail if principals don't exist)
        for principal in test pgdog-test server1 server2; do
            echo "Attempting to create keytab for ${principal}@PGDOG.LOCAL..."
            ${KADMIN_LOCAL} -q "ktadd -k ${KEYTAB_DIR}/${principal}.keytab ${principal}@PGDOG.LOCAL" 2>/dev/null || \
                echo "  Could not create keytab for ${principal}@PGDOG.LOCAL (principal may not exist)"
        done
    fi
else
    echo "Kerberos admin tools not found, creating mock keytab files..."
fi

# Create mock keytab files for testing (even if real keytab creation failed)
# These files will exist but won't be valid Kerberos keytabs
for keytab in test.keytab pgdog-test.keytab server1.keytab server2.keytab keytab1.keytab keytab2.keytab backend.keytab; do
    if [ ! -f "${KEYTAB_DIR}/${keytab}" ]; then
        echo "Creating mock keytab: ${KEYTAB_DIR}/${keytab}"
        # Create a file with mock keytab header (won't work for real auth but tests file existence)
        printf '\x05\x02' > "${KEYTAB_DIR}/${keytab}"
    fi
done

# Create a users file for GSSAPI testing
cat > "${SCRIPT_DIR}/test_users.toml" << 'EOF'
# Test users for GSSAPI authentication testing

[[user]]
name = "test"
principal = "test@PGDOG.LOCAL"
strip_realm = true

[[user]]
name = "pgdog-test"
principal = "pgdog-test@PGDOG.LOCAL"
strip_realm = true

[[user]]
name = "principal1"
principal = "principal1@REALM"
strip_realm = false

[[user]]
name = "principal2"
principal = "principal2@REALM"
strip_realm = false
EOF

echo "GSSAPI test setup complete!"
echo "Keytabs created in: ${KEYTAB_DIR}"
echo "Test users config: ${SCRIPT_DIR}/test_users.toml"
echo ""
echo "Note: Mock keytabs are not valid for real Kerberos authentication."
echo "      They exist only to test file handling and error paths."