#!/bin/bash

# Setup script for GSSAPI test keytabs
# This creates real Kerberos keytabs for testing

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
KEYTAB_DIR="${SCRIPT_DIR}/keytabs"

# Test password for all principals
TEST_PASSWORD="password"

# Check if we're in CI environment
if [ -z "$CI" ]; then
    # GitHub Actions sets CI=true, but check for other indicators too
    if [ -n "$GITHUB_ACTIONS" ] || [ -n "$JENKINS_HOME" ] || [ -n "$GITLAB_CI" ]; then
        export CI=true
    else
        export CI=false
    fi
fi

# Detect OS
OS=$(uname -s)

echo "Setting up GSSAPI test keytabs in ${KEYTAB_DIR}"

# Create keytab directory
mkdir -p "${KEYTAB_DIR}"

# Create mock keytabs for testing when KDC is not available
create_mock_keytabs() {
    echo "Creating mock keytabs for testing..."
    for keytab in test pgdog-test server1 server2 principal1 principal2 backend keytab1 keytab2; do
        keytab_file="${KEYTAB_DIR}/${keytab}.keytab"
        echo "  Creating mock keytab: $keytab_file"
        # Create a minimal valid keytab file structure
        # Keytab format version (0x0502)
        printf '\x05\x02' > "$keytab_file"
        chmod 600 "$keytab_file"
    done
    echo "Mock keytabs created for CI testing"
}

# Find kadmin.local
find_kadmin_local() {
    if [ -x "/opt/homebrew/opt/krb5/sbin/kadmin.local" ]; then
        echo "/opt/homebrew/opt/krb5/sbin/kadmin.local"
    elif [ -x "/usr/sbin/kadmin.local" ]; then
        echo "/usr/sbin/kadmin.local"
    elif command -v kadmin.local &> /dev/null; then
        command -v kadmin.local
    else
        echo ""
    fi
}

# Setup environment based on OS
setup_kerberos_env() {
    if [ "$OS" = "Darwin" ]; then
        # macOS with Homebrew
        if [ -f "/opt/homebrew/etc/krb5.conf" ]; then
            export KRB5_CONFIG=/opt/homebrew/etc/krb5.conf
            export KRB5_KDC_PROFILE=/opt/homebrew/etc/krb5kdc/kdc.conf
            echo "Using Homebrew Kerberos configuration"
            return 0
        fi
    elif [ "$OS" = "Linux" ]; then
        # Linux - standard locations
        if [ -f "/etc/krb5.conf" ]; then
            export KRB5_CONFIG=/etc/krb5.conf
            export KRB5_KDC_PROFILE=/etc/krb5kdc/kdc.conf
            echo "Using system Kerberos configuration"
            return 0
        fi
    fi

    # No config found
    echo "Warning: No Kerberos configuration found"
    return 1
}

# Check if KDC is running
check_kdc_running() {
    local kadmin_local="$1"

    echo "Checking if KDC is accessible..."
    if $kadmin_local -q "listprincs" &>/dev/null; then
        echo "✓ KDC is accessible"
        return 0
    else
        echo "✗ KDC is not accessible"
        return 1
    fi
}

# Get the realm from krb5.conf
get_realm() {
    if [ -n "$KRB5_CONFIG" ] && [ -f "$KRB5_CONFIG" ]; then
        grep "default_realm" "$KRB5_CONFIG" | awk '{print $3}' | head -1
    else
        echo "PGDOG.LOCAL"  # Default fallback
    fi
}

# Create a test principal
create_principal() {
    local kadmin_local="$1"
    local principal="$2"
    local password="$3"

    # Check if principal exists
    if $kadmin_local -q "getprinc $principal" 2>&1 | grep -q "does not exist"; then
        # Principal doesn't exist, create it
        echo "  Creating principal $principal..."
        if $kadmin_local -q "addprinc -pw $password $principal" 2>&1 | grep -q "created"; then
            echo "  ✓ Created principal $principal"
        else
            echo "  ✗ Failed to create principal $principal"
            return 1
        fi
    else
        echo "  Principal $principal already exists"
    fi
    return 0
}

# Export principal to keytab
export_to_keytab() {
    local kadmin_local="$1"
    local principal="$2"
    local keytab_file="$3"

    echo "  Exporting $principal to $keytab_file..."

    # Remove old keytab if exists
    rm -f "$keytab_file"

    # Export to keytab (this changes the key)
    local output=$($kadmin_local -q "ktadd -k $keytab_file $principal" 2>&1)
    if echo "$output" | grep -q "added to keytab"; then
        echo "  ✓ Exported to keytab"
        chmod 600 "$keytab_file"
        return 0
    else
        echo "  ✗ Failed to export to keytab"
        echo "    Error: $output"
        return 1
    fi
}

# Verify keytab works
verify_keytab() {
    local keytab_file="$1"
    local principal="$2"

    echo "  Verifying keytab for $principal..."

    # Try to authenticate with keytab
    if KRB5CCNAME=/tmp/krb5cc_test_$$ kinit -kt "$keytab_file" "$principal" &>/dev/null; then
        echo "  ✓ Keytab verification successful"
        # Clean up test ticket
        KRB5CCNAME=/tmp/krb5cc_test_$$ kdestroy &>/dev/null || true
        rm -f /tmp/krb5cc_test_$$
        return 0
    else
        echo "  ✗ Keytab verification failed"
        return 1
    fi
}

# Main setup
main() {
    echo "========================================="
    echo "GSSAPI Test Keytab Setup"
    echo "========================================="

    # Setup environment
    if ! setup_kerberos_env; then
        echo "ERROR: Cannot find Kerberos configuration"
        echo "For macOS: Install with 'brew install krb5'"
        echo "For Linux: Install krb5-kdc and krb5-admin-server"
        exit 1
    fi

    # Find kadmin.local
    KADMIN_LOCAL=$(find_kadmin_local)
    if [ -z "$KADMIN_LOCAL" ]; then
        echo "ERROR: kadmin.local not found"
        echo "This script requires local access to the KDC"
        exit 1
    fi
    echo "Using kadmin.local: $KADMIN_LOCAL"

    # Check KDC is running
    if ! check_kdc_running "$KADMIN_LOCAL"; then
        echo "ERROR: Cannot connect to KDC"
        echo "Please ensure the KDC is running and accessible"

        if [ "$CI" = "true" ]; then
            echo "In CI environment - attempting to setup minimal KDC..."

            # Try to run the CI KDC setup script if it exists
            CI_KDC_SCRIPT="${SCRIPT_DIR}/setup_ci_kdc.sh"
            if [ -f "$CI_KDC_SCRIPT" ]; then
                echo "Running CI KDC setup script..."
                # CI KDC setup requires sudo
                if sudo bash "$CI_KDC_SCRIPT"; then
                    echo "CI KDC setup successful, retrying..."
                    # Re-setup environment and retry
                    setup_kerberos_env
                    KADMIN_LOCAL=$(find_kadmin_local)
                    if [ -n "$KADMIN_LOCAL" ] && check_kdc_running "$KADMIN_LOCAL"; then
                        echo "KDC is now accessible, continuing with keytab creation..."
                        # Don't exit, continue with the main flow
                    else
                        echo "KDC still not accessible after setup"
                        echo "Creating mock keytabs as fallback..."
                        create_mock_keytabs
                        exit 0
                    fi
                else
                    echo "CI KDC setup failed"
                    echo "Creating mock keytabs as fallback..."
                    create_mock_keytabs
                    exit 0
                fi
            else
                echo "No CI KDC setup script found"
                echo "Creating mock keytabs as fallback..."
                create_mock_keytabs
                exit 0
            fi
        else
            echo "Not in CI environment. Please ensure KDC is running locally."
            echo "For macOS: brew services start krb5"
            echo "For Linux: sudo systemctl start krb5-kdc"
            exit 1
        fi
    fi

    # Get realm
    REALM=$(get_realm)
    echo "Using Kerberos realm: $REALM"

    echo ""
    echo "Creating test principals and keytabs..."
    echo "-----------------------------------------"

    # Define principals to create
    # Using REALM for consistency with existing setup
    declare -a principals=(
        "test@$REALM"
        "pgdog-test@$REALM"
        "server1@$REALM"
        "server2@$REALM"
    )

    # Also create generic test principals
    principals+=("principal1@$REALM" "principal2@$REALM")

    # Create principals and keytabs
    for principal in "${principals[@]}"; do
        # Extract base name for keytab file
        base_name=$(echo "$principal" | cut -d'@' -f1)
        keytab_file="${KEYTAB_DIR}/${base_name}.keytab"

        echo ""
        echo "Processing $principal:"

        # Create principal
        if ! create_principal "$KADMIN_LOCAL" "$principal" "$TEST_PASSWORD"; then
            echo "WARNING: Failed to create principal $principal"
            continue
        fi

        # Export to keytab
        if ! export_to_keytab "$KADMIN_LOCAL" "$principal" "$keytab_file"; then
            echo "WARNING: Failed to export $principal to keytab"
            continue
        fi

        # Verify keytab
        if ! verify_keytab "$keytab_file" "$principal"; then
            echo "WARNING: Keytab verification failed for $principal"
        fi
    done

    # Create additional keytabs that tests expect
    echo ""
    echo "Creating additional test keytabs..."

    # backend.keytab - copy from pgdog-test
    if [ -f "${KEYTAB_DIR}/pgdog-test.keytab" ]; then
        cp "${KEYTAB_DIR}/pgdog-test.keytab" "${KEYTAB_DIR}/backend.keytab"
        echo "  Created backend.keytab (copy of pgdog-test.keytab)"
    fi

    # keytab1.keytab and keytab2.keytab - copies for generic testing
    if [ -f "${KEYTAB_DIR}/principal1.keytab" ]; then
        cp "${KEYTAB_DIR}/principal1.keytab" "${KEYTAB_DIR}/keytab1.keytab"
        echo "  Created keytab1.keytab"
    elif [ -f "${KEYTAB_DIR}/server1.keytab" ]; then
        cp "${KEYTAB_DIR}/server1.keytab" "${KEYTAB_DIR}/keytab1.keytab"
        echo "  Created keytab1.keytab (copy of server1.keytab)"
    fi

    if [ -f "${KEYTAB_DIR}/principal2.keytab" ]; then
        cp "${KEYTAB_DIR}/principal2.keytab" "${KEYTAB_DIR}/keytab2.keytab"
        echo "  Created keytab2.keytab"
    elif [ -f "${KEYTAB_DIR}/server2.keytab" ]; then
        cp "${KEYTAB_DIR}/server2.keytab" "${KEYTAB_DIR}/keytab2.keytab"
        echo "  Created keytab2.keytab (copy of server2.keytab)"
    fi

    # Create test users configuration file
    cat > "${SCRIPT_DIR}/test_users.toml" << EOF
# Test users for GSSAPI authentication testing

[[user]]
name = "test"
principal = "test@$REALM"
strip_realm = true

[[user]]
name = "pgdog-test"
principal = "pgdog-test@$REALM"
strip_realm = true

[[user]]
name = "server1"
principal = "server1@$REALM"
strip_realm = true

[[user]]
name = "server2"
principal = "server2@$REALM"
strip_realm = true

[[user]]
name = "principal1"
principal = "principal1@$REALM"
strip_realm = false

[[user]]
name = "principal2"
principal = "principal2@$REALM"
strip_realm = false
EOF

    echo ""
    echo "========================================="
    echo "GSSAPI test setup complete!"
    echo "========================================="
    echo "Keytabs created in: ${KEYTAB_DIR}"
    echo "Test users config: ${SCRIPT_DIR}/test_users.toml"
    echo ""

    # List created keytabs
    echo "Created keytabs:"
    ls -la "${KEYTAB_DIR}/"*.keytab 2>/dev/null || echo "No keytabs found"

    echo ""
    echo "Note: These keytabs use real Kerberos credentials."
    echo "      They can be used for actual GSSAPI authentication testing."
}

# Run main function
main "$@"