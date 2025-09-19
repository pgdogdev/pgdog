#!/bin/bash

# Unified GSSAPI setup script - fully automated, headless, unattended
# Works on both Linux and macOS without manual intervention

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
KEYTAB_DIR="${SCRIPT_DIR}/keytabs"
TEST_PASSWORD="password"
OS=$(uname -s)
REALM="PGDOG.LOCAL"
DOMAIN="pgdog.local"
KDC_PASSWORD="admin123"
SUCCESS_COUNT=0
WARNING_COUNT=0

# Silent mode by default, verbose with DEBUG
[ -n "$DEBUG" ] && set -x

log() {
    [ -n "$VERBOSE" ] && echo "$@" || true
}

error() {
    echo "ERROR: $@" >&2
}

# Find tools based on OS
find_tool() {
    local tool=$1
    if [ "$OS" = "Darwin" ]; then
        # macOS paths
        for path in "/opt/homebrew/opt/krb5/sbin/$tool" "/opt/homebrew/opt/krb5/bin/$tool" "/usr/local/opt/krb5/sbin/$tool" "/usr/local/opt/krb5/bin/$tool"; do
            [ -x "$path" ] && echo "$path" && return 0
        done
    fi
    # Standard paths
    for path in "/usr/sbin/$tool" "/usr/bin/$tool" "/sbin/$tool" "/bin/$tool"; do
        [ -x "$path" ] && echo "$path" && return 0
    done
    command -v "$tool" 2>/dev/null || echo ""
}

# Auto-install dependencies if missing
ensure_dependencies() {
    if [ "$OS" = "Darwin" ]; then
        if ! command -v kinit &>/dev/null && ! [ -x "/opt/homebrew/opt/krb5/bin/kinit" ]; then
            log "Installing Kerberos via Homebrew..."
            brew install krb5 2>/dev/null || true
        fi
    elif [ "$OS" = "Linux" ]; then
        if ! command -v kinit &>/dev/null; then
            log "Installing Kerberos packages..."
            if [ "$CI" = "true" ] || [ "$EUID" = "0" ]; then
                export DEBIAN_FRONTEND=noninteractive
                apt-get update &>/dev/null || true
                apt-get install -y krb5-kdc krb5-admin-server krb5-user libkrb5-dev 2>/dev/null || true
            fi
        fi
    fi
}

# Setup environment based on OS
setup_kerberos_env() {
    if [ "$OS" = "Darwin" ]; then
        # macOS with Homebrew
        for dir in "/opt/homebrew/etc" "/usr/local/etc"; do
            if [ -f "$dir/krb5.conf" ]; then
                export KRB5_CONFIG="$dir/krb5.conf"
                export KRB5_KDC_PROFILE="$dir/krb5kdc/kdc.conf"
                log "macOS: Found existing config at $dir/krb5.conf"
                return 0
            fi
        done
        # Create default location if missing
        export KRB5_CONFIG="/opt/homebrew/etc/krb5.conf"
        export KRB5_KDC_PROFILE="/opt/homebrew/etc/krb5kdc/kdc.conf"
        log "macOS: Using default config location $KRB5_CONFIG"
    else
        # Linux standard locations
        export KRB5_CONFIG="/etc/krb5.conf"
        export KRB5_KDC_PROFILE="/etc/krb5kdc/kdc.conf"
        log "Linux: Using standard config location $KRB5_CONFIG"
    fi

    # Export globally for all commands
    log "Environment: KRB5_CONFIG=$KRB5_CONFIG"
    log "Environment: KRB5_KDC_PROFILE=$KRB5_KDC_PROFILE"
}

# Create krb5.conf if missing
create_krb5_conf() {
    local config_file="$1"
    local config_dir=$(dirname "$config_file")

    # In CI mode, always recreate the config to avoid conflicts
    if [ "$CI" = "true" ] && [ -f "$config_file" ]; then
        log "CI mode: Backing up existing $config_file to ${config_file}.bak"
        sudo mv "$config_file" "${config_file}.bak" 2>/dev/null || true
    elif [ -f "$config_file" ] && [ "$CI" != "true" ]; then
        log "Using existing Kerberos configuration at $config_file"
        return 0
    fi

    log "Creating Kerberos configuration at $config_file..."

    # Create directory if needed
    if [ "$EUID" = "0" ] || [ "$CI" = "true" ]; then
        mkdir -p "$config_dir" 2>/dev/null || sudo mkdir -p "$config_dir"
    else
        sudo mkdir -p "$config_dir" 2>/dev/null || mkdir -p "$config_dir"
    fi

    # Write config
    local config_content="[libdefaults]
    default_realm = $REALM
    dns_lookup_realm = false
    dns_lookup_kdc = false
    ticket_lifetime = 24h
    renew_lifetime = 7d
    forwardable = true

[realms]
    $REALM = {
        kdc = localhost:88
        admin_server = localhost:749
        default_domain = $DOMAIN
    }

[domain_realm]
    .$DOMAIN = $REALM
    $DOMAIN = $REALM
    localhost = $REALM

[logging]
    kdc = FILE:/var/log/krb5kdc.log
    admin_server = FILE:/var/log/kadmin.log
    default = FILE:/var/log/krb5lib.log"

    if [ "$EUID" = "0" ] || [ "$CI" = "true" ]; then
        echo "$config_content" > "$config_file" 2>/dev/null || echo "$config_content" | sudo tee "$config_file" > /dev/null
    else
        echo "$config_content" | sudo tee "$config_file" > /dev/null 2>/dev/null || echo "$config_content" > "$config_file"
    fi
}

# Create kdc.conf if missing
create_kdc_conf() {
    local kdc_conf="$1"
    local kdc_dir=$(dirname "$kdc_conf")

    # In CI mode, always recreate the config
    if [ "$CI" = "true" ] && [ -f "$kdc_conf" ]; then
        log "CI mode: Backing up existing $kdc_conf to ${kdc_conf}.bak"
        sudo mv "$kdc_conf" "${kdc_conf}.bak" 2>/dev/null || true
    elif [ -f "$kdc_conf" ] && [ "$CI" != "true" ]; then
        log "Using existing KDC configuration at $kdc_conf"
        return 0
    fi

    log "Creating KDC configuration at $kdc_conf..."

    # Create directory
    if [ "$EUID" = "0" ] || [ "$CI" = "true" ]; then
        mkdir -p "$kdc_dir" 2>/dev/null || sudo mkdir -p "$kdc_dir"
    else
        sudo mkdir -p "$kdc_dir" 2>/dev/null || mkdir -p "$kdc_dir"
    fi

    # Write config
    local kdc_content="[kdcdefaults]
    kdc_ports = 88
    kdc_tcp_ports = 88

[realms]
    $REALM = {
        acl_file = $kdc_dir/kadm5.acl
        database_name = /var/lib/krb5kdc/principal
        key_stash_file = $kdc_dir/.k5.$REALM
        max_renewable_life = 7d 0h 0m 0s
        max_life = 1d 0h 0m 0s
        master_key_type = aes256-cts-hmac-sha1-96
        supported_enctypes = aes256-cts-hmac-sha1-96:normal aes128-cts-hmac-sha1-96:normal
        default_principal_flags = +renewable, +forwardable
    }

[logging]
    kdc = FILE:/var/log/krb5kdc.log
    admin_server = FILE:/var/log/kadmin.log"

    if [ "$EUID" = "0" ] || [ "$CI" = "true" ]; then
        echo "$kdc_content" > "$kdc_conf" 2>/dev/null || echo "$kdc_content" | sudo tee "$kdc_conf" > /dev/null
    else
        echo "$kdc_content" | sudo tee "$kdc_conf" > /dev/null 2>/dev/null || echo "$kdc_content" > "$kdc_conf"
    fi

    # Create ACL file
    local acl_file="$kdc_dir/kadm5.acl"
    local acl_content="*/admin@$REALM *"

    if [ "$EUID" = "0" ] || [ "$CI" = "true" ]; then
        echo "$acl_content" > "$acl_file" 2>/dev/null || echo "$acl_content" | sudo tee "$acl_file" > /dev/null
    else
        echo "$acl_content" | sudo tee "$acl_file" > /dev/null 2>/dev/null || echo "$acl_content" > "$acl_file"
    fi
}

# Initialize KDC database if needed
initialize_kdc() {
    local kdb5_util=$(find_tool "kdb5_util")
    [ -z "$kdb5_util" ] && return 1

    log "Initializing KDC database..."

    # Check if database exists
    local kadmin_local=$(find_tool "kadmin.local")
    if [ -n "$kadmin_local" ]; then
        if [ -n "$VERBOSE" ]; then
            local check_output=$(KRB5_CONFIG="$KRB5_CONFIG" $kadmin_local -q "listprincs" 2>&1)
            if echo "$check_output" | grep -q "krbtgt/$REALM@$REALM"; then
                log "KDC database already exists"
                return 0
            else
                log "KDC database check output: $check_output"
            fi
        else
            if KRB5_CONFIG="$KRB5_CONFIG" $kadmin_local -q "listprincs" 2>/dev/null | grep -q "krbtgt/$REALM@$REALM"; then
                log "KDC database already exists"
                return 0
            fi
        fi
    fi

    # Create database
    log "Creating KDC database for realm $REALM..."
    local create_cmd="KRB5_CONFIG=\"$KRB5_CONFIG\" $kdb5_util create -s -r $REALM -P $KDC_PASSWORD"

    if [ -n "$VERBOSE" ]; then
        local output
        if [ "$EUID" = "0" ] || [ "$CI" = "true" ]; then
            output=$(eval $create_cmd 2>&1) || output=$(sudo -E sh -c "$create_cmd" 2>&1)
        else
            output=$(sudo -E sh -c "$create_cmd" 2>&1) || output=$(eval $create_cmd 2>&1)
        fi
        log "kdb5_util output: $output"
    else
        if [ "$EUID" = "0" ] || [ "$CI" = "true" ]; then
            eval $create_cmd 2>/dev/null || sudo -E sh -c "$create_cmd" 2>/dev/null
        else
            sudo -E sh -c "$create_cmd" 2>/dev/null || eval $create_cmd 2>/dev/null
        fi
    fi

    # Create admin principal
    if [ -n "$kadmin_local" ]; then
        log "Creating admin principal..."
        if [ -n "$VERBOSE" ]; then
            local admin_output=$(KRB5_CONFIG="$KRB5_CONFIG" $kadmin_local -q "addprinc -pw $KDC_PASSWORD admin/admin" 2>&1) || \
            admin_output=$(sudo -E sh -c "KRB5_CONFIG=\"$KRB5_CONFIG\" $kadmin_local -q 'addprinc -pw $KDC_PASSWORD admin/admin'" 2>&1)
            log "Admin principal creation: $admin_output"
        else
            KRB5_CONFIG="$KRB5_CONFIG" $kadmin_local -q "addprinc -pw $KDC_PASSWORD admin/admin" 2>/dev/null || \
            sudo -E sh -c "KRB5_CONFIG=\"$KRB5_CONFIG\" $kadmin_local -q 'addprinc -pw $KDC_PASSWORD admin/admin'" 2>/dev/null || true
        fi
    fi

    return 0
}

# Start KDC services
start_kdc_services() {
    local krb5kdc=$(find_tool "krb5kdc")

    log "Starting KDC services..."
    log "Current KDC processes: $(pgrep -f krb5kdc 2>/dev/null | wc -l) running"

    if [ "$OS" = "Darwin" ]; then
        # macOS: try brew services first
        if command -v brew &>/dev/null; then
            brew services restart krb5 2>/dev/null || true
        fi
        # Try direct launch if brew services failed
        if [ -n "$krb5kdc" ] && ! pgrep -f krb5kdc >/dev/null; then
            $krb5kdc 2>/dev/null &
            sleep 1
        fi
    else
        # Linux: try systemctl first
        if command -v systemctl &>/dev/null; then
            sudo systemctl restart krb5-kdc 2>/dev/null || true
            sudo systemctl restart krb5-admin-server 2>/dev/null || true
        elif command -v service &>/dev/null; then
            sudo service krb5-kdc restart 2>/dev/null || true
            sudo service krb5-admin-server restart 2>/dev/null || true
        fi
        # Try direct launch if services failed
        if [ -n "$krb5kdc" ] && ! pgrep -f krb5kdc >/dev/null; then
            if [ "$EUID" = "0" ] || [ "$CI" = "true" ]; then
                $krb5kdc 2>/dev/null &
            else
                sudo $krb5kdc 2>/dev/null &
            fi
            sleep 1
        fi
    fi

    # Check if services started
    sleep 1
    log "KDC processes after start: $(pgrep -f krb5kdc 2>/dev/null | wc -l) running"

    if command -v netstat &>/dev/null; then
        log "KDC listening on port 88: $(netstat -ln 2>/dev/null | grep ':88 ' | wc -l) listeners"
    elif command -v ss &>/dev/null; then
        log "KDC listening on port 88: $(ss -ln 2>/dev/null | grep ':88 ' | wc -l) listeners"
    fi
}

# Check if KDC is running
check_kdc_running() {
    local kadmin_local="$1"
    [ -z "$kadmin_local" ] && return 1

    if [ -n "$VERBOSE" ]; then
        local check_output=$(KRB5_CONFIG="$KRB5_CONFIG" $kadmin_local -q "listprincs" 2>&1)
        log "KDC check output: $check_output"
        if echo "$check_output" | grep -q "krbtgt"; then
            return 0
        fi
    else
        if KRB5_CONFIG="$KRB5_CONFIG" $kadmin_local -q "listprincs" 2>/dev/null | grep -q "krbtgt"; then
            return 0
        fi
    fi
    return 1
}

# Create a principal with recovery
create_principal() {
    local kadmin_local="$1"
    local principal="$2"
    local password="$3"

    # Check if principal exists
    if ! KRB5_CONFIG="$KRB5_CONFIG" $kadmin_local -q "getprinc $principal" 2>&1 | grep -q "does not exist"; then
        log "  Principal $principal already exists"
        return 0
    fi

    # Create principal
    log "  Creating principal $principal..."
    if KRB5_CONFIG="$KRB5_CONFIG" $kadmin_local -q "addprinc -pw $password $principal" 2>&1 | grep -q "created\|added"; then
        log "  ✓ Created principal $principal"
        return 0
    fi

    # Retry with sudo if needed
    if sudo -E sh -c "KRB5_CONFIG=\"$KRB5_CONFIG\" $kadmin_local -q 'addprinc -pw $password $principal'" 2>&1 | grep -q "created\|added"; then
        log "  ✓ Created principal $principal"
        return 0
    fi

    log "  Warning: Could not create principal $principal"
    return 1
}

# Export to keytab with recovery
export_to_keytab() {
    local kadmin_local="$1"
    local principal="$2"
    local keytab_file="$3"

    log "  Exporting $principal to keytab..."

    rm -f "$keytab_file" 2>/dev/null || true

    # Try export
    local output=$(KRB5_CONFIG="$KRB5_CONFIG" $kadmin_local -q "ktadd -k $keytab_file $principal" 2>&1 || \
                   sudo -E sh -c "KRB5_CONFIG=\"$KRB5_CONFIG\" $kadmin_local -q 'ktadd -k $keytab_file $principal'" 2>&1)

    if echo "$output" | grep -q "added to keytab"; then
        chmod 600 "$keytab_file" 2>/dev/null || true
        log "  ✓ Exported to keytab"
        return 0
    fi

    log "  Warning: Could not export $principal to keytab"
    if [ -n "$VERBOSE" ]; then
        log "  Export output: $output"
    fi
    return 1
}

# Verify keytab
verify_keytab() {
    local keytab_file="$1"
    local principal="$2"

    local kinit=$(find_tool "kinit")
    local kdestroy=$(find_tool "kdestroy")

    [ -z "$kinit" ] && return 1

    log "  Verifying keytab for $principal..."

    if KRB5_CONFIG="$KRB5_CONFIG" KRB5CCNAME=/tmp/krb5cc_test_$$ $kinit -kt "$keytab_file" "$principal" 2>/dev/null; then
        log "  ✓ Keytab verification successful"
        [ -n "$kdestroy" ] && KRB5_CONFIG="$KRB5_CONFIG" KRB5CCNAME=/tmp/krb5cc_test_$$ $kdestroy 2>/dev/null || true
        rm -f /tmp/krb5cc_test_$$ 2>/dev/null || true
        return 0
    fi

    log "  Warning: Keytab verification failed for $principal"
    return 1
}

# Main setup function
main() {
    log "========================================="
    log "GSSAPI Automated Setup"
    log "========================================="

    # Step 1: Install dependencies if missing
    ensure_dependencies

    # Step 2: Setup environment
    setup_kerberos_env

    # Step 3: Create configs if missing
    create_krb5_conf "$KRB5_CONFIG"
    create_kdc_conf "$KRB5_KDC_PROFILE"

    # Show config content in verbose mode for debugging
    if [ -n "$VERBOSE" ] && [ -f "$KRB5_CONFIG" ]; then
        log "Content of $KRB5_CONFIG:"
        log "$(head -20 "$KRB5_CONFIG" | sed 's/^/  /')"
    fi

    # Step 4: Find required tools
    KADMIN_LOCAL=$(find_tool "kadmin.local")
    if [ -z "$KADMIN_LOCAL" ]; then
        error "kadmin.local not found - cannot manage Kerberos principals"
        exit 1
    fi
    log "Found kadmin.local at: $KADMIN_LOCAL"

    KDB5_UTIL=$(find_tool "kdb5_util")
    log "Found kdb5_util at: ${KDB5_UTIL:-not found}"

    KINIT=$(find_tool "kinit")
    log "Found kinit at: ${KINIT:-not found}"

    # Step 5: Initialize KDC if needed
    initialize_kdc

    # Step 6: Start KDC services
    start_kdc_services
    sleep 2

    # Step 7: Verify KDC is running, retry if needed
    if [ -n "$KADMIN_LOCAL" ]; then
        if ! check_kdc_running "$KADMIN_LOCAL"; then
            log "KDC not responding, attempting restart..."
            start_kdc_services
            sleep 3

            if ! check_kdc_running "$KADMIN_LOCAL"; then
                log "KDC still not responding, reinitializing..."
                initialize_kdc
                start_kdc_services
                sleep 3

                if ! check_kdc_running "$KADMIN_LOCAL"; then
                    error "KDC failed to start after multiple attempts"
                    exit 1
                fi
            fi
        fi
    fi

    # Step 8: Create keytab directory
    mkdir -p "${KEYTAB_DIR}"

    # Step 9: Define principals
    declare -a principals=(
        "test@$REALM"
        "pgdog-test@$REALM"
        "server1@$REALM"
        "server2@$REALM"
        "principal1@$REALM"
        "principal2@$REALM"
    )

    # Step 10: Create principals and keytabs
    if [ -n "$KADMIN_LOCAL" ]; then
        for principal in "${principals[@]}"; do
            base_name=$(echo "$principal" | cut -d'@' -f1)
            keytab_file="${KEYTAB_DIR}/${base_name}.keytab"

            log ""
            log "Processing $principal:"

            if create_principal "$KADMIN_LOCAL" "$principal" "$TEST_PASSWORD"; then
                if export_to_keytab "$KADMIN_LOCAL" "$principal" "$keytab_file"; then
                    SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
                    # Verification is optional - tests will verify keytabs work
                    verify_keytab "$keytab_file" "$principal" || true
                else
                    WARNING_COUNT=$((WARNING_COUNT + 1))
                fi
            else
                WARNING_COUNT=$((WARNING_COUNT + 1))
            fi
        done
    fi

    # Step 11: Create additional keytabs
    log ""
    log "Creating additional test keytabs..."

    if [ -f "${KEYTAB_DIR}/pgdog-test.keytab" ]; then
        cp "${KEYTAB_DIR}/pgdog-test.keytab" "${KEYTAB_DIR}/backend.keytab"
        log "  Created backend.keytab"
    fi

    if [ -f "${KEYTAB_DIR}/principal1.keytab" ]; then
        cp "${KEYTAB_DIR}/principal1.keytab" "${KEYTAB_DIR}/keytab1.keytab"
        log "  Created keytab1.keytab"
    elif [ -f "${KEYTAB_DIR}/server1.keytab" ]; then
        cp "${KEYTAB_DIR}/server1.keytab" "${KEYTAB_DIR}/keytab1.keytab"
        log "  Created keytab1.keytab"
    fi

    if [ -f "${KEYTAB_DIR}/principal2.keytab" ]; then
        cp "${KEYTAB_DIR}/principal2.keytab" "${KEYTAB_DIR}/keytab2.keytab"
        log "  Created keytab2.keytab"
    elif [ -f "${KEYTAB_DIR}/server2.keytab" ]; then
        cp "${KEYTAB_DIR}/server2.keytab" "${KEYTAB_DIR}/keytab2.keytab"
        log "  Created keytab2.keytab"
    fi

    # Step 12: Create test users configuration
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

    # Count total keytabs created
    KEYTAB_COUNT=$(ls "${KEYTAB_DIR}/"*.keytab 2>/dev/null | wc -l)

    # Output final status
    if [ "$KEYTAB_COUNT" -gt 0 ]; then
        if [ "$WARNING_COUNT" -eq 0 ]; then
            echo "✓ GSSAPI setup successful: $KEYTAB_COUNT keytabs created in ${KEYTAB_DIR}"
            exit 0
        else
            echo "⚠ GSSAPI setup completed with warnings: $KEYTAB_COUNT keytabs created, $WARNING_COUNT operations failed"
            exit 0
        fi
    else
        error "GSSAPI setup failed: No keytabs created"
        exit 1
    fi
}

# Run main
main "$@"