# GSSAPI (Kerberos) Authentication Configuration Example

This directory contains example configurations for using GSSAPI/Kerberos authentication with PGDog.

## Overview

PGDog supports GSSAPI authentication in a dual-context model:
- **Frontend**: Accepts GSSAPI authentication from clients
- **Backend**: Uses service credentials to authenticate to PostgreSQL servers

This approach preserves connection pooling while providing strong authentication.

## Files

- `pgdog.toml` - Main configuration with GSSAPI settings
- `users.toml` - User mappings for GSSAPI principals

## Key Features Demonstrated

### 1. Global GSSAPI Configuration
- Server keytab for accepting client connections
- Default backend credentials for PostgreSQL servers
- Ticket refresh intervals
- Realm stripping for username mapping

### 2. Per-Server Backend Authentication
- Different keytabs for different PostgreSQL servers
- Useful for multi-tenant or sharded deployments
- Fine-grained access control per database

### 3. Mixed Authentication
- GSSAPI for some databases, password for others
- Fallback options for migration scenarios

## Setup Requirements

### Prerequisites
1. Kerberos KDC (Key Distribution Center) configured
2. Service principals created for PGDog and PostgreSQL servers
3. Keytab files generated and placed in appropriate locations
4. PostgreSQL servers configured to accept GSSAPI authentication

### Keytab Files

#### Frontend (Client-facing)
```bash
# Create service principal for PGDog
kadmin.local -q "addprinc -randkey postgres/pgdog.example.com"
kadmin.local -q "ktadd -k /etc/pgdog/pgdog.keytab postgres/pgdog.example.com"
```

#### Backend (PostgreSQL-facing)
```bash
# Create service principal for backend connections
kadmin.local -q "addprinc -randkey pgdog-service"
kadmin.local -q "ktadd -k /etc/pgdog/backend.keytab pgdog-service"

# For per-server authentication
kadmin.local -q "addprinc -randkey pgdog-shard1"
kadmin.local -q "ktadd -k /etc/pgdog/shard1.keytab pgdog-shard1"
```

### PostgreSQL Configuration

Configure PostgreSQL servers to accept GSSAPI authentication from PGDog's service principal:

```postgresql
# pg_hba.conf
host    all    pgdog-service@EXAMPLE.COM    0.0.0.0/0    gss
host    all    pgdog-shard1@EXAMPLE.COM     0.0.0.0/0    gss
```

## Authentication Flow

1. **Client → PGDog**: Client authenticates using their Kerberos principal (e.g., alice@EXAMPLE.COM)
2. **Username Mapping**: PGDog maps the principal to a user in users.toml (strips realm if configured)
3. **PGDog → PostgreSQL**: PGDog uses its service credentials to connect to the backend
4. **Connection Pooling**: PGDog maintains pooled connections using its service identity

## Security Considerations

- Keytab files should be readable only by the PGDog process user
- Use separate service principals for different environments (dev/staging/prod)
- Regularly rotate keytabs and update Kerberos passwords
- Consider using GSSAPI encryption if SQL inspection is not required
- Monitor ticket refresh logs for authentication issues

## Testing

```bash
# Test client authentication
kinit alice@EXAMPLE.COM
psql -h pgdog.example.com -p 6432 -d production -U alice

# Verify PGDog's service ticket
klist -k /etc/pgdog/pgdog.keytab

# Check backend connectivity
kinit -kt /etc/pgdog/backend.keytab pgdog-service@EXAMPLE.COM
psql -h pg1.example.com -p 5432 -d postgres -U pgdog-service
```

## Troubleshooting

### Common Issues

1. **Clock Skew**: Ensure all servers have synchronized time (use NTP)
2. **DNS Resolution**: Kerberos requires proper forward and reverse DNS
3. **Keytab Permissions**: Check file ownership and permissions (600 or 400)
4. **Principal Names**: Verify exact principal names including realm
5. **Ticket Expiration**: Monitor ticket refresh intervals

### Debug Logging

Enable Kerberos debug output:
```bash
export KRB5_TRACE=/tmp/krb5_trace.log
```

## Migration from Password Authentication

1. Enable `fallback_enabled = true` in GSSAPI configuration
2. Deploy PGDog with both authentication methods available
3. Migrate users gradually to GSSAPI
4. Once all users migrated, disable fallback