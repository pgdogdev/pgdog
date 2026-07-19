<?php

declare(strict_types=1);

function connectPgDog(): PDO
{
    return new PDO(
        "pgsql:host=127.0.0.1;port=6432;dbname=postgres",
        "postgres",
        "postgres",
        [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_EMULATE_PREPARES => false,
        ]
    );
}

function connectPrimary(): PDO
{
    return new PDO(
        "pgsql:host=127.0.0.1;port=45000;dbname=postgres",
        "postgres",
        "postgres",
        [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]
    );
}

function getCallsOnPrimary(string $queryFragment): int
{
    $primary = connectPrimary();
    $stmt = $primary->prepare(
        "SELECT COALESCE(SUM(calls), 0) FROM pg_stat_statements WHERE query ILIKE :pattern"
    );
    $stmt->execute([':pattern' => '%' . $queryFragment . '%']);
    return (int) $stmt->fetchColumn();
}

function resetStats(): void
{
    foreach ([45000, 45001, 45002] as $port) {
        $pdo = new PDO(
            "pgsql:host=127.0.0.1;port={$port};dbname=postgres",
            "postgres",
            "postgres",
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
        );
        $pdo->exec("SELECT pg_stat_statements_reset()");
    }
}

// --- Test: CTE with FOR UPDATE SKIP LOCKED routes to primary ---

$db = connectPgDog();

$db->exec("DROP TABLE IF EXISTS pdo_rw_test_retries");
$db->exec("CREATE TABLE pdo_rw_test_retries (
    id BIGSERIAL PRIMARY KEY,
    next_retry_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ
)");

// Insert a few rows so the query has data to work with.
$db->exec("INSERT INTO pdo_rw_test_retries (next_retry_at) VALUES (NOW()), (NOW()), (NOW())");

// Wait for replicas to catch up.
sleep(2);

resetStats();

// Run the CTE query that uses FOR UPDATE SKIP LOCKED through PgDog.
// This must be routed to the primary because FOR UPDATE is a write-lock.
$query = <<<'SQL'
WITH retries_available AS (
    SELECT 1
    FROM pdo_rw_test_retries
    WHERE next_retry_at <= NOW()
      AND deleted_at IS NULL
    FOR UPDATE SKIP LOCKED
)
SELECT COUNT(*)
FROM retries_available
SQL;

for ($i = 0; $i < 5; $i++) {
    $stmt = $db->query($query);
    $count = (int) $stmt->fetchColumn();
    if ($count !== 3) {
        fwrite(STDERR, "expected count 3, got {$count}\n");
        exit(1);
    }
}

$primaryCalls = getCallsOnPrimary("pdo_rw_test_retries");

if ($primaryCalls < 5) {
    fwrite(STDERR, "expected at least 5 calls on primary, got {$primaryCalls} — query was not routed to primary\n");
    exit(1);
}

// Cleanup
$db->exec("DROP TABLE IF EXISTS pdo_rw_test_retries");

echo "php pdo read/write split test passed\n";
