<?php

declare(strict_types=1);

function connectDb(string $dbName): PDO
{
    $pdo = new PDO(
        "pgsql:host=127.0.0.1;port=6432;dbname={$dbName}",
        "pgdog",
        "pgdog",
        [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_EMULATE_PREPARES => false,
        ]
    );

    return $pdo;
}

function sleepForMirror(): void
{
    // Mirror delivery is asynchronous; a short delay keeps the test stable.
    usleep(500_000);
}

$source = connectDb("pgdog");
$mirror = connectDb("pgdog_mirror");

$source->exec("DROP TABLE IF EXISTS public.mirror_php_test");
$source->exec("CREATE TABLE public.mirror_php_test (id BIGINT PRIMARY KEY, value TEXT NOT NULL)");
sleepForMirror();

$source->beginTransaction();

$insert = $source->prepare("INSERT INTO public.mirror_php_test (id, value) VALUES (?, ?)");
$insert->execute([1, "one"]);
$insert->execute([2, "two"]);

$source->commit();

sleepForMirror();

$sourceCount = (int) $source->query("SELECT COUNT(*) FROM public.mirror_php_test")->fetchColumn();
$mirrorCount = (int) $mirror->query("SELECT COUNT(*) FROM public.mirror_php_test")->fetchColumn();

if ($sourceCount !== 2) {
    fwrite(STDERR, "expected 2 rows on source, got {$sourceCount}\n");
    exit(1);
}

if ($mirrorCount !== 2) {
    fwrite(STDERR, "expected 2 rows on mirror, got {$mirrorCount}\n");
    exit(1);
}

$mirrorRows = $mirror
    ->query("SELECT id, value FROM public.mirror_php_test ORDER BY id")
    ->fetchAll(PDO::FETCH_ASSOC);

$mirrorRows = array_map(
    static fn(array $row): array => [
        "id" => (int) $row["id"],
        "value" => (string) $row["value"],
    ],
    $mirrorRows
);

$expected = [
    ["id" => 1, "value" => "one"],
    ["id" => 2, "value" => "two"],
];

if ($mirrorRows !== $expected) {
    fwrite(STDERR, "unexpected mirror rows: " . json_encode($mirrorRows) . "\n");
    exit(1);
}

$source->exec("DROP TABLE IF EXISTS public.mirror_php_test");
sleepForMirror();

echo "php mirror test passed\n";
