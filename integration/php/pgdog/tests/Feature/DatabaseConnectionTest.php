<?php

use Illuminate\Support\Facades\DB;

beforeEach(function () {
    DB::statement("TRUNCATE TABLE sharded");
});

test("can connect to database and select 1", function () {
    $result = DB::select("SELECT 1 as value");

    expect($result)->toHaveCount(1);
    expect($result[0]->value)->toBe(1);
});

test("can perform transactions on sharded table", function () {
    for ($i = 1; $i <= 5; $i++) {
        
        // Test successful transaction
        DB::transaction(function () use ($i) {
            DB::insert("INSERT INTO sharded (id, value) VALUES (?, ?)", [$i * 10 + 1, "test{$i}_1"]);
            DB::insert("INSERT INTO sharded (id, value) VALUES (?, ?)", [$i * 10 + 2, "test{$i}_2"]);
        });

        $result = DB::select("SELECT * FROM sharded WHERE id >= ? ORDER BY id", [$i * 10]);
        expect($result)->toHaveCount(2);
        expect($result[0]->id)->toBe($i * 10 + 1);
        expect($result[0]->value)->toBe("test{$i}_1");
        expect($result[1]->id)->toBe($i * 10 + 2);
        expect($result[1]->value)->toBe("test{$i}_2");

        // Clear for next iteration
        DB::statement("TRUNCATE TABLE sharded");

        // Test rollback on exception
        try {
            DB::transaction(function () use ($i) {
                DB::insert("INSERT INTO sharded (id, value) VALUES (?, ?)", [$i * 10 + 3, "test{$i}_3"]);
                throw new Exception("Force rollback in loop $i");
            });
        } catch (Exception $e) {
            // Expected exception
        }

        $result = DB::select("SELECT * FROM sharded");
        expect($result)->toHaveCount(0);
    }
});
