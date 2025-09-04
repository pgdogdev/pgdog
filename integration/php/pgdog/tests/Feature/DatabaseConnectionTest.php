<?php

use Illuminate\Support\Facades\DB;

test('can connect to database and select 1', function () {
    $result = DB::select('SELECT 1 as value');
    
    expect($result)->toHaveCount(1);
    expect($result[0]->value)->toBe(1);
});