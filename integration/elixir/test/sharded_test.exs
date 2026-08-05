defmodule Pgdog.ShardedTest do
  use ExUnit.Case, async: false

  @ids Enum.to_list(1..20)

  setup do
    conn = Pgdog.connect(database: "pgdog_sharded")
    Postgrex.query!(conn, "TRUNCATE TABLE sharded", [])
    %{conn: conn}
  end

  defp insert_all(conn, ids) do
    for id <- ids do
      Postgrex.query!(conn, "INSERT INTO sharded (id, value) VALUES ($1, $2)", [id, "v#{id}"])
    end
  end

  test "rows route to a shard and come back", %{conn: conn} do
    ids = [1, 2, 3, 10, 10_000_000_000]
    insert_all(conn, ids)

    for id <- ids do
      result = Postgrex.query!(conn, "SELECT value FROM sharded WHERE id = $1", [id])
      assert Pgdog.column(result) == ["v#{id}"]
    end
  end

  test "INSERT ... RETURNING", %{conn: conn} do
    result =
      Postgrex.query!(
        conn,
        "INSERT INTO sharded (id, value) VALUES ($1, $2) RETURNING id, value",
        [42, "answer"]
      )

    assert result.rows == [[42, "answer"]]
  end

  test "cross-shard select returns every row", %{conn: conn} do
    insert_all(conn, @ids)

    result = Postgrex.query!(conn, "SELECT id FROM sharded", [])
    assert result |> Pgdog.column() |> Enum.sort() == @ids
  end

  test "UPDATE and DELETE by sharding key", %{conn: conn} do
    insert_all(conn, @ids)

    Postgrex.query!(conn, "UPDATE sharded SET value = $1 WHERE id = $2", ["updated", 7])

    assert Pgdog.column(Postgrex.query!(conn, "SELECT value FROM sharded WHERE id = $1", [7])) ==
             ["updated"]

    Postgrex.query!(conn, "DELETE FROM sharded WHERE id = $1", [7])

    assert Pgdog.column(Postgrex.query!(conn, "SELECT value FROM sharded WHERE id = $1", [7])) ==
             []
  end

  test "SET pgdog.shard pins queries to one shard", %{conn: conn} do
    insert_all(conn, @ids)

    per_shard =
      for shard <- [0, 1] do
        Postgrex.query!(conn, "SET pgdog.shard TO '#{shard}'", [])
        Pgdog.column(Postgrex.query!(conn, "SELECT id FROM sharded", []))
      end

    # Every row lands on exactly one shard, and both shards get used.
    assert per_shard |> List.flatten() |> Enum.sort() == @ids
    assert Enum.all?(per_shard, &(&1 != []))
  end

  test "pgdog.shard_id() reports the shard a query landed on", %{conn: conn} do
    for shard <- [0, 1] do
      Postgrex.query!(conn, "SET pgdog.shard TO '#{shard}'", [])
      Postgrex.query!(conn, "SELECT pgdog.install_shard_id($1)", [shard])
      assert Pgdog.one(Postgrex.query!(conn, "SELECT pgdog.shard_id()", [])) == shard
    end
  end

  test "uuid sharding key", %{conn: conn} do
    Postgrex.query!(conn, "TRUNCATE TABLE sharded_uuid", [])
    uuids = for _ <- 1..10, do: :crypto.strong_rand_bytes(16)

    for uuid <- uuids do
      Postgrex.query!(conn, "INSERT INTO sharded_uuid (id_uuid) VALUES ($1)", [uuid])
    end

    for uuid <- uuids do
      result =
        Postgrex.query!(conn, "SELECT id_uuid FROM sharded_uuid WHERE id_uuid = $1", [uuid])

      assert Pgdog.column(result) == [uuid]
    end

    all = Postgrex.query!(conn, "SELECT id_uuid FROM sharded_uuid", [])
    assert all |> Pgdog.column() |> Enum.sort() == Enum.sort(uuids)
  end

  test "prepared statements against a sharded database", %{conn: conn} do
    insert =
      Postgrex.prepare!(
        conn,
        "elixir_sharded_insert",
        "INSERT INTO sharded (id, value) VALUES ($1, $2)"
      )

    select =
      Postgrex.prepare!(
        conn,
        "elixir_sharded_select",
        "SELECT value FROM sharded WHERE id = $1"
      )

    for id <- @ids, do: Postgrex.execute!(conn, insert, [id, "v#{id}"])
    for id <- @ids, do: assert(Pgdog.column(Postgrex.execute!(conn, select, [id])) == ["v#{id}"])
  end

  test "transactions against a sharded database", %{conn: conn} do
    Postgrex.transaction(conn, fn tx ->
      Postgrex.query!(tx, "INSERT INTO sharded (id, value) VALUES ($1, $2)", [1, "one"])

      assert Pgdog.column(Postgrex.query!(tx, "SELECT value FROM sharded WHERE id = $1", [1])) ==
               ["one"]
    end)

    assert Pgdog.column(Postgrex.query!(conn, "SELECT value FROM sharded WHERE id = $1", [1])) ==
             ["one"]

    assert {:error, :rolled_back} =
             Postgrex.transaction(conn, fn tx ->
               Postgrex.query!(tx, "INSERT INTO sharded (id, value) VALUES ($1, $2)", [2, "two"])
               Postgrex.rollback(tx, :rolled_back)
             end)

    assert Pgdog.column(Postgrex.query!(conn, "SELECT value FROM sharded WHERE id = $1", [2])) ==
             []
  end

  # Cross-shard aggregates fail for any client that asks for binary result
  # format over the extended protocol: PgDog's aggregate accumulator can't find
  # the row description and bails out with
  #
  #   FATAL 58000 net error: missing column at index 0 that was assumed to be
  #   present (this indicates a bug in pgdog ...)
  #
  # and drops the connection. `psql` (simple protocol, text format) gets the
  # right answer, and Go's pgx fails the same way ("net error: not an integer"),
  # so this is not Postgrex-specific.
  @tag :known_bug
  test "cross-shard aggregates", %{conn: conn} do
    insert_all(conn, @ids)

    assert Pgdog.one(Postgrex.query!(conn, "SELECT count(*) FROM sharded", [])) == length(@ids)
    assert Pgdog.one(Postgrex.query!(conn, "SELECT sum(id) FROM sharded", [])) == Enum.sum(@ids)
    assert Pgdog.one(Postgrex.query!(conn, "SELECT max(id) FROM sharded", [])) == Enum.max(@ids)
  end

  # Same root cause as the aggregate bug above, but it fails silently instead:
  # the per-shard results are concatenated rather than merge-sorted, so rows
  # come back interleaved. `psql` sorts correctly; Go's pgx does not.
  @tag :known_bug
  test "cross-shard ORDER BY is merge-sorted", %{conn: conn} do
    insert_all(conn, @ids)

    assert Pgdog.column(Postgrex.query!(conn, "SELECT id FROM sharded ORDER BY id", [])) == @ids

    assert Pgdog.column(Postgrex.query!(conn, "SELECT id FROM sharded ORDER BY id LIMIT 5", [])) ==
             Enum.take(@ids, 5)
  end
end
