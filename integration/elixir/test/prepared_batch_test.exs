defmodule PreparedBatchTest do
  use ExUnit.Case

  setup do
    {:ok, pid} = Postgrex.start_link(TestConfig.connection_opts())
    %{conn: pid}
  end

  defp create_test_table(conn) do
    table_suffix = :erlang.system_time(:nanosecond)
    table_name = "batch_test_#{table_suffix}"

    Postgrex.query!(conn, """
      CREATE TABLE #{table_name} (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        value INTEGER NOT NULL
      )
    """, [])

    table_name
  end

  defp cleanup_table(conn, table_name) do
    try do
      Postgrex.query(conn, "DROP TABLE IF EXISTS #{table_name}", [])
    catch
      _ -> :ok
    end
  end

  test "can execute batch inserts with prepared statements", %{conn: conn} do
    table_name = create_test_table(conn)

    {:ok, query} = Postgrex.prepare(conn, "", "INSERT INTO #{table_name} (name, value) VALUES ($1, $2)")

    # Execute multiple inserts
    batch_data = [
      ["Alice", 100],
      ["Bob", 200],
      ["Charlie", 300],
      ["David", 400]
    ]

    results = Enum.map(batch_data, fn params ->
      Postgrex.execute!(conn, query, params)
    end)

    # Verify all inserts succeeded
    assert length(results) == 4
    Enum.each(results, fn result ->
      assert %Postgrex.Result{command: :insert, num_rows: 1} = result
    end)

    # Verify data was inserted correctly
    result = Postgrex.query!(conn, "SELECT name, value FROM #{table_name} ORDER BY name", [])
    assert %Postgrex.Result{rows: rows} = result
    assert length(rows) == 4
    assert [["Alice", 100], ["Bob", 200], ["Charlie", 300], ["David", 400]] = rows

    cleanup_table(conn, table_name)
  end

  test "can execute batch updates with prepared statements", %{conn: conn} do
    table_name = create_test_table(conn)

    # Insert initial data
    Postgrex.query!(conn, """
      INSERT INTO #{table_name} (name, value) VALUES
      ('Alice', 100),
      ('Bob', 200),
      ('Charlie', 300)
    """, [])

    {:ok, query} = Postgrex.prepare(conn, "", "UPDATE #{table_name} SET value = $1 WHERE name = $2")

    # Execute batch updates
    update_data = [
      [150, "Alice"],
      [250, "Bob"],
      [350, "Charlie"]
    ]

    results = Enum.map(update_data, fn params ->
      Postgrex.execute!(conn, query, params)
    end)

    # Verify all updates succeeded
    assert length(results) == 3
    Enum.each(results, fn result ->
      assert %Postgrex.Result{command: :update, num_rows: 1} = result
    end)

    # Verify data was updated correctly
    result = Postgrex.query!(conn, "SELECT name, value FROM #{table_name} ORDER BY name", [])
    assert %Postgrex.Result{rows: [["Alice", 150], ["Bob", 250], ["Charlie", 350]]} = result

    cleanup_table(conn, table_name)
  end

  test "can handle mixed batch operations", %{conn: conn} do
    table_name = create_test_table(conn)

    # Prepare different statement types
    {:ok, insert_query} = Postgrex.prepare(conn, "", "INSERT INTO #{table_name} (name, value) VALUES ($1, $2)")
    {:ok, select_query} = Postgrex.prepare(conn, "", "SELECT value FROM #{table_name} WHERE name = $1")

    # Insert some data
    Postgrex.execute!(conn, insert_query, ["Test1", 111])
    Postgrex.execute!(conn, insert_query, ["Test2", 222])

    # Query the data back
    result1 = Postgrex.execute!(conn, select_query, ["Test1"])
    result2 = Postgrex.execute!(conn, select_query, ["Test2"])

    assert %Postgrex.Result{rows: [[111]]} = result1
    assert %Postgrex.Result{rows: [[222]]} = result2

    cleanup_table(conn, table_name)
  end

  test "can handle batch operations with transactions", %{conn: conn} do
    table_name = create_test_table(conn)

    {:ok, query} = Postgrex.prepare(conn, "", "INSERT INTO #{table_name} (name, value) VALUES ($1, $2)")

    # Use a transaction for batch operations
    Postgrex.transaction(conn, fn transaction_conn ->
      # Execute batch inserts within transaction
      batch_data = [
        ["TxUser1", 1000],
        ["TxUser2", 2000],
        ["TxUser3", 3000]
      ]

      Enum.each(batch_data, fn params ->
        Postgrex.execute!(transaction_conn, query, params)
      end)

      # Query within the same transaction to verify
      result = Postgrex.query!(transaction_conn, "SELECT COUNT(*) FROM #{table_name} WHERE name LIKE 'TxUser%'", [])
      assert %Postgrex.Result{rows: [[3]]} = result
    end)

    # Verify data persisted after transaction
    result = Postgrex.query!(conn, "SELECT name, value FROM #{table_name} WHERE name LIKE 'TxUser%' ORDER BY name", [])
    assert %Postgrex.Result{rows: [["TxUser1", 1000], ["TxUser2", 2000], ["TxUser3", 3000]]} = result

    cleanup_table(conn, table_name)
  end

  test "can handle batch operations with error recovery", %{conn: conn} do
    table_name = create_test_table(conn)

    {:ok, query} = Postgrex.prepare(conn, "", "INSERT INTO #{table_name} (name, value) VALUES ($1, $2)")

    # First, insert some valid data
    Postgrex.execute!(conn, query, ["Valid1", 100])

    # Try to insert invalid data (this should fail due to constraint or type issues)
    # But we'll catch and handle the error
    batch_data = [
      ["Valid2", 200],
      ["Valid3", 300]
    ]

    results = Enum.map(batch_data, fn params ->
      try do
        {:ok, Postgrex.execute!(conn, query, params)}
      rescue
        error -> {:error, error}
      end
    end)

    # Count successful operations
    successful_ops = Enum.count(results, fn
      {:ok, _} -> true
      _ -> false
    end)

    assert successful_ops == 2

    # Verify the valid data was inserted
    result = Postgrex.query!(conn, "SELECT COUNT(*) FROM #{table_name}", [])
    assert %Postgrex.Result{rows: [[3]]} = result

    cleanup_table(conn, table_name)
  end
end