defmodule PreparedTest do
  use ExUnit.Case

  setup do
    {:ok, pid} = Postgrex.start_link(TestConfig.connection_opts())
    %{conn: pid}
  end

  test "can prepare and execute simple queries", %{conn: conn} do
    {:ok, query} = Postgrex.prepare(conn, "", "SELECT $1::text AS message")
    result = Postgrex.execute!(conn, query, ["hello world"])

    assert %Postgrex.Result{rows: [["hello world"]]} = result
  end

  test "can prepare and execute numeric queries", %{conn: conn} do
    {:ok, query} = Postgrex.prepare(conn, "", "SELECT $1::integer + $2::integer AS sum")
    result = Postgrex.execute!(conn, query, [10, 20])

    assert %Postgrex.Result{rows: [[30]]} = result
  end

  test "can prepare and execute boolean queries", %{conn: conn} do
    {:ok, query} = Postgrex.prepare(conn, "", "SELECT $1::boolean AND $2::boolean AS result")
    result = Postgrex.execute!(conn, query, [true, false])

    assert %Postgrex.Result{rows: [[false]]} = result
  end

  test "can prepare and execute date queries", %{conn: conn} do
    {:ok, query} = Postgrex.prepare(conn, "", "SELECT $1::date AS input_date")
    date = ~D[2024-01-15]
    result = Postgrex.execute!(conn, query, [date])

    assert %Postgrex.Result{rows: [[^date]]} = result
  end

  test "can reuse prepared statements", %{conn: conn} do
    {:ok, query} = Postgrex.prepare(conn, "", "SELECT $1::text || ' - ' || $2::text AS combined")

    result1 = Postgrex.execute!(conn, query, ["hello", "world"])
    assert %Postgrex.Result{rows: [["hello - world"]]} = result1

    result2 = Postgrex.execute!(conn, query, ["foo", "bar"])
    assert %Postgrex.Result{rows: [["foo - bar"]]} = result2
  end
end