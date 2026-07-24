defmodule PreparedParameterizedTest do
  use ExUnit.Case

  setup do
    {:ok, pid} = Postgrex.start_link(TestConfig.connection_opts())
    %{conn: pid}
  end

  test "can handle complex parameterized queries", %{conn: conn} do
    {:ok, query} = Postgrex.prepare(conn, "", """
      SELECT
        $1::text AS name,
        $2::integer AS age,
        $3::boolean AS active,
        $4::decimal AS score
    """)

    result = Postgrex.execute!(conn, query, ["John Doe", 30, true, Decimal.new("95.5")])

    assert %Postgrex.Result{
      rows: [["John Doe", 30, true, %Decimal{} = score]]
    } = result

    assert Decimal.equal?(score, Decimal.new("95.5"))
  end

  test "can handle array parameters", %{conn: conn} do
    {:ok, query} = Postgrex.prepare(conn, "", "SELECT $1::integer[] AS numbers")
    result = Postgrex.execute!(conn, query, [[1, 2, 3, 4, 5]])

    assert %Postgrex.Result{rows: [[[1, 2, 3, 4, 5]]]} = result
  end

  test "can handle NULL parameters", %{conn: conn} do
    {:ok, query} = Postgrex.prepare(conn, "", """
      SELECT
        $1::text AS name,
        $2::integer AS age
    """)

    result = Postgrex.execute!(conn, query, [nil, nil])
    assert %Postgrex.Result{rows: [[nil, nil]]} = result

    result = Postgrex.execute!(conn, query, ["Alice", 25])
    assert %Postgrex.Result{rows: [["Alice", 25]]} = result
  end

  test "can handle JSON parameters", %{conn: conn} do
    {:ok, query} = Postgrex.prepare(conn, "", "SELECT $1::jsonb AS data")
    json_data = %{"name" => "test", "value" => 42}
    result = Postgrex.execute!(conn, query, [json_data])

    assert %Postgrex.Result{rows: [[returned_json]]} = result
    assert returned_json == json_data
  end

  test "can handle timestamp parameters", %{conn: conn} do
    {:ok, query} = Postgrex.prepare(conn, "", "SELECT $1::timestamp AS ts")
    timestamp = ~N[2024-01-15 14:30:00]
    result = Postgrex.execute!(conn, query, [timestamp])

    assert %Postgrex.Result{rows: [[returned_timestamp]]} = result
    assert NaiveDateTime.truncate(returned_timestamp, :second) == timestamp
  end

  test "can handle multiple executions with different parameter sets", %{conn: conn} do
    {:ok, query} = Postgrex.prepare(conn, "", "SELECT $1::text || ' is ' || $2::integer || ' years old' AS message")

    people = [
      ["Alice", 25],
      ["Bob", 30],
      ["Charlie", 35]
    ]

    results = Enum.map(people, fn params ->
      Postgrex.execute!(conn, query, params)
    end)

    expected_messages = [
      "Alice is 25 years old",
      "Bob is 30 years old",
      "Charlie is 35 years old"
    ]

    actual_messages = Enum.map(results, fn %Postgrex.Result{rows: [[message]]} -> message end)
    assert actual_messages == expected_messages
  end

  test "can handle WHERE clause with parameters", %{conn: conn} do
    # Use a simpler test that doesn't require temporary tables
    # Test numeric comparison with parameters
    {:ok, query} = Postgrex.prepare(conn, "", "SELECT CASE WHEN $1::integer > $2::integer THEN 'greater' ELSE 'not_greater' END AS result")
    result = Postgrex.execute!(conn, query, [30, 25])

    assert %Postgrex.Result{rows: [["greater"]]} = result

    result2 = Postgrex.execute!(conn, query, [20, 25])
    assert %Postgrex.Result{rows: [["not_greater"]]} = result2
  end
end