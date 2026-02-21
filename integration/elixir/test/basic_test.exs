defmodule BasicTest do
  use ExUnit.Case

  test "can connect to pgdog" do
    {:ok, pid} = Postgrex.start_link(TestConfig.connection_opts())

    result = Postgrex.query!(pid, "SELECT $1::bigint AS one", [1])
    assert %Postgrex.Result{rows: [[1]]} = result

    GenServer.stop(pid)
  end

  test "can perform basic queries" do
    {:ok, pid} = Postgrex.start_link(TestConfig.connection_opts())

    result = Postgrex.query!(pid, "SELECT 'hello' AS greeting", [])
    assert %Postgrex.Result{rows: [["hello"]]} = result

    result = Postgrex.query!(pid, "SELECT 42 AS answer", [])
    assert %Postgrex.Result{rows: [[42]]} = result

    GenServer.stop(pid)
  end
end