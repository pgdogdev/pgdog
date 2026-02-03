defmodule PgdogElixirTests.MixProject do
  use Mix.Project

  def project do
    [
      app: :pgdog_elixir_tests,
      version: "0.1.0",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:postgrex, "~> 0.17"},
      {:decimal, "~> 2.0"},
      {:jason, "~> 1.4"}
    ]
  end
end