defmodule PgdogIntegration.MixProject do
  use Mix.Project

  def project do
    [
      app: :pgdog_integration,
      version: "0.1.0",
      elixir: "~> 1.15",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: false,
      deps: deps()
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp elixirc_paths(:test), do: ["test/support"]
  defp elixirc_paths(_), do: []

  defp deps do
    [
      {:postgrex, "~> 0.22"},
      {:jason, "~> 1.4"}
    ]
  end
end
