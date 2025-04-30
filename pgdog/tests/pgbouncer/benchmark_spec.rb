# frozen_string_literal: true

require "active_record"
require "datadog/statsd"
require "rspec"

class BenchmarkTable < ActiveRecord::Base
  self.table_name = 'benchmark_table'
  self.primary_key = 'id'
end

# Measure timing of something
def timing
  start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  yield
  finish = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  diff = ((finish - start) * 1000.0).round(2)
  $statsd.histogram("benchmark", diff, tags: ["env:development"])
  $statsd.flush(sync: true)
end

describe "benchmark" do
  before do
    $statsd = Datadog::Statsd.new('127.0.0.1', 8125)

    ActiveRecord::Base.establish_connection(
      adapter: 'postgresql',
      host: '127.0.0.1',
      port: 6432,
      database: 'pgdog',
      password: 'pgdog',
      user: 'pgdog',
      prepared_statements: false,
    )

    BenchmarkTable.connection.execute "DROP TABLE IF EXISTS benchmark_table"
    BenchmarkTable.connection.execute "CREATE TABLE benchmark_table (id BIGINT, value TEXT)"
  end

  it "runs" do
    25000000.times do |i|
      timing do
        BenchmarkTable.create id: i, value: "test"
        b = BenchmarkTable.find i
        b.value = "apples"
        b.save
        b.reload
        b.destroy
      end
    end
  end

  after do
    $statsd.close
  end
end
