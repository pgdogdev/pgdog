# frozen_string_literal: true

require "active_record"
require "datadog/statsd"
require "rspec"
require "yaml"

db_config_file = File.open('config/database.yaml')
db_config = YAML::load(db_config_file)

ActiveRecord::Base.configurations = db_config

class PgDog < ActiveRecord::Base
  self.abstract_class = true
  connects_to database: { writing: :pgdog, reading: :pgdog }
end

class PgBouncer < ActiveRecord::Base
  self.abstract_class = true
  connects_to database: { writing: :pgbouncer, reading: :pgbouncer }
end

class BenchmarkPgDog < PgDog
  self.table_name = 'benchmark_table_pgdog'
  self.primary_key = 'id'

  def self.benchmark_name
    "pgdog"
  end
end

class BenchmarkPgBouncer < PgBouncer
  self.table_name = 'benchmark_table_pgb'
  self.primary_key = 'id'

  def self.benchmark_name
    "pgbouncer"
  end
end

# Measure timing of something
def timing(name)
  start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  yield
  finish = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  diff = ((finish - start) * 1000.0).round(2)
  $statsd.histogram("benchmark.#{name}", diff, tags: ["env:development"])
  $statsd.flush(sync: true)
end

describe "benchmark" do
  before do
    $statsd = Datadog::Statsd.new('127.0.0.1', 8125)
    # Different tables to avoid lock contention.
    [["pgdog", BenchmarkPgDog], ["pgb", BenchmarkPgBouncer]].each do |pair|
      pair[1].connection.execute "DROP TABLE IF EXISTS benchmark_table_#{pair[0]}"
      pair[1].connection.execute "CREATE TABLE benchmark_table_#{pair[0]} (id BIGINT PRIMARY KEY, value TEXT)"
    end
  end

  it "runs" do
    25000000.times do |i|
      [BenchmarkPgDog, BenchmarkPgBouncer].each do |klass|
        timing(klass.benchmark_name) do
          klass.create id: i, value: "test"
          v = klass.find i
          v.value = "apples"
          v.save
          v.reload
          v.destroy
        end
      end
    end
  end

  after do
    $statsd.close
  end
end
