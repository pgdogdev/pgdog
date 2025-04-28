require 'rspec'
require 'active_record'
require 'pgdog'

describe "basics" do
  before do
    ActiveRecord::Base.establish_connection(
      adapter: 'postgresql',
      host: '127.0.0.1',
      port: 6432,
      database: 'pgdog_sharded',
      password: 'pgdog',
      user: 'pgdog',
      prepared_statements: true,
    )
  end

  it "doesn't crash if no shard set" do
    expect(PgDog.shard).to be nil
    expect(PgDog.sharding_key).to be nil
  end

  it "can select shard" do
    PgDog.with_shard(1) do
      ActiveRecord::Base.connection.execute "SELECT 1"
      shard = PgDog.shard
      expect(shard).to eq(1)
    end
  end

  it "can select sharding key" do
    PgDog.with_sharding_key(1234) do
      ActiveRecord::Base.connection.execute "SELECT 1"
      key = PgDog.sharding_key
      expect(key.to_i).to eq(1234)
    end
  end

  it "checks transaction isn't started" do
    ActiveRecord::Base.transaction do
      ActiveRecord::Base.connection.execute "SELECT 1"
      expect {
        PgDog.with_shard(1) do
          ActiveRecord::Base.connection.execute "SELECT 1"
        end
      }.to raise_error /Transaction already started/
    end
  end
end
