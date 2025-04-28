require_relative 'rspec_helper'

class ShardedOmni < ActiveRecord::Base
  self.table_name = "sharded_omni"
  self.primary_key = 'id'
end

describe "omnisharded tables" do
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
    ActiveRecord::Base.connection.execute "TRUNCATE TABLE sharded_omni"
  end

  it "can insert and select" do
    25.times do |id|
      res = ShardedOmni.create id: id, value: "test"
      expect(res.id).to eq(id)

      25.times do
        res = ShardedOmni.where(id: id)
        expect(res.size).to eq(1)
        expect(res[0].id).to eq(id)
      end
    end
  end
end
