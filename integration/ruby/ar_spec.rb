# frozen_string_literal: true

require_relative 'rspec_helper'

class Sharded < ActiveRecord::Base
  self.table_name = 'sharded'
  self.primary_key = 'id'
end

def conn(db, prepared)
  ActiveRecord::Base.establish_connection(
    adapter: 'postgresql',
    host: '127.0.0.1',
    port: 6432,
    database: db,
    password: 'pgdog',
    user: 'pgdog',
    prepared_statements: prepared
  )
end

describe 'active record' do
  describe 'normal' do
    before do
      conn('pgdog', false)
      ActiveRecord::Base.connection.execute 'DROP TABLE IF EXISTS sharded'
      ActiveRecord::Base.connection.execute 'CREATE TABLE sharded (id BIGINT, value TEXT)'
    end

    it 'can connect' do
      res = ActiveRecord::Base.connection.execute 'SELECT 1 AS one'
      expect(res.num_tuples).to eq(1)
      expect(res[0]['one']).to eq(1)
    end

    it 'can execute normal statements' do
      res = Sharded.create id: 1, value: 'test'
      expect(res.id).to eq(1)
      expect(res.value).to eq('test')
      250.times do
        expect(Sharded.find(1).id).to eq(1)
      end
    end
  end

  describe 'sharded' do
    before do
      conn('pgdog_sharded', false)

      ActiveRecord::Base.connection.execute 'DROP TABLE IF EXISTS sharded'
      ActiveRecord::Base.connection.execute 'CREATE TABLE sharded (id BIGSERIAL PRIMARY KEY, value TEXT)'
    end

    it 'can connect' do
      250.times do
        res = ActiveRecord::Base.connection.execute 'SELECT 1 AS one'
        expect(res.num_tuples).to eq(1)
        expect(res[0]['one']).to eq(1)
      end
    end

    it 'can execute normal statements' do
      250.times do |id|
        res = Sharded.create id: id, value: "value_#{id}"
        expect(res.id).to eq(id)
        expect(res.value).to eq("value_#{id}")
        expect(Sharded.find(id).value).to eq("value_#{id}")
      end
    end

    it 'can assign to a shard' do
      250.times do |i|
        res = Sharded.new
        res.value = 'test'
        created = res.save
        expect(created).to be_truthy
        expect(res.id).to eq(i / 2 + 1)
      end
    end
  end

  describe 'active record prepared' do
    describe 'normal' do
      before do
        conn('pgdog', true)
        ActiveRecord::Base.connection.execute 'DROP TABLE IF EXISTS sharded'
        ActiveRecord::Base.connection.execute 'CREATE TABLE sharded (id BIGSERIAL PRIMARY KEY, value TEXT)'
      end

      it 'can create and read record' do
        15.times do |j|
          res = Sharded.create value: 'test'
          expect(res.id).to eq(j + 1)
          250.times do |_i|
            Sharded.find(j + 1)
          end
        end
      end
    end

    describe 'sharded' do
      before do
        conn('pgdog_sharded', true)
        ActiveRecord::Base.connection.execute 'DROP TABLE IF EXISTS sharded'
        ActiveRecord::Base.connection.execute 'CREATE TABLE sharded (id BIGSERIAL PRIMARY KEY, value TEXT)'
        # Automatic primary key assignment.
        ActiveRecord::Base.connection.execute "/* pgdog_shard: 0 */ SELECT pgdog.install_next_id('pgdog', 'sharded', 'id', 2, 0)"
        ActiveRecord::Base.connection.execute "/* pgdog_shard: 1 */ SELECT pgdog.install_next_id('pgdog', 'sharded', 'id', 2, 1)"
      end

      it 'can create and read record' do
        30.times do |j|
          res = Sharded.create value: "test_#{j}"
          res = Sharded.find(res.id)
          expect(res.value).to eq("test_#{j}")
          count = Sharded.where(id: res.id).count
          expect(count).to eq(1)
        end
      end
    end
  end
end
