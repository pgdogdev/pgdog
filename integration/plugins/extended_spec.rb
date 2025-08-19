# frozen_string_literal: true

require 'pg'
require 'rspec'

describe 'extended protocol' do
  it 'can pass params to plugin' do
    10.times do
      conn = PG.connect('postgres://pgdog:pgdog@127.0.0.1:6432/pgdog')
      25.times do |i|
        result = conn.exec 'SELECT t.id FROM (SELECT $1 AS id) t WHERE t.id = $1', [i]
        expect(result[0]['id'].to_i).to eq(i)
      end
    end
  end
end
