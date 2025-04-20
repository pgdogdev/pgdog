# frozen_string_literal: true

require_relative 'rspec_helper'

describe 'load balancer' do
  it 'can connect' do
    c = failover
    c.exec 'SELECT 1'
    c.close
  end

  describe 'random' do
    it 'distributes traffic evenly' do
      conn = failover

      before = admin_stats('failover')
      250.times do
        conn.exec 'SELECT 1'
      end
      after = admin_stats('failover')
      transactions = after.zip(before).map do |stats|
        stats[0]['total_xact_count'].to_i - stats[1]['total_xact_count'].to_i
      end

      transactions.each do |transaction|
        expect(transaction - 250 / 4).to be < 5
      end
    end

  end
end
