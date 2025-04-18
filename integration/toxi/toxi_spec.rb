# frozen_string_literal: true

require_relative 'rspec_helper'

def warm_up
  conn.exec 'SELECT 1'
  admin.exec 'RECONNECT'
  sleep 1
  conn.exec 'SELECT 1'
end

shared_examples 'minimal errors' do |role, toxic|
  it 'executes with reconnecting' do
    Toxiproxy[role].toxic(toxic).apply do
      errors = 0
      25.times do
        c = conn
        c.exec 'SELECT 1::bigint AS one'
        c.close
      rescue PG::SystemError
        errors += 1
      end
      expect(errors).to be < 3
    end
  end

  it 'some connections survive' do
    threads = []
    errors = 0
    sem = Concurrent::Semaphore.new(0)
    (5.0 / 25 * 25.0).ceil
    25.times do
      t = Thread.new do
        c = 1
        sem.acquire
        loop do
          c = conn
          break
        rescue StandardError
          errors += 1
        end
        25.times do
          c.exec 'SELECT 1'
        rescue PG::SystemError
          c = conn # reconnect
          errors += 1
        end
      end
      threads << t
    end
    Toxiproxy[role].toxic(toxic).apply do
      sem.release(25)
      threads.each(&:join)
    end
    expect(errors).to be < 25 # 5% error rate (instead of 100%)
  end
end

describe 'tcp' do
  it 'can connect' do
    c = conn
    tup = c.exec 'SELECT 1::bigint AS one'
    expect(tup[0]['one']).to eq('1')
  end

  describe 'broken database' do
    before do
      warm_up
    end

    after do
      admin.exec 'RECONNECT'
    end

    describe 'broken primary' do
      it_behaves_like 'minimal errors', :primary, :reset_peer
    end

    describe 'broken primary with existing conns' do
      it_behaves_like 'minimal errors', :primary, :reset_peer
    end

    describe 'broken replica' do
      it_behaves_like 'minimal errors', :replica, :reset_peer
    end

    describe 'timeout primary' do
      describe 'cancels query' do
        it_behaves_like 'minimal errors', :primary, :timeout
      end

      after do
        admin.exec 'RELOAD'
      end
    end

    describe 'both down' do
      it 'unbans all pools' do
        25.times do
          Toxiproxy[:primary].toxic(:reset_peer).apply do
            Toxiproxy[:replica].toxic(:reset_peer).apply do
              2.times do
                conn.exec_params 'SELECT $1::bigint', [1]
              rescue StandardError
              end
              banned = admin.exec('SHOW POOLS').select do |pool|
                pool['database'] == 'failover'
              end.select { |item| item['banned'] == 't' }
              expect(banned.size).to eq(2)
            end
          end
          conn.exec 'SELECT $1::bigint', [25]
          banned = admin.exec('SHOW POOLS').select do |pool|
            pool['database'] == 'failover'
          end.select { |item| item['banned'] == 't' }
          expect(banned.size).to eq(0)
        end
      end
    end
  end
end
