class PgDog
  # Get a connection from ActiveRecord.
  def self.connection
    return ActiveRecord::Base.connection
  end

  # Start a transaction and set the shard number
  # manually using SET.
  def self.with_shard(shard)
    # Basic SQL injection protection
    shard = shard.to_i

    PgDog.check_transaction
    ActiveRecord::Base.transaction do
      self.connection.execute "SET \"pgdog.shard\" TO #{shard}"
      yield
    end
  end

  # Start a transaction and set the sharding key
  # manually using SET.
  def self.with_sharding_key(key)
    # Basic SQL injection protection.
    key = key.to_s.sub "'", "''"

    PgDog.check_transaction
    ActiveRecord::Base.transaction do
      self.connection.execute "SET \"pgdog.sharding_key\" TO '#{key}'"
      yield
    end
  end

  # Get the number of configured shards
  #
  # Can only work outside of a transaction, because
  # a started transaction is most likely already routed to a shard
  # and the PgDog query parser won't be used.
  def self.shards
    PgDog.check_transaction
    shards = self.connection.execute "SHOW \"pgdog.shards\""
    return shards[0]["shards"].to_i
  end

  # Get currently set shard, if any.
  def self.shard
    shard = self.connection.execute "SELECT current_setting('pgdog.shard', true)"
    shard = shard[0]["current_setting"]

    if shard.nil?
      return nil
    else
      return shard.to_i
    end
  end

  # Get currently set sharding key, if any.
  def self.sharding_key
    key = self.connection.execute "SELECT current_setting('pgdog.sharding_key', true)"
    key[0]["current_setting"]
  end

  # Ensure a transaction isn't started already.
  def self.check_transaction
    if ActiveRecord::Base.connection.open_transactions != 0
      raise PgDogError, "Transaction already started, can't set route"
    end
  end
end

# Error raised if a transaction is already started.
class PgDogError < StandardError
end

# class ActiveRecord::Schema
#   def self.install_sharded_primary_key(table)
#     shards = PgDog.shards
#     table = table.to_s.sub "'", "''"
#     shards.times do |shard|
#       PgDog.connection.execute "/* pgdog_shard: #{shard} */ SELECT pgdog.install_next_id('public', '#{table}', 'id', #{shards}, #{shard})"
#     end
#   end
# end
