require 'active_record'

def establish_connection(prepared_statements = true)
  ActiveRecord::Base.establish_connection(
    :adapter => "postgresql",
    :host => "127.0.0.1",
    :port => 6432,
    :database => "pgdog_sharded",
    :password => "pgdog",
    :user => "pgdog",
    :prepared_statements => prepared_statements,
  )
end

class Sharded < ActiveRecord::Base
  self.table_name = "sharded"
  self.primary_key = "id"
end

[false, true].each do |prepared_statements|
  puts "Connecting to database..."
  establish_connection prepared_statements
  puts "Connection established"

  15.times do |i|
    count = Sharded.where(id: i).count
  end
end
