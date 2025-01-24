require 'active_record'

ActiveRecord::Base.establish_connection(
  :adapter => "postgresql",
  :host => "127.0.0.1",
  :port => 6432,
  :database => "pgdog_sharded",
  :password => "pgdog",
  :user => "pgdog",
  :prepared_statements => false,
)

class Sharded < ActiveRecord::Base
  self.table_name = "sharded"
  self.primary_key = "id"
end

1.times do |i|
  count = Sharded.where(id: 1).count
end
