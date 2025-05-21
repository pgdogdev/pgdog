
require_relative 'rspec_helper'
require 'pp'

require 'active_record/migration'

# --- Migration Helper ---
def migrate_tables
  ActiveRecord::Schema.define do
    suppress_messages do
      create_table :customers, if_not_exists: true, id: :bigserial do |t|
        t.string :name, null: false
        t.string :email, null: false
        t.timestamps
      end
      add_index :customers, :email, unique: true, if_not_exists: true

      create_table :orders, if_not_exists: true, id: :bigserial do |t|
        t.references :customer, null: false, foreign_key: true, type: :bigint
        t.decimal :amount, null: false
        t.datetime :order_date, null: false
        t.timestamps
      end
    end
  end
end

def drop_tables
  ActiveRecord::Schema.define do
    suppress_messages do
      drop_table :orders, if_exists: true
      drop_table :customers, if_exists: true
    end
  end
end

# --- Model Definitions ---
class Customer < ActiveRecord::Base
  has_many :orders, dependent: :destroy
  validates :name, presence: true
  validates :email, presence: true, uniqueness: true
end

class Order < ActiveRecord::Base
  belongs_to :customer
  validates :amount, presence: true
  validates :order_date, presence: true
end

# --- Test Suite ---
describe 'CRUD and Join for Customer and Order', type: :model do
  before(:all) do
    ActiveRecord::Base.establish_connection(
      adapter: 'postgresql',
      host: '127.0.0.1',
      port: 6432,
      database: "pgdog_sharded",
      password: 'pgdog',
      user: 'pgdog',
      prepared_statements: true
    )

    migrate_tables

    Order.delete_all
    Customer.delete_all
  end

  after(:all) do
    Order.delete_all
    Customer.delete_all
    drop_tables
  end

  it 'does full CRUD and join' do
    # CREATE Customer
    customer = Customer.create!(name: 'Bob', email: 'bob@example.com')
    expect(customer.id).to be_present

    # CREATE Order
    order = Order.create!(customer: customer, amount: 123.45, order_date: Time.now)
    expect(order.id).to be_present

    # SELECT with JOIN by customer_id
    joined = Order.joins(:customer).where(customer_id: customer.id, id: order.id)
    expect(joined.count).to eq(1)
    expect(joined.first.customer.name).to eq('Bob')
    expect(joined.first.amount.to_f).to eq(123.45)

    # UPDATE Order amount (by customer_id)
    order.update!(amount: 200.00)
    order.reload
    expect(order.amount.to_f).to eq(200.00)

    # Confirm update with join
    joined = Order.joins(:customer).where(customer_id: customer.id, id: order.id)
    expect(joined.first.amount.to_f).to eq(200.00)

    # DELETE order (by customer_id)
    order.destroy
    expect(Order.where(id: order.id, customer_id: customer.id)).to be_empty

    # Confirm order is deleted (join returns no rows)
    joined = Order.joins(:customer).where(customer_id: customer.id, id: order.id)
    expect(joined).to be_empty
  end
end
