require_relative "rspec_helper"

def warm_up
  conn.exec "SELECT 1"
  admin.exec "RECONNECT"
  sleep 1
  conn.exec "SELECT 1"
end

shared_examples "minimal errors" do |role, toxic|
  it "executes" do
    Toxiproxy[role].toxic(toxic).apply do
      errors = 0
      25.times do
        begin
          c = conn
          res = c.exec "SELECT 1::bigint AS one"
          c.close
        rescue PG::SystemError
          errors += 1
        end
      end
      expect(errors).to be < 3
    end
  end
end


describe "tcp" do
  it "can connect" do
    c = conn
    tup = c.exec "SELECT 1::bigint AS one"
    expect(tup[0]["one"]).to eq("1")
  end

  describe "broken database" do
    before do
      warm_up
    end

    after do
      admin.exec "RECONNECT"
    end

    describe "broken primary" do
      it_behaves_like "minimal errors", :primary, :reset_peer
    end

    describe "broken replica" do
      it_behaves_like "minimal errors", :replica, :reset_peer
    end

    describe "timeout primary" do
      before do
        admin.exec "SET query_timeout TO 500"
      end

      describe "cancels query" do
        it_behaves_like "minimal errors", :primary, :timeout
      end

      after do
        admin.exec "RELOAD"
      end
    end
  end
end
