require_relative "rspec_helper"

describe "tcp" do
  it "can connect" do
    c = conn
    tup = c.exec "SELECT 1::bigint AS one"
    expect(tup[0]["one"]).to eq("1")
  end

  describe "broken database" do
    before do
      conn.exec "SELECT 1"
      admin.exec "RECONNECT"
      sleep 1
      conn.exec "SELECT 1"
    end

    after do
      admin.exec "RECONNECT"
    end

    it "broken primary" do
      errors = 0
      Toxiproxy[:primary].toxic(:reset_peer).apply do
        25.times do
          begin
            c = conn
            tup = c.exec "SELECT 1::bigint AS one"
          rescue PG::SystemError
            errors += 1
          end
        end
        expect(errors).to be < 2
      end
    end

    it "broken replica" do
      errors = 0
      Toxiproxy[:replica].toxic(:reset_peer).apply do
        25.times do
          begin
            c = conn
            tup = c.exec "SELECT 1::bigint AS one"
          rescue PG::SystemError
            errors += 1
          end
        end
        expect(errors).to be < 2
      end
    end
  end
end
