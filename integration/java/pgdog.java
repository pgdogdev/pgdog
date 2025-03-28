import java.sql.*;

abstract class TestCase {

    protected Connection connection;
    protected String name;

    TestCase(String database, String name) throws Exception {
        String url =
            "jdbc:postgresql://127.0.0.1:6432/" +
            database +
            "?user=pgdog&password=pgdog&ssl=false";
        Connection conn = DriverManager.getConnection(url);
        this.connection = conn;
        this.name = name;
    }

    public void execute() throws Exception {
        System.out.println("Executing " + this.name);
        run();
    }

    abstract void run() throws Exception;
}

class SelectOne extends TestCase {

    SelectOne() throws Exception {
        super("pgdog", "SelectOne");
    }

    void run() throws Exception {
        Statement st = this.connection.createStatement();
        ResultSet rs = st.executeQuery("SELECT 1::integer AS one");
        int rows = 0;
        while (rs.next()) {
            rows += 1;
            assert rs.getInt("one") == 1;
        }
        assert rows == 1;
    }
}

class Prepared extends TestCase {

    Prepared() throws Exception {
        super("pgdog", "Prepared");
    }

    void run() throws Exception {
        PreparedStatement st =
            this.connection.prepareStatement(
                    "INSERT INTO sharded (id, value) VALUES (?, ?) RETURNING *"
                );

        int rows = 0;

        for (int i = 0; i < 25; i++) {
            st.setInt(1, i);
            st.setString(2, "value_" + i);
            ResultSet rs = st.executeQuery();

            while (rs.next()) {
                rows += 1;
                assert i == rs.getInt("id");
                assert rs.getString("value").equals("value_" + i);
            }
        }

        assert rows == 25;
    }
}

class Pgdog {

    public static Connection connect() throws Exception {
        String url =
            "jdbc:postgresql://127.0.0.1:6432/pgdog?user=pgdog&password=pgdog&ssl=false";
        Connection conn = DriverManager.getConnection(url);

        return conn;
    }

    public static void main(String[] args) throws Exception {
        new SelectOne().execute();
        new Prepared().execute();
    }
}
