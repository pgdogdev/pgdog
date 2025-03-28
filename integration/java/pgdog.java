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
        ResultSet rs = st.executeQuery("SELECT 1 AS one");
        int rows = 0;
        while (rs.next()) {
            rows += 1;
            assert rs.getInt(0) == 1;
        }
        assert rows == 1;
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
    }
}
