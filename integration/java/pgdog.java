import java.sql.*;

abstract class TestCase {

    protected Connection connection;
    protected String test_name;
    protected String database;

    TestCase(String database) throws Exception {
        this.database = database;
        String url =
            "jdbc:postgresql://127.0.0.1:6432/" +
            database +
            "?user=pgdog&password=pgdog&ssl=false";
        Connection conn = DriverManager.getConnection(url);
        this.connection = conn;
    }

    public void execute() throws Exception {
        String className = this.getClass().getSimpleName();
        System.out.println(
            "Executing " + className + " [" + this.database + "]"
        );
        before();
        run();
    }

    abstract void run() throws Exception;

    public void before() throws Exception {
        // Run some code before a test.
    }

    public static void assert_equals(int left, int right) throws Exception {
        if (left != right) {
            throw new Exception(left + " != " + right);
        }
    }

    public static void assert_equals(String left, String right)
        throws Exception {
        if (!left.equals(right)) {
            throw new Exception(left + " != " + right);
        }
    }
}

class SelectOne extends TestCase {

    SelectOne(String database) throws Exception {
        super(database);
    }

    void run() throws Exception {
        Statement st = this.connection.createStatement();
        ResultSet rs = st.executeQuery("SELECT 1::integer AS one");
        int rows = 0;
        while (rs.next()) {
            rows += 1;
            assert rs.getInt("one") == 1;
        }
        TestCase.assert_equals(rows, 1);
    }
}

class Prepared extends TestCase {

    Prepared(String database) throws Exception {
        super(database);
    }

    public void before() throws Exception {
        Statement trunc = this.connection.createStatement();
        trunc.execute("TRUNCATE TABLE sharded");
    }

    void run() throws Exception {
        PreparedStatement st = this.connection.prepareStatement(
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

class Transaction extends TestCase {

    Transaction(String database) throws Exception {
        super(database);
    }

    public void before() throws Exception {
        Statement setup = this.connection.createStatement();
        setup.execute("TRUNCATE TABLE sharded");
    }

    void run() throws Exception {
        this.connection.setAutoCommit(false);

        Statement st = this.connection.createStatement();

        ResultSet rs = st.executeQuery("SELECT COUNT(*) as count FROM sharded");
        rs.next();
        assert_equals(rs.getInt("count"), 0);

        st.execute("INSERT INTO sharded (id, value) VALUES (1, 'test1')");
        st.execute("INSERT INTO sharded (id, value) VALUES (2, 'test2')");

        rs = st.executeQuery("SELECT COUNT(*) as count FROM sharded");
        rs.next();
        assert_equals(rs.getInt("count"), 2);

        this.connection.rollback();

        rs = st.executeQuery("SELECT COUNT(*) as count FROM sharded");
        rs.next();
        assert_equals(rs.getInt("count"), 0);

        st.execute("INSERT INTO sharded (id, value) VALUES (3, 'test3')");
        st.execute("INSERT INTO sharded (id, value) VALUES (4, 'test4')");

        this.connection.commit();

        rs = st.executeQuery("SELECT COUNT(*) as count FROM sharded");
        rs.next();
        assert_equals(rs.getInt("count"), 2);

        this.connection.setAutoCommit(true);
    }
}

class TransactionPrepared extends TestCase {

    TransactionPrepared(String database) throws Exception {
        super(database);
    }

    public void before() throws Exception {
        Statement setup = this.connection.createStatement();
        setup.execute("TRUNCATE TABLE sharded");
    }

    void run() throws Exception {
        this.connection.setAutoCommit(false);

        PreparedStatement insertStmt = this.connection.prepareStatement(
            "INSERT INTO sharded (id, value) VALUES (?, ?)"
        );
        PreparedStatement countStmt = this.connection.prepareStatement(
            "SELECT COUNT(*) as count FROM sharded"
        );
        PreparedStatement selectStmt = this.connection.prepareStatement(
            "SELECT id, value FROM sharded WHERE id = ?"
        );

        // Verify table is empty
        ResultSet rs = countStmt.executeQuery();
        rs.next();
        assert_equals(rs.getInt("count"), 0);

        // Insert records using prepared statements
        insertStmt.setInt(1, 1);
        insertStmt.setString(2, "test1");
        insertStmt.execute();

        insertStmt.setInt(1, 2);
        insertStmt.setString(2, "test2");
        insertStmt.execute();

        // Verify records were inserted within transaction
        rs = countStmt.executeQuery();
        rs.next();
        assert_equals(rs.getInt("count"), 2);

        // Verify specific record using prepared statement
        selectStmt.setInt(1, 1);
        rs = selectStmt.executeQuery();
        rs.next();
        assert_equals(rs.getInt("id"), 1);
        assert_equals(rs.getString("value"), "test1");

        // Rollback transaction
        this.connection.rollback();

        // Verify rollback worked
        rs = countStmt.executeQuery();
        rs.next();
        assert_equals(rs.getInt("count"), 0);

        // Insert more records and commit
        insertStmt.setInt(1, 3);
        insertStmt.setString(2, "test3");
        insertStmt.execute();

        insertStmt.setInt(1, 4);
        insertStmt.setString(2, "test4");
        insertStmt.execute();

        this.connection.commit();

        // Verify commit worked
        rs = countStmt.executeQuery();
        rs.next();
        assert_equals(rs.getInt("count"), 2);

        // Verify committed records
        selectStmt.setInt(1, 3);
        rs = selectStmt.executeQuery();
        rs.next();
        assert_equals(rs.getInt("id"), 3);
        assert_equals(rs.getString("value"), "test3");

        this.connection.setAutoCommit(true);
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
        new SelectOne("pgdog").execute();
        new SelectOne("pgdog_sharded").execute();
        new Prepared("pgdog").execute();
        new Prepared("pgdog_sharded").execute();
        new Transaction("pgdog").execute();
        new Transaction("pgdog_sharded").execute();
        new TransactionPrepared("pgdog").execute();
        new TransactionPrepared("pgdog_sharded").execute();
    }
}
