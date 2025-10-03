import java.sql.*;

abstract class TestCase {

    protected Connection connection;
    protected String test_name;
    protected String database;
    protected String user;

    TestCase(String user, String database) throws Exception {
        this.database = database;
        this.user = user;
        String url =
            "jdbc:postgresql://127.0.0.1:6432/" +
            database +
            "?user=" +
            user +
            "&password=pgdog&ssl=false";
        Connection conn = DriverManager.getConnection(url);
        this.connection = conn;
    }

    TestCase(String database) throws Exception {
        this("pgdog", database);
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
        this.connection.rollback();

        st.execute("INSERT INTO sharded (id, value) VALUES (1, 'test1')");
        st.execute("INSERT INTO sharded (id, value) VALUES (2, 'test2')");

        rs = st.executeQuery("SELECT COUNT(*) as count FROM sharded");
        rs.next();
        assert_equals(rs.getInt("count"), 2);

        this.connection.rollback();

        rs = st.executeQuery("SELECT COUNT(*) as count FROM sharded");
        rs.next();
        assert_equals(rs.getInt("count"), 0);
        this.connection.rollback();

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
        this.connection.rollback();

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
        this.connection.rollback();

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
        this.connection.rollback();

        // Verify committed records
        selectStmt.setInt(1, 3);
        rs = selectStmt.executeQuery();
        rs.next();
        assert_equals(rs.getInt("id"), 3);
        assert_equals(rs.getString("value"), "test3");

        this.connection.setAutoCommit(true);
    }
}

class TransactionDirectShard extends TestCase {

    TransactionDirectShard() throws Exception {
        super("pgdog_no_cross_shard", "pgdog_sharded");
    }

    void run() throws Exception {
        this.connection.setAutoCommit(false);

        Statement st = this.connection.createStatement();

        ResultSet rs = st.executeQuery(
            "SELECT COUNT(*) as count FROM sharded WHERE id = 1"
        );
        rs.next();
        assert_equals(rs.getInt("count"), 0);

        st.execute("INSERT INTO sharded (id, value) VALUES (1, 'test1')");
        st.execute("INSERT INTO sharded (id, value) VALUES (11, 'test11')");

        rs = st.executeQuery(
            "SELECT COUNT(*) as count FROM sharded WHERE id IN (1)"
        );
        rs.next();
        assert_equals(rs.getInt("count"), 1);

        rs = st.executeQuery("SELECT id, value FROM sharded WHERE id = 1");
        rs.next();
        assert_equals(rs.getInt("id"), 1);
        assert_equals(rs.getString("value"), "test1");

        rs = st.executeQuery("SELECT id, value FROM sharded WHERE id = 11");
        rs.next();
        assert_equals(rs.getInt("id"), 11);
        assert_equals(rs.getString("value"), "test11");

        this.connection.rollback();

        rs = st.executeQuery(
            "SELECT COUNT(*) as count FROM sharded WHERE id IN (1)"
        );
        rs.next();
        assert_equals(rs.getInt("count"), 0);

        st.execute("INSERT INTO sharded (id, value) VALUES (2, 'test2')");
        st.execute("INSERT INTO sharded (id, value) VALUES (12, 'test12')");

        rs = st.executeQuery("SELECT id, value FROM sharded WHERE id = 2");
        rs.next();
        assert_equals(rs.getInt("id"), 2);
        assert_equals(rs.getString("value"), "test2");

        st.execute("UPDATE sharded SET value = 'updated2' WHERE id = 2");
        st.execute("UPDATE sharded SET value = 'updated12' WHERE id = 12");

        rs = st.executeQuery("SELECT value FROM sharded WHERE id = 2");
        rs.next();
        assert_equals(rs.getString("value"), "updated2");

        rs = st.executeQuery("SELECT value FROM sharded WHERE id = 12");
        rs.next();
        assert_equals(rs.getString("value"), "updated12");

        this.connection.commit();

        rs = st.executeQuery(
            "SELECT COUNT(*) as count FROM sharded WHERE id IN (2)"
        );
        rs.next();
        assert_equals(rs.getInt("count"), 1);

        rs = st.executeQuery("SELECT value FROM sharded WHERE id = 2");
        rs.next();
        assert_equals(rs.getString("value"), "updated2");

        rs = st.executeQuery("SELECT value FROM sharded WHERE id = 12");
        rs.next();
        assert_equals(rs.getString("value"), "updated12");

        st.execute("DELETE FROM sharded WHERE id = 2");
        st.execute("DELETE FROM sharded WHERE id = 12");

        this.connection.commit();

        rs = st.executeQuery(
            "SELECT COUNT(*) as count FROM sharded WHERE id IN (2)"
        );
        rs.next();
        assert_equals(rs.getInt("count"), 0);

        this.connection.setAutoCommit(true);
        st.close();
    }
}

class ManualRoutingShardNumber extends TestCase {

    ManualRoutingShardNumber() throws Exception {
        super("pgdog_no_cross_shard", "pgdog_sharded");
    }

    public void before() throws Exception {
        Statement setup = this.connection.createStatement();
        setup.execute("/* pgdog_shard: 0 */ TRUNCATE TABLE sharded");
        setup.execute("/* pgdog_shard: 1 */ TRUNCATE TABLE sharded");
    }

    void run() throws Exception {
        Statement st = this.connection.createStatement();

        // Test routing to shard 0 using pgdog_shard comment with RETURNING
        ResultSet rs = st.executeQuery(
            "/* pgdog_shard: 0 */ INSERT INTO sharded (id, value) VALUES (100, 'shard0_test') RETURNING *"
        );
        int rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 100);
            assert_equals(rs.getString("value"), "shard0_test");
        }
        assert_equals(rows, 1);

        // Test routing to shard 1 using pgdog_shard comment with RETURNING
        rs = st.executeQuery(
            "/* pgdog_shard: 1 */ INSERT INTO sharded (id, value) VALUES (200, 'shard1_test') RETURNING *"
        );
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 200);
            assert_equals(rs.getString("value"), "shard1_test");
        }
        assert_equals(rows, 1);

        // Verify data was inserted into correct shards
        rs = st.executeQuery(
            "/* pgdog_shard: 0 */ SELECT COUNT(*) as count FROM sharded"
        );
        rs.next();
        assert_equals(rs.getInt("count"), 1);

        rs = st.executeQuery(
            "/* pgdog_shard: 1 */ SELECT COUNT(*) as count FROM sharded"
        );
        rs.next();
        assert_equals(rs.getInt("count"), 1);

        // Verify specific data from each shard
        rs = st.executeQuery(
            "/* pgdog_shard: 0 */ SELECT id, value FROM sharded WHERE id = 100"
        );
        rs.next();
        assert_equals(rs.getInt("id"), 100);
        assert_equals(rs.getString("value"), "shard0_test");

        rs = st.executeQuery(
            "/* pgdog_shard: 1 */ SELECT id, value FROM sharded WHERE id = 200"
        );
        rs.next();
        assert_equals(rs.getInt("id"), 200);
        assert_equals(rs.getString("value"), "shard1_test");

        // Test updates with RETURNING
        rs = st.executeQuery(
            "/* pgdog_shard: 0 */ UPDATE sharded SET value = 'updated_shard0_test' WHERE id = 100 RETURNING *"
        );
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 100);
            assert_equals(rs.getString("value"), "updated_shard0_test");
        }
        assert_equals(rows, 1);

        rs = st.executeQuery(
            "/* pgdog_shard: 1 */ UPDATE sharded SET value = 'updated_shard1_test' WHERE id = 200 RETURNING *"
        );
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 200);
            assert_equals(rs.getString("value"), "updated_shard1_test");
        }
        assert_equals(rows, 1);

        // Verify updates took effect
        rs = st.executeQuery(
            "/* pgdog_shard: 0 */ SELECT value FROM sharded WHERE id = 100"
        );
        rs.next();
        assert_equals(rs.getString("value"), "updated_shard0_test");

        rs = st.executeQuery(
            "/* pgdog_shard: 1 */ SELECT value FROM sharded WHERE id = 200"
        );
        rs.next();
        assert_equals(rs.getString("value"), "updated_shard1_test");

        // Clean up with RETURNING
        rs = st.executeQuery(
            "/* pgdog_shard: 0 */ DELETE FROM sharded WHERE id = 100 RETURNING *"
        );
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 100);
            assert_equals(rs.getString("value"), "updated_shard0_test");
        }
        assert_equals(rows, 1);

        rs = st.executeQuery(
            "/* pgdog_shard: 1 */ DELETE FROM sharded WHERE id = 200 RETURNING *"
        );
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 200);
            assert_equals(rs.getString("value"), "updated_shard1_test");
        }
        assert_equals(rows, 1);
    }
}

class ManualRoutingShardNumberPrepared extends TestCase {

    ManualRoutingShardNumberPrepared() throws Exception {
        super("pgdog_no_cross_shard", "pgdog_sharded");
    }

    public void before() throws Exception {
        Statement setup = this.connection.createStatement();
        setup.execute("/* pgdog_shard: 0 */ TRUNCATE TABLE sharded");
        setup.execute("/* pgdog_shard: 1 */ TRUNCATE TABLE sharded");
    }

    void run() throws Exception {
        // Test prepared statement with manual shard routing using pgdog_shard comment
        PreparedStatement insertStmt0 = this.connection.prepareStatement(
            "/* pgdog_shard: 0 */ INSERT INTO sharded (id, value) VALUES (?, ?) RETURNING *"
        );
        PreparedStatement insertStmt1 = this.connection.prepareStatement(
            "/* pgdog_shard: 1 */ INSERT INTO sharded (id, value) VALUES (?, ?) RETURNING *"
        );
        PreparedStatement selectStmt0 = this.connection.prepareStatement(
            "/* pgdog_shard: 0 */ SELECT id, value FROM sharded WHERE id = ?"
        );
        PreparedStatement selectStmt1 = this.connection.prepareStatement(
            "/* pgdog_shard: 1 */ SELECT id, value FROM sharded WHERE id = ?"
        );
        PreparedStatement countStmt0 = this.connection.prepareStatement(
            "/* pgdog_shard: 0 */ SELECT COUNT(*) as count FROM sharded"
        );
        PreparedStatement countStmt1 = this.connection.prepareStatement(
            "/* pgdog_shard: 1 */ SELECT COUNT(*) as count FROM sharded"
        );

        // Insert data into shard 0 using prepared statement with RETURNING
        insertStmt0.setInt(1, 300);
        insertStmt0.setString(2, "shard0_prepared");
        ResultSet rs = insertStmt0.executeQuery();
        int rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 300);
            assert_equals(rs.getString("value"), "shard0_prepared");
        }
        assert_equals(rows, 1);

        // Insert data into shard 1 using prepared statement with RETURNING
        insertStmt1.setInt(1, 400);
        insertStmt1.setString(2, "shard1_prepared");
        rs = insertStmt1.executeQuery();
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 400);
            assert_equals(rs.getString("value"), "shard1_prepared");
        }
        assert_equals(rows, 1);

        // Verify counts per shard
        rs = countStmt0.executeQuery();
        rs.next();
        assert_equals(rs.getInt("count"), 1);

        rs = countStmt1.executeQuery();
        rs.next();
        assert_equals(rs.getInt("count"), 1);

        // Verify specific data from each shard using prepared statements
        selectStmt0.setInt(1, 300);
        rs = selectStmt0.executeQuery();
        rs.next();
        assert_equals(rs.getInt("id"), 300);
        assert_equals(rs.getString("value"), "shard0_prepared");

        selectStmt1.setInt(1, 400);
        rs = selectStmt1.executeQuery();
        rs.next();
        assert_equals(rs.getInt("id"), 400);
        assert_equals(rs.getString("value"), "shard1_prepared");

        // Test update using prepared statements with manual routing and RETURNING
        PreparedStatement updateStmt0 = this.connection.prepareStatement(
            "/* pgdog_shard: 0 */ UPDATE sharded SET value = ? WHERE id = ? RETURNING *"
        );
        PreparedStatement updateStmt1 = this.connection.prepareStatement(
            "/* pgdog_shard: 1 */ UPDATE sharded SET value = ? WHERE id = ? RETURNING *"
        );

        updateStmt0.setString(1, "updated_shard0_prepared");
        updateStmt0.setInt(2, 300);
        rs = updateStmt0.executeQuery();
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 300);
            assert_equals(rs.getString("value"), "updated_shard0_prepared");
        }
        assert_equals(rows, 1);

        updateStmt1.setString(1, "updated_shard1_prepared");
        updateStmt1.setInt(2, 400);
        rs = updateStmt1.executeQuery();
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 400);
            assert_equals(rs.getString("value"), "updated_shard1_prepared");
        }
        assert_equals(rows, 1);

        // Verify updates with regular SELECT
        selectStmt0.setInt(1, 300);
        rs = selectStmt0.executeQuery();
        rs.next();
        assert_equals(rs.getString("value"), "updated_shard0_prepared");

        selectStmt1.setInt(1, 400);
        rs = selectStmt1.executeQuery();
        rs.next();
        assert_equals(rs.getString("value"), "updated_shard1_prepared");

        // Clean up using prepared statements with RETURNING
        PreparedStatement deleteStmt0 = this.connection.prepareStatement(
            "/* pgdog_shard: 0 */ DELETE FROM sharded WHERE id = ? RETURNING *"
        );
        PreparedStatement deleteStmt1 = this.connection.prepareStatement(
            "/* pgdog_shard: 1 */ DELETE FROM sharded WHERE id = ? RETURNING *"
        );

        deleteStmt0.setInt(1, 300);
        rs = deleteStmt0.executeQuery();
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 300);
            assert_equals(rs.getString("value"), "updated_shard0_prepared");
        }
        assert_equals(rows, 1);

        deleteStmt1.setInt(1, 400);
        rs = deleteStmt1.executeQuery();
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 400);
            assert_equals(rs.getString("value"), "updated_shard1_prepared");
        }
        assert_equals(rows, 1);

        // Verify cleanup
        rs = countStmt0.executeQuery();
        rs.next();
        assert_equals(rs.getInt("count"), 0);

        rs = countStmt1.executeQuery();
        rs.next();
        assert_equals(rs.getInt("count"), 0);
    }
}

class ManualRoutingShardNumberManualCommit extends TestCase {

    ManualRoutingShardNumberManualCommit() throws Exception {
        super("pgdog_no_cross_shard", "pgdog_sharded");
    }

    public void before() throws Exception {
        Statement setup = this.connection.createStatement();
        setup.execute("/* pgdog_shard: 0 */ TRUNCATE TABLE sharded");
        setup.execute("/* pgdog_shard: 1 */ TRUNCATE TABLE sharded");
    }

    void run() throws Exception {
        // Explicitly disable autocommit for manual transaction control
        this.connection.setAutoCommit(false);

        Statement st = this.connection.createStatement();

        // Test routing to shard 0 using pgdog_shard comment with RETURNING
        ResultSet rs = st.executeQuery(
            "/* pgdog_shard: 0 */ INSERT INTO sharded (id, value) VALUES (500, 'shard0_manual_commit') RETURNING *"
        );
        int rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 500);
            assert_equals(rs.getString("value"), "shard0_manual_commit");
        }
        assert_equals(rows, 1);

        // Manually commit the transaction
        this.connection.commit();

        // Test routing to shard 1 using pgdog_shard comment with RETURNING
        rs = st.executeQuery(
            "/* pgdog_shard: 1 */ INSERT INTO sharded (id, value) VALUES (600, 'shard1_manual_commit') RETURNING *"
        );
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 600);
            assert_equals(rs.getString("value"), "shard1_manual_commit");
        }
        assert_equals(rows, 1);

        // Manually commit the transaction
        this.connection.commit();

        // Verify data was inserted and committed to correct shards
        rs = st.executeQuery(
            "/* pgdog_shard: 0 */ SELECT COUNT(*) as count FROM sharded"
        );
        rs.next();
        assert_equals(rs.getInt("count"), 1);

        rs = st.executeQuery(
            "/* pgdog_shard: 1 */ SELECT COUNT(*) as count FROM sharded"
        );
        rs.next();
        assert_equals(rs.getInt("count"), 1);

        // Test updates with RETURNING and manual commit
        rs = st.executeQuery(
            "/* pgdog_shard: 0 */ UPDATE sharded SET value = 'updated_shard0_manual_commit' WHERE id = 500 RETURNING *"
        );
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 500);
            assert_equals(
                rs.getString("value"),
                "updated_shard0_manual_commit"
            );
        }
        assert_equals(rows, 1);

        // Manually commit the update
        this.connection.commit();

        rs = st.executeQuery(
            "/* pgdog_shard: 1 */ UPDATE sharded SET value = 'updated_shard1_manual_commit' WHERE id = 600 RETURNING *"
        );
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 600);
            assert_equals(
                rs.getString("value"),
                "updated_shard1_manual_commit"
            );
        }
        assert_equals(rows, 1);

        // Manually commit the update
        this.connection.commit();

        // Verify updates took effect
        rs = st.executeQuery(
            "/* pgdog_shard: 0 */ SELECT value FROM sharded WHERE id = 500"
        );
        rs.next();
        assert_equals(rs.getString("value"), "updated_shard0_manual_commit");

        rs = st.executeQuery(
            "/* pgdog_shard: 1 */ SELECT value FROM sharded WHERE id = 600"
        );
        rs.next();
        assert_equals(rs.getString("value"), "updated_shard1_manual_commit");

        // Clean up with RETURNING and manual commit
        rs = st.executeQuery(
            "/* pgdog_shard: 0 */ DELETE FROM sharded WHERE id = 500 RETURNING *"
        );
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 500);
            assert_equals(
                rs.getString("value"),
                "updated_shard0_manual_commit"
            );
        }
        assert_equals(rows, 1);

        // Manually commit the delete
        this.connection.commit();

        rs = st.executeQuery(
            "/* pgdog_shard: 1 */ DELETE FROM sharded WHERE id = 600 RETURNING *"
        );
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 600);
            assert_equals(
                rs.getString("value"),
                "updated_shard1_manual_commit"
            );
        }
        assert_equals(rows, 1);

        // Manually commit the delete
        this.connection.commit();

        // Restore autocommit mode
        this.connection.setAutoCommit(true);
    }
}

class ManualRoutingShardNumberPreparedManualCommit extends TestCase {

    ManualRoutingShardNumberPreparedManualCommit() throws Exception {
        super("pgdog_no_cross_shard", "pgdog_sharded");
    }

    public void before() throws Exception {
        Statement setup = this.connection.createStatement();
        setup.execute("/* pgdog_shard: 0 */ TRUNCATE TABLE sharded");
        setup.execute("/* pgdog_shard: 1 */ TRUNCATE TABLE sharded");
    }

    void run() throws Exception {
        // Explicitly disable autocommit for manual transaction control
        this.connection.setAutoCommit(false);

        // Test prepared statement with manual shard routing using pgdog_shard comment
        PreparedStatement insertStmt0 = this.connection.prepareStatement(
            "/* pgdog_shard: 0 */ INSERT INTO sharded (id, value) VALUES (?, ?) RETURNING *"
        );
        PreparedStatement insertStmt1 = this.connection.prepareStatement(
            "/* pgdog_shard: 1 */ INSERT INTO sharded (id, value) VALUES (?, ?) RETURNING *"
        );
        PreparedStatement selectStmt0 = this.connection.prepareStatement(
            "/* pgdog_shard: 0 */ SELECT id, value FROM sharded WHERE id = ?"
        );
        PreparedStatement selectStmt1 = this.connection.prepareStatement(
            "/* pgdog_shard: 1 */ SELECT id, value FROM sharded WHERE id = ?"
        );
        PreparedStatement countStmt0 = this.connection.prepareStatement(
            "/* pgdog_shard: 0 */ SELECT COUNT(*) as count FROM sharded"
        );
        PreparedStatement countStmt1 = this.connection.prepareStatement(
            "/* pgdog_shard: 1 */ SELECT COUNT(*) as count FROM sharded"
        );

        // Insert data into shard 0 using prepared statement with RETURNING
        insertStmt0.setInt(1, 700);
        insertStmt0.setString(2, "shard0_prepared_manual_commit");
        ResultSet rs = insertStmt0.executeQuery();
        int rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 700);
            assert_equals(
                rs.getString("value"),
                "shard0_prepared_manual_commit"
            );
        }
        assert_equals(rows, 1);

        // Manually commit the insert
        this.connection.commit();

        // Insert data into shard 1 using prepared statement with RETURNING
        insertStmt1.setInt(1, 800);
        insertStmt1.setString(2, "shard1_prepared_manual_commit");
        rs = insertStmt1.executeQuery();
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 800);
            assert_equals(
                rs.getString("value"),
                "shard1_prepared_manual_commit"
            );
        }
        assert_equals(rows, 1);

        // Manually commit the insert
        this.connection.commit();

        // Verify counts per shard (data should be committed)
        rs = countStmt0.executeQuery();
        rs.next();
        assert_equals(rs.getInt("count"), 1);

        rs = countStmt1.executeQuery();
        rs.next();
        assert_equals(rs.getInt("count"), 1);

        // Test update using prepared statements with manual routing and RETURNING
        PreparedStatement updateStmt0 = this.connection.prepareStatement(
            "/* pgdog_shard: 0 */ UPDATE sharded SET value = ? WHERE id = ? RETURNING *"
        );
        PreparedStatement updateStmt1 = this.connection.prepareStatement(
            "/* pgdog_shard: 1 */ UPDATE sharded SET value = ? WHERE id = ? RETURNING *"
        );

        updateStmt0.setString(1, "updated_shard0_prepared_manual_commit");
        updateStmt0.setInt(2, 700);
        rs = updateStmt0.executeQuery();
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 700);
            assert_equals(
                rs.getString("value"),
                "updated_shard0_prepared_manual_commit"
            );
        }
        assert_equals(rows, 1);

        // Manually commit the update
        this.connection.commit();

        updateStmt1.setString(1, "updated_shard1_prepared_manual_commit");
        updateStmt1.setInt(2, 800);
        rs = updateStmt1.executeQuery();
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 800);
            assert_equals(
                rs.getString("value"),
                "updated_shard1_prepared_manual_commit"
            );
        }
        assert_equals(rows, 1);

        // Manually commit the update
        this.connection.commit();

        // Verify updates with regular SELECT (should be committed)
        selectStmt0.setInt(1, 700);
        rs = selectStmt0.executeQuery();
        rs.next();
        assert_equals(
            rs.getString("value"),
            "updated_shard0_prepared_manual_commit"
        );

        selectStmt1.setInt(1, 800);
        rs = selectStmt1.executeQuery();
        rs.next();
        assert_equals(
            rs.getString("value"),
            "updated_shard1_prepared_manual_commit"
        );

        // Clean up using prepared statements with RETURNING
        PreparedStatement deleteStmt0 = this.connection.prepareStatement(
            "/* pgdog_shard: 0 */ DELETE FROM sharded WHERE id = ? RETURNING *"
        );
        PreparedStatement deleteStmt1 = this.connection.prepareStatement(
            "/* pgdog_shard: 1 */ DELETE FROM sharded WHERE id = ? RETURNING *"
        );

        deleteStmt0.setInt(1, 700);
        rs = deleteStmt0.executeQuery();
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 700);
            assert_equals(
                rs.getString("value"),
                "updated_shard0_prepared_manual_commit"
            );
        }
        assert_equals(rows, 1);

        // Manually commit the delete
        this.connection.commit();

        deleteStmt1.setInt(1, 800);
        rs = deleteStmt1.executeQuery();
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 800);
            assert_equals(
                rs.getString("value"),
                "updated_shard1_prepared_manual_commit"
            );
        }
        assert_equals(rows, 1);

        // Manually commit the delete
        this.connection.commit();

        // Verify cleanup (should be permanently deleted)
        rs = countStmt0.executeQuery();
        rs.next();
        assert_equals(rs.getInt("count"), 0);

        rs = countStmt1.executeQuery();
        rs.next();
        assert_equals(rs.getInt("count"), 0);

        // Restore autocommit mode
        this.connection.setAutoCommit(true);
    }
}

class ManualRoutingShardingKey extends TestCase {

    ManualRoutingShardingKey() throws Exception {
        super("pgdog_no_cross_shard", "single_sharded_list");
    }

    public void before() throws Exception {
        Statement setup = this.connection.createStatement();
        setup.execute("/* pgdog_shard: 0 */ TRUNCATE TABLE sharded");
        setup.execute("/* pgdog_shard: 1 */ TRUNCATE TABLE sharded");
    }

    void run() throws Exception {
        Statement st = this.connection.createStatement();

        // Test routing using pgdog_sharding_key comment with integer keys 0-20
        // Use sharding key 5 (matches id 5)
        ResultSet rs = st.executeQuery(
            "/* pgdog_sharding_key: 5 */ INSERT INTO sharded (id, value) VALUES (5, 'key5_test') RETURNING *"
        );
        int rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 5);
            assert_equals(rs.getString("value"), "key5_test");
        }
        assert_equals(rows, 1);

        // Use sharding key 15 (matches id 15)
        rs = st.executeQuery(
            "/* pgdog_sharding_key: 15 */ INSERT INTO sharded (id, value) VALUES (15, 'key15_test') RETURNING *"
        );
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 15);
            assert_equals(rs.getString("value"), "key15_test");
        }
        assert_equals(rows, 1);

        // Verify data using same sharding keys
        rs = st.executeQuery(
            "/* pgdog_sharding_key: 5 */ SELECT id, value FROM sharded WHERE id = 5"
        );
        rs.next();
        assert_equals(rs.getInt("id"), 5);
        assert_equals(rs.getString("value"), "key5_test");

        rs = st.executeQuery(
            "/* pgdog_sharding_key: 15 */ SELECT id, value FROM sharded WHERE id = 15"
        );
        rs.next();
        assert_equals(rs.getInt("id"), 15);
        assert_equals(rs.getString("value"), "key15_test");

        // Test updates with sharding key routing and RETURNING
        rs = st.executeQuery(
            "/* pgdog_sharding_key: 5 */ UPDATE sharded SET value = 'updated_key5' WHERE id = 5 RETURNING *"
        );
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 5);
            assert_equals(rs.getString("value"), "updated_key5");
        }
        assert_equals(rows, 1);

        rs = st.executeQuery(
            "/* pgdog_sharding_key: 15 */ UPDATE sharded SET value = 'updated_key15' WHERE id = 15 RETURNING *"
        );
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 15);
            assert_equals(rs.getString("value"), "updated_key15");
        }
        assert_equals(rows, 1);

        // Verify updates with SELECT
        rs = st.executeQuery(
            "/* pgdog_sharding_key: 5 */ SELECT value FROM sharded WHERE id = 5"
        );
        rs.next();
        assert_equals(rs.getString("value"), "updated_key5");

        rs = st.executeQuery(
            "/* pgdog_sharding_key: 15 */ SELECT value FROM sharded WHERE id = 15"
        );
        rs.next();
        assert_equals(rs.getString("value"), "updated_key15");

        // Clean up with RETURNING
        rs = st.executeQuery(
            "/* pgdog_sharding_key: 5 */ DELETE FROM sharded WHERE id = 5 RETURNING *"
        );
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 5);
            assert_equals(rs.getString("value"), "updated_key5");
        }
        assert_equals(rows, 1);

        rs = st.executeQuery(
            "/* pgdog_sharding_key: 15 */ DELETE FROM sharded WHERE id = 15 RETURNING *"
        );
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 15);
            assert_equals(rs.getString("value"), "updated_key15");
        }
        assert_equals(rows, 1);
    }
}

class ManualRoutingShardingKeyPrepared extends TestCase {

    ManualRoutingShardingKeyPrepared() throws Exception {
        super("pgdog_no_cross_shard", "single_sharded_list");
    }

    public void before() throws Exception {
        Statement setup = this.connection.createStatement();
        setup.execute("/* pgdog_shard: 0 */ TRUNCATE TABLE sharded");
        setup.execute("/* pgdog_shard: 1 */ TRUNCATE TABLE sharded");
    }

    void run() throws Exception {
        // Test prepared statement with manual sharding key routing
        PreparedStatement insertStmt5 = this.connection.prepareStatement(
            "/* pgdog_sharding_key: 5 */ INSERT INTO sharded (id, value) VALUES (?, ?) RETURNING *"
        );
        PreparedStatement insertStmt15 = this.connection.prepareStatement(
            "/* pgdog_sharding_key: 15 */ INSERT INTO sharded (id, value) VALUES (?, ?) RETURNING *"
        );
        PreparedStatement selectStmt5 = this.connection.prepareStatement(
            "/* pgdog_sharding_key: 5 */ SELECT id, value FROM sharded WHERE id = ?"
        );
        PreparedStatement selectStmt15 = this.connection.prepareStatement(
            "/* pgdog_sharding_key: 15 */ SELECT id, value FROM sharded WHERE id = ?"
        );

        // Insert data using sharding key 5 with prepared statement
        insertStmt5.setInt(1, 5);
        insertStmt5.setString(2, "key5_prepared");
        ResultSet rs = insertStmt5.executeQuery();
        int rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 5);
            assert_equals(rs.getString("value"), "key5_prepared");
        }
        assert_equals(rows, 1);

        // Insert data using sharding key 15 with prepared statement
        insertStmt15.setInt(1, 15);
        insertStmt15.setString(2, "key15_prepared");
        rs = insertStmt15.executeQuery();
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 15);
            assert_equals(rs.getString("value"), "key15_prepared");
        }
        assert_equals(rows, 1);

        // Verify data using same sharding keys with prepared statements
        selectStmt5.setInt(1, 5);
        rs = selectStmt5.executeQuery();
        rs.next();
        assert_equals(rs.getInt("id"), 5);
        assert_equals(rs.getString("value"), "key5_prepared");

        selectStmt15.setInt(1, 15);
        rs = selectStmt15.executeQuery();
        rs.next();
        assert_equals(rs.getInt("id"), 15);
        assert_equals(rs.getString("value"), "key15_prepared");

        // Test updates with prepared statements and RETURNING
        PreparedStatement updateStmt5 = this.connection.prepareStatement(
            "/* pgdog_sharding_key: 5 */ UPDATE sharded SET value = ? WHERE id = ? RETURNING *"
        );
        PreparedStatement updateStmt15 = this.connection.prepareStatement(
            "/* pgdog_sharding_key: 15 */ UPDATE sharded SET value = ? WHERE id = ? RETURNING *"
        );

        updateStmt5.setString(1, "updated_key5_prepared");
        updateStmt5.setInt(2, 5);
        rs = updateStmt5.executeQuery();
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 5);
            assert_equals(rs.getString("value"), "updated_key5_prepared");
        }
        assert_equals(rows, 1);

        updateStmt15.setString(1, "updated_key15_prepared");
        updateStmt15.setInt(2, 15);
        rs = updateStmt15.executeQuery();
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 15);
            assert_equals(rs.getString("value"), "updated_key15_prepared");
        }
        assert_equals(rows, 1);

        // Verify updates with SELECT
        selectStmt5.setInt(1, 5);
        rs = selectStmt5.executeQuery();
        rs.next();
        assert_equals(rs.getString("value"), "updated_key5_prepared");

        selectStmt15.setInt(1, 15);
        rs = selectStmt15.executeQuery();
        rs.next();
        assert_equals(rs.getString("value"), "updated_key15_prepared");

        // Clean up with prepared statements and RETURNING
        PreparedStatement deleteStmt5 = this.connection.prepareStatement(
            "/* pgdog_sharding_key: 5 */ DELETE FROM sharded WHERE id = ? RETURNING *"
        );
        PreparedStatement deleteStmt15 = this.connection.prepareStatement(
            "/* pgdog_sharding_key: 15 */ DELETE FROM sharded WHERE id = ? RETURNING *"
        );

        deleteStmt5.setInt(1, 5);
        rs = deleteStmt5.executeQuery();
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 5);
            assert_equals(rs.getString("value"), "updated_key5_prepared");
        }
        assert_equals(rows, 1);

        deleteStmt15.setInt(1, 15);
        rs = deleteStmt15.executeQuery();
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 15);
            assert_equals(rs.getString("value"), "updated_key15_prepared");
        }
        assert_equals(rows, 1);
    }
}

class ManualRoutingShardingKeyManualCommit extends TestCase {

    ManualRoutingShardingKeyManualCommit() throws Exception {
        super("pgdog_no_cross_shard", "single_sharded_list");
    }

    public void before() throws Exception {
        Statement setup = this.connection.createStatement();
        setup.execute("/* pgdog_shard: 0 */ TRUNCATE TABLE sharded");
        setup.execute("/* pgdog_shard: 1 */ TRUNCATE TABLE sharded");
    }

    void run() throws Exception {
        // Explicitly disable autocommit for manual transaction control
        this.connection.setAutoCommit(false);

        Statement st = this.connection.createStatement();

        // Test routing using pgdog_sharding_key comment with integer keys 0-20 and manual commits
        // Use sharding key 5 (matches id 5)
        ResultSet rs = st.executeQuery(
            "/* pgdog_sharding_key: 5 */ INSERT INTO sharded (id, value) VALUES (5, 'key5_manual_commit') RETURNING *"
        );
        int rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 5);
            assert_equals(rs.getString("value"), "key5_manual_commit");
        }
        assert_equals(rows, 1);

        // Manually commit the insert
        this.connection.commit();

        // Use sharding key 15 (matches id 15)
        rs = st.executeQuery(
            "/* pgdog_sharding_key: 15 */ INSERT INTO sharded (id, value) VALUES (15, 'key15_manual_commit') RETURNING *"
        );
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 15);
            assert_equals(rs.getString("value"), "key15_manual_commit");
        }
        assert_equals(rows, 1);

        // Manually commit the insert
        this.connection.commit();

        // Verify data using same sharding keys
        rs = st.executeQuery(
            "/* pgdog_sharding_key: 5 */ SELECT id, value FROM sharded WHERE id = 5"
        );
        rs.next();
        assert_equals(rs.getInt("id"), 5);
        assert_equals(rs.getString("value"), "key5_manual_commit");

        rs = st.executeQuery(
            "/* pgdog_sharding_key: 15 */ SELECT id, value FROM sharded WHERE id = 15"
        );
        rs.next();
        assert_equals(rs.getInt("id"), 15);
        assert_equals(rs.getString("value"), "key15_manual_commit");

        // Test updates with sharding key routing and RETURNING
        rs = st.executeQuery(
            "/* pgdog_sharding_key: 5 */ UPDATE sharded SET value = 'updated_key5_manual_commit' WHERE id = 5 RETURNING *"
        );
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 5);
            assert_equals(rs.getString("value"), "updated_key5_manual_commit");
        }
        assert_equals(rows, 1);

        // Manually commit the update
        this.connection.commit();

        rs = st.executeQuery(
            "/* pgdog_sharding_key: 15 */ UPDATE sharded SET value = 'updated_key15_manual_commit' WHERE id = 15 RETURNING *"
        );
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 15);
            assert_equals(rs.getString("value"), "updated_key15_manual_commit");
        }
        assert_equals(rows, 1);

        // Manually commit the update
        this.connection.commit();

        // Verify updates with SELECT
        rs = st.executeQuery(
            "/* pgdog_sharding_key: 5 */ SELECT value FROM sharded WHERE id = 5"
        );
        rs.next();
        assert_equals(rs.getString("value"), "updated_key5_manual_commit");

        rs = st.executeQuery(
            "/* pgdog_sharding_key: 15 */ SELECT value FROM sharded WHERE id = 15"
        );
        rs.next();
        assert_equals(rs.getString("value"), "updated_key15_manual_commit");

        // Clean up with RETURNING and manual commits
        rs = st.executeQuery(
            "/* pgdog_sharding_key: 5 */ DELETE FROM sharded WHERE id = 5 RETURNING *"
        );
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 5);
            assert_equals(rs.getString("value"), "updated_key5_manual_commit");
        }
        assert_equals(rows, 1);

        // Manually commit the delete
        this.connection.commit();

        rs = st.executeQuery(
            "/* pgdog_sharding_key: 15 */ DELETE FROM sharded WHERE id = 15 RETURNING *"
        );
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 15);
            assert_equals(rs.getString("value"), "updated_key15_manual_commit");
        }
        assert_equals(rows, 1);

        // Manually commit the delete
        this.connection.commit();

        // Restore autocommit mode
        this.connection.setAutoCommit(true);
    }
}

class ManualRoutingSetShardingKey extends TestCase {

    ManualRoutingSetShardingKey() throws Exception {
        super("pgdog_no_cross_shard", "single_sharded_list");
    }

    public void before() throws Exception {
        Statement setup = this.connection.createStatement();
        setup.execute("/* pgdog_shard: 0 */ TRUNCATE TABLE sharded");
        setup.execute("/* pgdog_shard: 1 */ TRUNCATE TABLE sharded");
    }

    void run() throws Exception {
        this.connection.setAutoCommit(false);

        Statement st = this.connection.createStatement();

        // Transaction 1: Test simple statement with SET pgdog.sharding_key
        st.execute("SET pgdog.sharding_key TO '5'");
        ResultSet rs = st.executeQuery(
            "INSERT INTO sharded (id, value) VALUES (5, 'set_key5_simple') RETURNING *"
        );
        int rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 5);
            assert_equals(rs.getString("value"), "set_key5_simple");
        }
        assert_equals(rows, 1);

        // Verify data with simple statement using same key
        rs = st.executeQuery("SELECT id, value FROM sharded WHERE id = 5");
        rs.next();
        assert_equals(rs.getInt("id"), 5);
        assert_equals(rs.getString("value"), "set_key5_simple");

        this.connection.commit();

        // Transaction 2: Test prepared statement with SET pgdog.sharding_key
        st.execute("SET pgdog.sharding_key TO '15'");
        PreparedStatement insertStmt = this.connection.prepareStatement(
            "INSERT INTO sharded (id, value) VALUES (?, ?) RETURNING *"
        );
        insertStmt.setInt(1, 15);
        insertStmt.setString(2, "set_key15_prepared");
        rs = insertStmt.executeQuery();
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 15);
            assert_equals(rs.getString("value"), "set_key15_prepared");
        }
        assert_equals(rows, 1);

        // Verify data with prepared statement
        PreparedStatement selectStmt = this.connection.prepareStatement(
            "SELECT id, value FROM sharded WHERE id = ?"
        );
        selectStmt.setInt(1, 15);
        rs = selectStmt.executeQuery();
        rs.next();
        assert_equals(rs.getInt("id"), 15);
        assert_equals(rs.getString("value"), "set_key15_prepared");

        this.connection.commit();

        // Transaction 3: Test update with simple statement
        st.execute("SET pgdog.sharding_key TO '5'");
        rs = st.executeQuery(
            "UPDATE sharded SET value = 'updated_set_key5_simple' WHERE id = 5 RETURNING *"
        );
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 5);
            assert_equals(rs.getString("value"), "updated_set_key5_simple");
        }
        assert_equals(rows, 1);

        this.connection.commit();

        // Transaction 4: Test update with prepared statement
        st.execute("SET pgdog.sharding_key TO '15'");
        PreparedStatement updateStmt = this.connection.prepareStatement(
            "UPDATE sharded SET value = ? WHERE id = ? RETURNING *"
        );
        updateStmt.setString(1, "updated_set_key15_prepared");
        updateStmt.setInt(2, 15);
        rs = updateStmt.executeQuery();
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 15);
            assert_equals(rs.getString("value"), "updated_set_key15_prepared");
        }
        assert_equals(rows, 1);

        this.connection.commit();

        // Transaction 5: Verify updates took effect
        st.execute("SET pgdog.sharding_key TO '5'");
        rs = st.executeQuery("SELECT value FROM sharded WHERE id = 5");
        rs.next();
        assert_equals(rs.getString("value"), "updated_set_key5_simple");

        this.connection.commit();

        // Transaction 6: Verify second update took effect
        st.execute("SET pgdog.sharding_key TO '15'");
        rs = st.executeQuery("SELECT value FROM sharded WHERE id = 15");
        rs.next();
        assert_equals(rs.getString("value"), "updated_set_key15_prepared");

        this.connection.commit();

        // Transaction 7: Clean up with simple statement
        st.execute("SET pgdog.sharding_key TO '5'");
        rs = st.executeQuery(
            "DELETE FROM sharded WHERE id = 5 RETURNING *"
        );
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 5);
            assert_equals(rs.getString("value"), "updated_set_key5_simple");
        }
        assert_equals(rows, 1);

        this.connection.commit();

        // Transaction 8: Clean up with prepared statement
        st.execute("SET pgdog.sharding_key TO '15'");
        PreparedStatement deleteStmt = this.connection.prepareStatement(
            "DELETE FROM sharded WHERE id = ? RETURNING *"
        );
        deleteStmt.setInt(1, 15);
        rs = deleteStmt.executeQuery();
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 15);
            assert_equals(rs.getString("value"), "updated_set_key15_prepared");
        }
        assert_equals(rows, 1);

        this.connection.commit();

        // Transaction 9: Test queries without sharding key in WHERE clause - using SET for routing
        st.execute("SET pgdog.sharding_key TO '5'");

        // Insert data on shard for key 5
        st.execute("INSERT INTO sharded (id, value) VALUES (25, 'no_key_test_shard5')");

        // Query without sharding key - should only find data on the shard we're routed to
        rs = st.executeQuery("SELECT id, value FROM sharded WHERE value LIKE '%no_key_test%'");
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 25);
            assert_equals(rs.getString("value"), "no_key_test_shard5");
        }
        assert_equals(rows, 1);

        this.connection.commit();

        // Transaction 10: Insert on different shard and verify isolation
        st.execute("SET pgdog.sharding_key TO '15'");

        // Insert different data on shard for key 15
        st.execute("INSERT INTO sharded (id, value) VALUES (35, 'no_key_test_shard15')");

        // Query without sharding key - should only find data on current shard
        rs = st.executeQuery("SELECT id, value FROM sharded WHERE value LIKE '%no_key_test%'");
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 35);
            assert_equals(rs.getString("value"), "no_key_test_shard15");
        }
        assert_equals(rows, 1);

        this.connection.commit();

        // Transaction 11: Test prepared statement without sharding key in WHERE
        st.execute("SET pgdog.sharding_key TO '5'");

        PreparedStatement countByValueStmt = this.connection.prepareStatement(
            "SELECT COUNT(*) as count FROM sharded WHERE value LIKE ?"
        );
        countByValueStmt.setString(1, "%no_key_test%");
        rs = countByValueStmt.executeQuery();
        rs.next();
        assert_equals(rs.getInt("count"), 1); // Should only see data from shard 5

        PreparedStatement selectByValueStmt = this.connection.prepareStatement(
            "SELECT id, value FROM sharded WHERE value = ?"
        );
        selectByValueStmt.setString(1, "no_key_test_shard5");
        rs = selectByValueStmt.executeQuery();
        rs.next();
        assert_equals(rs.getInt("id"), 25);
        assert_equals(rs.getString("value"), "no_key_test_shard5");

        this.connection.commit();

        // Transaction 12: Switch shard and verify different data with prepared statement
        st.execute("SET pgdog.sharding_key TO '15'");

        countByValueStmt.setString(1, "%no_key_test%");
        rs = countByValueStmt.executeQuery();
        rs.next();
        assert_equals(rs.getInt("count"), 1); // Should only see data from shard 15

        selectByValueStmt.setString(1, "no_key_test_shard15");
        rs = selectByValueStmt.executeQuery();
        rs.next();
        assert_equals(rs.getInt("id"), 35);
        assert_equals(rs.getString("value"), "no_key_test_shard15");

        this.connection.commit();

        // Transaction 13: Clean up test data from shard 5
        st.execute("SET pgdog.sharding_key TO '5'");
        rs = st.executeQuery(
            "DELETE FROM sharded WHERE value = 'no_key_test_shard5' RETURNING *"
        );
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 25);
            assert_equals(rs.getString("value"), "no_key_test_shard5");
        }
        assert_equals(rows, 1);

        this.connection.commit();

        // Transaction 14: Clean up test data from shard 15
        st.execute("SET pgdog.sharding_key TO '15'");
        PreparedStatement deleteByValueStmt = this.connection.prepareStatement(
            "DELETE FROM sharded WHERE value = ? RETURNING *"
        );
        deleteByValueStmt.setString(1, "no_key_test_shard15");
        rs = deleteByValueStmt.executeQuery();
        rows = 0;
        while (rs.next()) {
            rows++;
            assert_equals(rs.getInt("id"), 35);
            assert_equals(rs.getString("value"), "no_key_test_shard15");
        }
        assert_equals(rows, 1);

        this.connection.commit();
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
        new TransactionDirectShard().execute();
        new ManualRoutingShardNumber().execute();
        new ManualRoutingShardNumberPrepared().execute();
        new ManualRoutingShardNumberManualCommit().execute();
        new ManualRoutingShardNumberPreparedManualCommit().execute();
        new ManualRoutingShardingKey().execute();
        new ManualRoutingShardingKeyPrepared().execute();
        new ManualRoutingShardingKeyManualCommit().execute();
        new ManualRoutingSetShardingKey().execute();
    }
}
