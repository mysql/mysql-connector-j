package testsuite.regression;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import testsuite.BaseTestCase;


/**
 * Regression test cases for the ResultSet class.
 * 
 * @author Mark Matthews
 */
public class ResultSetRegressionTest
    extends BaseTestCase {
    public ResultSetRegressionTest(String name) {
        super(name);
    }

    public void testUpdatability()
                          throws Exception {

        PreparedStatement pstmt = null;
        ResultSet rs = null;
        int retCount = 0;
        stmt.execute("DROP TABLE IF EXISTS updatabilityBug");
        stmt.execute(
                "CREATE TABLE IF NOT EXISTS updatabilityBug ("
                + " id int(10) unsigned NOT NULL auto_increment,"
                + " field1 varchar(32) NOT NULL default '',"
                + " field2 varchar(128) NOT NULL default '',"
                + " field3 varchar(128) default NULL,"
                + " field4 varchar(128) default NULL,"
                + " field5 varchar(64) default NULL,"
                + " field6 int(10) unsigned default NULL,"
                + " field7 varchar(64) default NULL," + " PRIMARY KEY  (id)"
                + ") TYPE=InnoDB;");
        stmt.executeUpdate("insert into updatabilityBug (id) values (1)");

        try {

            String sQuery = " SELECT * FROM updatabilityBug WHERE id = ? ";
            pstmt = conn.prepareStatement(sQuery, 
                                          ResultSet.TYPE_SCROLL_SENSITIVE, 
                                          ResultSet.CONCUR_UPDATABLE);
            conn.setAutoCommit(false);
            pstmt.setInt(1, 1);
            rs = pstmt.executeQuery();

            if (rs.next()) {
                rs.absolute(1);
                rs.updateInt("id", 1);
                rs.updateString("field1", "1");
                rs.updateString("field2", "1");
                rs.updateString("field3", "1");
                rs.updateString("field4", "1");
                rs.updateString("field5", "1");
                rs.updateInt("field6", 1);
                rs.updateString("field7", "1");
                rs.updateRow();
            }

            conn.commit();
            conn.setAutoCommit(true);
        } finally {

            if (pstmt != null) {

                try {
                    pstmt.close();
                } catch (Exception e) {
                }
            }

            stmt.execute("DROP TABLE IF EXISTS updatabilityBug");
        }
    }

    public void testStreamingRegBug()
                             throws Exception {

        try {
            stmt.executeUpdate("DROP TABLE IF EXISTS StreamingRegBug");
            stmt.executeUpdate(
                    "CREATE TABLE StreamingRegBug ( DUMMYID "
                    + " INTEGER NOT NULL, DUMMYNAME VARCHAR(32),PRIMARY KEY (DUMMYID) )");
            stmt.executeUpdate(
                    "INSERT INTO StreamingRegBug (DUMMYID, DUMMYNAME) VALUES (0, NULL)");
            stmt.executeUpdate(
                    "INSERT INTO StreamingRegBug (DUMMYID, DUMMYNAME) VALUES (1, 'nro 1')");
            stmt.executeUpdate(
                    "INSERT INTO StreamingRegBug (DUMMYID, DUMMYNAME) VALUES (2, 'nro 2')");
            stmt.executeUpdate(
                    "INSERT INTO StreamingRegBug (DUMMYID, DUMMYNAME) VALUES (3, 'nro 3')");

            Statement streamStmt = null;

            try {
                streamStmt = conn.createStatement(
                                     java.sql.ResultSet.TYPE_FORWARD_ONLY, 
                                     java.sql.ResultSet.CONCUR_READ_ONLY);
                streamStmt.setFetchSize(Integer.MIN_VALUE);

                ResultSet rs = streamStmt.executeQuery(
                                       "SELECT DUMMYID, DUMMYNAME "
                                       + "FROM StreamingRegBug ORDER BY DUMMYID");

                while (rs.next()) {
                }

                rs.close(); // error occurs here
            } finally {

                if (streamStmt != null) {
                    streamStmt.close();
                }
            }
        } finally {
            stmt.executeUpdate("DROP TABLE IF EXISTS StreamingRegBug");
        }
    }

    public void testGetLongBug()
                        throws Exception {
        stmt.executeUpdate("DROP TABLE IF EXISTS getLongBug");
        stmt.executeUpdate(
                "CREATE TABLE IF NOT EXISTS getLongBug (int_col int, bigint_col bigint)");

        int intVal = 123456;
        long longVal1 = 123456789012345678L;
        long longVal2 = -2079305757640172711L;
        stmt.executeUpdate(
                "INSERT INTO getLongBug " + "(int_col, bigint_col) "
                + "VALUES (" + intVal + ", " + longVal1 + "), " + "(" + intVal
                + ", " + longVal2 + ")");

        try {
            rs = stmt.executeQuery(
                         "SELECT int_col, bigint_col FROM getLongBug ORDER BY bigint_col DESC");
            rs.next();
            assertTrue("Values not decoded correctly", 
                       (rs.getInt(1) == intVal && rs.getLong(2) == longVal1));
            rs.next();
            assertTrue("Values not decoded correctly", 
                       (rs.getInt(1) == intVal && rs.getLong(2) == longVal2));
        } finally {

            if (rs != null) {

                try {
                    rs.close();
                } catch (Exception ex) {
                }
            }

            stmt.executeUpdate("DROP TABLE IF EXISTS getLongBug");
        }
    }
}