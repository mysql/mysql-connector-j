/**
 * Tests the ResultSet traversal methods
 */
package testsuite;

import java.sql.*;


public class Traversal
{
    //~ Instance/static variables .............................................

    Connection Conn = null;
    ResultSet RS = null;
    Statement Stmt = null;
    static String DBUrl = "jdbc:mysql:///test";

    //~ Constructors ..........................................................

    /**
     * Creates a new Traversal object.
     * 
     * @throws Exception DOCUMENT ME!
     */
    public Traversal()
              throws Exception
    {
        try {
            Class.forName("org.gjt.mm.mysql.Driver").newInstance();
            Conn = DriverManager.getConnection(DBUrl);
            Stmt = Conn.createStatement();
            System.out.print("Create test data: ");

            boolean create_ok = createTestData();
            System.out.println(create_ok ? "passed" : "failed");
            System.out.println("Selecting Rows");
            RS = Stmt.executeQuery("SELECT * FROM TRAVERSAL ORDER BY pos");

            int count = 0;
            System.out.println("Positioning before start of result set");
            RS.beforeFirst();
            System.out.print("Traversing forward: ");

            boolean forward_ok = true;

            while (RS.next()) {
                int pos = RS.getInt("POS");

                // test case-sensitive column names
                pos = RS.getInt("pos");
                pos = RS.getInt("Pos");
                pos = RS.getInt("POs");
                pos = RS.getInt("PoS");
                pos = RS.getInt("pOS");
                pos = RS.getInt("pOs");
                pos = RS.getInt("poS");

                if (pos == count) {

                    //System.out.print("+");
                } else {

                    //System.out.print("-");
                    forward_ok = false;
                }

                count++;
            }
            //System.out.println();
            if (forward_ok) {
                System.out.println("OK");
            } else {
                System.out.println("FAILED! Only traversed " + count + 
                                   "/100 rows");
            }

            boolean isAfterLast = RS.isAfterLast();
            System.out.println("Checking ResultSet.isAfterLast(): " + 
                               (isAfterLast ? "OK" : "FAILED!"));
            System.out.print("Positioning after end of result set: ");

            try {
                RS.afterLast();
                System.out.println("OK");
            } catch (SQLException E) {
                System.out.println("FAILED! (" + E.toString() + ")");
            }

            System.out.print("Scrolling backwards: ");
            count = 99;

            boolean reverse_ok = true;

            while (RS.previous()) {
                int pos = RS.getInt("pos");

                if (pos == count) {

                    //System.out.print("+");
                } else {

                    //System.out.print("-");
                    reverse_ok = false;
                }

                count--;
            }
            if (reverse_ok) {
                System.out.println("OK");
            } else {
                System.out.println("FAILED!");
            }

            boolean is_first = RS.isFirst();
            System.out.println("Checking ResultSet.isFirst(): " + 
                               (is_first ? "OK" : "FAILED!"));
            System.out.println("Absolute positioning");
            RS.absolute(50);

            int pos = RS.getInt("pos");
            System.out.println(pos);

            boolean on_result_set = RS.absolute(200);
            System.out.println(on_result_set);
            System.out.println(RS.isAfterLast());
            RS.absolute(100);
            System.out.println(RS.getInt("pos"));
            System.out.println(RS.isLast());
            RS.absolute(-99);
            System.out.println(RS.getInt("pos"));
        } catch (SQLException E) {
            throw E;
        } finally {
            if (RS != null) {
                try {
                    RS.close();
                } catch (SQLException SQLE) {
                }
            }
            if (Stmt != null) {
                try {
                    Stmt.close();
                } catch (SQLException SQLE) {
                }
            }
            if (Conn != null) {
                try {
                    Conn.close();
                } catch (SQLException SQLE) {
                }
            }
        }
    }
    //~ Methods ...............................................................

    /**
     * DOCUMENT ME!
     * 
     * @param Args DOCUMENT ME!
     * @throws Exception DOCUMENT ME!
     */
    public static void main(String[] Args)
                     throws Exception
    {
        Traversal T = new Traversal();
    }

    private boolean createTestData()
                            throws java.sql.SQLException
    {
        try {

            //
            // Catch the error, the table might exist
            //
            try {
                Stmt.executeUpdate("DROP TABLE TRAVERSAL");
            } catch (SQLException SQLE) {
            }

            Stmt.executeUpdate("CREATE TABLE TRAVERSAL (pos int PRIMARY KEY, stringdata char(32))");

            for (int i = 0; i < 100; i++) {
                Stmt.executeUpdate("INSERT INTO TRAVERSAL VALUES (" + i + 
                                   ", 'StringData')");
            }
        } catch (SQLException E) {
            E.printStackTrace();

            return false;
        }

        return true;
    }
}
;