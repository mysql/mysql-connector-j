/*
   
 * Statements.java
   
 *
   
 * Created on September 11, 2001, 7:41 AM
   
 */
package testsuite;

import java.sql.*;


/**
 * @author Administrator
 */
public class Numbers
{
    //~ Instance/static variables .............................................

    static String DBUrl = "jdbc:mysql:///test";

    //~ Constructors ..........................................................

    /**
     * Creates new Statements
     */
    private Numbers()
    {
    }
    //~ Methods ...............................................................

    /**
     * DOCUMENT ME!
     * 
     * @param args DOCUMENT ME!
     * @throws Exception DOCUMENT ME!
     */
    public static void main(String[] args)
                     throws Exception
    {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;

        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();

            long begin = System.currentTimeMillis();
            conn = DriverManager.getConnection(DBUrl);

            long end = System.currentTimeMillis();
            System.out.println(end - begin);
            stmt = conn.createStatement();

            try {
                stmt.executeUpdate("DROP TABLE number_test");
            } catch (SQLException sqlEx) { /* ignore */
            }

            stmt.executeUpdate("CREATE TABLE number_test (minBigInt bigint, maxBigInt bigint, testBigInt bigint)");

            
            stmt.executeUpdate("INSERT INTO number_test (minBigInt,maxBigInt,testBigInt) values (" + Long.MIN_VALUE + "," + Long.MAX_VALUE + ",6147483647)");
            

            rs = stmt.executeQuery("SELECT * from number_test");

			
            while (rs.next()) {
            	long minBigInt = rs.getLong(1);
            	long maxBigInt = rs.getLong(2);
            	long testBigInt = rs.getLong(3);
            	
            	System.out.println("minBigInt (" + minBigInt + ") == Long.MIN_VALUE (" + Long.MIN_VALUE + "): " + (minBigInt == Long.MIN_VALUE));
            	System.out.println("maxBigInt (" + maxBigInt + ") == Long.MAX_VALUE (" + Long.MAX_VALUE + "): " + (maxBigInt == Long.MAX_VALUE));
            	System.out.println("testBigInt = " + testBigInt);
            	
            }

            
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlEx) { /* ignore */
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException sqlEx) { /* ignore */
                }
            }
        }
    }
}