package testsuite.simple;

import java.sql.*;

import testsuite.BaseTestCase;

public class StatementsTest extends BaseTestCase
{
    
    public StatementsTest(String name)
    {
    	super(name);
    }
   
    public void setUp() throws Exception
    {
    	super.setUp();
    	
            try {
                stmt.executeUpdate("DROP TABLE statement_test");
            } catch (SQLException sqlEx) { /* ignore */
            }

            stmt.executeUpdate("CREATE TABLE statement_test (id int not null primary key auto_increment, strdata1 varchar(255) not null, strdata2 varchar(255))");
    }
    
    public void tearDown() throws Exception
    {
    	stmt.executeUpdate("DROP TABLE statement_test");
    	
    	super.tearDown();
    }
    
    public void testInsert() throws SQLException
    {
            for (int i = 0; i < 10; i++) {
                int updateCount = stmt.executeUpdate("INSERT INTO statement_test (strdata1,strdata2) values ('abcdefg', 'poi')");
                assertTrue("Update count must be '1', was '" + updateCount + "'", (updateCount == 1));
            }
    }
    
    public void testPreparedStatement() throws SQLException
    {
			stmt.executeUpdate("INSERT INTO statement_test (id, strdata1,strdata2) values (1,'abcdefg', 'poi')");
            pstmt = conn.prepareStatement("UPDATE statement_test SET strdata1=?, strdata2=? where id=?");
            pstmt.setString(1, "iop");
            pstmt.setString(2, "higjklmn");
            pstmt.setInt(3, 1);
            int updateCount = pstmt.executeUpdate();
            
            assertTrue("Update count must be '1', was '" + updateCount + "'", (updateCount == 1));
    }
    
    public void testPreparedStatementBatch() throws SQLException
    {
       

            pstmt = conn.prepareStatement(
                                                   "INSERT INTO " + 
                                                   "statement_test (strdata1, strdata2) VALUES (?,?)");

            for (int i = 0; i < 10; i++) {
                pstmt.setString(1, "batch_" + i);
                pstmt.setString(2, "batch_" + i);
                pstmt.addBatch();
            }

            int[] updateCounts = pstmt.executeBatch();
            
            for (int i = 0; i < updateCounts.length; i++) {
                assertTrue("Update count must be '1', was '" + updateCounts[i] + "'", (updateCounts[i] == 1));
            }

    }
}