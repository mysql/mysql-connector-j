/*
 * Statements.java
 *
 * Created on September 11, 2001, 7:41 AM
 */

package testsuite;

import java.sql.*;

/**
 *
 * @author  Administrator
 */
public class Statements {
 
    static String DBUrl = "jdbc:mysql:///test";
    
    /** Creates new Statements */
    private Statements() 
    {
    }

    public static void main(String[] args) throws Exception
    {
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pStmt = null;

        try 
        {
            Class.forName("org.gjt.mm.mysql.Driver").newInstance();

			long begin = System.currentTimeMillis();
			
            conn = DriverManager.getConnection(DBUrl);
            
            long end = System.currentTimeMillis();
            
            System.out.println(end - begin);

            stmt = conn.createStatement();

            try {
                stmt.executeUpdate("DROP TABLE statement_test");
            }
            catch (SQLException sqlEx) { /* ignore */}

            stmt.executeUpdate("CREATE TABLE statement_test (id int not null primary key auto_increment, strdata1 varchar(255), strdata2 varchar(255))");
            
            for (int i = 0; i < 10; i++)
            {
            	stmt.executeUpdate("INSERT INTO statement_test (strdata1,strdata2) values ('abcdefg', 'poi')");
            }
            
            pStmt = conn.prepareStatement("UPDATE statement_test SET strdata1=?, strdata2=? where id=?");
            
            
            pStmt.setString(1, "iop");
            pStmt.setString(2, "higjklmn");
            pStmt.setInt(3, 1);
            pStmt.executeUpdate();
	    
	    	PreparedStatement pStmtBad = conn.prepareStatement("SELECT * from statement_test");
	    	
	    	pStmtBad.executeQuery().close();
	    	pStmtBad.close();
	    	
	    PreparedStatement pStmtBatch = 
	    	conn.prepareStatement("INSERT INTO "
	    	+ "statement_test (strdata1, strdata2) VALUES (?,?)");
		
	    for (int i = 0; i < 10; i++) {
	    	
	    	pStmtBatch.setString(1, "batch_" + i);
	    	pStmtBatch.setString(2, "batch_" + i);
		pStmtBatch.addBatch();
	    }
	    
	    int [] updateCounts = pStmtBatch.executeBatch();
	    
	    System.out.println("Batch update counts: ");
	    
	    for (int i = 0; i < updateCounts.length; i++) {
		    System.out.println("  " + updateCounts[i]);
            }
            
            ResultSet rs = stmt.executeQuery("SELECT * from statement_test");
            
            while (rs.next())
            {
            	System.out.println("isAfterLast() = " + rs.isAfterLast());
            }
            
            System.out.println("isAfterLast() ends at " + rs.isAfterLast());
 
        }
        finally 
        {
            if (pStmt != null)
            {
                try
                {
                    pStmt.close();
                }
                catch (SQLException sqlEx) { /* ignore */ }
            }
            
            if (stmt != null) 
            {
                try 
                {
                    stmt.close();
                }
                catch (SQLException sqlEx) { /* ignore */ }
            }
      
            if (conn != null) 
            {
                try 
                {
                    conn.close();
                }
                catch (SQLException sqlEx) { /* ignore */ }
            }
        }
    }     
}
