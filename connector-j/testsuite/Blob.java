package testsuite;

/*
 * Blob.java
 *
 * Created on July 4, 2000, 8:49 AM
 */
 
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.DriverManager;


/** 
 *
 * @author  Administrator
 * @version 
 */
public class Blob extends Object {

   Connection Conn = null;
    ResultSet RS = null;
    Statement Stmt = null;

    static String DBUrl = "jdbc:mysql:///test";

    public static void main(String[] Args) throws Exception
    {
	if (Args.length > 0) {
		DBUrl = Args[0];
	}

	Blob  B = new Blob();

    }

    public Blob() throws Exception
    {
	try {
	    Class.forName("org.gjt.mm.mysql.Driver").newInstance();
	    
	    Conn = DriverManager.getConnection(DBUrl);

	    Stmt = Conn.createStatement();

	    System.out.print("Create test data: ");
	    boolean create_ok = createTestData();
	    System.out.println(create_ok ? "passed" : "failed");
	}
	catch (SQLException E) {
	    throw E;
	}
	finally {
	    if (RS != null) {
		try {
		    RS.close();
		}
		catch (SQLException SQLE) {}
	    }
	    
	    if (Stmt != null) {
		try {
		    Stmt.close();
		}
		catch (SQLException SQLE) {}
	    }

	    if (Conn != null) {
		try {
		    Conn.close();
		}
		catch (SQLException SQLE) {}
	    }
	}
    }

    private boolean createTestData() throws java.sql.SQLException
    {
	try {
	    
	    //
	    // Catch the error, the table might exist
	    //
	    
	    try {
		Stmt.executeUpdate("DROP TABLE BLOBTEST");
	    }
	    catch (SQLException SQLE) {}
	    
	    Stmt.executeUpdate("CREATE TABLE BLOBTEST (pos int PRIMARY KEY auto_increment, blobdata LONGBLOB)");
	    
            byte[] testBlob = new byte[1024 * 1024 * 12]; // 128k blob
            
            int dataRange = Byte.MAX_VALUE - Byte.MIN_VALUE;
            
	    for (int i = 0; i < testBlob.length; i++) {
		testBlob[i] = (byte)((Math.random() * dataRange) + Byte.MIN_VALUE);
              }
              
              PreparedStatement pstmt = Conn.prepareStatement("INSERT INTO BLOBTEST(blobdata) VALUES (?)");
              
              pstmt.setBytes(1, testBlob);
              
              pstmt.execute();

	      int rowsUpdated = pstmt.getUpdateCount();
              
              System.out.println("Updated " + rowsUpdated + " row(s) with byte[] data");

	      pstmt.clearParameters();

	      java.io.ByteArrayInputStream bIn = new java.io.ByteArrayInputStream(testBlob);

              pstmt.setBinaryStream(1, bIn, 0);
              
              pstmt.execute();
              
	      rowsUpdated = pstmt.getUpdateCount();
              System.out.println("Updated " + rowsUpdated + " row(s) with binary stream data");

	      pstmt.clearParameters();

	      System.out.println("Testing parameter check...");

	      boolean passed = false;

	      try 
	      {
		   pstmt.execute();
	      }
	     catch (SQLException sqlEx) {
		if (sqlEx.getMessage().equals("No value specified for parameter 1")) {
			passed = true;
		}
		else {
			sqlEx.printStackTrace();
		}
	     }

	     System.out.println((passed ? "Passed" : "Failed"));
                
                
	}
	catch (SQLException E) {
	    E.printStackTrace();
	    return false;
	}

	return true;
    }
  
}
