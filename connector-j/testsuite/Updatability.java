/**
 * Tests the ResultSet updatability methods
 */

package testsuite;

import java.sql.*;

public class Updatability
{
    static Connection Conn = null;
    static ResultSet RS = null;
    static Statement Stmt = null;

    static String DBUrl = "jdbc:mysql:///test";

    public static void main(String[] Args) throws Exception
    {
	try {
	    Class.forName("org.gjt.mm.mysql.Driver").newInstance();
	    
	    Conn = DriverManager.getConnection(DBUrl);

	    Stmt = Conn.createStatement();

	    System.out.print("Create test data: ");
	    boolean create_ok = createTestData();
	    System.out.println(create_ok ? "passed" : "failed");
   	    
	    System.out.println("Selecting result set");
	    
	    RS = Stmt.executeQuery("SELECT * FROM UPDATABLE ORDER BY pos1");
	    
	    System.out.println("Modifying result set in-place");

		int numCols = RS.getMetaData().getColumnCount();
		
	    while (RS.next()) {
	    	System.out.print("Before modification: ");
	    	for (int i = 0; i < numCols; i++)
	    	{
	    		System.out.print(RS.getString(i + 1)  + ",");
	    	}
	    	System.out.println();
	    		
		int row_pos = RS.getInt(1);
		RS.updateString(3, "New Data" + (100 - row_pos));
		RS.updateRow();
		
		System.out.print("After modification: ");
	    	for (int i = 0; i < numCols; i++)
	    	{
	    		System.out.print(RS.getString(i + 1) + ",");
	    	}
	    	System.out.println("\n");
		
	    }

	    //
	    // Insert a new row
	    //

	    System.out.println("Inserting new row");

	    RS.moveToInsertRow();
	    RS.updateInt(1, 400);
            RS.updateInt(2, 400);
	    RS.updateString(3, "New Data" + (100-400));
	    RS.insertRow();

	    

	    RS.close();
	    Stmt.close();

	    //
	    // Look for the updated row
	    //

	    Stmt = Conn.createStatement();
	    RS = Stmt.executeQuery("SELECT * FROM UPDATABLE ORDER BY pos1");
	    
	    boolean data_good = true;

	    System.out.print("Checking for updates in database: ");

	    while (RS.next()) {
		int row_pos = RS.getInt(1);
		 
		if (!RS.getString(3).equals("New Data" + (100 - row_pos))) {
		   data_good = false;
		}
	    }

	    RS.close();

	    if (data_good) {
		System.out.println("passed.");
	    }
	    else {
		System.out.println("Failed.");
	    }
	    
	    Stmt.close();
	    Stmt = null;
            
            System.out.println("Checking nullability of result sets...");
            
            Statement stmt = Conn.createStatement (ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE) ;

      
            stmt.executeUpdate ("DROP TABLE IF EXISTS test") ;
            stmt.executeUpdate ("CREATE TABLE test (ident INTEGER PRIMARY KEY, name TINYTEXT, expiry DATETIME default null)" ) ;
            stmt.executeUpdate ("INSERT INTO test SET ident=1, name='original'") ;

            //Select to get a resultset to work on
            ResultSet rs = stmt.executeQuery ("SELECT ident, name, expiry FROM test") ;

            //Check that the expiry field was null before we did our update
            rs.first () ;
            java.sql.Date before = rs.getDate ("expiry") ;
            
            if (rs.wasNull () )
            {
                System.out.println ("Expiry was correctly SQL null before update") ;
            }


            //Update a different field
            rs.updateString ("name", "Updated") ;
            rs.updateRow () ;

            //Test to see if field has been altered
            java.sql.Date after = rs.getDate (3) ;
            if (rs.wasNull () )
                System.out.println ("Bug disproved - expiry SQL null after update") ;
            else
                System.out.println ("Bug proved - expiry corrupted to '"+after+"'") ;

            rs.close();
            stmt.close();


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

    private static boolean createTestData() throws java.sql.SQLException
    {
	try {
	    
	    //
	    // Catch the error, the table might exist
	    //
	    
	    try {
		Stmt.executeUpdate("DROP TABLE UPDATABLE");
	    }
	    catch (SQLException SQLE) {}
	    
	    Stmt.executeUpdate("CREATE TABLE UPDATABLE (pos1 int not null, pos2 int not null, char_field VARCHAR(32), PRIMARY KEY (pos1, pos2))");
	    
	    for (int i = 0; i < 100; i++) {
		Stmt.executeUpdate("INSERT INTO UPDATABLE VALUES (" + i + ", " + i + ",'StringData" + i + "')");
	    }
	}
	catch (SQLException E) {
	    E.printStackTrace();
	    return false;
	}

	return true;
    }
};
