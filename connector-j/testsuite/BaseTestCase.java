package testsuite;

/**
 * @author Administrator
 *
 * To change this generated comment edit the template variable "typecomment":
 * Window>Preferences>Java>Templates.
 * To enable and disable the creation of type comments go to
 * Window>Preferences>Java>Code Generation.
 */

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.DriverManager;

import junit.framework.TestCase;

public abstract class BaseTestCase extends TestCase
{
	
	protected Connection conn = null;
	protected ResultSet rs = null;
	protected Statement stmt = null;
	protected PreparedStatement pstmt = null;

	protected static String dbUrl = "jdbc:mysql:///test";

	public BaseTestCase(String name)
	{
		super(name);
		
		String newDbUrl = System.getProperty("com.mysql.jdbc.testsuite.url");
		
		if (newDbUrl != null && newDbUrl.trim().length() != 0)
		{
			dbUrl = newDbUrl;
		}
	}
	
	public void setUp() throws Exception {
		
			Class.forName("com.mysql.jdbc.Driver").newInstance();

			conn = DriverManager.getConnection(dbUrl);

			stmt = conn.createStatement();	
	}

	public void tearDown() throws Exception {
		
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException SQLE) {
			}
		}

		if (stmt != null) {
			try {
				stmt.close();
			} catch (SQLException SQLE) {
			}
		}

		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException SQLE) {
			}
		}
	}


}
