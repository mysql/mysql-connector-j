package testsuite.simple;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import testsuite.BaseTestCase;
import testsuite.regression.ConnectionRegressionTest;

public class ReadOnlyCallableStatementTest extends BaseTestCase {
	public ReadOnlyCallableStatementTest(String name) {
		super(name);
	}

	public void testReadOnlyWithProcBodyAccess() throws Exception {
		if (versionMeetsMinimum(5, 0)) {
			Connection replConn = null;
			Properties props = getMasterSlaveProps();
			props.setProperty("autoReconnect", "true");
	
			
			try {
				createProcedure("testProc1", "()\n"
								+ "READS SQL DATA\n"
								+ "begin\n"
								+ "SELECT NOW();\n"
								+ "end\n");

				createProcedure("`testProc.1`", "()\n"
						+ "READS SQL DATA\n"
						+ "begin\n"
						+ "SELECT NOW();\n"
						+ "end\n");
				
				replConn = getMasterSlaveReplicationConnection();
				replConn.setReadOnly(true);
				
				CallableStatement stmt = replConn.prepareCall("CALL testProc1()");
				stmt.execute();
				stmt.execute();
				
				stmt = replConn.prepareCall("CALL `" + replConn.getCatalog() + "`.testProc1()");
				stmt.execute();
				
				stmt = replConn.prepareCall("CALL `" + replConn.getCatalog() + "`.`testProc.1`()");
				stmt.execute();
				
			} finally {
			
				closeMemberJDBCResources();
				
				if (replConn != null) {
					replConn.close();
				}
			}
		}
	}
	
	public void testNotReadOnlyWithProcBodyAccess() throws Exception {
		if (versionMeetsMinimum(5, 0)) {
			
			Connection replConn = null;
			Properties props = getMasterSlaveProps();
			props.setProperty("autoReconnect", "true");
		
			
			try {
				createProcedure("testProc2", "()\n"
								+ "MODIFIES SQL DATA\n"
								+ "begin\n"
								+ "SELECT NOW();\n"
								+ "end\n");

				createProcedure("`testProc.2`", "()\n"
						+ "MODIFIES SQL DATA\n"
						+ "begin\n"
						+ "SELECT NOW();\n"
						+ "end\n");
				
				replConn = getMasterSlaveReplicationConnection();
				replConn.setReadOnly(true);
				
				CallableStatement stmt = replConn.prepareCall("CALL testProc2()");

				try{
					stmt.execute();
					fail("Should not execute because procedure modifies data.");
				} catch (SQLException e) {
					assertEquals("Should error for read-only connection.", e.getSQLState(), "S1009");
				}

				stmt = replConn.prepareCall("CALL `" + replConn.getCatalog() + "`.testProc2()");

				try{
					stmt.execute();
					fail("Should not execute because procedure modifies data.");
				} catch (SQLException e) {
					assertEquals("Should error for read-only connection.", e.getSQLState(), "S1009");
				}

				stmt = replConn.prepareCall("CALL `" + replConn.getCatalog() + "`.`testProc.2`()");

				try{
					stmt.execute();
					fail("Should not execute because procedure modifies data.");
				} catch (SQLException e) {
					assertEquals("Should error for read-only connection.", e.getSQLState(), "S1009");
				}

				
			} finally {
			
				closeMemberJDBCResources();
				
				if (replConn != null) {
					replConn.close();
				}
			}
		}
	}
	


}
