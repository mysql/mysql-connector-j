/**
 * JDBC4 connection regression tests
 */
package testsuite.regression.jdbc4;

import java.sql.PreparedStatement;
import java.util.Properties;

import com.mysql.jdbc.MySQLConnection;
import com.mysql.jdbc.NonRegisteringDriver;
import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;

import testsuite.BaseTestCase;

/**
 * @author Tonci
 */
public class ConnectionRegressionTest extends BaseTestCase {

	/**
	 * @param name
	 */
	public ConnectionRegressionTest(String name) {
		super(name);
		// TODO Auto-generated constructor stub
	}

	/**
	 * Runs all test cases in this test suite
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		junit.textui.TestRunner.run(ConnectionRegressionTest.class);
	}

	/**
	 * @author Tonci
	 * Bypassing the server protocol bug where DB should be null-terminated
	 * whether it exists or not. Affects COM_CHANGE_USER.
	 */
	public void testBug54425() throws Exception {
		Properties parsedProps = new NonRegisteringDriver().parseURL(dbUrl,
				null);
		String host = parsedProps
				.getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY);
		String port = parsedProps
				.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY);
		String user = parsedProps
				.getProperty(NonRegisteringDriver.USER_PROPERTY_KEY);
		String password = parsedProps
				.getProperty(NonRegisteringDriver.PASSWORD_PROPERTY_KEY);

		String newUrl = String
				.format("jdbc:mysql://address=(protocol=tcp)(host=%s)(port=%s)(user=%s)(password=%s)/",
						host, port, user != null ? user : "",
						password != null ? password : "");
		
		MysqlConnectionPoolDataSource pds = new MysqlConnectionPoolDataSource();
		pds.setUrl(newUrl);

		((MySQLConnection) pds.getPooledConnection().getConnection().unwrap(com.mysql.jdbc.MySQLConnection.class)).changeUser(user, password);
	}
}
