/**
 * 
 */
package testsuite.simple;

import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.Properties;

import com.mysql.jdbc.ConnectionLifecycleInterceptor;

public class TestLifecycleInterceptor implements ConnectionLifecycleInterceptor {
	static int transactionsBegun = 0;
	static int transactionsCompleted = 0;

	public void close() throws SQLException {

	}

	public boolean commit() throws SQLException {

		return true;
	}

	public boolean rollback() throws SQLException {

		return true;
	}

	public boolean rollback(Savepoint s) throws SQLException {

		return true;
	}

	public boolean setAutoCommit(boolean flag) throws SQLException {

		return true;
	}

	public boolean setCatalog(String catalog) throws SQLException {

		return true;
	}

	public boolean transactionBegun() throws SQLException {
		transactionsBegun++;
		return true;
	}

	public boolean transactionCompleted() throws SQLException {
		transactionsCompleted++;
		return true;
	}

	public void destroy() {

	}

	public void init(com.mysql.jdbc.Connection conn, Properties props)
			throws SQLException {

	}

}