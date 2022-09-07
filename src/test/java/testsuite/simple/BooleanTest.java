package testsuite.simple;

import com.mysql.cj.MysqlType;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.conf.PropertyKey;
import org.junit.jupiter.api.Test;
import testsuite.BaseTestCase;

import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BooleanTest extends BaseTestCase {

    @Test
    public void testServerPrepStmtsParams() throws SQLException {
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), PropertyDefinitions.SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true");

        this.conn = getConnectionWithProps(props);
        this.pstmt = this.conn.prepareStatement("SELECT ?, ?, ?, ?, ?");

        executeStatement(true);
        executeStatement(false);
    }

    private void executeStatement(boolean value) throws SQLException {
        this.pstmt.setBoolean(1, value);
        this.pstmt.setObject(2, value);
        this.pstmt.setObject(3, value, MysqlType.BOOLEAN);
        this.pstmt.setObject(4, value, MysqlType.TINYINT);
        this.pstmt.setObject(5, value, MysqlType.BIT);

        this.rs = this.pstmt.executeQuery();
        assertTrue(this.rs.next());

        assertEquals(value, this.rs.getBoolean(1));
        assertEquals(value, this.rs.getBoolean(2));
        assertEquals(value, this.rs.getBoolean(3));
        assertEquals(value, this.rs.getBoolean(4));
        assertEquals(value, this.rs.getBoolean(5));

        this.rs.close();
    }
}
