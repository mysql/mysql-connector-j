package testsuite.simple;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Properties;
import java.util.TimeZone;

import testsuite.BaseTestCase;

/**
 * @author mmatthew
 *
 * To change this generated comment edit the template variable "typecomment":
 * Window>Preferences>Java>Templates.
 * To enable and disable the creation of type comments go to
 * Window>Preferences>Java>Code Generation.
 */
public class TimeTest extends BaseTestCase {

	/**
	 * Constructor for TimeTest.
	 * @param name
	 */
	public TimeTest(String name) {
		super(name);
	}

	public static void main(String[] args) {
	}
    
    public void testTimezone() throws Exception {
        try {
            String clientTimezoneName = "America/Los_Angeles";
            
            TimeZone.setDefault(TimeZone.getTimeZone(clientTimezoneName));
            
            Properties props = new Properties();
            props.put("useTimezone", "true");
           
            conn = DriverManager.getConnection(dbUrl, props);
            stmt = conn.createStatement();
            stmt.executeUpdate("DROP TABLE IF EXISTS timeTest");
            stmt.executeUpdate("CREATE TABLE timeTest (tstamp DATETIME, t TIME)");
          
            PreparedStatement pstmt = conn.prepareStatement("INSERT INTO timeTest VALUES (?, ?)");
            
            long now = System.currentTimeMillis(); // Time in milliseconds since 1/1/1970 GMT
            
            Timestamp nowTstamp = new Timestamp(now);
            Time nowTime = new Time(now); 
            
            pstmt.setTimestamp(1, nowTstamp);
            pstmt.setTime(2, nowTime);
            pstmt.executeUpdate();
            
                                    
            rs = stmt.executeQuery("SELECT * from timeTest");
            
            while (rs.next()) {
                String retrTimestampString = rs.getString(1);
                Timestamp retrTimestamp = rs.getTimestamp(1);
                Time retrTime = rs.getTime(2);
                
                System.out.println(retrTimestampString + " in database is " + retrTimestamp + " in " + clientTimezoneName);
            }
           
        } finally {
            
            stmt.executeUpdate("DROP TABLE IF EXISTS timeTest");
        }
    }
}
