/*
   Copyright (C) 2002 MySQL AB
   
      This program is free software; you can redistribute it and/or modify
      it under the terms of the GNU General Public License as published by
      the Free Software Foundation; either version 2 of the License, or
      (at your option) any later version.
   
      This program is distributed in the hope that it will be useful,
      but WITHOUT ANY WARRANTY; without even the implied warranty of
      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
      GNU General Public License for more details.
   
      You should have received a copy of the GNU General Public License
      along with this program; if not, write to the Free Software
      Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
      
 */
package testsuite.simple;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Properties;
import java.util.TimeZone;

import testsuite.BaseTestCase;

/**
 * Tests time functionality.
 * @author Mark Matthews
 * @version TimeTest.java,v 1.2 2002/12/17 21:41:02 mmatthew Exp
 */
public class TimeTest extends BaseTestCase {

	/**
	 * Constructor for TimeTest.
	 * @param name the test to run
	 */
	public TimeTest(String name) {
		super(name);
	}

    /**
     * Runs all tests
     * 
     * @param args ignored
     */
	public static void main(String[] args) {
        new TimeTest("testTimezone").run();
	}
    
    /**
     * Tests timezone-related functionality
     * 
     * @throws Exception if an error occurs
     */
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
