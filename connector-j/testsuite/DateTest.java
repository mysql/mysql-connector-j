package testsuite;

import java.sql.*;
import java.util.*;

public class DateTest
{
    public static void main(String[] args)
    {
        try
        {
            Class.forName("org.gjt.mm.mysql.Driver").newInstance();
            Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/test");
            conn.createStatement().executeUpdate("create table if not exists calendar_event " +
    			"(calendar_event_id int not null auto_increment primary key, " +
     			"start_date datetime not null) type = MyISAM;");

            String sql = "insert into calendar_event (start_date) values (?)";
            PreparedStatement pst = conn.prepareStatement(sql);
            long startDate = 1017684000000L;
            System.out.println("About to insert date " + startDate);
            pst.setTimestamp(1, new Timestamp(startDate));
            pst.executeUpdate();
            pst.close();
            
            sql = "select LAST_INSERT_ID()";
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(sql);
            rs.next();
            int eventID = rs.getInt(1);
            System.out.println("Inserted row " + eventID);
            
            sql = "select start_date from calendar_event where calendar_event_id = ?";
            pst = conn.prepareStatement(sql);
            pst.setInt(1, eventID);
            rs = pst.executeQuery();
            rs.next();

            Timestamp ts = rs.getTimestamp("start_date");
            System.out.println("Selected date from row " + eventID +
                               ": " + ts.getTime());
            pst.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }        
    }
}
