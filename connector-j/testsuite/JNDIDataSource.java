/* $Id$ */
package testsuite;

import java.sql.*;

import java.util.*;

import javax.naming.*;

import javax.sql.*;


/**
 * You cannot run this example unless you have the JNDI and JDBC 2.0 standard
 * extension classes installed.  It also requires the fscontext service
 * provider.  First make the directory /tmp/jdbc.  Then, run the example once
 * with the "install" argument. Then run it normally.  Running it with the
 * "install" argument sets up a data source in the fscontext you have
 * created.  For example:
 * <PRE>
 * > mkdir /tmp/jdbc
 * > java JNDIUser install
 *   Data source 'jdbc/test' installed.
 * > java JNDIUser
 * </PRE>
 * Note that you normally will NOT code the setup of data sources.  I am only
 * doing this for the purposes of making sure this example will run for
 * everyone. <BR> Last modified $Date$
 * 
 * @version $Revision$
 */
public class JNDIDataSource
{
    //~ Methods ...............................................................

    /**
     * DOCUMENT ME!
     * 
     * @param args DOCUMENT ME!
     */
    public static void main(String[] args)
    {
        if (args.length > 0 && args[0].equals("install")) {
            try {
                registerDataSource();
                System.out.println("Data source 'jdbc/test' installed.");
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Install failed.");
            }

            return;
        }
        try {
            Context ctx = new InitialContext();
            ctx.addToEnvironment(Context.INITIAL_CONTEXT_FACTORY, 
                                 "com.sun.jndi.fscontext.RefFSContextFactory");

            DataSource ds = (DataSource)ctx.lookup("/tmp/jdbc/test");
            Connection con = ds.getConnection("root", "eggs/ez");
            Statement stmt;
            ResultSet rs;
            System.out.println("Connection is: " + con);

            /*
               
        stmt = con.createStatement();
               
        rs = stmt.executeQuery("SELECT test_id, test_int, test_date, " +
               
                   "test_char, test_val " +
               
                   "FROM test ORDER BY test_id");
               
        System.out.println("Got results:");
               
        while( rs.next() ) {
               
        int i = rs.getInt(1);
               
        String s, comma = "";
               
        java.util.Date d;
               

               
        System.out.print("\tkey: " + i + "(");
               
        i = rs.getInt(2);
               
        if( !rs.wasNull() ) {
               
            System.out.print("test_int=" + i);
               
            comma = ",";
               
        }
               
        d = rs.getDate(3);
               
        if( !rs.wasNull() ) {
               
            System.out.print(comma + "test_date=" + d);
               
            comma = ",";
               
        }
               
        s = rs.getString(4);
               
        if( !rs.wasNull() ) {
               
            System.out.print(comma + "test_char='" + s + "'");
               
            comma = ",";
               
        }
               
        s = rs.getString(5);
               
        if( !rs.wasNull() ) {
               
            System.out.print(comma + "test_val='" + s + "'");
               
        }
               
        System.out.println(")");
               
        }
               
        */
            con.close();
            System.out.println("Done.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    /**
     * This method is separated from the rest of the example since you
     * normally would NOT register a JDBC driver in your code.  It would
     * likely be configered into your naming and directory service using some
     * GUI.
     * @throws Exception DOCUMENT ME!
     */
    static public void registerDataSource()
                                   throws Exception
    {
        com.mysql.jdbc.jdbc2.optional.MysqlDataSource ds;
        Context ctx;
        Hashtable env = new Hashtable();
        env.put(Context.INITIAL_CONTEXT_FACTORY, 
                "com.sun.jndi.fscontext.RefFSContextFactory");
        ctx = new InitialContext(env);
        System.out.println("Context is: " + ctx);
        ds = new com.mysql.jdbc.jdbc2.optional.MysqlDataSource();
        System.out.println("DataSource is: " + ds);
        ds.setServerName("localhost");
        ds.setDatabaseName("test");
        ds.setUser("root");
        ds.setPassword("eggs/ez");
        ctx.bind("/tmp/jdbc/test", ds);
        ctx.close();
    }
}