package testsuite;

import java.sql.*;


/**
 * @author Administrator
 */
public class Metadata
{
    //~ Instance/static variables .............................................

    static String DBUrl = "jdbc:mysql:///test";

    //~ Methods ...............................................................

    /**
     * DOCUMENT ME!
     * 
     * @param args DOCUMENT ME!
     * @throws Exception DOCUMENT ME!
     */
    public static void main(String[] args)
                     throws Exception
    {
        Connection conn = null;
        Statement stmt = null;
 
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            conn = DriverManager.getConnection(DBUrl);
            stmt = conn.createStatement();

            try {
                stmt.executeUpdate("DROP TABLE parent");
                stmt.executeUpdate("DROP TABLE child");
            } catch (SQLException sqlEx) {
            }

            stmt.executeUpdate("CREATE TABLE parent(id INT NOT NULL, PRIMARY KEY (id)) TYPE=INNODB");
            stmt.executeUpdate("CREATE TABLE child(id INT, parent_id INT, INDEX par_ind (parent_id), FOREIGN KEY (parent_id) REFERENCES parent(id)) TYPE=INNODB");

            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getImportedKeys(null, null, "child");

            while (rs.next()) {
                System.out.println(rs.getString("PKCOLUMN_NAME") + " -> " + 
                                   rs.getString("FKCOLUMN_NAME"));
            }

            rs.close();
            rs = dbmd.getExportedKeys(null, null, "parent");

            while (rs.next()) {
                System.out.println(rs.getString("PKCOLUMN_NAME") + " -> " + 
                                   rs.getString("FKCOLUMN_NAME"));
            }

            rs.close();
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlEx) { /* ignore */
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException sqlEx) { /* ignore */
                }
            }
        }
    }
}