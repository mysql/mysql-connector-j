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
package testsuite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.TestCase;

/** 
 * Base class for all test cases. Creates connections, 
 * statements, etc. and closes them.
 * 
 * @author  Mark Matthews
 * @version $Id$
 */
public abstract class BaseTestCase
    extends TestCase {

    //~ Instance/static variables .............................................

    protected Connection conn = null;
    protected ResultSet rs = null;
    protected Statement stmt = null;
    protected PreparedStatement pstmt = null;
    protected static String dbUrl = "jdbc:mysql:///test";

    //~ Constructors ..........................................................

    /**
     * Creates a new BaseTestCase object.
     * 
     * @param name DOCUMENT ME!
     */
    public BaseTestCase(String name) {
        super(name);

        String newDbUrl = System.getProperty("com.mysql.jdbc.testsuite.url");

        if (newDbUrl != null && newDbUrl.trim().length() != 0) {
            dbUrl = newDbUrl;
        }
    }

    //~ Methods ...............................................................

    /**
     * DOCUMENT ME!
     * 
     * @throws Exception DOCUMENT ME!
     */
    public void setUp()
               throws Exception {
        Class.forName("com.mysql.jdbc.Driver").newInstance();
        conn = DriverManager.getConnection(dbUrl);
        stmt = conn.createStatement();
    }

    /**
     * DOCUMENT ME!
     * 
     * @throws Exception DOCUMENT ME!
     */
    public void tearDown()
                  throws Exception {

        if (rs != null) {

            try {
                rs.close();
            } catch (SQLException SQLE) {
                ;
            }
        }

        if (stmt != null) {

            try {
                stmt.close();
            } catch (SQLException SQLE) {
                ;
            }
        }

        if (conn != null) {

            try {
                conn.close();
            } catch (SQLException SQLE) {
                ;
            }
        }
    }
    
    /**
     * Checks whether a certain system property is defined,
     * in order to run/not-run certain tests
     */
    protected boolean runTestIfSysPropDefined(String propName) {
        String prop = System.getProperty(propName);
        
        return prop != null && prop.length() > 0;
    }
        
        
}