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

    /** 
     * Connection to server, initialized in setUp()
     * Cleaned up in tearDown().
     */
    protected Connection conn = null;
    
    /**
     * ResultSet to be used in tests, not initialized.
     * Cleaned up in tearDown().
     */
    protected ResultSet rs = null;
    
    /**
     * Statement to be used in tests, initialized in setUp().
     * Cleaned up in tearDown().
     */
    protected Statement stmt = null;
    
    /**
     * PreparedStatement to be used in tests, not initialized.
     * Cleaned up in tearDown().
     */
    protected PreparedStatement pstmt = null;
    
    /**
     * JDBC URL, initialized from com.mysql.jdbc.testsuite.url
     * system property, or defaults to jdbc:mysql:///test
     */
    protected static String dbUrl = "jdbc:mysql:///test";

    //~ Constructors ..........................................................

    /**
     * Creates a new BaseTestCase object.
     * 
     * @param name The name of the JUnit test case
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
     * Creates resources used by all tests.
     * 
     * @throws Exception if an error occurs.
     */
    public void setUp()
               throws Exception {
        Class.forName("com.mysql.jdbc.Driver").newInstance();
        this.conn = DriverManager.getConnection(dbUrl);
        this.stmt = conn.createStatement();
    }

    /**
     * Destroys resources created during the test case.
     * 
     * @throws Exception DOCUMENT ME!
     */
    public void tearDown()
                  throws Exception {

        if (this.rs != null) {

            try {
                this.rs.close();
            } catch (SQLException SQLE) {
                ;
            }
        }

        if (this.stmt != null) {

            try {
                this.stmt.close();
            } catch (SQLException SQLE) {
                ;
            }
        }
        
        if (this.pstmt != null) {

            try {
                this.pstmt.close();
            } catch (SQLException SQLE) {
                ;
            }
        }

        if (this.conn != null) {

            try {
                this.conn.close();
            } catch (SQLException SQLE) {
                ;
            }
        }
    }
    
    /**
     * Checks whether a certain system property is defined,
     * in order to run/not-run certain tests
     * 
     * @param propName the property name to check for
     * @return true if the property is defined.
     */
    protected boolean runTestIfSysPropDefined(String propName) {
        String prop = System.getProperty(propName);
        
        return prop != null && prop.length() > 0;
    }
        
        
}