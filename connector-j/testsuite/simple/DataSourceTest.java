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

import java.io.File;
import java.sql.Connection;
import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.Name;
import javax.naming.NameParser;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;
import javax.sql.DataSource;

import testsuite.BaseTestCase;


/** 
 *
 * @author  Mark Matthews
 * @version $Id$
 */
public class DataSourceTest
    extends BaseTestCase {

    //~ Instance/static variables .............................................

    private File tempDir;
    private Context ctx;

    //~ Constructors ..........................................................

    /**
     * Creates a new DataSourceTest object.
     * 
     * @param name DOCUMENT ME!
     */
    public DataSourceTest(String name) {
        super(name);
    }

    //~ Methods ...............................................................

    /**
     * DOCUMENT ME!
     * 
     * @param args DOCUMENT ME!
     */
    public static void main(String[] args) {
        new DataSourceTest("testDataSource").run();
    }

    /**
     * Sets up this test, calling registerDataSource() to bind a 
     * DataSource into JNDI, using the FSContext JNDI provider from Sun
     */
    public void setUp()
               throws Exception {
        super.setUp();
        registerDataSource();
    }

    /**
     * Un-binds the DataSource, and cleans up the filesystem
     */
    public void tearDown()
                  throws Exception {
        ctx.unbind(tempDir.getAbsolutePath() + "/test");
        ctx.close();
        tempDir.delete();
        super.tearDown();
    }

    /**
     * Tests that we can get a connection from the DataSource bound
     * in JNDI during test setup
     */
    public void testDataSource()
                        throws Exception {

        NameParser nameParser = ctx.getNameParser("");
        Name datasourceName = nameParser.parse(
                                      tempDir.getAbsolutePath() + "/test");
        Object obj = ctx.lookup(datasourceName);
        DataSource boundDs = null;

        if (obj instanceof DataSource) {
            boundDs = (DataSource) obj;
        } else if (obj instanceof Reference) {

            //
            // For some reason, this comes back as a Reference
            // instance under CruiseControl !?
            //
            Reference objAsRef = (Reference) obj;
            ObjectFactory factory = (ObjectFactory) Class.forName(objAsRef.getFactoryClassName())
                 .newInstance();
            boundDs = (DataSource) factory.getObjectInstance(objAsRef, 
                                                             datasourceName, 
                                                             ctx, 
                                                             new Hashtable());
        }

        assertTrue("Datasource not bound", boundDs != null);

        Connection con = boundDs.getConnection();
        con.close();
        assertTrue("Connection can not be obtained from data source", 
                   con != null);
    }

    /**
     * This method is separated from the rest of the example since you
     * normally would NOT register a JDBC driver in your code.  It would
     * likely be configered into your naming and directory service using some
     * GUI.
     * @throws Exception DOCUMENT ME!
     */
    private void registerDataSource()
                             throws Exception {
        tempDir = File.createTempFile("jnditest", null);
        tempDir.delete();
        tempDir.mkdir();
        tempDir.deleteOnExit();

        com.mysql.jdbc.jdbc2.optional.MysqlDataSource ds;
        Hashtable env = new Hashtable();
        env.put(Context.INITIAL_CONTEXT_FACTORY, 
                "com.sun.jndi.fscontext.RefFSContextFactory");
        ctx = new InitialContext(env);
        assertTrue("Naming Context not created", ctx != null);
        ds = new com.mysql.jdbc.jdbc2.optional.MysqlDataSource();
        ds.setUrl(dbUrl); // from BaseTestCase
        ctx.bind(tempDir.getAbsolutePath() + "/test", ds);
    }
}