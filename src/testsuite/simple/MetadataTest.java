/*
 Copyright (c) 2002, 2012, Oracle and/or its affiliates. All rights reserved.
 

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
  <http://www.mysql.com/about/legal/licensing/foss-exception.html>.

  This program is free software; you can redistribute it and/or modify it under the terms
  of the GNU General Public License as published by the Free Software Foundation; version 2
  of the License.

  This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
  See the GNU General Public License for more details.

  You should have received a copy of the GNU General Public License along with this
  program; if not, write to the Free Software Foundation, Inc., 51 Franklin St, Fifth
  Floor, Boston, MA 02110-1301  USA



 */
package testsuite.simple;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import testsuite.BaseTestCase;

import com.mysql.jdbc.StringUtils;

/**
 * Tests DatabaseMetaData methods.
 * 
 * @author Mark Matthews
 * @version $Id$
 */
public class MetadataTest extends BaseTestCase {
	// ~ Constructors
	// -----------------------------------------------------------

	/**
	 * Creates a new MetadataTest object.
	 * 
	 * @param name
	 *            DOCUMENT ME!
	 */
	public MetadataTest(String name) {
		super(name);
	}

	// ~ Methods
	// ----------------------------------------------------------------

	/**
	 * Runs all test cases in this test suite
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		junit.textui.TestRunner.run(MetadataTest.class);
	}

	/**
	 * DOCUMENT ME!
	 * 
	 * @throws Exception
	 *             DOCUMENT ME!
	 */
	public void setUp() throws Exception {
		super.setUp();
	}

	/**
	 * DOCUMENT ME!
	 * 
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public void testForeignKeys() throws SQLException {
		try {
			createTestTable();

			DatabaseMetaData dbmd = this.conn.getMetaData();
			this.rs = dbmd.getImportedKeys(null, null, "child");

			while (this.rs.next()) {
				String pkColumnName = this.rs.getString("PKCOLUMN_NAME");
				String fkColumnName = this.rs.getString("FKCOLUMN_NAME");
				assertTrue("Primary Key not returned correctly ('"
						+ pkColumnName + "' != 'parent_id')", pkColumnName
						.equalsIgnoreCase("parent_id"));
				assertTrue("Foreign Key not returned correctly ('"
						+ fkColumnName + "' != 'parent_id_fk')", fkColumnName
						.equalsIgnoreCase("parent_id_fk"));
			}

			this.rs.close();
			this.rs = dbmd.getExportedKeys(null, null, "parent");

			while (this.rs.next()) {
				String pkColumnName = this.rs.getString("PKCOLUMN_NAME");
				String fkColumnName = this.rs.getString("FKCOLUMN_NAME");
				String fkTableName = this.rs.getString("FKTABLE_NAME");
				assertTrue("Primary Key not returned correctly ('"
						+ pkColumnName + "' != 'parent_id')", pkColumnName
						.equalsIgnoreCase("parent_id"));
				assertTrue(
						"Foreign Key table not returned correctly for getExportedKeys ('"
								+ fkTableName + "' != 'child')", fkTableName
								.equalsIgnoreCase("child"));
				assertTrue(
						"Foreign Key not returned correctly for getExportedKeys ('"
								+ fkColumnName + "' != 'parent_id_fk')",
						fkColumnName.equalsIgnoreCase("parent_id_fk"));
			}

			this.rs.close();

			this.rs = dbmd.getCrossReference(null, null, "cpd_foreign_3", null,
					null, "cpd_foreign_4");

			assertTrue(this.rs.next());

			String pkColumnName = this.rs.getString("PKCOLUMN_NAME");
			String pkTableName = this.rs.getString("PKTABLE_NAME");
			String fkColumnName = this.rs.getString("FKCOLUMN_NAME");
			String fkTableName = this.rs.getString("FKTABLE_NAME");
			String deleteAction = cascadeOptionToString(this.rs
					.getInt("DELETE_RULE"));
			String updateAction = cascadeOptionToString(this.rs
					.getInt("UPDATE_RULE"));

			assertEquals(pkColumnName, "cpd_foreign_1_id");
			assertEquals(pkTableName, "cpd_foreign_3");
			assertEquals(fkColumnName, "cpd_foreign_1_id");
			assertEquals(fkTableName, "cpd_foreign_4");
			assertEquals(deleteAction, "NO ACTION");
			assertEquals(updateAction, "CASCADE");

			this.rs.close();
			this.rs = null;
		} finally {
			if (this.rs != null) {
				this.rs.close();
				this.rs = null;
			}
			this.stmt.executeUpdate("DROP TABLE IF EXISTS child");
			this.stmt.executeUpdate("DROP TABLE IF EXISTS parent");
			this.stmt.executeUpdate("DROP TABLE IF EXISTS cpd_foreign_4");
			this.stmt.executeUpdate("DROP TABLE IF EXISTS cpd_foreign_3");
			this.stmt.executeUpdate("DROP TABLE IF EXISTS cpd_foreign_2");
			this.stmt.executeUpdate("DROP TABLE IF EXISTS cpd_foreign_1");
			this.stmt.executeUpdate("DROP TABLE IF EXISTS fktable2");
			this.stmt.executeUpdate("DROP TABLE IF EXISTS fktable1");
		}

	}

	/**
	 * DOCUMENT ME!
	 * 
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public void testGetPrimaryKeys() throws SQLException {
		try {
			createTable("multikey", "(d INT NOT NULL, b INT NOT NULL, a INT NOT NULL, c INT NOT NULL, PRIMARY KEY (d, b, a, c))");
			DatabaseMetaData dbmd = this.conn.getMetaData();
			this.rs = dbmd.getPrimaryKeys(this.conn.getCatalog(), "",
					"multikey");

			short[] keySeqs = new short[4];
			String[] columnNames = new String[4];
			int i = 0;

			while (this.rs.next()) {
				this.rs.getString("TABLE_NAME");
				columnNames[i] = this.rs.getString("COLUMN_NAME");

				this.rs.getString("PK_NAME");
				keySeqs[i] = this.rs.getShort("KEY_SEQ");
				i++;
			}

			if ((keySeqs[0] != 3) && (keySeqs[1] != 2) && (keySeqs[2] != 4)
					&& (keySeqs[4] != 1)) {
				fail("Keys returned in wrong order");
			}
		} finally {
			if (this.rs != null) {
				try {
					this.rs.close();
				} catch (SQLException sqlEx) {
					/* ignore */
				}
			}
		}
	}

	private static String cascadeOptionToString(int option) {
		switch (option) {
		case DatabaseMetaData.importedKeyCascade:
			return "CASCADE";

		case DatabaseMetaData.importedKeySetNull:
			return "SET NULL";

		case DatabaseMetaData.importedKeyRestrict:
			return "RESTRICT";

		case DatabaseMetaData.importedKeyNoAction:
			return "NO ACTION";
		}

		return "SET DEFAULT";
	}

	private void createTestTable() throws SQLException {
		//Needed for previous runs that did not clean-up
		this.stmt.executeUpdate("DROP TABLE IF EXISTS child");
		this.stmt.executeUpdate("DROP TABLE IF EXISTS parent");
		this.stmt.executeUpdate("DROP TABLE IF EXISTS multikey");
		this.stmt.executeUpdate("DROP TABLE IF EXISTS cpd_foreign_4");
		this.stmt.executeUpdate("DROP TABLE IF EXISTS cpd_foreign_3");
		this.stmt.executeUpdate("DROP TABLE IF EXISTS cpd_foreign_2");
		this.stmt.executeUpdate("DROP TABLE IF EXISTS cpd_foreign_1");
		this.stmt.executeUpdate("DROP TABLE IF EXISTS fktable2");
		this.stmt.executeUpdate("DROP TABLE IF EXISTS fktable1");
		
		createTable("parent", "(parent_id INT NOT NULL, PRIMARY KEY (parent_id))", "INNODB");
		createTable("child", "(child_id INT, parent_id_fk INT, INDEX par_ind (parent_id_fk), "
						+ "FOREIGN KEY (parent_id_fk) REFERENCES parent(parent_id)) ", "INNODB");

		// Test compound foreign keys
		try{
			createTable("cpd_foreign_1", "("
					+ "id int(8) not null auto_increment primary key,"
					+ "name varchar(255) not null unique," + "key (id)"
					+ ")", "InnoDB");
		} catch (SQLException sqlEx) {
			if (sqlEx.getMessage().indexOf("max key length") != -1) {
				createTable("cpd_foreign_1", "("
						+ "id int(8) not null auto_increment primary key,"
						+ "name varchar(180) not null unique," + "key (id)"
						+ ")", "InnoDB");
			}
		}

		createTable("cpd_foreign_2", "("
				+ "id int(8) not null auto_increment primary key,"
				+ "key (id)," + "name varchar(255)" + ") ", "InnoDB");
		createTable("cpd_foreign_3", "("
						+ "cpd_foreign_1_id int(8) not null,"
						+ "cpd_foreign_2_id int(8) not null,"
						+ "key(cpd_foreign_1_id),"
						+ "key(cpd_foreign_2_id),"
						+ "primary key (cpd_foreign_1_id, cpd_foreign_2_id),"
						+ "foreign key (cpd_foreign_1_id) references cpd_foreign_1(id),"
						+ "foreign key (cpd_foreign_2_id) references cpd_foreign_2(id)"
						+ ") ", "InnoDB");
		createTable("cpd_foreign_4", "("
						+ "cpd_foreign_1_id int(8) not null,"
						+ "cpd_foreign_2_id int(8) not null,"
						+ "key(cpd_foreign_1_id),"
						+ "key(cpd_foreign_2_id),"
						+ "primary key (cpd_foreign_1_id, cpd_foreign_2_id),"
						+ "foreign key (cpd_foreign_1_id, cpd_foreign_2_id) "
						+ "references cpd_foreign_3(cpd_foreign_1_id, cpd_foreign_2_id) "
						+ "ON DELETE RESTRICT ON UPDATE CASCADE"
						+ ") ", "InnoDB");

		createTable("fktable1", "(TYPE_ID int not null, TYPE_DESC varchar(32), primary key(TYPE_ID))", "InnoDB");
		createTable("fktable2", "(KEY_ID int not null, COF_NAME varchar(32), PRICE float, TYPE_ID int, primary key(KEY_ID), "
						+ "index(TYPE_ID), foreign key(TYPE_ID) references fktable1(TYPE_ID)) ", "InnoDB");
	}

	/**
	 * Tests the implementation of metadata for views.
	 * 
	 * This test automatically detects whether or not the server it is running
	 * against supports the creation of views.
	 * 
	 * @throws SQLException
	 *             if the test fails.
	 */
	public void testViewMetaData() throws SQLException {
		try {
			this.rs = this.conn.getMetaData().getTableTypes();

			while (this.rs.next()) {
				if ("VIEW".equalsIgnoreCase(this.rs.getString(1))) {

					this.stmt
						.executeUpdate("DROP VIEW IF EXISTS vTestViewMetaData");
					createTable("testViewMetaData", "(field1 INT)");
					this.stmt
							.executeUpdate("CREATE VIEW vTestViewMetaData AS SELECT field1 FROM testViewMetaData");

					ResultSet tablesRs = null;

					try {
						tablesRs = this.conn.getMetaData().getTables(
								this.conn.getCatalog(), null, "%ViewMetaData",
								new String[] { "TABLE", "VIEW" });
						assertTrue(tablesRs.next());
						assertTrue("testViewMetaData".equalsIgnoreCase(tablesRs
								.getString(3)));
						assertTrue(tablesRs.next());
						assertTrue("vTestViewMetaData"
								.equalsIgnoreCase(tablesRs.getString(3)));

					} finally {
						if (tablesRs != null) {
							tablesRs.close();
						}
					}

					try {
						tablesRs = this.conn.getMetaData().getTables(
								this.conn.getCatalog(), null, "%ViewMetaData",
								new String[] { "TABLE" });
						assertTrue(tablesRs.next());
						assertTrue("testViewMetaData".equalsIgnoreCase(tablesRs
								.getString(3)));
						assertTrue(!tablesRs.next());
					} finally {
						if (tablesRs != null) {
							tablesRs.close();
						}
					}
					break;
				}
			}

		} finally {
			if (this.rs != null) {
				this.rs.close();
			}
			this.stmt
				.executeUpdate("DROP VIEW IF EXISTS vTestViewMetaData");
		}
	}

	/**
	 * Tests detection of read-only fields when the server is 4.1.0 or newer.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testRSMDIsReadOnly() throws Exception {
		try {
			this.rs = this.stmt.executeQuery("SELECT 1");

			ResultSetMetaData rsmd = this.rs.getMetaData();

			if (versionMeetsMinimum(4, 1)) {
				assertTrue(rsmd.isReadOnly(1));

				try {
					createTable("testRSMDIsReadOnly", "(field1 INT)");
					this.stmt
							.executeUpdate("INSERT INTO testRSMDIsReadOnly VALUES (1)");

					this.rs = this.stmt
							.executeQuery("SELECT 1, field1 + 1, field1 FROM testRSMDIsReadOnly");
					rsmd = this.rs.getMetaData();

					assertTrue(rsmd.isReadOnly(1));
					assertTrue(rsmd.isReadOnly(2));
					assertTrue(!rsmd.isReadOnly(3));
				} finally {
				}
			} else {
				assertTrue(rsmd.isReadOnly(1) == false);
			}
		} finally {
			if (this.rs != null) {
				this.rs.close();
			}
		}
	}

	public void testBitType() throws Exception {
		if (versionMeetsMinimum(5, 0, 3)) {
			try {
				createTable("testBitType", "(field1 BIT, field2 BIT, field3 BIT)");
				this.stmt
						.executeUpdate("INSERT INTO testBitType VALUES (1, 0, NULL)");
				this.rs = this.stmt
						.executeQuery("SELECT field1, field2, field3 FROM testBitType");
				this.rs.next();

				assertTrue(((Boolean) this.rs.getObject(1)).booleanValue());
				assertTrue(!((Boolean) this.rs.getObject(2)).booleanValue());
				assertEquals(this.rs.getObject(3), null);

				System.out.println(this.rs.getObject(1) + ", "
						+ this.rs.getObject(2) + ", " + this.rs.getObject(3));

				this.rs = this.conn.prepareStatement(
						"SELECT field1, field2, field3 FROM testBitType")
						.executeQuery();
				this.rs.next();

				assertTrue(((Boolean) this.rs.getObject(1)).booleanValue());
				assertTrue(!((Boolean) this.rs.getObject(2)).booleanValue());

				assertEquals(this.rs.getObject(3), null);
				byte[] asBytesTrue = this.rs.getBytes(1);
				byte[] asBytesFalse = this.rs.getBytes(2);
				byte[] asBytesNull = this.rs.getBytes(3);

				assertEquals(asBytesTrue[0], 1);
				assertEquals(asBytesFalse[0], 0);
				assertEquals(asBytesNull, null);

				createTable("testBitField", "(field1 BIT(9))");
				this.rs = this.stmt
						.executeQuery("SELECT field1 FROM testBitField");
				System.out.println(this.rs.getMetaData().getColumnClassName(1));
			} finally {
			}
		}
	}

	public void testSupportsSelectForUpdate() throws Exception {
		boolean supportsForUpdate = this.conn.getMetaData()
				.supportsSelectForUpdate();

		if (this.versionMeetsMinimum(4, 0)) {
			assertTrue(supportsForUpdate);
		} else {
			assertTrue(!supportsForUpdate);
		}
	}

	public void testTinyint1IsBit() throws Exception {
		String tableName = "testTinyint1IsBit";
		// Can't use 'BIT' or boolean
		createTable(tableName, "(field1 TINYINT(1))");
		this.stmt.executeUpdate("INSERT INTO " + tableName + " VALUES (1)");

		Properties props = new Properties();
		props.setProperty("tinyint1IsBit", "true");
		props.setProperty("transformedBitIsBoolean", "true");
		Connection boolConn = getConnectionWithProps(props);

		this.rs = boolConn.createStatement().executeQuery(
				"SELECT field1 FROM " + tableName);
		checkBitOrBooleanType(false);

		this.rs = boolConn.prepareStatement("SELECT field1 FROM " + tableName)
				.executeQuery();
		checkBitOrBooleanType(false);

		this.rs = boolConn.getMetaData().getColumns(boolConn.getCatalog(),
				null, tableName, "field1");
		assertTrue(this.rs.next());

		if (versionMeetsMinimum(4, 1)) {
			assertEquals(Types.BOOLEAN, this.rs.getInt("DATA_TYPE"));
		} else {
			assertEquals(Types.BIT, this.rs.getInt("DATA_TYPE"));
		}

		if (versionMeetsMinimum(4, 1)) {
			assertEquals("BOOLEAN", this.rs.getString("TYPE_NAME"));
		} else {
			assertEquals("BIT", this.rs.getString("TYPE_NAME"));
		}

		props.clear();
		props.setProperty("transformedBitIsBoolean", "false");
		props.setProperty("tinyint1IsBit", "true");

		Connection bitConn = getConnectionWithProps(props);

		this.rs = bitConn.createStatement().executeQuery(
				"SELECT field1 FROM " + tableName);
		checkBitOrBooleanType(true);

		this.rs = bitConn.prepareStatement("SELECT field1 FROM " + tableName)
				.executeQuery();
		checkBitOrBooleanType(true);

		this.rs = bitConn.getMetaData().getColumns(boolConn.getCatalog(), null,
				tableName, "field1");
		assertTrue(this.rs.next());

		assertEquals(Types.BIT, this.rs.getInt("DATA_TYPE"));

		assertEquals("BIT", this.rs.getString("TYPE_NAME"));
	}

	private void checkBitOrBooleanType(boolean usingBit) throws SQLException {

		assertTrue(this.rs.next());
		assertEquals("java.lang.Boolean", this.rs.getObject(1).getClass()
				.getName());
		if (!usingBit) {
			if (versionMeetsMinimum(4, 1)) {
				assertEquals(Types.BOOLEAN, this.rs.getMetaData()
						.getColumnType(1));
			} else {
				assertEquals(Types.BIT, this.rs.getMetaData().getColumnType(1));
			}
		} else {
			assertEquals(Types.BIT, this.rs.getMetaData().getColumnType(1));
		}

		assertEquals("java.lang.Boolean", this.rs.getMetaData()
				.getColumnClassName(1));
	}
    
    /**
     * Tests the implementation of Information Schema for primary keys.
     */
    public void testGetPrimaryKeysUsingInfoShcema() throws Exception {
        if (versionMeetsMinimum(5, 0, 7)) {
        	createTable("t1", "(c1 int(1) primary key)");
            Properties props = new Properties();
            props.put("useInformationSchema", "true");
            Connection conn1 = null;
            try {
                conn1 = getConnectionWithProps(props);
                DatabaseMetaData metaData = conn1.getMetaData();
                this.rs = metaData.getPrimaryKeys(null, null, "t1");
                this.rs.next();
                assertEquals("t1", this.rs.getString("TABLE_NAME"));
                assertEquals("c1", this.rs.getString("COLUMN_NAME"));
            } finally {
                if (conn1 != null) {
					conn1.close();
				}
            }
        }
    }
    
    /**
     * Tests the implementation of Information Schema for index info.
     */
    public void testGetIndexInfoUsingInfoSchema() throws Exception {
        if (versionMeetsMinimum(5, 0, 7)) {
        	createTable("t1", "(c1 int(1))");
            this.stmt.executeUpdate("CREATE INDEX index1 ON t1 (c1)");

            Connection conn1 = null;
            
            try {
                conn1 = getConnectionWithProps("useInformationSchema=true");
                DatabaseMetaData metaData = conn1.getMetaData();
                this.rs = metaData.getIndexInfo(conn1.getCatalog(), null, "t1", false, true);
                this.rs.next();
                assertEquals("t1", this.rs.getString("TABLE_NAME"));
                assertEquals("c1", this.rs.getString("COLUMN_NAME"));
                assertEquals("1", this.rs.getString("NON_UNIQUE"));
                assertEquals("index1", this.rs.getString("INDEX_NAME"));
            } finally {
                if (conn1 != null) {
					conn1.close();
				}
            }
        }
    }
    
    /**
     * Tests the implementation of Information Schema for columns.
     */
    public void testGetColumnsUsingInfoSchema() throws Exception {
        if (versionMeetsMinimum(5, 0, 7)) {
        	createTable("t1", "(c1 char(1))");
            Properties props = new Properties();
            props.put("useInformationSchema", "true");
            Connection conn1 = null;
            try {
            conn1 = getConnectionWithProps(props);
                DatabaseMetaData metaData = conn1.getMetaData();
                this.rs = metaData.getColumns(null, null, "t1", null);
                this.rs.next();
                assertEquals("t1", this.rs.getString("TABLE_NAME"));
                assertEquals("c1", this.rs.getString("COLUMN_NAME"));
                assertEquals("CHAR", this.rs.getString("TYPE_NAME"));
                assertEquals("1", this.rs.getString("COLUMN_SIZE"));
            } finally {
                if (conn1 != null) {
					conn1.close();
				}
            }
        }
    }
    
    /**
     * Tests the implementation of Information Schema for tables.
     */
    public void testGetTablesUsingInfoSchema() throws Exception {
        if (versionMeetsMinimum(5, 0, 7)) {
            createTable("`t1-1`", "(c1 char(1))");
            createTable("`t1-2`", "(c1 char(1))");
            createTable("`t2`", "(c1 char(1))");
            Set<String> tableNames = new HashSet<String>();
            tableNames.add("t1-1");
            tableNames.add("t1-2");
            Properties props = new Properties();
            props.put("useInformationSchema", "true");
            Connection conn1 = null;
            try {
                conn1 = getConnectionWithProps(props);
                DatabaseMetaData metaData = conn1.getMetaData();
                // pattern matching for table name
                this.rs = metaData.getTables(null, null, "t1-_", null);
                while (this.rs.next()) {
                    assertTrue(tableNames.remove(this.rs.getString("TABLE_NAME")));
                }
                assertTrue(tableNames.isEmpty());
            } finally {
                if (conn1 != null) {
					conn1.close();
				}
            }
        }
    }
    
    /**
     * Tests the implementation of Information Schema for column privileges.
     */
    public void testGetColumnPrivilegesUsingInfoSchema() throws Exception {
    	String dontRunPropertyName = "com.mysql.jdbc.testsuite.cantGrant";
    	
    	if (!runTestIfSysPropDefined(dontRunPropertyName)) {
	        if (versionMeetsMinimum(5, 0, 7)) {
	            Properties props = new Properties();
	            
	            props.put("useInformationSchema", "true");
	            Connection conn1 = null;
	            Statement stmt1 = null;
	            String userHostQuoted = null;
	            
	            boolean grantFailed = true;
	            
	            try {
	                conn1 = getConnectionWithProps(props);
	                stmt1 = conn1.createStatement();
	                createTable("t1", "(c1 int)");
	                this.rs = stmt1.executeQuery("SELECT USER()");
	                this.rs.next();
	                String user = this.rs.getString(1);
	                List<String> userHost = StringUtils.split(user, "@", false);
	                if (userHost.size() < 2) {
	                	fail("This test requires a JDBC URL with a user, and won't work with the anonymous user. " +
	                			"You can skip this test by setting the system property " + dontRunPropertyName);
	                }
	                userHostQuoted = "'" + userHost.get(0) + "'@'" + userHost.get(1) + "'";
	                
	                try {
	                	stmt1.executeUpdate("GRANT update (c1) on t1 to " + userHostQuoted);
	                	
	                	grantFailed = false;
	                	
	                } catch (SQLException sqlEx) {
	                	fail("This testcase needs to be run with a URL that allows the user to issue GRANTs "
	                			+ " in the current database. You can skip this test by setting the system property \""
	                			+ dontRunPropertyName + "\".");
	                }
	                
	                if (!grantFailed) {
		                DatabaseMetaData metaData = conn1.getMetaData();
		                this.rs = metaData.getColumnPrivileges(null, null, "t1", null);
		                this.rs.next();
		                assertEquals("t1", this.rs.getString("TABLE_NAME"));
		                assertEquals("c1", this.rs.getString("COLUMN_NAME"));
		                assertEquals(userHostQuoted, this.rs.getString("GRANTEE"));
		                assertEquals("UPDATE", this.rs.getString("PRIVILEGE"));
	                }
	            } finally {
		            if (stmt1 != null) {
		       
		            	if (!grantFailed) {
		            		stmt1.executeUpdate("REVOKE UPDATE (c1) ON t1 FROM " + userHostQuoted);
		            	}
		            	
		            	stmt1.close();
	            	}
	            	
	            	if (conn1 != null) {
	            		conn1.close();
	            	}
	            }
	        }
    	}
    }
    
    /**
     * Tests the implementation of Information Schema for description
     * of stored procedures available in a catalog.
     */
    public void testGetProceduresUsingInfoSchema() throws Exception {
        if (versionMeetsMinimum(5, 0, 7)) {
        	createProcedure("sp1", "()\n BEGIN\n" + "SELECT 1;" + "end\n");
            Properties props = new Properties();
            props.put("useInformationSchema", "true");
            Connection conn1 = null;
            try {
                conn1 = getConnectionWithProps(props);
                DatabaseMetaData metaData = conn1.getMetaData();
                this.rs = metaData.getProcedures(null, null, "sp1");
                this.rs.next();
                assertEquals("sp1", this.rs.getString("PROCEDURE_NAME"));
                assertEquals("1", this.rs.getString("PROCEDURE_TYPE"));
            } finally {
                if (conn1 != null) {
					conn1.close();
				}
            }
        }
    }
    
    /**
     * Tests the implementation of Information Schema for foreign key.
     */
    public void testGetCrossReferenceUsingInfoSchema() throws Exception {
        if (versionMeetsMinimum(5, 0, 7)) {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS child");
            this.stmt.executeUpdate("DROP TABLE If EXISTS parent");
            this.stmt.executeUpdate("CREATE TABLE parent(id INT NOT NULL, "
                + "PRIMARY KEY (id)) ENGINE=INNODB");
            this.stmt.executeUpdate("CREATE TABLE child(id INT, parent_id INT, "
                + "FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE SET NULL) ENGINE=INNODB");
            Properties props = new Properties();
            props.put("useInformationSchema", "true");
            Connection conn1 = null;
            try {
                conn1 = getConnectionWithProps(props);
                DatabaseMetaData metaData = conn1.getMetaData();
                this.rs = metaData.getCrossReference(null, null, "parent", null, null, "child");
                this.rs.next();
                assertEquals("parent", this.rs.getString("PKTABLE_NAME"));
                assertEquals("id", this.rs.getString("PKCOLUMN_NAME"));
                assertEquals("child", this.rs.getString("FKTABLE_NAME"));
                assertEquals("parent_id", this.rs.getString("FKCOLUMN_NAME"));
            } finally {
                this.stmt.executeUpdate("DROP TABLE IF EXISTS child");
                this.stmt.executeUpdate("DROP TABLE If EXISTS parent");
                if (conn1 != null) {
					conn1.close();
				}
            }
        }
    }
    
    /**
     * Tests the implementation of Information Schema for foreign key.
     */
    public void testGetExportedKeysUsingInfoSchema() throws Exception {
        if (versionMeetsMinimum(5, 0, 7)) {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS child");
            this.stmt.executeUpdate("DROP TABLE If EXISTS parent");
            this.stmt.executeUpdate("CREATE TABLE parent(id INT NOT NULL, "
                + "PRIMARY KEY (id)) ENGINE=INNODB");
            this.stmt.executeUpdate("CREATE TABLE child(id INT, parent_id INT, "
                + "FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE SET NULL) ENGINE=INNODB");
            Properties props = new Properties();
            props.put("useInformationSchema", "true");
            Connection conn1 = null;
            try {
                conn1 = getConnectionWithProps(props);
                DatabaseMetaData metaData = conn1.getMetaData();
                this.rs = metaData.getExportedKeys(null, null, "parent");
                this.rs.next();
                assertEquals("parent", this.rs.getString("PKTABLE_NAME"));
                assertEquals("id", this.rs.getString("PKCOLUMN_NAME"));
                assertEquals("child", this.rs.getString("FKTABLE_NAME"));
                assertEquals("parent_id", this.rs.getString("FKCOLUMN_NAME"));
            } finally {
                this.stmt.executeUpdate("DROP TABLE IF EXISTS child");
                this.stmt.executeUpdate("DROP TABLE If EXISTS parent");
                if (conn1 != null) {
					conn1.close();
				}
            }
        }
    }
    
    /**
     * Tests the implementation of Information Schema for foreign key.
     */
    public void testGetImportedKeysUsingInfoSchema() throws Exception {
        if (versionMeetsMinimum(5, 0, 7)) {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS child");
            this.stmt.executeUpdate("DROP TABLE If EXISTS parent");
            this.stmt.executeUpdate("CREATE TABLE parent(id INT NOT NULL, "
                + "PRIMARY KEY (id)) ENGINE=INNODB");
            this.stmt.executeUpdate("CREATE TABLE child(id INT, parent_id INT, "
                + "FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE SET NULL) ENGINE=INNODB");
            Properties props = new Properties();
            props.put("useInformationSchema", "true");
            Connection conn1 = null;
            try {
                conn1 = getConnectionWithProps(props);
                DatabaseMetaData metaData = conn1.getMetaData();
                this.rs = metaData.getImportedKeys(null, null, "child");
                this.rs.next();
                assertEquals("parent", this.rs.getString("PKTABLE_NAME"));
                assertEquals("id", this.rs.getString("PKCOLUMN_NAME"));
                assertEquals("child", this.rs.getString("FKTABLE_NAME"));
                assertEquals("parent_id", this.rs.getString("FKCOLUMN_NAME"));
            } finally {
                this.stmt.executeUpdate("DROP TABLE IF EXISTS child");
                this.stmt.executeUpdate("DROP TABLE If EXISTS parent");
                if (conn1 != null) {
					conn1.close();
				}
            }
        }
    }
}
