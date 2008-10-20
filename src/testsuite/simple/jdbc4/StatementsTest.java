/*
 Copyright  2002-2007 MySQL AB, 2008 Sun Microsystems

 This program is free software; you can redistribute it and/or modify
 it under the terms of version 2 of the GNU General Public License as 
 published by the Free Software Foundation.

 There are special exceptions to the terms and conditions of the GPL 
 as it is applied to this software. View the full text of the 
 exception in file EXCEPTIONS-CONNECTOR-J in the directory of this 
 software distribution.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA



 */
package testsuite.simple.jdbc4;

import java.io.Reader;
import java.io.StringReader;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import testsuite.BaseTestCase;

public class StatementsTest extends BaseTestCase {

	public StatementsTest(String name) {
		super(name);
	
	}

	/**
	 * Tests for ResultSet.getNCharacterStream()
	 * 
	 * @throws Exception
	 */
	public void testGetNCharacterSteram() throws Exception {
	    createTable("testGetNCharacterStream", "(c1 NATIONAL CHARACTER(10), c2 NATIONAL CHARACTER(10))");
	    this.stmt.executeUpdate("INSERT INTO testGetNCharacterStream (c1, c2) VALUES (_utf8 'aaa', _utf8 'bbb')");
	    this.rs = this.stmt.executeQuery("SELECT c1, c2 FROM testGetNCharacterStream");
	    this.rs.next();
	    char[] c1 = new char[3];
	    this.rs.getNCharacterStream(1).read(c1);
	    assertEquals("aaa", new String(c1));
	    char[] c2 = new char[3];
	    this.rs.getNCharacterStream("c2").read(c2);
	    assertEquals("bbb", new String(c2));
	    this.rs.close();
	}

	/**
	 * Tests for ResultSet.getNClob()
	 * 
	 * @throws Exception
	 */
	public void testGetNClob() throws Exception {
	    createTable("testGetNClob", "(c1 NATIONAL CHARACTER(10), c2 NATIONAL CHARACTER(10))");
	    this.stmt.executeUpdate("INSERT INTO testGetNClob (c1, c2) VALUES (_utf8 'aaa', _utf8 'bbb')");
	    this.rs = this.stmt.executeQuery("SELECT c1, c2 FROM testGetNClob");
	    this.rs.next();
	    char[] c1 = new char[3];
	    this.rs.getNClob(1).getCharacterStream().read(c1);
	    assertEquals("aaa", new String(c1));
	    char[] c2 = new char[3];
	    this.rs.getNClob("c2").getCharacterStream().read(c2);
	    assertEquals("bbb", new String(c2));
	    this.rs.close();
	    
	    // for isBinaryEncoded = true, using PreparedStatement
	    createTable("testGetNClob", "(c1 NATIONAL CHARACTER(10), c2 NATIONAL CHARACTER(10))");
	    this.stmt.executeUpdate("INSERT INTO testGetNClob (c1, c2) VALUES (_utf8 'aaa', _utf8 'bbb')");
	    this.pstmt = this.conn.prepareStatement("SELECT c1, c2 FROM testGetNClob");
	    this.rs = this.pstmt.executeQuery();
	    this.rs.next();
	    c1 = new char[3];
	    this.rs.getNClob(1).getCharacterStream().read(c1);
	    assertEquals("aaa", new String(c1));
	    c2 = new char[3];
	    this.rs.getNClob("c2").getCharacterStream().read(c2);
	    assertEquals("bbb", new String(c2));
	    this.rs.close();
	}

	/**
	 * Tests for ResultSet.getNString()
	 * 
	 * @throws Exception
	 */
	public void testGetNString() throws Exception {
	    createTable("testGetNString", "(c1 NATIONAL CHARACTER(10), c2 NATIONAL CHARACTER(10))");
	    this.stmt.executeUpdate("INSERT INTO testGetNString (c1, c2) VALUES (_utf8 'aaa', _utf8 'bbb')");
	    this.rs = this.stmt.executeQuery("SELECT c1, c2 FROM testGetNString");
	    this.rs.next();
	    assertEquals("aaa", this.rs.getNString(1));
	    assertEquals("bbb", this.rs.getNString("c2"));
	    this.rs.close();
	}

	/**
	 * Tests for PreparedStatement.setNCharacterSteam()
	 * 
	 * @throws Exception
	 */
	public void testSetNCharacterStream() throws Exception {
	    // suppose sql_mode don't include "NO_BACKSLASH_ESCAPES"
	    
	    createTable("testSetNCharacterStream", "(c1 NATIONAL CHARACTER(10), c2 NATIONAL CHARACTER(10), " +
	            "c3 NATIONAL CHARACTER(10))");
	    Properties props1 = new Properties();
	    props1.put("useServerPrepStmts", "false"); // use client-side prepared statement
	    props1.put("useUnicode", "true");
	    props1.put("characterEncoding", "latin1"); // ensure charset isn't utf8 here
	    Connection conn1 = getConnectionWithProps(props1);
	    com.mysql.jdbc.PreparedStatement pstmt1 = (com.mysql.jdbc.PreparedStatement)
	        conn1.prepareStatement("INSERT INTO testSetNCharacterStream (c1, c2, c3) VALUES (?, ?, ?)");
	    pstmt1.setNCharacterStream(1, null, 0);
	    pstmt1.setNCharacterStream(2, new StringReader("aaa"), 3);
	    pstmt1.setNCharacterStream(3, new StringReader("\'aaa\'"), 5);
	    pstmt1.execute();
	    ResultSet rs1 = this.stmt.executeQuery("SELECT c1, c2, c3 FROM testSetNCharacterStream");
	    rs1.next();
	    assertEquals(null, rs1.getString(1));
	    assertEquals("aaa", rs1.getString(2));
	    assertEquals("\'aaa\'", rs1.getString(3));
	    rs1.close();
	    pstmt1.close();
	    conn1.close();
	    
	    createTable("testSetNCharacterStream", "(c1 NATIONAL CHARACTER(10), c2 NATIONAL CHARACTER(10), " +
	    "c3 NATIONAL CHARACTER(10))");
	    Properties props2 = new Properties();
	    props2.put("useServerPrepStmts", "false"); // use client-side prepared statement
	    props2.put("useUnicode", "true");
	    props2.put("characterEncoding", "UTF-8"); // ensure charset is utf8 here
	    Connection conn2 = getConnectionWithProps(props2);
	    com.mysql.jdbc.PreparedStatement pstmt2 = (com.mysql.jdbc.PreparedStatement)
	        conn2.prepareStatement("INSERT INTO testSetNCharacterStream (c1, c2, c3) VALUES (?, ?, ?)");
	    pstmt2.setNCharacterStream(1, null, 0);
	    pstmt2.setNCharacterStream(2, new StringReader("aaa"), 3);
	    pstmt2.setNCharacterStream(3, new StringReader("\'aaa\'"), 5);
	    pstmt2.execute();
	    ResultSet rs2 = this.stmt.executeQuery("SELECT c1, c2, c3 FROM testSetNCharacterStream");
	    rs2.next();
	    assertEquals(null, rs2.getString(1));
	    assertEquals("aaa", rs2.getString(2));
	    assertEquals("\'aaa\'", rs2.getString(3));
	    rs2.close();
	    pstmt2.close();
	    conn2.close();
	}

	/**
	 * Tests for ServerPreparedStatement.setNCharacterSteam()
	 * 
	 * @throws Exception
	 */
	public void testSetNCharacterStreamServer() throws Exception {
	    createTable("testSetNCharacterStreamServer", "(c1 NATIONAL CHARACTER(10))");
	    Properties props1 = new Properties();
	    props1.put("useServerPrepStmts", "true"); // use server-side prepared statement
	    props1.put("useUnicode", "true");
	    props1.put("characterEncoding", "latin1"); // ensure charset isn't utf8 here
	    Connection conn1 = getConnectionWithProps(props1);
	    PreparedStatement pstmt1 =  conn1.prepareStatement("INSERT INTO testSetNCharacterStreamServer (c1) VALUES (?)");
	    try {
	        pstmt1.setNCharacterStream(1, new StringReader("aaa"), 3);
	        fail();
	    } catch (SQLException e) {
	        // ok
	        assertEquals("Can not call setNCharacterStream() when connection character set isn't UTF-8",
	            e.getMessage());  
	    }
	    pstmt1.close();
	    conn1.close();
	    
	    createTable("testSetNCharacterStreamServer", "(c1 LONGTEXT charset utf8)");
	    Properties props2 = new Properties();
	    props2.put("useServerPrepStmts", "true"); // use server-side prepared statement
	    props2.put("useUnicode", "true");
	    props2.put("characterEncoding", "UTF-8"); // ensure charset is utf8 here
	    Connection conn2 = getConnectionWithProps(props2);
	    PreparedStatement pstmt2 = 
	        conn2.prepareStatement("INSERT INTO testSetNCharacterStreamServer (c1) VALUES (?)");
	    pstmt2.setNCharacterStream(1, new StringReader(
	            new String(new char[81921])), 81921); // 10 Full Long Data Packet's chars + 1 char
	    pstmt2.execute();
	    ResultSet rs2 = this.stmt.executeQuery("SELECT c1 FROM testSetNCharacterStreamServer");
	    rs2.next();
	    assertEquals(new String(new char[81921]), rs2.getString(1));
	    rs2.close();
	    pstmt2.close();
	    conn2.close();
	}

	/**
	 * Tests for PreparedStatement.setNClob()
	 * 
	 * @throws Exception
	 */
	public void testSetNClob() throws Exception {
	    // suppose sql_mode don't include "NO_BACKSLASH_ESCAPES"
	    
	    createTable("testSetNClob", "(c1 NATIONAL CHARACTER(10), c2 NATIONAL CHARACTER(10), " +
	            "c3 NATIONAL CHARACTER(10))");
	    Properties props1 = new Properties();
	    props1.put("useServerPrepStmts", "false"); // use client-side prepared statement
	    props1.put("useUnicode", "true");
	    props1.put("characterEncoding", "latin1"); // ensure charset isn't utf8 here
	    Connection conn1 = getConnectionWithProps(props1);
	    PreparedStatement pstmt1 = 
	        conn1.prepareStatement("INSERT INTO testSetNClob (c1, c2, c3) VALUES (?, ?, ?)");
	    pstmt1.setNClob(1, (NClob)null);
	    NClob nclob2 = conn1.createNClob();
	    nclob2.setString(1, "aaa");
	    pstmt1.setNClob(2, nclob2);                   // for setNClob(int, NClob)
	    Reader reader3 = new StringReader("\'aaa\'");
	    pstmt1.setNClob(3, reader3, 5);               // for setNClob(int, Reader, long)
	    pstmt1.execute();
	    ResultSet rs1 = this.stmt.executeQuery("SELECT c1, c2, c3 FROM testSetNClob");
	    rs1.next();
	    assertEquals(null, rs1.getString(1));
	    assertEquals("aaa", rs1.getString(2));
	    assertEquals("\'aaa\'", rs1.getString(3));
	    rs1.close();
	    pstmt1.close();
	    conn1.close();
	    
	    createTable("testSetNClob", "(c1 NATIONAL CHARACTER(10), c2 NATIONAL CHARACTER(10), " +
	    "c3 NATIONAL CHARACTER(10))");
	    Properties props2 = new Properties();
	    props2.put("useServerPrepStmts", "false"); // use client-side prepared statement
	    props2.put("useUnicode", "true");
	    props2.put("characterEncoding", "UTF-8"); // ensure charset is utf8 here
	    Connection conn2 = getConnectionWithProps(props2);
	    PreparedStatement pstmt2 = 
	        conn2.prepareStatement("INSERT INTO testSetNClob (c1, c2, c3) VALUES (?, ?, ?)");
	    pstmt2.setNClob(1, (NClob)null);
	    nclob2 = conn2.createNClob();
	    nclob2.setString(1, "aaa");
	    pstmt2.setNClob(2, nclob2);             // for setNClob(int, NClob)
	    reader3 = new StringReader("\'aaa\'");
	    pstmt2.setNClob(3, reader3, 5);         // for setNClob(int, Reader, long)
	    pstmt2.execute();
	    ResultSet rs2 = this.stmt.executeQuery("SELECT c1, c2, c3 FROM testSetNClob");
	    rs2.next();
	    assertEquals(null, rs2.getString(1));
	    assertEquals("aaa", rs2.getString(2));
	    assertEquals("\'aaa\'", rs2.getString(3));
	    rs2.close();
	    pstmt2.close();
	    conn2.close();
	}

	/**
	 * Tests for ServerPreparedStatement.setNClob()
	 * 
	 * @throws Exception
	 */
	public void testSetNClobServer() throws Exception {
	    createTable("testSetNClobServer", "(c1 NATIONAL CHARACTER(10), c2 NATIONAL CHARACTER(10))");
	    Properties props1 = new Properties();
	    props1.put("useServerPrepStmts", "true"); // use server-side prepared statement
	    props1.put("useUnicode", "true");
	    props1.put("characterEncoding", "latin1"); // ensure charset isn't utf8 here
	    Connection conn1 = getConnectionWithProps(props1);
	    PreparedStatement pstmt1 = 
	        conn1.prepareStatement("INSERT INTO testSetNClobServer (c1, c2) VALUES (?, ?)");
	    NClob nclob1 = conn1.createNClob();
	    nclob1.setString(1, "aaa");
	    Reader reader2 = new StringReader("aaa");
	    try {
	        pstmt1.setNClob(1, nclob1);
	        fail();
	    } catch (SQLException e) {
	        // ok
	        assertEquals("Can not call setNClob() when connection character set isn't UTF-8",
	            e.getMessage());  
	    }
	    try {
	        pstmt1.setNClob(2, reader2, 3);
	        fail();
	    } catch (SQLException e) {
	        // ok
	        assertEquals("Can not call setNClob() when connection character set isn't UTF-8",
	            e.getMessage());  
	    }
	    pstmt1.close();
	    conn1.close();
	    
	    createTable("testSetNClobServer", "(c1 NATIONAL CHARACTER(10), c2 LONGTEXT charset utf8)");
	    Properties props2 = new Properties();
	    props2.put("useServerPrepStmts", "true"); // use server-side prepared statement
	    props2.put("useUnicode", "true");
	    props2.put("characterEncoding", "UTF-8"); // ensure charset is utf8 here
	    Connection conn2 = getConnectionWithProps(props2);
	    PreparedStatement pstmt2 = 
	        conn2.prepareStatement("INSERT INTO testSetNClobServer (c1, c2) VALUES (?, ?)");
	    nclob1 = conn2.createNClob();
	    nclob1.setString(1, "aaa");
	    pstmt2.setNClob(1, nclob1);
	    pstmt2.setNClob(2, new StringReader(
	            new String(new char[81921])), 81921); // 10 Full Long Data Packet's chars + 1 char
	    pstmt2.execute();
	    ResultSet rs2 = this.stmt.executeQuery("SELECT c1, c2 FROM testSetNClobServer");
	    rs2.next();
	    assertEquals("aaa", rs2.getString(1));
	    assertEquals(new String(new char[81921]), rs2.getString(2));
	    rs2.close();
	    pstmt2.close();
	    conn2.close();
	}

	/**
	 * Tests for PreparedStatement.setNString()
	 * 
	 * @throws Exception
	 */
	public void testSetNString() throws Exception {
	    // suppose sql_mode don't include "NO_BACKSLASH_ESCAPES"
	    
	    createTable("testSetNString", "(c1 NATIONAL CHARACTER(10), c2 NATIONAL CHARACTER(10), " +
	            "c3 NATIONAL CHARACTER(10)) DEFAULT CHARACTER SET cp932");
	    Properties props1 = new Properties();
	    props1.put("useServerPrepStmts", "false"); // use client-side prepared statement
	    props1.put("useUnicode", "true");
	    props1.put("characterEncoding", "MS932"); // ensure charset isn't utf8 here
	    Connection conn1 = getConnectionWithProps(props1);
	    PreparedStatement pstmt1 = 
	        conn1.prepareStatement("INSERT INTO testSetNString (c1, c2, c3) VALUES (?, ?, ?)");
	    pstmt1.setNString(1, null);
	    pstmt1.setNString(2, "aaa");
	    pstmt1.setNString(3, "\'aaa\'");
	    pstmt1.execute();
	    ResultSet rs1 = this.stmt.executeQuery("SELECT c1, c2, c3 FROM testSetNString");
	    rs1.next();
	    assertEquals(null, rs1.getString(1));
	    assertEquals("aaa", rs1.getString(2));
	    assertEquals("\'aaa\'", rs1.getString(3));
	    rs1.close();
	    pstmt1.close();
	    conn1.close();
	    
	    createTable("testSetNString", "(c1 NATIONAL CHARACTER(10), c2 NATIONAL CHARACTER(10), " +
	    "c3 NATIONAL CHARACTER(10)) DEFAULT CHARACTER SET cp932");
	    Properties props2 = new Properties();
	    props2.put("useServerPrepStmts", "false"); // use client-side prepared statement
	    props2.put("useUnicode", "true");
	    props2.put("characterEncoding", "UTF-8"); // ensure charset is utf8 here
	    Connection conn2 = getConnectionWithProps(props2);
	    PreparedStatement pstmt2 = 
	        conn2.prepareStatement("INSERT INTO testSetNString (c1, c2, c3) VALUES (?, ?, ?)");
	    pstmt2.setNString(1, null);
	    pstmt2.setNString(2, "aaa");
	    pstmt2.setNString(3, "\'aaa\'");
	    pstmt2.execute();
	    ResultSet rs2 = this.stmt.executeQuery("SELECT c1, c2, c3 FROM testSetNString");
	    rs2.next();
	    assertEquals(null, rs2.getString(1));
	    assertEquals("aaa", rs2.getString(2));
	    assertEquals("\'aaa\'", rs2.getString(3));
	    rs2.close();
	    pstmt2.close();
	    conn2.close();
	}

	/**
	 * Tests for ServerPreparedStatement.setNString()
	 * 
	 * @throws Exception
	 */
	public void testSetNStringServer() throws Exception {
	    createTable("testSetNStringServer", "(c1 NATIONAL CHARACTER(10))");
	    Properties props1 = new Properties();
	    props1.put("useServerPrepStmts", "true"); // use server-side prepared statement
	    props1.put("useUnicode", "true");
	    props1.put("characterEncoding", "latin1"); // ensure charset isn't utf8 here
	    Connection conn1 = getConnectionWithProps(props1);
	    PreparedStatement pstmt1 = 
	        conn1.prepareStatement("INSERT INTO testSetNStringServer (c1) VALUES (?)");
	    try {
	        pstmt1.setNString(1, "aaa");
	        fail();
	    } catch (SQLException e) {
	        // ok
	        assertEquals("Can not call setNString() when connection character set isn't UTF-8",
	            e.getMessage());  
	    }
	    pstmt1.close();
	    conn1.close();
	    
	    createTable("testSetNStringServer", "(c1 NATIONAL CHARACTER(10))");
	    Properties props2 = new Properties();
	    props2.put("useServerPrepStmts", "true"); // use server-side prepared statement
	    props2.put("useUnicode", "true");
	    props2.put("characterEncoding", "UTF-8"); // ensure charset is utf8 here
	    Connection conn2 = getConnectionWithProps(props2);
	    PreparedStatement pstmt2 = 
	        conn2.prepareStatement("INSERT INTO testSetNStringServer (c1) VALUES (?)");
	    pstmt2.setNString(1, "\'aaa\'");
	    pstmt2.execute();
	    ResultSet rs2 = this.stmt.executeQuery("SELECT c1 FROM testSetNStringServer");
	    rs2.next();
	    assertEquals("\'aaa\'", rs2.getString(1));
	    rs2.close();
	    pstmt2.close();
	    conn2.close();
	}

	/**
	 * Tests for ResultSet.updateNCharacterStream()
	 * 
	 * @throws Exception
	 */
	public void testUpdateNCharacterStream() throws Exception {
	    createTable("testUpdateNCharacterStream", 
	            "(c1 CHAR(10) PRIMARY KEY, c2 NATIONAL CHARACTER(10)) default character set sjis");
	    Properties props1 = new Properties();
	    props1.put("useServerPrepStmts", "true"); // use server-side prepared statement
	    props1.put("characterEncoding", "UTF-8"); // ensure charset isn't utf8 here
	    Connection conn1 = getConnectionWithProps(props1);
	    PreparedStatement pstmt1 = conn1.prepareStatement(
	            "INSERT INTO testUpdateNCharacterStream (c1, c2) VALUES (?, ?)");
	    pstmt1.setString(1, "1");
	    pstmt1.setNCharacterStream(2, new StringReader("aaa"), 3);
	    pstmt1.execute();
	    Statement stmt1 = conn1.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
	    ResultSet rs1 = stmt1.executeQuery("SELECT c1, c2 FROM testUpdateNCharacterStream");
	    rs1.next();
	    rs1.updateNCharacterStream("c2", new StringReader("bbb"), 3);
	    rs1.updateRow();
	    rs1.moveToInsertRow();
	    rs1.updateString("c1", "2");
	    rs1.updateNCharacterStream("c2", new StringReader("ccc"), 3);
	    rs1.insertRow();
	    ResultSet rs2 = stmt1.executeQuery("SELECT c1, c2 FROM testUpdateNCharacterStream");
	    rs2.next();
	    assertEquals("1", rs2.getString("c1"));
	    assertEquals("bbb", rs2.getNString("c2"));
	    rs2.next();
	    assertEquals("2", rs2.getString("c1"));
	    assertEquals("ccc", rs2.getNString("c2"));
	    pstmt1.close();
	    stmt1.close();
	    conn1.close();
	    
	    createTable("testUpdateNCharacterStream", 
	            "(c1 CHAR(10) PRIMARY KEY, c2 CHAR(10)) default character set sjis"); // sjis field
	    Properties props2 = new Properties();
	    props2.put("useServerPrepStmts", "true"); // use server-side prepared statement
	    props2.put("characterEncoding", "SJIS"); // ensure charset isn't utf8 here
	    Connection conn2 = getConnectionWithProps(props2);
	    PreparedStatement pstmt2 = conn2.prepareStatement(
	            "INSERT INTO testUpdateNCharacterStream (c1, c2) VALUES (?, ?)");
	    pstmt2.setString(1, "1");
	    pstmt2.setString(2, "aaa");
	    pstmt2.execute();
	    Statement stmt2 = conn2.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
	    ResultSet rs3 = stmt2.executeQuery("SELECT c1, c2 FROM testUpdateNCharacterStream");
	    rs3.next();
	    try {
	        rs3.updateNCharacterStream("c2", new StringReader("bbb"), 3); // field's charset isn't utf8
	        fail();
	    } catch (SQLException ex) {
	        assertEquals("Can not call updateNCharacterStream() when field's character set isn't UTF-8",
	                ex.getMessage());
	    }
	    rs3.close();
	    pstmt2.close();
	    stmt2.close();
	    conn2.close();  
	}

	/**
	 * Tests for ResultSet.updateNClob()
	 * 
	 * @throws Exception
	 */
	public void testUpdateNClob() throws Exception {
	    createTable("testUpdateNChlob", 
	            "(c1 CHAR(10) PRIMARY KEY, c2 NATIONAL CHARACTER(10)) default character set sjis");
	    Properties props1 = new Properties();
	    props1.put("useServerPrepStmts", "true"); // use server-side prepared statement
	    props1.put("characterEncoding", "UTF-8"); // ensure charset isn't utf8 here
	    Connection conn1 = getConnectionWithProps(props1);
	    PreparedStatement pstmt1 = conn1.prepareStatement(
	            "INSERT INTO testUpdateNChlob (c1, c2) VALUES (?, ?)");
	    pstmt1.setString(1, "1");
	    NClob nClob1 = conn1.createNClob();
	    nClob1.setString(1, "aaa");
	    pstmt1.setNClob(2, nClob1);
	    pstmt1.execute();
	    Statement stmt1 = conn1.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
	    ResultSet rs1 = stmt1.executeQuery("SELECT c1, c2 FROM testUpdateNChlob");
	    rs1.next();
	    NClob nClob2 = conn1.createNClob();
	    nClob2.setString(1, "bbb");
	    rs1.updateNClob("c2", nClob2);
	    rs1.updateRow();
	    rs1.moveToInsertRow();
	    rs1.updateString("c1", "2");
	    NClob nClob3 = conn1.createNClob();
	    nClob3.setString(1, "ccc");
	    rs1.updateNClob("c2", nClob3);
	    rs1.insertRow();
	    ResultSet rs2 = stmt1.executeQuery("SELECT c1, c2 FROM testUpdateNChlob");
	    rs2.next();
	    assertEquals("1", rs2.getString("c1"));
	    assertEquals("bbb", rs2.getNString("c2"));
	    rs2.next();
	    assertEquals("2", rs2.getString("c1"));
	    assertEquals("ccc", rs2.getNString("c2"));
	    pstmt1.close();
	    stmt1.close();
	    conn1.close();
	    
	    createTable("testUpdateNChlob", 
	            "(c1 CHAR(10) PRIMARY KEY, c2 CHAR(10)) default character set sjis"); // sjis field
	    Properties props2 = new Properties();
	    props2.put("useServerPrepStmts", "true"); // use server-side prepared statement
	    props2.put("characterEncoding", "SJIS"); // ensure charset isn't utf8 here
	    Connection conn2 = getConnectionWithProps(props2);
	    PreparedStatement pstmt2 = conn2.prepareStatement(
	            "INSERT INTO testUpdateNChlob (c1, c2) VALUES (?, ?)");
	    pstmt2.setString(1, "1");
	    pstmt2.setString(2, "aaa");
	    pstmt2.execute();
	    Statement stmt2 = conn2.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
	    ResultSet rs3 = stmt2.executeQuery("SELECT c1, c2 FROM testUpdateNChlob");
	    rs3.next();
	    NClob nClob4 = conn2.createNClob();
	    nClob4.setString(1, "bbb");
	    try {
	        rs3.updateNClob("c2", nClob4); // field's charset isn't utf8
	        fail();
	    } catch (SQLException ex) {
	        assertEquals("Can not call updateNClob() when field's character set isn't UTF-8",
	                ex.getMessage());
	    }
	    rs3.close();
	    pstmt2.close();
	    stmt2.close();
	    conn2.close();  
	}

	/**
	 * Tests for ResultSet.updateNString()
	 * 
	 * @throws Exception
	 */
	public void testUpdateNString() throws Exception {
	    createTable("testUpdateNString", 
	            "(c1 CHAR(10) PRIMARY KEY, c2 NATIONAL CHARACTER(10)) default character set sjis");
	    Properties props1 = new Properties();
	    props1.put("useServerPrepStmts", "true"); // use server-side prepared statement
	    props1.put("characterEncoding", "UTF-8"); // ensure charset is utf8 here
	    Connection conn1 = getConnectionWithProps(props1);
	    PreparedStatement pstmt1 = conn1.prepareStatement(
	            "INSERT INTO testUpdateNString (c1, c2) VALUES (?, ?)");
	    pstmt1.setString(1, "1");
	    pstmt1.setNString(2, "aaa");
	    pstmt1.execute();
	    Statement stmt1 = conn1.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
	    ResultSet rs1 = stmt1.executeQuery("SELECT c1, c2 FROM testUpdateNString");
	    rs1.next();
	    rs1.updateNString("c2", "bbb");
	    rs1.updateRow();
	    rs1.moveToInsertRow();
	    rs1.updateString("c1", "2");
	    rs1.updateNString("c2", "ccc");
	    rs1.insertRow();
	    ResultSet rs2 = stmt1.executeQuery("SELECT c1, c2 FROM testUpdateNString");
	    rs2.next();
	    assertEquals("1", rs2.getString("c1"));
	    assertEquals("bbb", rs2.getNString("c2"));
	    rs2.next();
	    assertEquals("2", rs2.getString("c1"));
	    assertEquals("ccc", rs2.getNString("c2"));
	    pstmt1.close();
	    stmt1.close();
	    conn1.close();
	    
	    createTable("testUpdateNString", 
	            "(c1 CHAR(10) PRIMARY KEY, c2 CHAR(10)) default character set sjis"); // sjis field
	    Properties props2 = new Properties();
	    props2.put("useServerPrepStmts", "true"); // use server-side prepared statement
	    props2.put("characterEncoding", "SJIS"); // ensure charset isn't utf8 here
	    Connection conn2 = getConnectionWithProps(props2);
	    PreparedStatement pstmt2 = conn2.prepareStatement(
	            "INSERT INTO testUpdateNString (c1, c2) VALUES (?, ?)");
	    pstmt2.setString(1, "1");
	    pstmt2.setString(2, "aaa");
	    pstmt2.execute();
	    Statement stmt2 = conn2.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
	    ResultSet rs3 = stmt2.executeQuery("SELECT c1, c2 FROM testUpdateNString");
	    rs3.next();
	    try {
	        rs3.updateNString("c2", "bbb"); // field's charset isn't utf8
	        fail();
	    } catch (SQLException ex) {
	        assertEquals("Can not call updateNString() when field's character set isn't UTF-8",
	                ex.getMessage());
	    }
	    rs3.close();
	    pstmt2.close();
	    stmt2.close();
	    conn2.close();      
	}
}