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

import java.sql.*;

import testsuite.BaseTestCase;


public class StatementsTest
    extends BaseTestCase
{

    //~ Constructors ..........................................................

    public StatementsTest(String name)
    {
        super(name);
    }

    //~ Methods ...............................................................

    public static void main(String[] args)
                     throws Exception
    {
        new StatementsTest("testInsert").run();
		new StatementsTest("testAutoIncrement").run();
        new StatementsTest("testPreparedStatement").run();
        new StatementsTest("testPreparedStatementBatch").run();
    }

    public void setUp()
               throws Exception
    {
        super.setUp();

        try {
            stmt.executeUpdate("DROP TABLE statement_test");
        } /* ignore */ catch (SQLException sqlEx) {
        }

        stmt.executeUpdate(
                "CREATE TABLE statement_test (id int not null primary key auto_increment, strdata1 varchar(255) not null, strdata2 varchar(255))");
    }

    public void tearDown()
                  throws Exception
    {
        stmt.executeUpdate("DROP TABLE statement_test");
        super.tearDown();
    }

    public void testAutoIncrement()
                           throws SQLException
    {
        stmt.executeUpdate(
                "INSERT INTO statement_test (strdata1) values ('blah')");

        int autoIncKeyFromApi = -1;
        rs = stmt.getGeneratedKeys();

        if (rs.next()) {
            autoIncKeyFromApi = rs.getInt(1);
        }
        else {
        	fail("Failed to retrieve AUTO_INCREMENT using Statement.getGeneratedKeys()");
        }

        int autoIncKeyFromFunc = -1;
        rs = stmt.executeQuery("SELECT LAST_INSERT_ID()");

        if (rs.next()) {
            autoIncKeyFromFunc = rs.getInt(1);
        }
        else {
        	fail("Failed to retrieve AUTO_INCREMENT using LAST_INSERT_ID()");
        }
        
        if (autoIncKeyFromApi != -1 && autoIncKeyFromFunc != -1) {
        	assertTrue("Key retrieved from API (" + autoIncKeyFromApi + ") does not match key retrieved from LAST_INSERT_ID() " + autoIncKeyFromFunc + ") function", autoIncKeyFromApi == autoIncKeyFromFunc);
        }
        else {
        	fail("AutoIncrement keys were '0'");
        }
    }

    public void testInsert()
                    throws SQLException
    {

        for (int i = 0; i < 10; i++) {

            int updateCount = stmt.executeUpdate(
                                      "INSERT INTO statement_test (strdata1,strdata2) values ('abcdefg', 'poi')");
            assertTrue("Update count must be '1', was '" + updateCount + 
                       "'", (updateCount == 1));
        }
    }

    public void testPreparedStatement()
                               throws SQLException
    {
        stmt.executeUpdate(
                "INSERT INTO statement_test (id, strdata1,strdata2) values (999,'abcdefg', 'poi')");
        pstmt = conn.prepareStatement(
                        "UPDATE statement_test SET strdata1=?, strdata2=? where id=?");
        pstmt.setString(1, "iop");
        pstmt.setString(2, "higjklmn");
        pstmt.setInt(3, 999);

        int updateCount = pstmt.executeUpdate();
        assertTrue("Update count must be '1', was '" + updateCount + "'", 
                   (updateCount == 1));
    }

    public void testPreparedStatementBatch()
                                    throws SQLException
    {
        pstmt = conn.prepareStatement(
                        "INSERT INTO " + 
                        "statement_test (strdata1, strdata2) VALUES (?,?)");

        for (int i = 0; i < 10; i++) {
            pstmt.setString(1, "batch_" + i);
            pstmt.setString(2, "batch_" + i);
            pstmt.addBatch();
        }

        int[] updateCounts = pstmt.executeBatch();

        for (int i = 0; i < updateCounts.length; i++) {
            assertTrue("Update count must be '1', was '" + updateCounts[i] + 
                       "'", (updateCounts[i] == 1));
        }
    }
}