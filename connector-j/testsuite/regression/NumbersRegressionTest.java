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
package testsuite.regression;

import java.sql.ResultSetMetaData;

import testsuite.BaseTestCase;


/**
 * Tests various number-handling issues that have arrisen in the
 * JDBC driver at one time or another.
 * 
 * @author Mark Matthews
 */
public class NumbersRegressionTest
    extends BaseTestCase {

    /**
     * Constructor for NumbersRegressionTest.
     * @param name the test name
     */
    public NumbersRegressionTest(String name) {
        super(name);
    }

    /** 
     * Runs all test cases
     */
    public static void main(String[] args)
                     throws Exception {
        new NumbersRegressionTest("testBigInt").run();
    }

    /**
     * Tests that BIGINT retrieval works correctly.
     */
    public void testBigInt()
                    throws Exception {

        try {
            stmt.executeUpdate("DROP TABLE IF EXISTS bigIntRegression");
            stmt.executeUpdate(
                    "CREATE TABLE bigIntRegression ( val BIGINT NOT NULL)");
            stmt.executeUpdate(
                    "INSERT INTO bigIntRegression VALUES (6692730313872877584)");
            rs = stmt.executeQuery("SELECT val FROM bigIntRegression");

            while (rs.next()) {

                String retrieveAsString = rs.getString(1);
               
                // check retrieval
                long retrieveAsLong = rs.getLong(1);
                assertTrue(retrieveAsLong == 6692730313872877584L);
               
            }

            rs.close();
            stmt.executeUpdate("DROP TABLE IF EXISTS bigIntRegression");

            String bigIntAsString = "6692730313872877584";
            long bigIntAsLong = 6692730313872877584L;
            long parsedBigIntAsLong = Long.parseLong(bigIntAsString);
            // check JDK parsing
            assertTrue(bigIntAsString.equals(String.valueOf(parsedBigIntAsLong)));
        } finally {
            stmt.executeUpdate("DROP TABLE IF EXISTS bigIntRegression");
        }
    }
    
    public void testPrecisionAndScale() throws Exception {
        testPrecisionForType("TINYINT", 8, -1, false);
        testPrecisionForType("TINYINT", 8, -1, true);
        testPrecisionForType("SMALLINT", 8, -1, false);
        testPrecisionForType("SMALLINT", 8, -1, true);
        testPrecisionForType("MEDIUMINT", 8, -1, false);
        testPrecisionForType("MEDIUMINT", 8, -1, true);
        testPrecisionForType("INT", 8, -1, false);
        testPrecisionForType("INT", 8, -1, true);
        testPrecisionForType("BIGINT", 8, -1, false);
        testPrecisionForType("BIGINT", 8, -1, true);
        
        testPrecisionForType("FLOAT", 8, 4, false);
        testPrecisionForType("FLOAT", 8, 4, true);
        testPrecisionForType("DOUBLE", 8, 4, false);
        testPrecisionForType("DOUBLE", 8, 4, true);
        
        testPrecisionForType("DECIMAL", 8, 4, false);
        testPrecisionForType("DECIMAL", 8, 4, true);
        
        testPrecisionForType("DECIMAL", 9, 0, false);
        testPrecisionForType("DECIMAL", 9, 0, true);
        
    }
        
       
    
    private void testPrecisionForType(String typeName, int m, int d, boolean unsigned) throws Exception {
         try {
            stmt.executeUpdate("DROP TABLE IF EXISTS precisionAndScaleRegression");
            StringBuffer createStatement = new StringBuffer("CREATE TABLE precisionAndScaleRegression ( val ");
            createStatement.append(typeName);
            createStatement.append("(");
            createStatement.append(m);
            
            if (d != -1) {
                createStatement.append(",");
                createStatement.append(d);
            }
            
            createStatement.append(")");
            
            if (unsigned) {
                createStatement.append(" UNSIGNED ");
            }
            
            createStatement.append(" NOT NULL)");
            
            stmt.executeUpdate(createStatement.toString());
                    
            rs = stmt.executeQuery("SELECT val FROM precisionAndScaleRegression");
            
            ResultSetMetaData rsmd = rs.getMetaData();
            assertTrue("Precision returned incorrectly for type " + typeName + ", " + m + " != rsmd.getPrecision() = " + rsmd.getPrecision(1), rsmd.getPrecision(1) == m);
            
            if (d != -1) {
                assertTrue("Scale returned incorrectly for type " + typeName +", d  != rsmd.getScale() = " + rsmd.getScale(1), rsmd.getScale(1) == d);
            }
            
                    
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (Exception ex) {
                    // ignore
                }
            }
            
            stmt.executeUpdate("DROP TABLE IF EXISTS precisionAndScaleRegression");
        }
    }
}