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

import testsuite.BaseTestCase;


/**
 * Regression tests for the Statement class
 * 
 * @author Mark Matthews
 */
public class StatementRegressionTest
    extends BaseTestCase {

    /**
     * Constructor for StatementRegressionTest.
     * @param name
     */
    public StatementRegressionTest(String name) {
        super(name);
    }

    /**
     * Tests a bug where Statement.setFetchSize() does not
     * work for values other than 0 or Integer.MIN_VALUE
     */
    public void testSetFetchSize()
                          throws Exception {

        int oldFetchSize = stmt.getFetchSize();

        try {
            stmt.setFetchSize(10);
        } finally {
            stmt.setFetchSize(oldFetchSize);
        }
    }
    
    public void testMaxRowsProps() throws Exception {
        /*
        Driver driver =
        //props.put("maxRows", "3"); //(1)
        Connection connection =
        driver.connect("jdbc:mysql://tibi/ebs", props);
        Statement statement = connection.createStatement();
        //statement.execute("set option SQL_SELECT_LIMIT=3");
        //(2)
        //statement.setMaxRows(3); //(3)
        ResultSet rs = statement.executeQuery("select * from
        sometable");
        while(rs.next())
         System.out.println(rs.getString(1));
         */
    }
}