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

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import testsuite.BaseTestCase;


/**
 * Regression tests for DatabaseMetaData
 * 
 * @author Mark Matthews
 * @version $Id$
 */
public class MetaDataRegressionTest
    extends BaseTestCase {

    //~ Constructors ..........................................................

    /**
     * Creates a new MetaDataRegressionTest.
     * 
     * @param name the name of the test
     */
    public MetaDataRegressionTest(String name) {
        super(name);
    }

    //~ Methods ...............................................................

    /**
     * Tests bug reported by OpenOffice team with getColumns and LONGBLOB
     * 
     * @throws Exception if any errors occur
     */
    public void testGetColumns()
                        throws Exception {

        try {
            stmt.execute(
                    "CREATE TABLE IF NOT EXISTS longblob_regress(field_1 longblob)");

            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet dbmdRs = null;

            try {
                dbmdRs = dbmd.getColumns("", "", "longblob_regress", "%");

                while (dbmdRs.next()) {

                    int size = dbmdRs.getInt(7);
                }
            } finally {

                if (dbmdRs != null) {

                    try {
                        dbmdRs.close();
                    } catch (SQLException ex) {
                        ;
                    }
                }
            }
        } finally {
            stmt.execute("DROP TABLE IF EXISTS longblob_regress");
        }
    }
}