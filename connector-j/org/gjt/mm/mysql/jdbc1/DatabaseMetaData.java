/**
 * JDBC Interface to Mysql functions
 *
 * <p>
 * This class provides information about the database as a whole.
 *
 * <p>
 * Many of the methods here return lists of information in ResultSets.
 * You can use the normal ResultSet methods such as getString and getInt
 * to retrieve the data from these ResultSets.  If a given form of
 * metadata is not available, these methods show throw a java.sql.SQLException.
 * 
 * <p>
 * Some of these methods take arguments that are String patterns.  These
 * methods all have names such as fooPattern.  Within a pattern String "%"
 * means match any substring of 0 or more characters and "_" means match
 * any one character.
 *
 * @author Mark Matthews <mmatthew@worldserver.com>
 * @version $Id$
 */

package org.gjt.mm.mysql.jdbc1;

import java.sql.*;

public class DatabaseMetaData extends org.gjt.mm.mysql.DatabaseMetaData
				      implements java.sql.DatabaseMetaData
{
  
    public DatabaseMetaData(org.gjt.mm.mysql.Connection Conn, String Database) 
    {
	super(Conn, Database);
    }

    protected java.sql.ResultSet buildResultSet(org.gjt.mm.mysql.Field[] Fields, 
						java.util.Vector Rows,
						org.gjt.mm.mysql.Connection Conn)
    {
	return new org.gjt.mm.mysql.jdbc1.ResultSet(Fields, Rows, Conn);
    }
};
