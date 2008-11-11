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

package com.mysql.jdbc;

import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;

import java.util.List;

import com.mysql.jdbc.Field;

public class JDBC4DatabaseMetaData extends DatabaseMetaData {
	public JDBC4DatabaseMetaData(ConnectionImpl connToSet, String databaseToSet) {
		super(connToSet, databaseToSet);
	}

	public RowIdLifetime getRowIdLifetime() throws SQLException {
		return RowIdLifetime.ROWID_UNSUPPORTED;
	}

	/**
     * Returns true if this either implements the interface argument or is directly or indirectly a wrapper
     * for an object that does. Returns false otherwise. If this implements the interface then return true,
     * else if this is a wrapper then return the result of recursively calling <code>isWrapperFor</code> on the wrapped
     * object. If this does not implement the interface and is not a wrapper, return false.
     * This method should be implemented as a low-cost operation compared to <code>unwrap</code> so that
     * callers can use this method to avoid expensive <code>unwrap</code> calls that may fail. If this method
     * returns true then calling <code>unwrap</code> with the same argument should succeed.
     *
     * @param interfaces a Class defining an interface.
     * @return true if this implements the interface or directly or indirectly wraps an object that does.
     * @throws java.sql.SQLException  if an error occurs while determining whether this is a wrapper
     * for an object with the given interface.
     * @since 1.6
     */
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		// This works for classes that aren't actually wrapping
		// anything
		return iface.isInstance(this);
	}

    /**
     * Returns an object that implements the given interface to allow access to non-standard methods,
     * or standard methods not exposed by the proxy.
     * The result may be either the object found to implement the interface or a proxy for that object.
     * If the receiver implements the interface then that is the object. If the receiver is a wrapper
     * and the wrapped object implements the interface then that is the object. Otherwise the object is
     *  the result of calling <code>unwrap</code> recursively on the wrapped object. If the receiver is not a
     * wrapper and does not implement the interface, then an <code>SQLException</code> is thrown.
     *
     * @param iface A Class defining an interface that the result must implement.
     * @return an object that implements the interface. May be a proxy for the actual implementing object.
     * @throws java.sql.SQLException If no object found that implements the interface 
     * @since 1.6
     */
    public <T> T unwrap(java.lang.Class<T> iface) throws java.sql.SQLException {
    	try {
    		// This works for classes that aren't actually wrapping
    		// anything
            return iface.cast(this);
        } catch (ClassCastException cce) {
            throw SQLError.createSQLException("Unable to unwrap to " + iface.toString(), 
            		SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.conn.getExceptionInterceptor());
        }
    }

    /**
	 * Retrieves a list of the client info properties 
	 * that the driver supports.  The result set contains the following columns
	 * <p>
         * <ol>
	 * <li><b>NAME</b> String=> The name of the client info property<br>
	 * <li><b>MAX_LEN</b> int=> The maximum length of the value for the property<br>
	 * <li><b>DEFAULT_VALUE</b> String=> The default value of the property<br>
	 * <li><b>DESCRIPTION</b> String=> A description of the property.  This will typically 
	 * 						contain information as to where this property is 
	 * 						stored in the database.
	 * </ol>
         * <p>
	 * The <code>ResultSet</code> is sorted by the NAME column
	 * <p>
	 * @return	A <code>ResultSet</code> object; each row is a supported client info
         * property
	 * <p>
	 *  @exception SQLException if a database access error occurs
	 * <p>
	 * @since 1.6
	 */
	public ResultSet getClientInfoProperties()
		throws SQLException {
		// We don't have any built-ins, we actually support whatever
		// the client wants to provide, however we don't have a way
		// to express this with the interface given
		Field[] fields = new Field[4];
		fields[0] = new Field("", "NAME", Types.VARCHAR, 255);
		fields[1] = new Field("", "MAX_LEN", Types.INTEGER, 10);
		fields[2] = new Field("", "DEFAULT_VALUE", Types.VARCHAR, 255);
		fields[3] = new Field("", "DESCRIPTION", Types.VARCHAR, 255);
		
		ArrayList tuples = new ArrayList();
		
		return buildResultSet(fields, tuples, this.conn);
	}
	
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    	return false;
    }
    
    /**
     * Retrieves a description of the  system and user functions available 
     * in the given catalog.
     * <P>
     * Only system and user function descriptions matching the schema and
     * function name criteria are returned.  They are ordered by
     * <code>FUNCTION_CAT</code>, <code>FUNCTION_SCHEM</code>,
     * <code>FUNCTION_NAME</code> and 
     * <code>SPECIFIC_ NAME</code>.
     *
     * <P>Each function description has the the following columns:
     *  <OL>
     *	<LI><B>FUNCTION_CAT</B> String => function catalog (may be <code>null</code>)
     *	<LI><B>FUNCTION_SCHEM</B> String => function schema (may be <code>null</code>)
     *	<LI><B>FUNCTION_NAME</B> String => function name.  This is the name 
     * used to invoke the function
     *	<LI><B>REMARKS</B> String => explanatory comment on the function
     * <LI><B>FUNCTION_TYPE</B> short => kind of function:
     *      <UL>
     *      <LI>functionResultUnknown - Cannot determine if a return value
     *       or table will be returned
     *      <LI> functionNoTable- Does not return a table
     *      <LI> functionReturnsTable - Returns a table
     *      </UL>
     *	<LI><B>SPECIFIC_NAME</B> String  => the name which uniquely identifies 
     *  this function within its schema.  This is a user specified, or DBMS
     * generated, name that may be different then the <code>FUNCTION_NAME</code> 
     * for example with overload functions
     *  </OL>
     * <p>
     * A user may not have permission to execute any of the functions that are
     * returned by <code>getFunctions</code>
     *
     * @param catalog a catalog name; must match the catalog name as it
     *        is stored in the database; "" retrieves those without a catalog;
     *        <code>null</code> means that the catalog name should not be used to narrow
     *        the search
     * @param schemaPattern a schema name pattern; must match the schema name
     *        as it is stored in the database; "" retrieves those without a schema;
     *        <code>null</code> means that the schema name should not be used to narrow
     *        the search
     * @param functionNamePattern a function name pattern; must match the
     *        function name as it is stored in the database 
     * @return <code>ResultSet</code> - each row is a function description 
     * @exception SQLException if a database access error occurs
     * @see #getSearchStringEscape 
     * @since 1.6
     */
    public java.sql.ResultSet getFunctions(String catalog, String schemaPattern,
			    String functionNamePattern) throws SQLException {
    	Field[] fields = new Field[6];
    	
    	fields[0] = new Field("", "FUNCTION_CAT", Types.CHAR, 255);
		fields[1] = new Field("", "FUNCTION_SCHEM", Types.CHAR, 255);
		fields[2] = new Field("", "FUNCTION_NAME", Types.CHAR, 255);
		fields[3] = new Field("", "REMARKS", Types.CHAR, 255);
		fields[4] = new Field("", "FUNCTION_TYPE", Types.SMALLINT, 6);
		fields[5] = new Field("", "SPECIFIC_NAME", Types.CHAR, 255);
		
		return getProceduresAndOrFunctions(
				fields,
				catalog,
				schemaPattern,
				functionNamePattern,
				false,
				true);
    }
    
	protected int getJDBC4FunctionNoTableConstant() {
		return functionNoTable;
	}
}