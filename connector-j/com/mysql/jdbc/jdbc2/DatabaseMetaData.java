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
 * @version $Id: DatabaseMetaData.java,v 1.3 2002/04/25 01:09:07 mark_matthews Exp $
 */

package com.mysql.jdbc.jdbc2;

import java.sql.*;

public class DatabaseMetaData
	extends com.mysql.jdbc.DatabaseMetaData
	implements java.sql.DatabaseMetaData {

	public DatabaseMetaData(com.mysql.jdbc.Connection Conn, String Database) {
		super(Conn, Database);
	}

	/**
	 * JDBC 3.0
	 */
	
	public boolean supportsGetGeneratedKeys()
	{
		return true;
	}
	
	/**
	 * JDBC 2.0
	 *
	 * Does the database support the given result set type?
	 *
	 * @param type defined in java.sql.ResultSet
	 * @return true if so 
	 * @exception SQLException if a database-access error occurs.
	 * @see Connection
	 */

	public boolean supportsResultSetType(int type) throws SQLException {
		return (type == ResultSet.TYPE_SCROLL_INSENSITIVE);
	}

	/**
	 * JDBC 2.0
	 *
	 * Does the database support the concurrency type in combination
	 * with the given result set type?
	 *
	 * @param type defined in java.sql.ResultSet
	 * @param concurrency type defined in java.sql.ResultSet
	 * @return true if so 
	 * @exception SQLException if a database-access error occurs.
	 * @see Connection
	 */

	public boolean supportsResultSetConcurrency(int type, int concurrency)
		throws SQLException {
		return (
			type == ResultSet.TYPE_SCROLL_SENSITIVE
				&& concurrency == ResultSet.CONCUR_READ_ONLY);
	}

	/**
	 * JDBC 2.0
	 *
	 * Determine whether a result set's own changes visible.
	 *
	 * @param result set type, i.e. ResultSet.TYPE_XXX
	 * @return true if changes are visible for the result set type
	 * @exception SQLException if a database-access error occurs.
	 */

	public boolean ownUpdatesAreVisible(int type) throws SQLException {
		return false;
	}

	public boolean ownDeletesAreVisible(int type) throws SQLException {
		return false;
	}

	public boolean ownInsertsAreVisible(int type) throws SQLException {
		return false;
	}

	/**
	 * JDBC 2.0
	 *
	 * Determine whether changes made by others are visible.
	 *
	 * @param result set type, i.e. ResultSet.TYPE_XXX
	 * @return true if changes are visible for the result set type
	 * @exception SQLException if a database-access error occurs.
	 */

	public boolean othersUpdatesAreVisible(int type) throws SQLException {
		return false;
	}

	public boolean othersDeletesAreVisible(int type) throws SQLException {
		return false;
	}

	public boolean othersInsertsAreVisible(int type) throws SQLException {
		return false;
	}

	/**
	 * JDBC 2.0
	 *
	 * Determine whether or not a visible row update can be detected by 
	 * calling ResultSet.rowUpdated().
	 *
	 * @param result set type, i.e. ResultSet.TYPE_XXX
	 * @return true if changes are detected by the resultset type
	 * @exception SQLException if a database-access error occurs.
	 */

	public boolean updatesAreDetected(int type) throws SQLException {
		return false;
	}

	/**
	 * JDBC 2.0
	 *
	 * Determine whether or not a visible row delete can be detected by 
	 * calling ResultSet.rowDeleted().  If deletesAreDetected()
	 * returns false, then deleted rows are removed from the result set.
	 *
	 * @param result set type, i.e. ResultSet.TYPE_XXX
	 * @return true if changes are detected by the resultset type
	 * @exception SQLException if a database-access error occurs.
	 */

	public boolean deletesAreDetected(int type) throws SQLException {
		return false;
	}

	/**
	 * JDBC 2.0
	 *
	 * Determine whether or not a visible row insert can be detected
	 * by calling ResultSet.rowInserted().
	 *
	 * @param result set type, i.e. ResultSet.TYPE_XXX
	 * @return true if changes are detected by the resultset type
	 * @exception SQLException if a database-access error occurs.
	 */

	public boolean insertsAreDetected(int type) throws SQLException {
		return false;
	}

	/**
	 * JDBC 2.0
	 *
	 * Return true if the driver supports batch updates, else return false.
	 */

	public boolean supportsBatchUpdates() throws SQLException {
		return true;
	}

	/**
	 * JDBC 2.0
	 *
	 * Get a description of the user-defined types defined in a particular
	 * schema.  Schema specific UDTs may have type JAVA_OBJECT, STRUCT, 
	 * or DISTINCT.
	 *
	 * <P>Only types matching the catalog, schema, type name and type  
	 * criteria are returned.  They are ordered by DATA_TYPE, TYPE_SCHEM 
	 * and TYPE_NAME.  The type name parameter may be a fully qualified 
	 * name.  In this case, the catalog and schemaPattern parameters are
	 * ignored.
	 *
	 * <P>Each type description has the following columns:
	 *  <OL>
	 *	<LI><B>TYPE_CAT</B> String => the type's catalog (may be null)
	 *	<LI><B>TYPE_SCHEM</B> String => type's schema (may be null)
	 *	<LI><B>TYPE_NAME</B> String => type name
	 *  <LI><B>CLASS_NAME</B> String => Java class name
	 *	<LI><B>DATA_TYPE</B> String => type value defined in java.sql.Types.  
	 *  One of JAVA_OBJECT, STRUCT, or DISTINCT
	 *	<LI><B>REMARKS</B> String => explanatory comment on the type
	 *  </OL>
	 *
	 * <P><B>Note:</B> If the driver does not support UDTs then an empty
	 * result set is returned.
	 *
	 * @param catalog a catalog name; "" retrieves those without a
	 * catalog; null means drop catalog name from the selection criteria
	 * @param schemaPattern a schema name pattern; "" retrieves those
	 * without a schema
	 * @param typeNamePattern a type name pattern; may be a fully qualified
	 * name
	 * @param types a list of user-named types to include (JAVA_OBJECT, 
	 * STRUCT, or DISTINCT); null returns all types 
	 * @return ResultSet - each row is a type description
	 * @exception SQLException if a database-access error occurs.
	 */

	public java.sql.ResultSet getUDTs(
		String catalog,
		String schemaPattern,
		String typeNamePattern,
		int[] types)
		throws SQLException {
		throw new NotImplemented();
	}

	/**
	 * JDBC 2.0
	 *
	 * Return the connection that produced this metadata object.
	 */

	public java.sql.Connection getConnection() throws SQLException {
		return (java.sql.Connection) _conn;
	}

	protected java.sql.ResultSet buildResultSet(
		com.mysql.jdbc.Field[] Fields,
		java.util.Vector Rows,
		com.mysql.jdbc.Connection Conn) {
		return new com.mysql.jdbc.jdbc2.ResultSet(Fields, Rows, Conn);
	}
}
