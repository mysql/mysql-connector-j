/*
 * MM JDBC Drivers for MySQL
 *
 * $Id$
 *
 * Copyright (C) 1998 Mark Matthews <mmatthew@worldserver.com>
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 * 
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the
 * Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA  02111-1307, USA.
 *
 * See the COPYING file located in the top-level-directory of
 * the archive of this library for complete text of license.
 *
 * Some portions:
 *
 * Copyright (c) 1996 Bradley McLean / Jeffrey Medeiros
 * Modifications Copyright (c) 1996/1997 Martin Rode
 * Copyright (c) 1997 Peter T Mount
 */

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

package com.mysql.jdbc;

import java.sql.*;
import java.net.*;
import java.io.*;
import java.util.StringTokenizer;
import java.util.Vector;

public abstract class DatabaseMetaData
{
	protected Connection _conn;

	protected String _database = null;
	
	protected String _quotedId = null;

	private static final byte[] _TABLE_AS_BYTES = "TABLE".getBytes();

	public DatabaseMetaData(Connection Conn, String Database)
	{
		_conn = Conn;
		_database = Database;
		
		try
		{
			_quotedId = _conn.supportsQuotedIdentifiers() ? getIdentifierQuoteString() : "";
		}
		catch (SQLException sqlEx)
		{
			// Forced by API, never thrown from getIdentifierQuoteString() in this
			// implementation.
		}
	}

	/**
	 * Can all the procedures returned by getProcedures be called by the
	 * current user?
	 *
	 * @return true if so
	 */

	public boolean allProceduresAreCallable() throws java.sql.SQLException
	{
		return false; // not likely we will ever check
	}

	/**
	 * Can all the tables returned by getTable be SELECTed by the
	 * current user?
	 *
	 * @return true if so
	 */

	public boolean allTablesAreSelectable() throws java.sql.SQLException
	{
		return false; // not likely we will ever check
	}

	/**
	 * What's the url for this database?
	 *
	 * @return the url or null if it can't be generated
	 */

	public String getURL() throws java.sql.SQLException
	{
		return _conn.getURL();
	}

	/**
	 * What's our user name as known to the database?
	 *
	 * @return our database user name
	 */

	public String getUserName() throws java.sql.SQLException
	{
		return _conn.getUser();
	}

	/**
	 * Is the database in read-only mode?
	 *
	 * @return true if so
	 */

	public boolean isReadOnly() throws java.sql.SQLException
	{
		return false; // We can't do this without parsing
		// the entire statement, ick.
	}

	/**
	 * Are NULL values sorted high?
	 *
	 * @return true if so
	 */

	public boolean nullsAreSortedHigh() throws java.sql.SQLException
	{
		return false;
	}

	/**
	 * Are NULL values sorted low?
	 *
	 * @return true if so
	 */

	public boolean nullsAreSortedLow() throws java.sql.SQLException
	{
		return !nullsAreSortedHigh();
	}

	/**
	 * Are NULL values sorted at the start regardless of sort order?
	 *
	 * @return true if so
	 */

	public boolean nullsAreSortedAtStart() throws java.sql.SQLException
	{
		return false;
	}

	/**
	 * Are NULL values sorted at the end regardless of sort order?
	 *
	 * @return true if so
	 */

	public boolean nullsAreSortedAtEnd() throws java.sql.SQLException
	{
		return false;
	}

	/**
	 * What's the name of this database product?
	 *
	 * @return database product name
	 */

	public String getDatabaseProductName() throws java.sql.SQLException
	{
		return "MySQL";
	}

	/**
	 * What's the version of this database product?
	 *
	 * @return database version
	 */

	public String getDatabaseProductVersion() throws java.sql.SQLException
	{
		return _conn.getServerVersion();
	}

	/**
	 * What's the name of this JDBC driver?
	 *
	 * @return JDBC driver name
	 */

	public String getDriverName() throws java.sql.SQLException
	{
		return "Mark Matthews' MySQL Driver";
	}

	/**
	 * What's the version of this JDBC driver?
	 *
	 * @return JDBC driver version
	 */

	public String getDriverVersion() throws java.sql.SQLException
	{
		return "2.0.14";
	}

	/**
	 * What's this JDBC driver's major version number?
	 *
	 * @return JDBC driver major version
	 */

	public int getDriverMajorVersion()
	{
		return Driver._MAJORVERSION;
	}

	/**
	 * What's this JDBC driver's minor version number?
	 *
	 * @return JDBC driver minor version number
	 */

	public int getDriverMinorVersion()
	{
		return Driver._MINORVERSION;
	}

	/**
	 * Does the database store tables in a local file?
	 *
	 * @return true if so
	 */

	public boolean usesLocalFiles() throws java.sql.SQLException
	{
		return false;
	}

	/**
	 * Does the database use a file for each table?
	 *
	 * @return true if the database uses a local file for each table
	 */

	public boolean usesLocalFilePerTable() throws java.sql.SQLException
	{
		return false;
	}

	/**
	 * Does the database support mixed case unquoted SQL identifiers?
	 *
	 * @return true if so
	 */

	public boolean supportsMixedCaseIdentifiers() throws java.sql.SQLException
	{
		return false;
	}

	/**
	 * Does the database store mixed case unquoted SQL identifiers in
	 * upper case?
	 *
	 * @return true if so
	 */

	public boolean storesUpperCaseIdentifiers() throws java.sql.SQLException
	{
		return false;
	}

	/**
	 * Does the database store mixed case unquoted SQL identifiers in
	 * lower case?
	 *
	 * @return true if so
	 */

	public boolean storesLowerCaseIdentifiers() throws java.sql.SQLException
	{
		return false;
	}

	/**
	 * Does the database store mixed case unquoted SQL identifiers in
	 * mixed case?
	 *
	 * @return true if so
	 */

	public boolean storesMixedCaseIdentifiers() throws java.sql.SQLException
	{
		return true;
	}

	/**
	 * Does the database support mixed case quoted SQL identifiers?
	 *
	 * A JDBC compliant driver will always return true.
	 *
	 * @return true if so
	 */

	public boolean supportsMixedCaseQuotedIdentifiers()
		throws java.sql.SQLException
	{
		return false;
	}

	/**
	 * Does the database store mixed case quoted SQL identifiers in
	 * upper case?
	 *
	 * A JDBC compliant driver will always return true.
	 *
	 * @return true if so
	 */

	public boolean storesUpperCaseQuotedIdentifiers() throws java.sql.SQLException
	{
		return false;
	}

	/**
	 * Does the database store mixed case quoted SQL identifiers in
	 * lower case?
	 *
	 * A JDBC compliant driver will always return false.
	 *
	 * @return true if so
	 */

	public boolean storesLowerCaseQuotedIdentifiers() throws java.sql.SQLException
	{
		return false;
	}

	/**
	 * Does the database store mixed case quoted SQL identifiers in
	 * mixed case?
	 *
	 * A JDBC compliant driver will always return false.
	 *
	 * @return true if so
	 */

	public boolean storesMixedCaseQuotedIdentifiers() throws java.sql.SQLException
	{
		return false;
	}

	/**
	 * What's the string used to quote SQL identifiers?
	 * This returns a space " " if identifier quoting isn't supported.
	 *
	 * A JDBC compliant driver always uses a double quote character.
	 *
	 * @return the quoting string
	 */

	public String getIdentifierQuoteString() throws java.sql.SQLException
	{
		if (_conn.supportsQuotedIdentifiers())
		{
			if (!_conn.useAnsiQuotedIdentifiers())
			{
				return "`";
			}
			else
			{
				return "\"";
			}
		}
		else
		{
			return " ";
		}
	}

	/**
	 * Get a comma separated list of all a database's SQL keywords
	 * that are NOT also SQL92 keywords.
	 *
	 * @return the list
	 */

	public String getSQLKeywords() throws java.sql.SQLException
	{
		return "AUTO_INCREMENT,BINARY,BLOB,ENUM,INFILE,LOAD,MEDIUMINT,OPTION,OUTFILE,REPLACE,SET,TEXT,UNSIGNED,ZEROFILL";
	}

	/**
	 * Get a comma separated list of math functions.
	 *
	 * @return the list
	 */

	public String getNumericFunctions() throws java.sql.SQLException
	{
		return "ABS,ACOS,ASIN,ATAN,ATAN2,BIT_COUNT,CEILING,COS,COT,DEGREES,EXP,FLOOR,LOG,LOG10,MAX,MIN,MOD,PI,POW,POWER,RADIANS,RAND,ROUND,SIN,SQRT,TAN,TRUNCATE";
	}

	/**
	 * Get a comma separated list of string functions.
	 *
	 * @return the list
	 */

	public String getStringFunctions() throws java.sql.SQLException
	{
		return "ACII,CHAR,CHAR_LENGTH,CHARACTER_LENGTH,CONCAT,ELT,FIELD,FIND_IN_SET,INSERT,INSTR,INTERVAL,LCASE,LEFT,LENGTH,LOCATE,LOWER,LTRIM,MID,POSITION,OCTET_LENGTH,REPEAT,REPLACE,REVERSE,RIGHT,RTRIM,SPACE,SOUNDEX,SUBSTRING,SUBSTRING_INDEX,TRIM,UCASE,UPPER";
	}

	/**
	 * Get a comma separated list of system functions.
	 *
	 * @return the list
	 */

	public String getSystemFunctions() throws java.sql.SQLException
	{
		return "DATABASE,USER,SYSTEM_USER,SESSION_USER,PASSWORD,ENCRYPT,LAST_INSERT_ID,VERSION";
	}

	/**
	 * Get a comma separated list of time and date functions.
	 *
	 * @return the list
	 */

	public String getTimeDateFunctions() throws java.sql.SQLException
	{
		return "DAYOFWEEK,WEEKDAY,DAYOFMONTH,DAYOFYEAR,MONTH,DAYNAME,MONTHNAME,QUARTER,WEEK,YEAR,HOUR,MINUTE,SECOND,PERIOD_ADD,PERIOD_DIFF,TO_DAYS,FROM_DAYS,DATE_FORMAT,TIME_FORMAT,CURDATE,CURRENT_DATE,CURTIME,CURRENT_TIME,NOW,SYSDATE,CURRENT_TIMESTAMP,UNIX_TIMESTAMP,FROM_UNIXTIME,SEC_TO_TIME,TIME_TO_SEC";
	}

	/**
	 * This is the string that can be used to escape '_' or '%' in
	 * the string pattern style catalog search parameters.
	 *
	 * <P>The '_' character represents any single character.
	 * <P>The '%' character represents any sequence of zero or
	 * more characters.
	 * @return the string used to escape wildcard characters
	 */

	public String getSearchStringEscape() throws java.sql.SQLException
	{
		return "\\";
	}

	/**
	 * Get all the "extra" characters that can be used in unquoted
	 * identifier names (those beyond a-z, 0-9 and _).
	 *
	 * @return the string containing the extra characters
	 */

	public String getExtraNameCharacters() throws java.sql.SQLException
	{
		return "";
	}

	/**
	 * Is "ALTER TABLE" with add column supported?
	 *
	 * @return true if so
	 */

	public boolean supportsAlterTableWithAddColumn() throws java.sql.SQLException
	{
		return true;
	}

	/**
	 * Is "ALTER TABLE" with drop column supported?
	 *
	 * @return true if so
	 */

	public boolean supportsAlterTableWithDropColumn() throws java.sql.SQLException
	{
		return true;
	}

	/**
	 * Is column aliasing supported?
	 *
	 * <P>If so, the SQL AS clause can be used to provide names for
	 * computed columns or to provide alias names for columns as
	 * required.
	 *
	 * A JDBC compliant driver always returns true.
	 *
	 * @return true if so
	 */

	public boolean supportsColumnAliasing() throws java.sql.SQLException
	{
		return true;
	}

	/**
	 * Are concatenations between NULL and non-NULL values NULL?
	 *
	 * A JDBC compliant driver always returns true.
	 *
	 * @return true if so
	 */

	public boolean nullPlusNonNullIsNull() throws java.sql.SQLException
	{
		return true;
	}

	/**
	 * Is the CONVERT function between SQL types supported?
	 *
	 * @return true if so
	 */

	public boolean supportsConvert() throws java.sql.SQLException
	{
		return false;
	}

	/**
	 * Is CONVERT between the given SQL types supported?
	 *
	 * @param fromType the type to convert from
	 * @param toType the type to convert to
	 * @return true if so
	 * @see Types
	 */

	public boolean supportsConvert(int fromType, int toType)
		throws java.sql.SQLException
	{
		switch (fromType)
		{

			/* The char/binary types can be converted
			 * to pretty much anything.
			 */

			case java.sql.Types.CHAR :
			case java.sql.Types.VARCHAR :
			case java.sql.Types.LONGVARCHAR :
			case java.sql.Types.BINARY :
			case java.sql.Types.VARBINARY :
			case java.sql.Types.LONGVARBINARY :
				switch (toType)
				{
					case java.sql.Types.DECIMAL :
					case java.sql.Types.NUMERIC :
					case java.sql.Types.REAL :
					case java.sql.Types.TINYINT :
					case java.sql.Types.SMALLINT :
					case java.sql.Types.INTEGER :
					case java.sql.Types.BIGINT :
					case java.sql.Types.FLOAT :
					case java.sql.Types.DOUBLE :
					case java.sql.Types.CHAR :
					case java.sql.Types.VARCHAR :
					case java.sql.Types.LONGVARCHAR :
					case java.sql.Types.BINARY :
					case java.sql.Types.VARBINARY :
					case java.sql.Types.LONGVARBINARY :
					case java.sql.Types.OTHER :
					case java.sql.Types.DATE :
					case java.sql.Types.TIME :
					case java.sql.Types.TIMESTAMP :
						return true;
					default :
						return false;
				}

				/* We don't handle the BIT type
				 * yet.
				 */

			case java.sql.Types.BIT :
				return false;

				/* The numeric types. Basically they can convert
				 * among themselves, and with char/binary types.
				 */

			case java.sql.Types.DECIMAL :
			case java.sql.Types.NUMERIC :
			case java.sql.Types.REAL :
			case java.sql.Types.TINYINT :
			case java.sql.Types.SMALLINT :
			case java.sql.Types.INTEGER :
			case java.sql.Types.BIGINT :
			case java.sql.Types.FLOAT :
			case java.sql.Types.DOUBLE :
				switch (toType)
				{
					case java.sql.Types.DECIMAL :
					case java.sql.Types.NUMERIC :
					case java.sql.Types.REAL :
					case java.sql.Types.TINYINT :
					case java.sql.Types.SMALLINT :
					case java.sql.Types.INTEGER :
					case java.sql.Types.BIGINT :
					case java.sql.Types.FLOAT :
					case java.sql.Types.DOUBLE :
					case java.sql.Types.CHAR :
					case java.sql.Types.VARCHAR :
					case java.sql.Types.LONGVARCHAR :
					case java.sql.Types.BINARY :
					case java.sql.Types.VARBINARY :
					case java.sql.Types.LONGVARBINARY :
						return true;
					default :
						return false;
				}

				/* MySQL doesn't support a NULL type. */

			case java.sql.Types.NULL :
				return false;

				/* With this driver, this will always be a serialized
				 * object, so the char/binary types will work.
				 */

			case java.sql.Types.OTHER :
				switch (toType)
				{
					case java.sql.Types.CHAR :
					case java.sql.Types.VARCHAR :
					case java.sql.Types.LONGVARCHAR :
					case java.sql.Types.BINARY :
					case java.sql.Types.VARBINARY :
					case java.sql.Types.LONGVARBINARY :
						return true;
					default :
						return false;
				}

				/* Dates can be converted to char/binary types. */

			case java.sql.Types.DATE :
				switch (toType)
				{
					case java.sql.Types.CHAR :
					case java.sql.Types.VARCHAR :
					case java.sql.Types.LONGVARCHAR :
					case java.sql.Types.BINARY :
					case java.sql.Types.VARBINARY :
					case java.sql.Types.LONGVARBINARY :
						return true;
					default :
						return false;
				}

				/* Time can be converted to char/binary types */

			case java.sql.Types.TIME :
				switch (toType)
				{
					case java.sql.Types.CHAR :
					case java.sql.Types.VARCHAR :
					case java.sql.Types.LONGVARCHAR :
					case java.sql.Types.BINARY :
					case java.sql.Types.VARBINARY :
					case java.sql.Types.LONGVARBINARY :
						return true;
					default :
						return false;
				}

				/* Timestamp can be converted to char/binary types
				 * and date/time types (with loss of precision).
				 */

			case java.sql.Types.TIMESTAMP :
				switch (toType)
				{
					case java.sql.Types.CHAR :
					case java.sql.Types.VARCHAR :
					case java.sql.Types.LONGVARCHAR :
					case java.sql.Types.BINARY :
					case java.sql.Types.VARBINARY :
					case java.sql.Types.LONGVARBINARY :
					case java.sql.Types.TIME :
					case java.sql.Types.DATE :
						return true;
					default :
						return false;
				}

				/* We shouldn't get here! */

			default :
				return false; // not sure
		}
	}

	/**
	 * Are table correlation names supported?
	 *
	 * A JDBC compliant driver always returns true.
	 *
	 * @return true if so
	 */

	public boolean supportsTableCorrelationNames() throws java.sql.SQLException
	{
		return true; // not sure
	}

	/**
	 * If table correlation names are supported, are they restricted
	 * to be different from the names of the tables?
	 *
	 * A JDBC compliant driver always returns true.
	 *
	 * @return true if so
	 */

	public boolean supportsDifferentTableCorrelationNames()
		throws java.sql.SQLException
	{
		return true; // not sure
	}

	/**
	 * Are expressions in "ORDER BY" lists supported?
	 *
	 * @return true if so
	 */

	public boolean supportsExpressionsInOrderBy() throws java.sql.SQLException
	{
		return true;
	}

	/**
	 * Can an "ORDER BY" clause use columns not in the SELECT?
	 *
	 * @return true if so
	 */

	public boolean supportsOrderByUnrelated() throws java.sql.SQLException
	{
		return false; // not sure
	}

	/**
	 * Is some form of "GROUP BY" clause supported?
	 *
	 * @return true if so
	 */

	public boolean supportsGroupBy() throws java.sql.SQLException
	{
		return true;
	}

	/**
	 * Can a "GROUP BY" clause use columns not in the SELECT?
	 *
	 * @return true if so
	 */

	public boolean supportsGroupByUnrelated() throws java.sql.SQLException
	{
		return false; // not sure
	}

	/**
	 * Can a "GROUP BY" clause add columns not in the SELECT
	 * provided it specifies all the columns in the SELECT?
	 *
	 * @return true if so
	 */

	public boolean supportsGroupByBeyondSelect() throws java.sql.SQLException
	{
		return true; // not sure
	}

	/**
	 * Is the escape character in "LIKE" clauses supported?
	 *
	 * A JDBC compliant driver always returns true.
	 *
	 * @return true if so
	 */

	public boolean supportsLikeEscapeClause() throws java.sql.SQLException
	{
		return true;
	}

	/**
	 * Are multiple ResultSets from a single execute supported?
	 *
	 * @return true if so
	 */

	public boolean supportsMultipleResultSets() throws java.sql.SQLException
	{
		return false; // not sure
	}

	/**
	 * Can we have multiple transactions open at once (on different
	 * connections)?
	 *
	 * @return true if so
	 */

	public boolean supportsMultipleTransactions() throws java.sql.SQLException
	{
		return true;
	}

	/**
	 * Can columns be defined as non-nullable?
	 *
	 * A JDBC compliant driver always returns true.
	 *
	 * @return true if so
	 */

	public boolean supportsNonNullableColumns() throws java.sql.SQLException
	{
		return true;
	}

	/**
	 * Is the ODBC Minimum SQL grammar supported?
	 *
	 * All JDBC compliant drivers must return true.
	 *
	 * @return true if so
	 */

	public boolean supportsMinimumSQLGrammar() throws java.sql.SQLException
	{
		return true;
	}

	/**
	 * Is the ODBC Core SQL grammar supported?
	 *
	 * @return true if so
	 */

	public boolean supportsCoreSQLGrammar() throws java.sql.SQLException
	{
		return true;
	}

	/**
	 * Is the ODBC Extended SQL grammar supported?
	 *
	 * @return true if so
	 */

	public boolean supportsExtendedSQLGrammar() throws java.sql.SQLException
	{
		return false; // not sure at all
	}

	/**
	 * Is the ANSI92 entry level SQL grammar supported?
	 *
	 * All JDBC compliant drivers must return true.
	 *
	 * @return true if so
	 */

	public boolean supportsANSI92EntryLevelSQL() throws java.sql.SQLException
	{
		return true;
	}

	/**
	 * Is the ANSI92 intermediate SQL grammar supported?
	 *
	 * @return true if so
	 */

	public boolean supportsANSI92IntermediateSQL() throws java.sql.SQLException
	{
		return false; // not sure
	}

	/**
	 * Is the ANSI92 full SQL grammar supported?
	 *
	 * @return true if so
	 */

	public boolean supportsANSI92FullSQL() throws java.sql.SQLException
	{
		return false; // not sure
	}

	/**
	 * Is the SQL Integrity Enhancement Facility supported?
	 *
	 * @return true if so
	 */

	public boolean supportsIntegrityEnhancementFacility()
		throws java.sql.SQLException
	{
		return false; // not sure
	}

	/**
	 * Is some form of outer join supported?
	 *
	 * @return true if so
	 */

	public boolean supportsOuterJoins() throws java.sql.SQLException
	{
		return true;
	}

	/**
	 * Are full nested outer joins supported?
	 *
	 * @return true if so
	 */

	public boolean supportsFullOuterJoins() throws java.sql.SQLException
	{
		return false; // not sure
	}

	/**
	 * Is there limited support for outer joins?  (This will be true
	 * if supportFullOuterJoins is true.)
	 *
	 * @return true if so
	 */

	public boolean supportsLimitedOuterJoins() throws java.sql.SQLException
	{
		return true;
	}

	/**
	 * What's the database vendor's preferred term for "schema"?
	 *
	 * @return the vendor term
	 */

	public String getSchemaTerm() throws java.sql.SQLException
	{
		return "";
	}

	/**
	 * What's the database vendor's preferred term for "procedure"?
	 *
	 * @return the vendor term
	 */

	public String getProcedureTerm() throws java.sql.SQLException
	{
		return "";
	}

	/**
	 * What's the database vendor's preferred term for "catalog"?
	 *
	 * @return the vendor term
	 */

	public String getCatalogTerm() throws java.sql.SQLException
	{
		return "database";
	}

	/**
	 * Does a catalog appear at the start of a qualified table name?
	 * (Otherwise it appears at the end)
	 *
	 * @return true if it appears at the start
	 */

	public boolean isCatalogAtStart() throws java.sql.SQLException
	{
		return true;
	}

	/**
	 * What's the separator between catalog and table name?
	 *
	 * @return the separator string
	 */

	public String getCatalogSeparator() throws java.sql.SQLException
	{
		return ".";
	}

	/**
	 * Can a schema name be used in a data manipulation statement?
	 *
	 * @return true if so
	 */

	public boolean supportsSchemasInDataManipulation() throws java.sql.SQLException
	{
		return false;
	}

	/**
	 * Can a schema name be used in a procedure call statement?
	 *
	 * @return true if so
	 */

	public boolean supportsSchemasInProcedureCalls() throws java.sql.SQLException
	{
		return false;
	}

	/**
	 * Can a schema name be used in a table definition statement?
	 *
	 * @return true if so
	 */

	public boolean supportsSchemasInTableDefinitions() throws java.sql.SQLException
	{
		return false;
	}

	/**
	 * Can a schema name be used in an index definition statement?
	 *
	 * @return true if so
	 */

	public boolean supportsSchemasInIndexDefinitions() throws java.sql.SQLException
	{
		return false;
	}

	/**
	 * Can a schema name be used in a privilege definition statement?
	 *
	 * @return true if so
	 */

	public boolean supportsSchemasInPrivilegeDefinitions()
		throws java.sql.SQLException
	{
		return false;
	}

	/**
	 * Can a catalog name be used in a data manipulation statement?
	 *
	 * @return true if so
	 */

	public boolean supportsCatalogsInDataManipulation()
		throws java.sql.SQLException
	{
		// Servers before 3.22 could not do this
		if (_conn.getServerMajorVersion() >= 3)
		{ // newer than version 3?
			if (_conn.getServerMajorVersion() == 3)
			{
				if (_conn.getServerMinorVersion() >= 22)
				{ // minor 22?
					return true;
				}
				else
				{
					return false; // Old version 3
				}
			}
			else
			{
				return true; // newer than version 3.22
			}
		}
		else
		{
			return false; // older than version 3
		}
	}

	/**
	 * Can a catalog name be used in a procedure call statement?
	 *
	 * @return true if so
	 */

	public boolean supportsCatalogsInProcedureCalls() throws java.sql.SQLException
	{
		return false;
	}

	/**
	 * Can a catalog name be used in a table definition statement?
	 *
	 * @return true if so
	 */

	public boolean supportsCatalogsInTableDefinitions()
		throws java.sql.SQLException
	{
		return false;
	}

	/**
	 * Can a catalog name be used in a index definition statement?
	 *
	 * @return true if so
	 */

	public boolean supportsCatalogsInIndexDefinitions()
		throws java.sql.SQLException
	{
		return false;
	}

	/**
	 * Can a catalog name be used in a privilege definition statement?
	 *
	 * @return true if so
	 */

	public boolean supportsCatalogsInPrivilegeDefinitions()
		throws java.sql.SQLException
	{
		return false;
	}

	/**
	 * Is positioned DELETE supported?
	 *
	 * @return true if so
	 */

	public boolean supportsPositionedDelete() throws java.sql.SQLException
	{
		return false;
	}

	/**
	 * Is positioned UPDATE supported?
	 *
	 * @return true if so
	 */

	public boolean supportsPositionedUpdate() throws java.sql.SQLException
	{
		return false;
	}

	/**
	 * Is SELECT for UPDATE supported?
	 *
	 * @return true if so
	 */

	public boolean supportsSelectForUpdate() throws java.sql.SQLException
	{
		return false;
	}

	/**
	 * Are stored procedure calls using the stored procedure escape
	 * syntax supported?
	 *
	 * @return true if so
	 */

	public boolean supportsStoredProcedures() throws java.sql.SQLException
	{
		return false;
	}

	/**
	 * Are subqueries in comparison expressions supported?
	 *
	 * A JDBC compliant driver always returns true.
	 *
	 * @return true if so
	 */

	public boolean supportsSubqueriesInComparisons() throws java.sql.SQLException
	{
		return true; // not sure
	}

	/**
	 * Are subqueries in exists expressions supported?
	 *
	 * A JDBC compliant driver always returns true.
	 *
	 * @return true if so
	 */

	public boolean supportsSubqueriesInExists() throws java.sql.SQLException
	{
		return false; // no sub-queries yet
	}

	/**
	 * Are subqueries in "in" statements supported?
	 *
	 * A JDBC compliant driver always returns true.
	 *
	 * @return true if so
	 */

	public boolean supportsSubqueriesInIns() throws java.sql.SQLException
	{
		return false; // no sub-queries yet
	}

	/**
	 * Are subqueries in quantified expressions supported?
	 *
	 * A JDBC compliant driver always returns true.
	 *
	 * @return true if so
	 */

	public boolean supportsSubqueriesInQuantifieds() throws java.sql.SQLException
	{
		return false; // no sub-queries yet
	}

	/**
	 * Are correlated subqueries supported?
	 *
	 * A JDBC compliant driver always returns true.
	 *
	 * @return true if so
	 */

	public boolean supportsCorrelatedSubqueries() throws java.sql.SQLException
	{
		return false; // no sub-queries yet
	}

	/**
	 * Is SQL UNION supported?
	 *
	 * A JDBC compliant driver always returns true.
	 *
	 * @return true if so
	 */

	public boolean supportsUnion() throws java.sql.SQLException
	{
		return _conn._io.versionMeetsMinimum(4, 0, 0); 
	}

	/**
	 * Is SQL UNION ALL supported?
	 *
	 * A JDBC compliant driver always returns true.
	 *
	 * @return true if so
	 */

	public boolean supportsUnionAll() throws java.sql.SQLException
	{
		return _conn._io.versionMeetsMinimum(4, 0, 0);
	}

	/**
	 * Can cursors remain open across commits?
	 *
	 * @return true if so
	 * @see Connection#disableAutoClose
	 */

	public boolean supportsOpenCursorsAcrossCommit() throws java.sql.SQLException
	{
		return false;
	}

	/**
	 * Can cursors remain open across rollbacks?
	 *
	 * @return true if so
	 * @see Connection#disableAutoClose
	 */

	public boolean supportsOpenCursorsAcrossRollback() throws java.sql.SQLException
	{
		return false;
	}

	/**
	 * Can statements remain open across commits?
	 *
	 * @return true if so
	 * @see Connection#disableAutoClose
	 */
	public boolean supportsOpenStatementsAcrossCommit()
		throws java.sql.SQLException
	{
		return false;
	}

	/**
	 * Can statements remain open across rollbacks?
	 *
	 * @return true if so
	 * @see Connection#disableAutoClose
	 */

	public boolean supportsOpenStatementsAcrossRollback()
		throws java.sql.SQLException
	{
		return false;
	}

	//----------------------------------------------------------------------
	// The following group of methods exposes various limitations
	// based on the target database with the current driver.
	// Unless otherwise specified, a result of zero means there is no
	// limit, or the limit is not known.

	/**
	 * How many hex characters can you have in an inline binary literal?
	 *
	 * @return max literal length
	 */

	public int getMaxBinaryLiteralLength() throws java.sql.SQLException
	{
		return 16777208;
	}

	/**
	 * What's the max length for a character literal?
	 *
	 * @return max literal length
	 */

	public int getMaxCharLiteralLength() throws java.sql.SQLException
	{
		return 16777208;
	}

	/**
	 * What's the limit on column name length?
	 *
	 * @return max literal length
	 */

	public int getMaxColumnNameLength() throws java.sql.SQLException
	{
		return 64;
	}

	/**
	 * What's the maximum number of columns in a "GROUP BY" clause?
	 *
	 * @return max number of columns
	 */

	public int getMaxColumnsInGroupBy() throws java.sql.SQLException
	{
		return 16;
	}

	/**
	 * What's the maximum number of columns allowed in an index?
	 *
	 * @return max columns
	 */

	public int getMaxColumnsInIndex() throws java.sql.SQLException
	{
		return 16;
	}

	/**
	 * What's the maximum number of columns in an "ORDER BY" clause?
	 *
	 * @return max columns
	 */

	public int getMaxColumnsInOrderBy() throws java.sql.SQLException
	{
		return 16;
	}

	/**
	 * What's the maximum number of columns in a "SELECT" list?
	 *
	 * @return max columns
	 */

	public int getMaxColumnsInSelect() throws java.sql.SQLException
	{
		return 256;
	}

	/**
	 * What's maximum number of columns in a table?
	 *
	 * @return max columns
	 */

	public int getMaxColumnsInTable() throws java.sql.SQLException
	{
		return 512;
	}

	/**
	 * How many active connections can we have at a time to this database?
	 *
	 * @return max connections
	 */

	public int getMaxConnections() throws java.sql.SQLException
	{
		return 0;
	}

	/**
	 * What's the maximum cursor name length?
	 *
	 * @return max cursor name length in bytes
	 */

	public int getMaxCursorNameLength() throws java.sql.SQLException
	{
		return 64;
	}

	/**
	 * What's the maximum length of an index (in bytes)?
	 *
	 * @return max index length in bytes
	 */

	public int getMaxIndexLength() throws java.sql.SQLException
	{
		return 128;
	}

	/**
	 * What's the maximum length allowed for a schema name?
	 *
	 * @return max name length in bytes
	 */

	public int getMaxSchemaNameLength() throws java.sql.SQLException
	{
		return 0;
	}

	/**
	 * What's the maximum length of a procedure name?
	 *
	 * @return max name length in bytes
	 */

	public int getMaxProcedureNameLength() throws java.sql.SQLException
	{
		return 0;
	}

	/**
	 * What's the maximum length of a catalog name?
	 *
	 * @return max name length in bytes
	 */

	public int getMaxCatalogNameLength() throws java.sql.SQLException
	{
		return 32;
	}

	/**
	 * What's the maximum length of a single row?
	 *
	 * @return max row size in bytes
	 */

	public int getMaxRowSize() throws java.sql.SQLException
	{
		return Integer.MAX_VALUE - 8; // Max buffer size - HEADER
	}

	/**
	 * Did getMaxRowSize() include LONGVARCHAR and LONGVARBINARY
	 * blobs?
	 *
	 * @return true if so
	 */

	public boolean doesMaxRowSizeIncludeBlobs() throws java.sql.SQLException
	{
		return true;
	}

	/**
	 * What's the maximum length of a SQL statement?
	 *
	 * @return max length in bytes
	 */

	public int getMaxStatementLength() throws java.sql.SQLException
	{
		return MysqlIO.MAXBUF - 4; // Max buffer - header
	}

	/**
	 * How many active statements can we have open at one time to this
	 * database?
	 *
	 * @return the maximum
	 */

	public int getMaxStatements() throws java.sql.SQLException
	{
		return 0;
	}

	/**
	 * What's the maximum length of a table name?
	 *
	 * @return max name length in bytes
	 */

	public int getMaxTableNameLength() throws java.sql.SQLException
	{
		return 64;
	}

	/**
	 * What's the maximum number of tables in a SELECT?
	 *
	 * @return the maximum
	 */

	public int getMaxTablesInSelect() throws java.sql.SQLException
	{
		return 256;
	}

	/**
	 * What's the maximum length of a user name?
	 *
	 * @return max name length  in bytes
	 */

	public int getMaxUserNameLength() throws java.sql.SQLException
	{
		return 16;
	}

	//----------------------------------------------------------------------

	/**
	 * What's the database's default transaction isolation level?  The
	 * values are defined in java.sql.Connection.
	 *
	 * @return the default isolation level
	 * @see Connection
	 */

	public int getDefaultTransactionIsolation() throws java.sql.SQLException
	{
		if (_conn.supportsIsolationLevel())
		{
			return java.sql.Connection.TRANSACTION_READ_COMMITTED;
		}
		else
		{
			return java.sql.Connection.TRANSACTION_NONE;
		}
	}

	/**
	 * Are transactions supported? If not, commit is a noop and the
	 * isolation level is TRANSACTION_NONE.
	 *
	 * @return true if transactions are supported
	 */

	public boolean supportsTransactions() throws java.sql.SQLException
	{
		return _conn.supportsTransactions();
	}

	/**
	 * Does the database support the given transaction isolation level?
	 *
	 * @param level the values are defined in java.sql.Connection
	 * @return true if so
	 * @see Connection
	 */

	public boolean supportsTransactionIsolationLevel(int level)
		throws java.sql.SQLException
	{
		if (_conn.supportsIsolationLevel())
		{
			switch (level)
			{
				case java.sql.Connection.TRANSACTION_READ_COMMITTED :
				case java.sql.Connection.TRANSACTION_READ_UNCOMMITTED :
				case java.sql.Connection.TRANSACTION_REPEATABLE_READ :
				case java.sql.Connection.TRANSACTION_SERIALIZABLE :
					return true;
				default :
					return false;
			}
		}
		else
		{
			return false;
		}
	}

	/**
	 * Are both data definition and data manipulation statements
	 * within a transaction supported?
	 *
	 * @return true if so
	 */

	public boolean supportsDataDefinitionAndDataManipulationTransactions()
		throws java.sql.SQLException
	{
		return false;
	}

	/**
	 * Are only data manipulation statements within a transaction
	 * supported?
	 *
	 * @return true if so
	 */

	public boolean supportsDataManipulationTransactionsOnly()
		throws java.sql.SQLException
	{
		return false;
	}

	/**
	 * Does a data definition statement within a transaction force the
	 * transaction to commit?
	 *
	 * @return true if so
	 */

	public boolean dataDefinitionCausesTransactionCommit()
		throws java.sql.SQLException
	{
		return false;
	}

	/**
	 * Is a data definition statement within a transaction ignored?
	 *
	 * @return true if so
	 */

	public boolean dataDefinitionIgnoredInTransactions()
		throws java.sql.SQLException
	{
		return false;
	}

	/**
	 * Get a description of stored procedures available in a
	 * catalog.
	 *
	 * <P>Only procedure descriptions matching the schema and
	 * procedure name criteria are returned.  They are ordered by
	 * PROCEDURE_SCHEM, and PROCEDURE_NAME.
	 *
	 * <P>Each procedure description has the the following columns:
	 *  <OL>
	 *    <LI><B>PROCEDURE_CAT</B> String => procedure catalog (may be null)
	 *    <LI><B>PROCEDURE_SCHEM</B> String => procedure schema (may be null)
	 *    <LI><B>PROCEDURE_NAME</B> String => procedure name
	 *  <LI> reserved for future use
	 *  <LI> reserved for future use
	 *  <LI> reserved for future use
	 *    <LI><B>REMARKS</B> String => explanatory comment on the procedure
	 *    <LI><B>PROCEDURE_TYPE</B> short => kind of procedure:
	 *      <UL>
	 *      <LI> procedureResultUnknown - May return a result
	 *      <LI> procedureNoResult - Does not return a result
	 *      <LI> procedureReturnsResult - Returns a result
	 *      </UL>
	 *  </OL>
	 *
	 * @param catalog a catalog name; "" retrieves those without a catalog
	 * @param schemaPattern a schema name pattern; "" retrieves those
	 * without a schema
	 * @param procedureNamePattern a procedure name pattern
	 * @return ResultSet each row is a procedure description
	 * @see #getSearchStringEscape
	 */

	public java.sql.ResultSet getProcedures(
		String catalog,
		String schemaPattern,
		String procedureNamePattern)
		throws java.sql.SQLException
	{
		Field[] Fields = new Field[8];

		Fields[0] = new Field("", "PROCEDURE_CAT", Types.CHAR, 0);
		Fields[1] = new Field("", "PROCEDURE_SCHEM", Types.CHAR, 0);
		Fields[2] = new Field("", "PROCEDURE_NAME", Types.CHAR, 0);
		Fields[3] = new Field("", "resTABLE_CAT", Types.CHAR, 0);
		Fields[4] = new Field("", "resTABLE_CAT", Types.CHAR, 0);
		Fields[5] = new Field("", "resTABLE_CAT", Types.CHAR, 0);
		Fields[6] = new Field("", "REMARKS", Types.CHAR, 0);
		Fields[7] = new Field("", "PROCEDURE_TYPE", Types.SMALLINT, 0);

		return buildResultSet(Fields, new Vector(), _conn);
	}

	/**
	 * Get a description of a catalog's stored procedure parameters
	 * and result columns.
	 *
	 * <P>Only descriptions matching the schema, procedure and
	 * parameter name criteria are returned.  They are ordered by
	 * PROCEDURE_SCHEM and PROCEDURE_NAME. Within this, the return value,
	 * if any, is first. Next are the parameter descriptions in call
	 * order. The column descriptions follow in column number order.
	 *
	 * <P>Each row in the ResultSet is a parameter desription or
	 * column description with the following fields:
	 *  <OL>
	 *    <LI><B>PROCEDURE_CAT</B> String => procedure catalog (may be null)
	 *    <LI><B>PROCEDURE_SCHEM</B> String => procedure schema (may be null)
	 *    <LI><B>PROCEDURE_NAME</B> String => procedure name
	 *    <LI><B>COLUMN_NAME</B> String => column/parameter name
	 *    <LI><B>COLUMN_TYPE</B> Short => kind of column/parameter:
	 *      <UL>
	 *      <LI> procedureColumnUnknown - nobody knows
	 *      <LI> procedureColumnIn - IN parameter
	 *      <LI> procedureColumnInOut - INOUT parameter
	 *      <LI> procedureColumnOut - OUT parameter
	 *      <LI> procedureColumnReturn - procedure return value
	 *      <LI> procedureColumnResult - result column in ResultSet
	 *      </UL>
	 *  <LI><B>DATA_TYPE</B> short => SQL type from java.sql.Types
	 *    <LI><B>TYPE_NAME</B> String => SQL type name
	 *    <LI><B>PRECISION</B> int => precision
	 *    <LI><B>LENGTH</B> int => length in bytes of data
	 *    <LI><B>SCALE</B> short => scale
	 *    <LI><B>RADIX</B> short => radix
	 *    <LI><B>NULLABLE</B> short => can it contain NULL?
	 *      <UL>
	 *      <LI> procedureNoNulls - does not allow NULL values
	 *      <LI> procedureNullable - allows NULL values
	 *      <LI> procedureNullableUnknown - nullability unknown
	 *      </UL>
	 *    <LI><B>REMARKS</B> String => comment describing parameter/column
	 *  </OL>
	 *
	 * <P><B>Note:</B> Some databases may not return the column
	 * descriptions for a procedure. Additional columns beyond
	 * REMARKS can be defined by the database.
	 *
	 * @param catalog a catalog name; "" retrieves those without a catalog
	 * @param schemaPattern a schema name pattern; "" retrieves those
	 * without a schema
	 * @param procedureNamePattern a procedure name pattern
	 * @param columnNamePattern a column name pattern
	 * @return ResultSet each row is a stored procedure parameter or
	 *      column description
	 * @see #getSearchStringEscape
	 */

	public java.sql.ResultSet getProcedureColumns(
		String Catalog,
		String SchemaPattern,
		String ProcedureNamePattern,
		String ColumnNamePattern)
		throws java.sql.SQLException
	{
		Field[] Fields = new Field[14];

		Fields[0] = new Field("", "TABLE_CAT", Types.CHAR, 0);
		Fields[1] = new Field("", "PROCEDURE_CAT", Types.CHAR, 0);
		Fields[2] = new Field("", "PROCEDURE_SCHEM", Types.CHAR, 0);
		Fields[3] = new Field("", "PROCEDURE_NAME", Types.CHAR, 0);
		Fields[4] = new Field("", "COLUMN_NAME", Types.CHAR, 0);
		Fields[5] = new Field("", "COLUMN_TYPE", Types.CHAR, 0);
		Fields[6] = new Field("", "DATA_TYPE", Types.SMALLINT, 0);
		Fields[7] = new Field("", "TYPE_NAME", Types.CHAR, 0);
		Fields[8] = new Field("", "PRECISION", Types.INTEGER, 0);
		Fields[9] = new Field("", "LENGTH", Types.INTEGER, 0);
		Fields[10] = new Field("", "SCALE", Types.SMALLINT, 0);
		Fields[11] = new Field("", "RADIX", Types.SMALLINT, 0);
		Fields[12] = new Field("", "NULLABLE", Types.SMALLINT, 0);
		Fields[13] = new Field("", "REMARKS", Types.CHAR, 0);

		return buildResultSet(Fields, new Vector(), _conn);
	}

	/**
	 * Get a description of tables available in a catalog.
	 *
	 * <P>Only table descriptions matching the catalog, schema, table
	 * name and type criteria are returned.  They are ordered by
	 * TABLE_TYPE, TABLE_SCHEM and TABLE_NAME.
	 *
	 * <P>Each table description has the following columns:
	 *  <OL>
	 *    <LI><B>TABLE_CAT</B> String => table catalog (may be null)
	 *    <LI><B>TABLE_SCHEM</B> String => table schema (may be null)
	 *    <LI><B>TABLE_NAME</B> String => table name
	 *    <LI><B>TABLE_TYPE</B> String => table type.  Typical types are "TABLE",
	 *                    "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY",
	 *                    "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
	 *    <LI><B>REMARKS</B> String => explanatory comment on the table
	 *  </OL>
	 *
	 * <P><B>Note:</B> Some databases may not return information for
	 * all tables.
	 *
	 * @param catalog a catalog name; "" retrieves those without a catalog
	 * @param schemaPattern a schema name pattern; "" retrieves those
	 * without a schema
	 * @param tableNamePattern a table name pattern
	 * @param types a list of table types to include; null returns all types
	 * @return ResultSet each row is a table description
	 * @see #getSearchStringEscape
	 */

	public java.sql.ResultSet getTables(
		String Catalog,
		String SchemaPattern,
		String TableNamePattern,
		String Types[])
		throws java.sql.SQLException
	{

		String DB_Sub = "";

		if (Catalog != null)
		{
			if (!Catalog.equals(""))
			{
				DB_Sub = " FROM " + _quotedId + Catalog + _quotedId;
			}
		}
		else
		{
			DB_Sub = " FROM " + _quotedId + _database + _quotedId;
		}

		if (TableNamePattern == null)
		{
			TableNamePattern = "%";
		}

		java.sql.ResultSet RS =
			_conn.createStatement().executeQuery(
				"show tables " + DB_Sub + " like '" + TableNamePattern + "'");

		java.sql.ResultSetMetaData RsMd = RS.getMetaData();

		Field[] Fields = new Field[5];

		Fields[0] =
			new Field(
				"",
				"TABLE_CAT",
				java.sql.Types.VARCHAR,
				(Catalog == null) ? 0 : Catalog.length());
		Fields[1] = new Field("", "TABLE_SCHEM", java.sql.Types.VARCHAR, 0);
		Fields[2] = new Field("", "TABLE_NAME", java.sql.Types.VARCHAR, 255);
		Fields[3] = new Field("", "TABLE_TYPE", java.sql.Types.VARCHAR, 5);
		Fields[4] = new Field("", "REMARKS", java.sql.Types.VARCHAR, 0);

		Vector Tuples = new Vector();

		byte[][] Row = null;

		while (RS.next())
		{

			String Name = RS.getString(1);

			Row = new byte[5][];
			Row[0] = (Catalog == null) ? new byte[0] : Catalog.getBytes();
			Row[1] = new byte[0];
			Row[2] = Name.getBytes();
			Row[3] = _TABLE_AS_BYTES;
			Row[4] = new byte[0];
			Tuples.addElement(Row);
		}

		java.sql.ResultSet Results = buildResultSet(Fields, Tuples, _conn);

		return Results;
	}

	/**
	 * Get the schema names available in this database.  The results
	 * are ordered by schema name.
	 *
	 * <P>The schema column is:
	 *  <OL>
	 *    <LI><B>TABLE_SCHEM</B> String => schema name
	 *  </OL>
	 *
	 * @return ResultSet each row has a single String column that is a
	 * schema name
	 */

	public java.sql.ResultSet getSchemas() throws java.sql.SQLException
	{
		Field[] Fields = new Field[1];
		Fields[0] = new Field("", "TABLE_SCHEM", java.sql.Types.CHAR, 0);

		Vector Tuples = new Vector();
		java.sql.ResultSet RS = buildResultSet(Fields, Tuples, _conn);

		return RS;
	}

	/**
	 * Get the catalog names available in this database.  The results
	 * are ordered by catalog name.
	 *
	 * <P>The catalog column is:
	 *  <OL>
	 *    <LI><B>TABLE_CAT</B> String => catalog name
	 *  </OL>
	 *
	 * @return ResultSet each row has a single String column that is a
	 * catalog name
	 */

	public java.sql.ResultSet getCatalogs() throws java.sql.SQLException
	{
		java.sql.ResultSet RS = _conn.createStatement().executeQuery("SHOW DATABASES");
		java.sql.ResultSetMetaData RSMD = RS.getMetaData();

		Field[] Fields = new Field[1];
		Fields[0] =
			new Field("", "TABLE_CAT", Types.VARCHAR, RSMD.getColumnDisplaySize(1));

		Vector Tuples = new Vector();

		while (RS.next())
		{
			byte[][] RowVal = new byte[1][];
			RowVal[0] = RS.getBytes(1);
			Tuples.addElement(RowVal);
		}

		return buildResultSet(Fields, Tuples, _conn);
	}

	/**
	 * Get the table types available in this database.  The results
	 * are ordered by table type.
	 *
	 * <P>The table type is:
	 *  <OL>
	 *    <LI><B>TABLE_TYPE</B> String => table type.  Typical types are "TABLE",
	 *                    "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY",
	 *                    "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
	 *  </OL>
	 *
	 * @return ResultSet each row has a single String column that is a
	 * table type
	 */

	public java.sql.ResultSet getTableTypes() throws java.sql.SQLException
	{
		Vector Tuples = new Vector();
		Field[] Fields = new Field[1];
		Fields[0] = new Field("", "TABLE_TYPE", Types.VARCHAR, 5);
		byte[][] TType = new byte[1][];
		TType[0] = _TABLE_AS_BYTES;
		Tuples.addElement(TType);

		return buildResultSet(Fields, Tuples, _conn);
	}

	/**
	 * Get a description of table columns available in a catalog.
	 *
	 * <P>Only column descriptions matching the catalog, schema, table
	 * and column name criteria are returned.  They are ordered by
	 * TABLE_SCHEM, TABLE_NAME and ORDINAL_POSITION.
	 *
	 * <P>Each column description has the following columns:
	 *  <OL>
	 *    <LI><B>TABLE_CAT</B> String => table catalog (may be null)
	 *    <LI><B>TABLE_SCHEM</B> String => table schema (may be null)
	 *    <LI><B>TABLE_NAME</B> String => table name
	 *    <LI><B>COLUMN_NAME</B> String => column name
	 *    <LI><B>DATA_TYPE</B> short => SQL type from java.sql.Types
	 *    <LI><B>TYPE_NAME</B> String => Data source dependent type name
	 *    <LI><B>COLUMN_SIZE</B> int => column size.  For char or date
	 *        types this is the maximum number of characters, for numeric or
	 *        decimal types this is precision.
	 *    <LI><B>BUFFER_LENGTH</B> is not used.
	 *    <LI><B>DECIMAL_DIGITS</B> int => the number of fractional digits
	 *    <LI><B>NUM_PREC_RADIX</B> int => Radix (typically either 10 or 2)
	 *    <LI><B>NULLABLE</B> int => is NULL allowed?
	 *      <UL>
	 *      <LI> columnNoNulls - might not allow NULL values
	 *      <LI> columnNullable - definitely allows NULL values
	 *      <LI> columnNullableUnknown - nullability unknown
	 *      </UL>
	 *    <LI><B>REMARKS</B> String => comment describing column (may be null)
	 *    <LI><B>COLUMN_DEF</B> String => default value (may be null)
	 *    <LI><B>SQL_DATA_TYPE</B> int => unused
	 *    <LI><B>SQL_DATETIME_SUB</B> int => unused
	 *    <LI><B>CHAR_OCTET_LENGTH</B> int => for char types the
	 *       maximum number of bytes in the column
	 *    <LI><B>ORDINAL_POSITION</B> int => index of column in table
	 *      (starting at 1)
	 *    <LI><B>IS_NULLABLE</B> String => "NO" means column definitely
	 *      does not allow NULL values; "YES" means the column might
	 *      allow NULL values.  An empty string means nobody knows.
	 *  </OL>
	 *
	 * @param catalog a catalog name; "" retrieves those without a catalog
	 * @param schemaPattern a schema name pattern; "" retrieves those
	 * without a schema
	 * @param tableNamePattern a table name pattern
	 * @param columnNamePattern a column name pattern
	 * @return ResultSet each row is a column description
	 * @see #getSearchStringEscape
	 */

	public java.sql.ResultSet getColumns(
		String Catalog,
		String SchemaPattern,
		String TableName,
		String ColumnNamePattern)
		throws java.sql.SQLException
	{
		String DB_Sub = "";

		if (ColumnNamePattern == null)
		{
			ColumnNamePattern = "%";
		}

		if (Catalog != null)
		{
			if (!Catalog.equals(""))
			{
				DB_Sub = " FROM " + _quotedId + Catalog + _quotedId;
			}
		}
		else
		{
			DB_Sub = " FROM " + _quotedId + _database + _quotedId;
		}

		Vector TableNameList = new Vector();

		int tablename_length = 0;

		if (TableName == null)
		{
			// Select from all tables
			java.sql.ResultSet Tables =
				getTables(Catalog, SchemaPattern, "%", new String[0]);

			while (Tables.next())
			{
				String TN = Tables.getString("TABLE_NAME");

				TableNameList.addElement(TN);

				if (TN.length() > tablename_length)
				{
					tablename_length = TN.length();
				}
			}
			Tables.close();
		}
		else
		{
			java.sql.ResultSet Tables =
				getTables(Catalog, SchemaPattern, TableName, new String[0]);

			while (Tables.next())
			{
				String TN = Tables.getString("TABLE_NAME");

				TableNameList.addElement(TN);

				if (TN.length() > tablename_length)
				{
					tablename_length = TN.length();
				}
			}
			Tables.close();
		}

		int catalog_length = 0;

		if (Catalog != null)
		{
			catalog_length = Catalog.length();
		}
		else
		{
			Catalog = "";
			catalog_length = 0;
		}

		java.util.Enumeration TableNames = TableNameList.elements();

		Field[] Fields = new Field[18];

		Fields[0] = new Field("", "TABLE_CAT", Types.CHAR, catalog_length);
		Fields[1] = new Field("", "TABLE_SCHEM", Types.CHAR, 0);
		Fields[2] = new Field("", "TABLE_NAME", Types.CHAR, tablename_length);
		Fields[3] = new Field("", "COLUMN_NAME", Types.CHAR, 32);
		Fields[4] = new Field("", "DATA_TYPE", Types.SMALLINT, 5);
		Fields[5] = new Field("", "TYPE_NAME", Types.CHAR, 16);
		Fields[6] =
			new Field(
				"",
				"COLUMN_SIZE",
				Types.INTEGER,
				Integer.toString(Integer.MAX_VALUE).length());
		Fields[7] = new Field("", "BUFFER_LENGTH", Types.INTEGER, 10);
		Fields[8] = new Field("", "DECIMAL_DIGITS", Types.INTEGER, 10);
		Fields[9] = new Field("", "NUM_PREC_RADIX", Types.INTEGER, 10);
		Fields[10] = new Field("", "NULLABLE", Types.INTEGER, 10);
		Fields[11] = new Field("", "REMARKS", Types.CHAR, 0);
		Fields[12] = new Field("", "COLUMN_DEF", Types.CHAR, 0);
		Fields[13] = new Field("", "SQL_DATA_TYPE", Types.INTEGER, 10);
		Fields[14] = new Field("", "SQL_DATETIME_SUB", Types.INTEGER, 10);
		Fields[15] =
			new Field(
				"",
				"CHAR_OCTET_LENGTH",
				Types.INTEGER,
				Integer.toString(Integer.MAX_VALUE).length());
		Fields[16] = new Field("", "ORDINAL_POSITION", Types.INTEGER, 10);
		Fields[17] = new Field("", "IS_NULLABLE", Types.CHAR, 3);

		Vector Tuples = new Vector();

		while (TableNames.hasMoreElements())
		{

			String TableNamePattern = (String) TableNames.nextElement();

			com.mysql.jdbc.ResultSet RS =
				_conn.execSQL(
					"show columns from "
						+ TableNamePattern
						+ DB_Sub
						+ " like '"
						+ ColumnNamePattern
						+ "'",
					-1);
			RS.setConnection(_conn);

			java.sql.ResultSetMetaData RSMD = RS.getMetaData();

			int ord_pos = 1;

			while (RS.next())
			{
				byte[][] RowVal = new byte[18][];

				RowVal[0] = Catalog.getBytes(); // TABLE_CAT
				RowVal[1] = new byte[0]; // TABLE_SCHEM (No schemas in MySQL)

				RowVal[2] = TableNamePattern.getBytes(); // TABLE_NAME
				RowVal[3] = RS.getBytes("Field");

				String TypeInfo = RS.getString("Type");
				if (Driver.debug)
				{
					System.out.println("Type: " + TypeInfo);
				}
				String MysqlType = "";

				if (TypeInfo.indexOf("(") != -1)
				{
					MysqlType = TypeInfo.substring(0, TypeInfo.indexOf("("));
				}
				else
				{
					MysqlType = TypeInfo;
				}

				if (_conn.capitalizeDBMDTypes())
				{
					MysqlType = MysqlType.toUpperCase();
				}

				/* 
				 * Convert to XOPEN (thanks JK)
				 */

				RowVal[4] = Integer.toString(MysqlDefs.mysqlToJavaType(MysqlType)).getBytes();
				// DATA_TYPE (jdbc)

				RowVal[5] = MysqlType.getBytes(); // TYPE_NAME (native)

				// Figure Out the Size

				if (TypeInfo != null)
				{
					if (TypeInfo.indexOf("enum") != -1 || TypeInfo.indexOf("set") != -1)
					{
						String Temp =
							TypeInfo.substring(TypeInfo.indexOf("("), TypeInfo.lastIndexOf(")"));

						java.util.StringTokenizer ST = new java.util.StringTokenizer(Temp, ",");

						int max_length = 0;

						while (ST.hasMoreTokens())
						{
							max_length = Math.max(max_length, (ST.nextToken().length() - 2));
						}
						RowVal[6] = Integer.toString(max_length).getBytes();
						RowVal[8] = new byte[] {(byte) '0' };
					}
					else if (TypeInfo.indexOf(",") != -1)
					{
						// Numeric with decimals
						String Size =
							TypeInfo.substring((TypeInfo.indexOf("(") + 1), (TypeInfo.indexOf(",")));
						String Decimals =
							TypeInfo.substring((TypeInfo.indexOf(",") + 1), (TypeInfo.indexOf(")")));
						RowVal[6] = Size.getBytes();
						RowVal[8] = Decimals.getBytes();
					}
					else
					{
						String Size = "0";

						/* If the size is specified with the DDL, use that */

						if (TypeInfo.indexOf("(") != -1)
						{
							Size = TypeInfo.substring((TypeInfo.indexOf("(") + 1), (TypeInfo.indexOf(")")));
						}
						/* Otherwise resort to defaults */
						else if (TypeInfo.toLowerCase().equals("tinyint"))
						{
							Size = "1";
						}
						else if (TypeInfo.toLowerCase().equals("smallint"))
						{
							Size = "6";
						}
						else if (TypeInfo.toLowerCase().equals("mediumint"))
						{
							Size = "6";
						}
						else if (TypeInfo.toLowerCase().equals("int"))
						{
							Size = "11";
						}
						else if (TypeInfo.toLowerCase().equals("integer"))
						{
							Size = "11";
						}
						else if (TypeInfo.toLowerCase().equals("bigint"))
						{
							Size = "25";
						}
						else if (TypeInfo.toLowerCase().equals("int24"))
						{
							Size = "25";
						}
						else if (TypeInfo.toLowerCase().equals("real"))
						{
							Size = "12";
						}
						else if (TypeInfo.toLowerCase().equals("float"))
						{
							Size = "12";
						}
						else if (TypeInfo.toLowerCase().equals("decimal"))
						{
							Size = "12";
						}
						else if (TypeInfo.toLowerCase().equals("numeric"))
						{
							Size = "12";
						}
						else if (TypeInfo.toLowerCase().equals("double"))
						{
							Size = "22";
						}
						else if (TypeInfo.toLowerCase().equals("char"))
						{
							Size = "1";
						}
						else if (TypeInfo.toLowerCase().equals("varchar"))
						{
							Size = "255";
						}
						else if (TypeInfo.toLowerCase().equals("date"))
						{
							Size = "10";
						}
						else if (TypeInfo.toLowerCase().equals("time"))
						{
							Size = "8";
						}
						else if (TypeInfo.toLowerCase().equals("timestamp"))
						{
							Size = "19";
						}
						else if (TypeInfo.toLowerCase().equals("datetime"))
						{
							Size = "19";
						}
						else if (TypeInfo.toLowerCase().equals("tinyblob"))
						{
							Size = "255";
						}
						else if (TypeInfo.toLowerCase().equals("blob"))
						{
							Size = Integer.toString(Math.min(65535, MysqlIO.getMaxBuf()));
						}
						else if (TypeInfo.toLowerCase().equals("mediumblob"))
						{
							Size = Integer.toString(Math.min(16277215, MysqlIO.getMaxBuf()));
						}
						else if (TypeInfo.toLowerCase().equals("longblob"))
						{
							Size =
								(Integer.toString(MysqlIO.getMaxBuf()).compareTo("2147483657") < 0
									? Integer.toString(MysqlIO.getMaxBuf())
									: "2147483657");
						}
						else if (TypeInfo.toLowerCase().equals("tinytext"))
						{
							Size = "255";
						}
						else if (TypeInfo.toLowerCase().equals("text"))
						{
							Size = "65535";
						}
						else if (TypeInfo.toLowerCase().equals("mediumtext"))
						{
							Size = Integer.toString(Math.min(16277215, MysqlIO.getMaxBuf()));
						}
						else if (TypeInfo.toLowerCase().equals("enum"))
						{
							Size = "255";
						}
						else if (TypeInfo.toLowerCase().equals("set"))
						{
							Size = "255";
						}

						RowVal[6] = Size.getBytes();
						RowVal[8] = new byte[] {(byte) '0' };
					}
				}
				else
				{
					RowVal[8] = new byte[] {(byte) '0' };
					RowVal[6] = new byte[] {(byte) '0' };
				}

				RowVal[7] = Integer.toString(MysqlIO.MAXBUF).getBytes(); // BUFFER_LENGTH
				RowVal[9] = new byte[] {(byte) '1', (byte) '0' };
				// NUM_PREC_RADIX (is this right for char?)

				String Nullable = RS.getString("Null");

				// Nullable?

				if (Nullable != null)
				{
					if (Nullable.equals("YES"))
					{
						RowVal[10] =
							Integer.toString(java.sql.DatabaseMetaData.columnNullable).getBytes();
						RowVal[17] = new String("YES").getBytes(); // IS_NULLABLE
					}
					else
					{
						RowVal[10] =
							Integer.toString(java.sql.DatabaseMetaData.columnNoNulls).getBytes();
						RowVal[17] = "NO".getBytes();
					}
				}
				else
				{
					RowVal[10] =
						Integer.toString(java.sql.DatabaseMetaData.columnNoNulls).getBytes();
					RowVal[17] = "NO".getBytes();
				}

				//
				// Doesn't always have this field, depending on version
				//

				//
				// REMARK column
				//

				try
				{
					RowVal[11] = RS.getString("Extra").getBytes();
				}
				catch (Exception E)
				{
					RowVal[11] = new byte[0];
				}

				// COLUMN_DEF
				byte[] Default = RS.getBytes("Default");

				if (Default != null)
				{
					RowVal[12] = Default;
				}
				else
				{
					RowVal[12] = new byte[0];
				}

				RowVal[13] = new byte[] {(byte) '0' }; // SQL_DATA_TYPE
				RowVal[14] = new byte[] {(byte) '0' }; // SQL_DATE_TIME_SUB
				RowVal[15] = RowVal[6]; // CHAR_OCTET_LENGTH
				RowVal[16] = Integer.toString(ord_pos++).getBytes(); // ORDINAL_POSITION

				Tuples.addElement(RowVal);
			}
			RS.close();
		}

		java.sql.ResultSet Results = buildResultSet(Fields, Tuples, _conn);
		return Results;
	}

	/**
	 * Get a description of the access rights for a table's columns.
	 *
	 * <P>Only privileges matching the column name criteria are
	 * returned.  They are ordered by COLUMN_NAME and PRIVILEGE.
	 *
	 * <P>Each privilige description has the following columns:
	 *  <OL>
	 *    <LI><B>TABLE_CAT</B> String => table catalog (may be null)
	 *    <LI><B>TABLE_SCHEM</B> String => table schema (may be null)
	 *    <LI><B>TABLE_NAME</B> String => table name
	 *    <LI><B>COLUMN_NAME</B> String => column name
	 *    <LI><B>GRANTOR</B> => grantor of access (may be null)
	 *    <LI><B>GRANTEE</B> String => grantee of access
	 *    <LI><B>PRIVILEGE</B> String => name of access (SELECT,
	 *      INSERT, UPDATE, REFRENCES, ...)
	 *    <LI><B>IS_GRANTABLE</B> String => "YES" if grantee is permitted
	 *      to grant to others; "NO" if not; null if unknown
	 *  </OL>
	 *
	 * @param catalog a catalog name; "" retrieves those without a catalog
	 * @param schema a schema name; "" retrieves those without a schema
	 * @param table a table name
	 * @param columnNamePattern a column name pattern
	 * @return ResultSet each row is a column privilege description
	 * @see #getSearchStringEscape
	 */

	public java.sql.ResultSet getColumnPrivileges(
		String catalog,
		String schema,
		String table,
		String columnNamePattern)
		throws java.sql.SQLException
	{
		Field[] fields = new Field[8];

		fields[0] = new Field("", "TABLE_CAT", Types.CHAR, 64);
		fields[1] = new Field("", "TABLE_SCHEM", Types.CHAR, 1);
		fields[2] = new Field("", "TABLE_NAME", Types.CHAR, 64);
		fields[3] = new Field("", "COLUMN_NAME", Types.CHAR, 64);
		fields[4] = new Field("", "GRANTOR", Types.CHAR, 77);
		fields[5] = new Field("", "GRANTEE", Types.CHAR, 77);
		fields[6] = new Field("", "PRIVILEGE", Types.CHAR, 64);
		fields[7] = new Field("", "IS_GRANTABLE", Types.CHAR, 3);

		StringBuffer grantQuery =
			new StringBuffer("SELECT c.host, c.db, t.grantor, c.user, c.table_name, c.column_name, c.column_priv from mysql.columns_priv c, mysql.tables_priv t where c.host = t.host and c.db = t.db and c.table_name = t.table_name ");

		grantQuery.append(" WHERE ");

		if (catalog != null && catalog.length() != 0)
		{
			grantQuery.append(" c.db='");
			grantQuery.append(catalog);
			grantQuery.append("' AND ");
		}

		grantQuery.append("c.table_name ='");
		grantQuery.append(table);
		grantQuery.append("' AND c.column_name like '");
		grantQuery.append(columnNamePattern);

		com.mysql.jdbc.ResultSet results = null;

		Vector grantRows = new Vector();

		try
		{
			results = _conn.execSQL(grantQuery.toString(), -1);

			results.setConnection(_conn);

			while (results.next())
			{
				String host = results.getString(1);
				String database = results.getString(2);
				String grantor = results.getString(3);
				String user = results.getString(4);

				if (user == null || user.length() == 0)
				{
					user = "%";
				}

				StringBuffer fullUser = new StringBuffer(user);

				if (host != null)
				{
					fullUser.append("@");
					fullUser.append(host);
				}

				String columnName = results.getString(6);

				String allPrivileges = results.getString(7);

				if (allPrivileges != null)
				{
					allPrivileges = allPrivileges.toUpperCase();

					StringTokenizer st = new StringTokenizer(allPrivileges, ",");

					while (st.hasMoreTokens())
					{
						String privilege = st.nextToken().trim();

						byte[][] tuple = new byte[8][];

						tuple[0] = s2b(database);
						tuple[1] = null;
						tuple[2] = s2b(table);
						tuple[3] = s2b(columnName);

						if (grantor != null)
						{
							tuple[4] = s2b(grantor);
						}
						else
						{
							tuple[4] = null;
						}

						tuple[5] = s2b(fullUser.toString());
						tuple[6] = s2b(privilege);
						tuple[7] = null;

						grantRows.addElement(tuple);
					}
				}
			}
		}
		finally
		{
			if (results != null)
			{
				try
				{
					results.close();
				}
				catch (Exception ex)
				{
				}
			}
		}

		return buildResultSet(fields, grantRows, _conn);
	}

	/**
	 * Get a description of the access rights for each table available
	 * in a catalog.
	 *
	 * <P>Only privileges matching the schema and table name
	 * criteria are returned.  They are ordered by TABLE_SCHEM,
	 * TABLE_NAME, and PRIVILEGE.
	 *
	 * <P>Each privilige description has the following columns:
	 *  <OL>
	 *    <LI><B>TABLE_CAT</B> String => table catalog (may be null)
	 *    <LI><B>TABLE_SCHEM</B> String => table schema (may be null)
	 *    <LI><B>TABLE_NAME</B> String => table name
	 *    <LI><B>COLUMN_NAME</B> String => column name
	 *    <LI><B>GRANTOR</B> => grantor of access (may be null)
	 *    <LI><B>GRANTEE</B> String => grantee of access
	 *    <LI><B>PRIVILEGE</B> String => name of access (SELECT,
	 *      INSERT, UPDATE, REFRENCES, ...)
	 *    <LI><B>IS_GRANTABLE</B> String => "YES" if grantee is permitted
	 *      to grant to others; "NO" if not; null if unknown
	 *  </OL>
	 *
	 * @param catalog a catalog name; "" retrieves those without a catalog
	 * @param schemaPattern a schema name pattern; "" retrieves those
	 * without a schema
	 * @param tableNamePattern a table name pattern
	 * @return ResultSet each row is a table privilege description
	 * @see #getSearchStringEscape
	 */

	public java.sql.ResultSet getTablePrivileges(
		String catalog,
		String schemaPattern,
		String tableNamePattern)
		throws java.sql.SQLException
	{
		Field[] fields = new Field[7];

		fields[0] = new Field("", "TABLE_CAT", Types.CHAR, 64);
		fields[1] = new Field("", "TABLE_SCHEM", Types.CHAR, 1);
		fields[2] = new Field("", "TABLE_NAME", Types.CHAR, 64);
		fields[3] = new Field("", "GRANTOR", Types.CHAR, 77);
		fields[4] = new Field("", "GRANTEE", Types.CHAR, 77);
		fields[5] = new Field("", "PRIVILEGE", Types.CHAR, 64);
		fields[6] = new Field("", "IS_GRANTABLE", Types.CHAR, 3);

		StringBuffer grantQuery =
			new StringBuffer("SELECT host,db,table_name,grantor,user,table_priv from mysql.tables_priv ");

		grantQuery.append(" WHERE ");

		if (catalog != null && catalog.length() != 0)
		{
			grantQuery.append(" db='");
			grantQuery.append(catalog);
			grantQuery.append("' AND ");
		}

		grantQuery.append("table_name like '");
		grantQuery.append(tableNamePattern);
		grantQuery.append("'");

		com.mysql.jdbc.ResultSet results = null;

		Vector grantRows = new Vector();

		try
		{
			results = _conn.execSQL(grantQuery.toString(), -1);

			results.setConnection(_conn);

			while (results.next())
			{
				String host = results.getString(1);
				String database = results.getString(2);
				String table = results.getString(3);
				String grantor = results.getString(4);

				String user = results.getString(5);

				if (user == null || user.length() == 0)
				{
					user = "%";
				}

				StringBuffer fullUser = new StringBuffer(user);

				if (host != null)
				{
					fullUser.append("@");
					fullUser.append(host);
				}

				String allPrivileges = results.getString(6);

				if (allPrivileges != null)
				{
					allPrivileges = allPrivileges.toUpperCase();

					StringTokenizer st = new StringTokenizer(allPrivileges, ",");

					while (st.hasMoreTokens())
					{
						String privilege = st.nextToken().trim();

						// Loop through every column in the table

						java.sql.ResultSet columnResults = null;

						try
						{
							columnResults = getColumns(catalog, schemaPattern, table, "%");

							while (columnResults.next())
							{
								String columnName = columnResults.getString(4);

								byte[][] tuple = new byte[8][];

								tuple[0] = s2b(database);
								tuple[1] = null;
								tuple[2] = s2b(table);
								

								if (grantor != null)
								{
									tuple[3] = s2b(grantor);
								}
								else
								{
									tuple[3] = null;
								}

								tuple[4] = s2b(fullUser.toString());
								tuple[5] = s2b(privilege);
								tuple[6] = null;

								grantRows.addElement(tuple);
							}
						}
						finally
						{
							if (columnResults != null)
							{
								try
								{
									columnResults.close();
								}
								catch (Exception ex)
								{
								}
							}
						}

					}
				}
			}
		}
		finally
		{
			if (results != null)
			{
				try
				{
					results.close();
				}
				catch (Exception ex)
				{
				}
			}
		}

		return buildResultSet(fields, grantRows, _conn);
	}

	/**
	 * Get a description of a table's optimal set of columns that
	 * uniquely identifies a row. They are ordered by SCOPE.
	 *
	 * <P>Each column description has the following columns:
	 *  <OL>
	 *    <LI><B>SCOPE</B> short => actual scope of result
	 *      <UL>
	 *      <LI> bestRowTemporary - very temporary, while using row
	 *      <LI> bestRowTransaction - valid for remainder of current transaction
	 *      <LI> bestRowSession - valid for remainder of current session
	 *      </UL>
	 *    <LI><B>COLUMN_NAME</B> String => column name
	 *    <LI><B>DATA_TYPE</B> short => SQL data type from java.sql.Types
	 *    <LI><B>TYPE_NAME</B> String => Data source dependent type name
	 *    <LI><B>COLUMN_SIZE</B> int => precision
	 *    <LI><B>BUFFER_LENGTH</B> int => not used
	 *    <LI><B>DECIMAL_DIGITS</B> short  => scale
	 *    <LI><B>PSEUDO_COLUMN</B> short => is this a pseudo column
	 *      like an Oracle ROWID
	 *      <UL>
	 *      <LI> bestRowUnknown - may or may not be pseudo column
	 *      <LI> bestRowNotPseudo - is NOT a pseudo column
	 *      <LI> bestRowPseudo - is a pseudo column
	 *      </UL>
	 *  </OL>
	 *
	 * @param catalog a catalog name; "" retrieves those without a catalog
	 * @param schema a schema name; "" retrieves those without a schema
	 * @param table a table name
	 * @param scope the scope of interest; use same values as SCOPE
	 * @param nullable include columns that are nullable?
	 * @return ResultSet each row is a column description
	 */

	public java.sql.ResultSet getBestRowIdentifier(
		String Catalog,
		String Schema,
		String Table,
		int scope,
		boolean nullable)
		throws java.sql.SQLException
	{
		Field[] Fields = new Field[8];

		Fields[0] = new Field("", "SCOPE", Types.SMALLINT, 5);
		Fields[1] = new Field("", "COLUMN_NAME", Types.CHAR, 32);
		Fields[2] = new Field("", "DATA_TYPE", Types.SMALLINT, 32);
		Fields[3] = new Field("", "TYPE_NAME", Types.CHAR, 32);
		Fields[4] = new Field("", "COLUMN_SIZE", Types.INTEGER, 10);
		Fields[5] = new Field("", "BUFFER_LENGTH", Types.INTEGER, 10);
		Fields[6] = new Field("", "DECIMAL_DIGITS", Types.INTEGER, 10);
		Fields[7] = new Field("", "PSEUDO_COLUMN", Types.SMALLINT, 5);

		String DB_Sub = "";

		if (Catalog != null)
		{
			if (!Catalog.equals(""))
			{
				DB_Sub = " FROM " + _quotedId + Catalog + _quotedId;
			}
		}
		else
		{
			DB_Sub = " FROM " + _quotedId + _database + _quotedId;
		}

		if (Table == null)
		{
			throw new java.sql.SQLException("Table not specified.", "S1009");
		}

		com.mysql.jdbc.ResultSet RS =
			_conn.execSQL("show columns from " + Table + DB_Sub, -1);

		RS.setConnection(_conn);

		Vector Tuples = new Vector();

		while (RS.next())
		{
			String KeyType = RS.getString("Key");
			if (KeyType != null)
			{
				if (KeyType.toUpperCase().startsWith("PRI"))
				{
					byte[][] Tuple = new byte[8][];

					Tuple[0] =
						Integer.toString(java.sql.DatabaseMetaData.bestRowSession).getBytes();
					Tuple[1] = RS.getBytes("Field");

					String Type = RS.getString("Type");
					int size = MysqlIO.getMaxBuf();
					int decimals = 0;

					/*
					 * Parse the Type column from MySQL
					 */

					if (Type.indexOf("enum") != -1)
					{
						String Temp = Type.substring(Type.indexOf("("), Type.indexOf(")"));

						java.util.StringTokenizer ST = new java.util.StringTokenizer(Temp, ",");

						int max_length = 0;

						while (ST.hasMoreTokens())
						{
							max_length = Math.max(max_length, (ST.nextToken().length() - 2));
						}

						size = max_length;
						decimals = 0;

						Type = "enum";
					}
					else if (Type.indexOf("(") != -1)
					{
						if (Type.indexOf(",") != -1)
						{
							size =
								Integer.parseInt(Type.substring(Type.indexOf("(") + 1, Type.indexOf(",")));
							decimals =
								Integer.parseInt(Type.substring(Type.indexOf(",") + 1, Type.indexOf(")")));
						}
						else
						{
							size =
								Integer.parseInt(Type.substring(Type.indexOf("(") + 1, Type.indexOf(")")));
						}
						Type = Type.substring(Type.indexOf("("));
					}

					Tuple[2] = new byte[0]; // FIXME!
					Tuple[3] = Type.getBytes();
					Tuple[4] = Integer.toString(size + decimals).getBytes();
					Tuple[5] = Integer.toString(size + decimals).getBytes();
					Tuple[6] = Integer.toString(decimals).getBytes();
					Tuple[7] =
						Integer.toString(java.sql.DatabaseMetaData.bestRowNotPseudo).getBytes();

					Tuples.addElement(Tuple);
				}
			}
		}

		return buildResultSet(Fields, Tuples, _conn);
	}

	/**
	 * Get a description of a table's columns that are automatically
	 * updated when any value in a row is updated.  They are
	 * unordered.
	 *
	 * <P>Each column description has the following columns:
	 *  <OL>
	 *    <LI><B>SCOPE</B> short => is not used
	 *    <LI><B>COLUMN_NAME</B> String => column name
	 *    <LI><B>DATA_TYPE</B> short => SQL data type from java.sql.Types
	 *    <LI><B>TYPE_NAME</B> String => Data source dependent type name
	 *    <LI><B>COLUMN_SIZE</B> int => precision
	 *    <LI><B>BUFFER_LENGTH</B> int => length of column value in bytes
	 *    <LI><B>DECIMAL_DIGITS</B> short  => scale
	 *    <LI><B>PSEUDO_COLUMN</B> short => is this a pseudo column
	 *      like an Oracle ROWID
	 *      <UL>
	 *      <LI> versionColumnUnknown - may or may not be pseudo column
	 *      <LI> versionColumnNotPseudo - is NOT a pseudo column
	 *      <LI> versionColumnPseudo - is a pseudo column
	 *      </UL>
	 *  </OL>
	 *
	 * @param catalog a catalog name; "" retrieves those without a catalog
	 * @param schema a schema name; "" retrieves those without a schema
	 * @param table a table name
	 * @return ResultSet each row is a column description
	 */

	public java.sql.ResultSet getVersionColumns(
		String catalog,
		String schema,
		String table)
		throws java.sql.SQLException
	{
		Field[] Fields = new Field[8];

		Fields[0] = new Field("", "SCOPE", Types.SMALLINT, 5);
		Fields[1] = new Field("", "COLUMN_NAME", Types.CHAR, 32);
		Fields[2] = new Field("", "DATA_TYPE", Types.SMALLINT, 5);
		Fields[3] = new Field("", "TYPE_NAME", Types.CHAR, 16);
		Fields[4] = new Field("", "COLUMN_SIZE", Types.CHAR, 16);
		Fields[5] = new Field("", "BUFFER_LENGTH", Types.CHAR, 16);
		Fields[6] = new Field("", "DECIMAL_DIGITS", Types.CHAR, 16);
		Fields[7] = new Field("", "PSEUDO_COLUMN", Types.SMALLINT, 5);

		return buildResultSet(Fields, new Vector(), _conn);
		// do TIMESTAMP columns count?
	}

	/**
	 * Get a description of a table's primary key columns.  They
	 * are ordered by COLUMN_NAME.
	 *
	 * <P>Each column description has the following columns:
	 *  <OL>
	 *    <LI><B>TABLE_CAT</B> String => table catalog (may be null)
	 *    <LI><B>TABLE_SCHEM</B> String => table schema (may be null)
	 *    <LI><B>TABLE_NAME</B> String => table name
	 *    <LI><B>COLUMN_NAME</B> String => column name
	 *    <LI><B>KEY_SEQ</B> short => sequence number within primary key
	 *    <LI><B>PK_NAME</B> String => primary key name (may be null)
	 *  </OL>
	 *
	 * @param catalog a catalog name; "" retrieves those without a catalog
	 * @param schema a schema name pattern; "" retrieves those
	 * without a schema
	 * @param table a table name
	 * @return ResultSet each row is a primary key column description
	 */

	public java.sql.ResultSet getPrimaryKeys(
		String Catalog,
		String Schema,
		String Table)
		throws java.sql.SQLException
	{

		Field[] Fields = new Field[6];

		Fields[0] = new Field("", "TABLE_CAT", Types.CHAR, 255);
		Fields[1] = new Field("", "TABLE_SCHEM", Types.CHAR, 0);
		Fields[2] = new Field("", "TABLE_NAME", Types.CHAR, 255);
		Fields[3] = new Field("", "COLUMN_NAME", Types.CHAR, 32);
		Fields[4] = new Field("", "KEY_SEQ", Types.SMALLINT, 5);
		Fields[5] = new Field("", "PK_NAME", Types.CHAR, 32);

		String DB_Sub = "";

		if (Catalog != null)
		{
			if (!Catalog.equals(""))
			{
				DB_Sub = " FROM " + _quotedId + Catalog + _quotedId;
			}
		}
		else
		{
			DB_Sub = " FROM " + _quotedId + _database + _quotedId;
		}

		if (Table == null)
		{
			throw new java.sql.SQLException("Table not specified.", "S1009");
		}

		com.mysql.jdbc.ResultSet RS =
			_conn.execSQL("show keys from " + Table + DB_Sub, -1);

		RS.setConnection(_conn);

		Vector Tuples = new Vector();

		int row_number = 1;

		while (RS.next())
		{
			String KeyType = RS.getString("Key_name");
			if (KeyType != null)
			{
				if (KeyType.toUpperCase().startsWith("PRI"))
				{
					byte[][] Tuple = new byte[6][];
					Tuple[0] = (Catalog == null ? new byte[0] : Catalog.getBytes());
					Tuple[1] = new byte[0];
					Tuple[2] = Table.getBytes();
					Tuple[3] = RS.getBytes("Column_name");
					Tuple[4] = s2b(RS.getString("Seq_in_index"));
					Tuple[5] = Tuple[3];
					Tuples.addElement(Tuple);
				}
			}
		}
		return buildResultSet(Fields, Tuples, _conn);
	}

	/**
	 * Get a description of the primary key columns that are
	 * referenced by a table's foreign key columns (the primary keys
	 * imported by a table).  They are ordered by PKTABLE_CAT,
	 * PKTABLE_SCHEM, PKTABLE_NAME, and KEY_SEQ.
	 *
	 * <P>Each primary key column description has the following columns:
	 *  <OL>
	 *    <LI><B>PKTABLE_CAT</B> String => primary key table catalog
	 *      being imported (may be null)
	 *    <LI><B>PKTABLE_SCHEM</B> String => primary key table schema
	 *      being imported (may be null)
	 *    <LI><B>PKTABLE_NAME</B> String => primary key table name
	 *      being imported
	 *    <LI><B>PKCOLUMN_NAME</B> String => primary key column name
	 *      being imported
	 *    <LI><B>FKTABLE_CAT</B> String => foreign key table catalog (may be null)
	 *    <LI><B>FKTABLE_SCHEM</B> String => foreign key table schema (may be null)
	 *    <LI><B>FKTABLE_NAME</B> String => foreign key table name
	 *    <LI><B>FKCOLUMN_NAME</B> String => foreign key column name
	 *    <LI><B>KEY_SEQ</B> short => sequence number within foreign key
	 *    <LI><B>UPDATE_RULE</B> short => What happens to
	 *       foreign key when primary is updated:
	 *      <UL>
	 *      <LI> importedKeyCascade - change imported key to agree
	 *               with primary key update
	 *      <LI> importedKeyRestrict - do not allow update of primary
	 *               key if it has been imported
	 *      <LI> importedKeySetNull - change imported key to NULL if
	 *               its primary key has been updated
	 *      </UL>
	 *    <LI><B>DELETE_RULE</B> short => What happens to
	 *      the foreign key when primary is deleted.
	 *      <UL>
	 *      <LI> importedKeyCascade - delete rows that import a deleted key
	 *      <LI> importedKeyRestrict - do not allow delete of primary
	 *               key if it has been imported
	 *      <LI> importedKeySetNull - change imported key to NULL if
	 *               its primary key has been deleted
	 *      </UL>
	 *    <LI><B>FK_NAME</B> String => foreign key name (may be null)
	 *    <LI><B>PK_NAME</B> String => primary key name (may be null)
	 *  </OL>
	 *
	 * @param catalog a catalog name; "" retrieves those without a catalog
	 * @param schema a schema name pattern; "" retrieves those
	 * without a schema
	 * @param table a table name
	 * @return ResultSet each row is a primary key column description
	 * @see #getExportedKeys
	 */

	public java.sql.ResultSet getImportedKeys(
		String catalog,
		String schema,
		String table)
		throws java.sql.SQLException
	{

		if (Driver.trace)
		{
			Object[] args = { catalog, schema, table };
			Debug.methodCall(this, "getImportedKeys", args);
		}

		Field[] fields = new Field[14];

		fields[0] = new Field("", "PKTABLE_CAT", Types.CHAR, 255);
		fields[1] = new Field("", "PKTABLE_SCHEM", Types.CHAR, 0);
		fields[2] = new Field("", "PKTABLE_NAME", Types.CHAR, 255);
		fields[3] = new Field("", "PKCOLUMN_NAME", Types.CHAR, 32);
		fields[4] = new Field("", "FKTABLE_CAT", Types.CHAR, 255);
		fields[5] = new Field("", "FKTABLE_SCHEM", Types.CHAR, 0);
		fields[6] = new Field("", "FKTABLE_NAME", Types.CHAR, 255);
		fields[7] = new Field("", "FKCOLUMN_NAME", Types.CHAR, 32);
		fields[8] = new Field("", "KEY_SEQ", Types.SMALLINT, 2);
		fields[9] = new Field("", "UPDATE_RULE", Types.SMALLINT, 2);
		fields[10] = new Field("", "DELETE_RULE", Types.SMALLINT, 2);
		fields[11] = new Field("", "FK_NAME", Types.CHAR, 0);
		fields[12] = new Field("", "PK_NAME", Types.CHAR, 0);
		fields[13] = new Field("", "DEFERRABILITY", Types.INTEGER, 2);

		if (_conn.getIO().versionMeetsMinimum(3, 23, 0))
		{
			/*
			 * Get foreign key information for table
			 */

			String DB_Sub = "";

			if (catalog != null)
			{
				if (!catalog.equals(""))
				{
					DB_Sub = " from " + catalog;
				}
			}
			else
			{
				DB_Sub = " from " + _database;
			}

			if (table == null)
			{
				throw new java.sql.SQLException("Table not specified.", "S1009");
			}

			com.mysql.jdbc.ResultSet fkRS =
				_conn.execSQL("show table status " + DB_Sub + " like '" + table + "'", -1);

			fkRS.setConnection(_conn);

			/*
			 * Parse imported foreign key information
			 */

			Vector tuples = new Vector();
			String dummy;

			while (fkRS.next())
			{
				String tableType = fkRS.getString("Type");

				if (tableType != null && tableType.toLowerCase().equals("innodb"))
				{
					String comment = fkRS.getString("Comment");

					if (comment != null)
					{
						StringTokenizer commentTokens = new StringTokenizer(comment, ";", false);
						if (commentTokens.hasMoreTokens())
						{
							dummy = commentTokens.nextToken(); // Skip InnoDB comment
						}
						int keySeq = 0;
						while (commentTokens.hasMoreTokens())
						{
							String keys = commentTokens.nextToken();
							byte[][] tuple = new byte[14][];
							StringTokenizer keyTokens = new StringTokenizer(keys, "() /", false);
							tuple[4] = (catalog == null ? new byte[0] : catalog.getBytes()); // FKTABLE_CAT
							tuple[5] = null; // FKTABLE_SCHEM
							tuple[6] = table.getBytes(); // FKTABLE_NAME
							tuple[7] = s2b(keyTokens.nextToken()); // FKCOLUMN_NAME
							dummy = keyTokens.nextToken(); // Skip REFER
							tuple[0] = s2b(keyTokens.nextToken()); // PKTABLE_CAT
							tuple[1] = null; // PKTABLE_SCHEM
							tuple[2] = s2b(keyTokens.nextToken()); // PKTABLE_NAME
							tuple[3] = s2b(keyTokens.nextToken()); // PKCOLUMN_NAME
							tuple[8] = Integer.toString(keySeq).getBytes(); // KEY_SEQ
							tuple[9] =
								Integer.toString(java.sql.DatabaseMetaData.importedKeySetDefault).getBytes();
							// UPDATE_RULE
							tuple[10] =
								Integer.toString(java.sql.DatabaseMetaData.importedKeySetDefault).getBytes();
							// DELETE_RULE
							tuple[11] = null; // FK_NAME
							tuple[12] = null; // PK_NAME
							tuple[13] =
								Integer.toString(java.sql.DatabaseMetaData.importedKeyNotDeferrable).getBytes();
							// DEFERRABILITY
							tuples.addElement(tuple);
							keySeq++;
						}
					}
				}
			}

			if (Driver.trace)
			{

				StringBuffer rows = new StringBuffer();

				rows.append("\n");

				for (int i = 0; i < tuples.size(); i++)
				{
					byte[][] b = (byte[][]) tuples.elementAt(i);

					rows.append("[Row] ");

					boolean firstTime = true;

					for (int j = 0; j < b.length; j++)
					{
						if (!firstTime)
						{
							rows.append(", ");
						}
						else
						{
							firstTime = false;
						}

						if (b[j] == null)
						{
							rows.append("null");
						}
						else
						{
							rows.append(new String(b[j]));
						}

					}

					rows.append("\n");
				}

				Debug.returnValue(this, "getImportedKeys", rows.toString());
			}

			return buildResultSet(fields, tuples, _conn);
		}
		else
		{
			return buildResultSet(fields, new Vector(), _conn);
		}
	}

	/**
	 * Get a description of a foreign key columns that reference a
	 * table's primary key columns (the foreign keys exported by a
	 * table).  They are ordered by FKTABLE_CAT, FKTABLE_SCHEM,
	 * FKTABLE_NAME, and KEY_SEQ.
	 *
	 * <P>Each foreign key column description has the following columns:
	 *  <OL>
	 *    <LI><B>PKTABLE_CAT</B> String => primary key table catalog (may be null)
	 *    <LI><B>PKTABLE_SCHEM</B> String => primary key table schema (may be null)
	 *    <LI><B>PKTABLE_NAME</B> String => primary key table name
	 *    <LI><B>PKCOLUMN_NAME</B> String => primary key column name
	 *    <LI><B>FKTABLE_CAT</B> String => foreign key table catalog (may be null)
	 *      being exported (may be null)
	 *    <LI><B>FKTABLE_SCHEM</B> String => foreign key table schema (may be null)
	 *      being exported (may be null)
	 *    <LI><B>FKTABLE_NAME</B> String => foreign key table name
	 *      being exported
	 *    <LI><B>FKCOLUMN_NAME</B> String => foreign key column name
	 *      being exported
	 *    <LI><B>KEY_SEQ</B> short => sequence number within foreign key
	 *    <LI><B>UPDATE_RULE</B> short => What happens to
	 *       foreign key when primary is updated:
	 *      <UL>
	 *      <LI> importedKeyCascade - change imported key to agree
	 *               with primary key update
	 *      <LI> importedKeyRestrict - do not allow update of primary
	 *               key if it has been imported
	 *      <LI> importedKeySetNull - change imported key to NULL if
	 *               its primary key has been updated
	 *      </UL>
	 *    <LI><B>DELETE_RULE</B> short => What happens to
	 *      the foreign key when primary is deleted.
	 *      <UL>
	 *      <LI> importedKeyCascade - delete rows that import a deleted key
	 *      <LI> importedKeyRestrict - do not allow delete of primary
	 *               key if it has been imported
	 *      <LI> importedKeySetNull - change imported key to NULL if
	 *               its primary key has been deleted
	 *      </UL>
	 *    <LI><B>FK_NAME</B> String => foreign key identifier (may be null)
	 *    <LI><B>PK_NAME</B> String => primary key identifier (may be null)
	 *  </OL>
	 *
	 * @param catalog a catalog name; "" retrieves those without a catalog
	 * @param schema a schema name pattern; "" retrieves those
	 * without a schema
	 * @param table a table name
	 * @return ResultSet each row is a foreign key column description
	 * @see #getImportedKeys
	 */

	public java.sql.ResultSet getExportedKeys(
		String catalog,
		String schema,
		String table)
		throws java.sql.SQLException
	{

		if (Driver.trace)
		{
			Object[] args = { catalog, schema, table };
			Debug.methodCall(this, "getExportedKeys", args);
		}

		Field[] fields = new Field[14];

		fields[0] = new Field("", "PKTABLE_CAT", Types.CHAR, 255);
		fields[1] = new Field("", "PKTABLE_SCHEM", Types.CHAR, 0);
		fields[2] = new Field("", "PKTABLE_NAME", Types.CHAR, 255);
		fields[3] = new Field("", "PKCOLUMN_NAME", Types.CHAR, 32);
		fields[4] = new Field("", "FKTABLE_CAT", Types.CHAR, 255);
		fields[5] = new Field("", "FKTABLE_SCHEM", Types.CHAR, 0);
		fields[6] = new Field("", "FKTABLE_NAME", Types.CHAR, 255);
		fields[7] = new Field("", "FKCOLUMN_NAME", Types.CHAR, 32);
		fields[8] = new Field("", "KEY_SEQ", Types.SMALLINT, 2);
		fields[9] = new Field("", "UPDATE_RULE", Types.SMALLINT, 2);
		fields[10] = new Field("", "DELETE_RULE", Types.SMALLINT, 2);
		fields[11] = new Field("", "FK_NAME", Types.CHAR, 0);
		fields[12] = new Field("", "PK_NAME", Types.CHAR, 0);
		fields[13] = new Field("", "DEFERRABILITY", Types.INTEGER, 2);

		if (_conn.getIO().versionMeetsMinimum(3, 23, 0))
		{
			/*
			 * Get foreign key information for table
			 */

			String DB_Sub = "";

			if (catalog != null)
			{
				if (!catalog.equals(""))
				{
					DB_Sub = " FROM " + catalog;
				}
			}
			else
			{
				DB_Sub = " FROM " + _database;
			}

			if (table == null)
			{
				throw new java.sql.SQLException("Table not specified.", "S1009");
			}

			com.mysql.jdbc.ResultSet fkRS =
				_conn.execSQL("show table status " + DB_Sub, -1);

			fkRS.setConnection(_conn);

			/*
			 * Parse imported foreign key information
			 */

			Vector tuples = new Vector();
			String dummy;

			while (fkRS.next())
			{
				String tableType = fkRS.getString("Type");

				if (tableType != null && tableType.toLowerCase().equals("innodb"))
				{

					String comment = fkRS.getString("Comment");
					if (comment != null)
					{
						StringTokenizer commentTokens = new StringTokenizer(comment, ";", false);
						if (commentTokens.hasMoreTokens())
						{
							dummy = commentTokens.nextToken(); // Skip InnoDB comment
						}
						int keySeq = 0;

						while (commentTokens.hasMoreTokens())
						{
							String keys = commentTokens.nextToken();
							StringTokenizer keyTokens = new StringTokenizer(keys, "() /", false);
							byte[][] tuple = new byte[14][];
							tuple[4] = (catalog == null ? new byte[0] : catalog.getBytes()); // FKTABLE_CAT
							tuple[5] = null; // FKTABLE_SCHEM
							tuple[6] = fkRS.getString("Name").getBytes(); // FKTABLE_NAME
							tuple[7] = s2b(keyTokens.nextToken()); // FKCOLUMN_NAME
							dummy = keyTokens.nextToken(); // Skip REFER
							tuple[0] = s2b(keyTokens.nextToken()); // PKTABLE_CAT
							tuple[1] = null; // PKTABLE_SCHEM
							// Skip foreign key if it doesn't refer to the right table
							dummy = keyTokens.nextToken(); // PKTABLE_NAME
							if (dummy.compareTo(table) != 0)
							{
								continue;
							}
							tuple[2] = s2b(table); // PKTABLE_NAME
							tuple[3] = s2b(keyTokens.nextToken()); // PKCOLUMN_NAME
							tuple[8] = Integer.toString(keySeq).getBytes(); // KEY_SEQ
							tuple[9] =
								Integer.toString(java.sql.DatabaseMetaData.importedKeySetDefault).getBytes();
							// UPDATE_RULE
							tuple[10] =
								Integer.toString(java.sql.DatabaseMetaData.importedKeySetDefault).getBytes();
							// DELETE_RULE
							tuple[11] = null; // FK_NAME
							tuple[12] = null; // PK_NAME
							tuple[13] =
								Integer.toString(java.sql.DatabaseMetaData.importedKeyNotDeferrable).getBytes();
							// DEFERRABILITY
							tuples.addElement(tuple);
							keySeq++;
						}
					}
				}
			}

			if (Driver.trace)
			{

				StringBuffer rows = new StringBuffer();

				rows.append("\n");

				for (int i = 0; i < tuples.size(); i++)
				{
					byte[][] b = (byte[][]) tuples.elementAt(i);

					rows.append("[Row] ");

					boolean firstTime = true;

					for (int j = 0; j < b.length; j++)
					{
						if (!firstTime)
						{
							rows.append(", ");
						}
						else
						{
							firstTime = false;
						}

						if (b[j] == null)
						{
							rows.append("null");
						}
						else
						{
							rows.append(new String(b[j]));
						}

					}

					rows.append("\n");
				}

				Debug.returnValue(this, "getImportedKeys", rows.toString());
			}

			return buildResultSet(fields, tuples, _conn);
		}
		else
		{
			return buildResultSet(fields, new Vector(), _conn);
		}

	}

	/**
	 * Get a description of the foreign key columns in the foreign key
	 * table that reference the primary key columns of the primary key
	 * table (describe how one table imports another's key.) This
	 * should normally return a single foreign key/primary key pair
	 * (most tables only import a foreign key from a table once.)  They
	 * are ordered by FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, and
	 * KEY_SEQ.
	 *
	 * <P>Each foreign key column description has the following columns:
	 *  <OL>
	 *    <LI><B>PKTABLE_CAT</B> String => primary key table catalog (may be null)
	 *    <LI><B>PKTABLE_SCHEM</B> String => primary key table schema (may be null)
	 *    <LI><B>PKTABLE_NAME</B> String => primary key table name
	 *    <LI><B>PKCOLUMN_NAME</B> String => primary key column name
	 *    <LI><B>FKTABLE_CAT</B> String => foreign key table catalog (may be null)
	 *      being exported (may be null)
	 *    <LI><B>FKTABLE_SCHEM</B> String => foreign key table schema (may be null)
	 *      being exported (may be null)
	 *    <LI><B>FKTABLE_NAME</B> String => foreign key table name
	 *      being exported
	 *    <LI><B>FKCOLUMN_NAME</B> String => foreign key column name
	 *      being exported
	 *    <LI><B>KEY_SEQ</B> short => sequence number within foreign key
	 *    <LI><B>UPDATE_RULE</B> short => What happens to
	 *       foreign key when primary is updated:
	 *      <UL>
	 *      <LI> importedKeyCascade - change imported key to agree
	 *               with primary key update
	 *      <LI> importedKeyRestrict - do not allow update of primary
	 *               key if it has been imported
	 *      <LI> importedKeySetNull - change imported key to NULL if
	 *               its primary key has been updated
	 *      </UL>
	 *    <LI><B>DELETE_RULE</B> short => What happens to
	 *      the foreign key when primary is deleted.
	 *      <UL>
	 *      <LI> importedKeyCascade - delete rows that import a deleted key
	 *      <LI> importedKeyRestrict - do not allow delete of primary
	 *               key if it has been imported
	 *      <LI> importedKeySetNull - change imported key to NULL if
	 *               its primary key has been deleted
	 *      </UL>
	 *    <LI><B>FK_NAME</B> String => foreign key identifier (may be null)
	 *    <LI><B>PK_NAME</B> String => primary key identifier (may be null)
	 *  </OL>
	 *
	 * @param catalog a catalog name; "" retrieves those without a catalog
	 * @param schema a schema name pattern; "" retrieves those
	 * without a schema
	 * @param table a table name
	 * @return ResultSet each row is a foreign key column description
	 * @see #getImportedKeys
	 */

	public java.sql.ResultSet getCrossReference(
		String primaryCatalog,
		String primarySchema,
		String primaryTable,
		String foreignCatalog,
		String foreignSchema,
		String foreignTable)
		throws java.sql.SQLException
	{

		if (Driver.trace)
		{
			Object[] args =
				{
					primaryCatalog,
					primarySchema,
					primaryTable,
					foreignCatalog,
					foreignSchema,
					foreignTable };
			Debug.methodCall(this, "getCrossReference", args);
		}

		Field[] fields = new Field[14];

		fields[0] = new Field("", "PKTABLE_CAT", Types.CHAR, 255);
		fields[1] = new Field("", "PKTABLE_SCHEM", Types.CHAR, 0);
		fields[2] = new Field("", "PKTABLE_NAME", Types.CHAR, 255);
		fields[3] = new Field("", "PKCOLUMN_NAME", Types.CHAR, 32);
		fields[4] = new Field("", "FKTABLE_CAT", Types.CHAR, 255);
		fields[5] = new Field("", "FKTABLE_SCHEM", Types.CHAR, 0);
		fields[6] = new Field("", "FKTABLE_NAME", Types.CHAR, 255);
		fields[7] = new Field("", "FKCOLUMN_NAME", Types.CHAR, 32);
		fields[8] = new Field("", "KEY_SEQ", Types.SMALLINT, 2);
		fields[9] = new Field("", "UPDATE_RULE", Types.SMALLINT, 2);
		fields[10] = new Field("", "DELETE_RULE", Types.SMALLINT, 2);
		fields[11] = new Field("", "FK_NAME", Types.CHAR, 0);
		fields[12] = new Field("", "PK_NAME", Types.CHAR, 0);
		fields[13] = new Field("", "DEFERRABILITY", Types.INTEGER, 2);

		if (_conn.getIO().versionMeetsMinimum(3, 23, 0))
		{
			/*
			 * Get foreign key information for table
			 */

			String DB_Sub = "";

			if (foreignCatalog != null)
			{
				if (!foreignCatalog.equals(""))
				{
					DB_Sub = " FROM " + foreignCatalog;
				}
			}
			else
			{
				DB_Sub = " FROM " + _database;
			}

			if (primaryTable == null)
			{
				throw new java.sql.SQLException("Table not specified.", "S1009");
			}

			com.mysql.jdbc.ResultSet fkRS =
				_conn.execSQL("show table status " + DB_Sub, -1);

			fkRS.setConnection(_conn);

			/*
			 * Parse imported foreign key information
			 */

			Vector tuples = new Vector();
			String dummy;

			while (fkRS.next())
			{
				String tableType = fkRS.getString("Type");

				if (tableType != null && tableType.toLowerCase().equals("innodb"))
				{

					String comment = fkRS.getString("Comment");
					if (comment != null)
					{
						StringTokenizer commentTokens = new StringTokenizer(comment, ";", false);
						if (commentTokens.hasMoreTokens())
						{
							dummy = commentTokens.nextToken(); // Skip InnoDB comment
						}
						int keySeq = 0;

						while (commentTokens.hasMoreTokens())
						{
							String keys = commentTokens.nextToken();
							StringTokenizer keyTokens = new StringTokenizer(keys, "() /", false);
							byte[][] tuple = new byte[14][];
							tuple[4] = (foreignCatalog == null ? null : foreignCatalog.getBytes());
							// FKTABLE_CAT
							tuple[5] = (foreignSchema == null ? null : foreignSchema.getBytes());
							// FKTABLE_SCHEM
							dummy = fkRS.getString("Name");
							if (dummy.compareTo(foreignTable) != 0)
							{
								continue;
							}
							else
							{
								tuple[6] = dummy.getBytes(); // FKTABLE_NAME
							}

							tuple[7] = s2b(keyTokens.nextToken()); // FKCOLUMN_NAME
							dummy = keyTokens.nextToken(); // Skip REFER
							tuple[0] = s2b(keyTokens.nextToken()); // PKTABLE_CAT
							tuple[1] = (primarySchema == null ? null : primarySchema.getBytes());
							// PKTABLE_SCHEM
							// Skip foreign key if it doesn't refer to the right table
							dummy = keyTokens.nextToken(); // PKTABLE_NAME
							if (dummy.compareTo(primaryTable) != 0)
							{
								continue;
							}
							tuple[2] = s2b(primaryTable); // PKTABLE_NAME
							tuple[3] = s2b(keyTokens.nextToken()); // PKCOLUMN_NAME
							tuple[8] = Integer.toString(keySeq).getBytes(); // KEY_SEQ
							tuple[9] =
								Integer.toString(java.sql.DatabaseMetaData.importedKeySetDefault).getBytes();
							// UPDATE_RULE
							tuple[10] =
								Integer.toString(java.sql.DatabaseMetaData.importedKeySetDefault).getBytes();
							// DELETE_RULE
							tuple[11] = null; // FK_NAME
							tuple[12] = null; // PK_NAME
							tuple[13] =
								Integer.toString(java.sql.DatabaseMetaData.importedKeyNotDeferrable).getBytes();
							// DEFERRABILITY
							tuples.addElement(tuple);
							keySeq++;
						}
					}
				}
			}

			if (Driver.trace)
			{

				StringBuffer rows = new StringBuffer();

				rows.append("\n");

				for (int i = 0; i < tuples.size(); i++)
				{
					byte[][] b = (byte[][]) tuples.elementAt(i);

					rows.append("[Row] ");

					boolean firstTime = true;

					for (int j = 0; j < b.length; j++)
					{
						if (!firstTime)
						{
							rows.append(", ");
						}
						else
						{
							firstTime = false;
						}

						if (b[j] == null)
						{
							rows.append("null");
						}
						else
						{
							rows.append(new String(b[j]));
						}

					}

					rows.append("\n");
				}

				Debug.returnValue(this, "getImportedKeys", rows.toString());
			}

			return buildResultSet(fields, tuples, _conn);
		}
		else
		{
			return buildResultSet(fields, new Vector(), _conn);
		}
	}

	/**
	 * Get a description of all the standard SQL types supported by
	 * this database. They are ordered by DATA_TYPE and then by how
	 * closely the data type maps to the corresponding JDBC SQL type.
	 *
	 * <P>Each type description has the following columns:
	 *  <OL>
	 *    <LI><B>TYPE_NAME</B> String => Type name
	 *    <LI><B>DATA_TYPE</B> short => SQL data type from java.sql.Types
	 *    <LI><B>PRECISION</B> int => maximum precision
	 *    <LI><B>LITERAL_PREFIX</B> String => prefix used to quote a literal
	 *      (may be null)
	 *    <LI><B>LITERAL_SUFFIX</B> String => suffix used to quote a literal
	 (may be null)
	 *    <LI><B>CREATE_PARAMS</B> String => parameters used in creating
	 *      the type (may be null)
	 *    <LI><B>NULLABLE</B> short => can you use NULL for this type?
	 *      <UL>
	 *      <LI> typeNoNulls - does not allow NULL values
	 *      <LI> typeNullable - allows NULL values
	 *      <LI> typeNullableUnknown - nullability unknown
	 *      </UL>
	 *    <LI><B>CASE_SENSITIVE</B> boolean=> is it case sensitive?
	 *    <LI><B>SEARCHABLE</B> short => can you use "WHERE" based on this type:
	 *      <UL>
	 *      <LI> typePredNone - No support
	 *      <LI> typePredChar - Only supported with WHERE .. LIKE
	 *      <LI> typePredBasic - Supported except for WHERE .. LIKE
	 *      <LI> typeSearchable - Supported for all WHERE ..
	 *      </UL>
	 *    <LI><B>UNSIGNED_ATTRIBUTE</B> boolean => is it unsigned?
	 *    <LI><B>FIXED_PREC_SCALE</B> boolean => can it be a money value?
	 *    <LI><B>AUTO_INCREMENT</B> boolean => can it be used for an
	 *      auto-increment value?
	 *    <LI><B>LOCAL_TYPE_NAME</B> String => localized version of type name
	 *      (may be null)
	 *    <LI><B>MINIMUM_SCALE</B> short => minimum scale supported
	 *    <LI><B>MAXIMUM_SCALE</B> short => maximum scale supported
	 *    <LI><B>SQL_DATA_TYPE</B> int => unused
	 *    <LI><B>SQL_DATETIME_SUB</B> int => unused
	 *    <LI><B>NUM_PREC_RADIX</B> int => usually 2 or 10
	 *  </OL>
	 *
	 * @return ResultSet each row is a SQL type description
	 */

	/**
	* Get a description of all the standard SQL types supported by
	* this database. They are ordered by DATA_TYPE and then by how
	* closely the data type maps to the corresponding JDBC SQL type.
	*
	* <P>Each type description has the following columns:
	*  <OL>
	*    <LI><B>TYPE_NAME</B> String => Type name
	*    <LI><B>DATA_TYPE</B> short => SQL data type from java.sql.Types
	*    <LI><B>PRECISION</B> int => maximum precision
	*    <LI><B>LITERAL_PREFIX</B> String => prefix used to quote a literal
	*      (may be null)
	*    <LI><B>LITERAL_SUFFIX</B> String => suffix used to quote a literal
	(may be null)
	*    <LI><B>CREATE_PARAMS</B> String => parameters used in creating
	*      the type (may be null)
	*    <LI><B>NULLABLE</B> short => can you use NULL for this type?
	*      <UL>
	*      <LI> typeNoNulls - does not allow NULL values
	*      <LI> typeNullable - allows NULL values
	*      <LI> typeNullableUnknown - nullability unknown
	*      </UL>
	*    <LI><B>CASE_SENSITIVE</B> boolean=> is it case sensitive?
	*    <LI><B>SEARCHABLE</B> short => can you use "WHERE" based on this type:
	*      <UL>
	*      <LI> typePredNone - No support
	*      <LI> typePredChar - Only supported with WHERE .. LIKE
	*      <LI> typePredBasic - Supported except for WHERE .. LIKE
	*      <LI> typeSearchable - Supported for all WHERE ..
	*      </UL>
	*    <LI><B>UNSIGNED_ATTRIBUTE</B> boolean => is it unsigned?
	*    <LI><B>FIXED_PREC_SCALE</B> boolean => can it be a money value?
	*    <LI><B>AUTO_INCREMENT</B> boolean => can it be used for an
	*      auto-increment value?
	*    <LI><B>LOCAL_TYPE_NAME</B> String => localized version of type name
	*      (may be null)
	*    <LI><B>MINIMUM_SCALE</B> short => minimum scale supported
	*    <LI><B>MAXIMUM_SCALE</B> short => maximum scale supported
	*    <LI><B>SQL_DATA_TYPE</B> int => unused
	*    <LI><B>SQL_DATETIME_SUB</B> int => unused
	*    <LI><B>NUM_PREC_RADIX</B> int => usually 2 or 10
	*  </OL>
	*
	* @return ResultSet each row is a SQL type description
	*/

	public java.sql.ResultSet getTypeInfo() throws java.sql.SQLException
	{
		Field[] Fields = new Field[18];

		Fields[0] = new Field("", "TYPE_NAME", Types.CHAR, 32);
		Fields[1] = new Field("", "DATA_TYPE", Types.SMALLINT, 5);
		Fields[2] = new Field("", "PRECISION", Types.INTEGER, 10);
		Fields[3] = new Field("", "LITERAL_PREFIX", Types.CHAR, 4);
		Fields[4] = new Field("", "LITERAL_SUFFIX", Types.CHAR, 4);
		Fields[5] = new Field("", "CREATE_PARAMS", Types.CHAR, 32);
		Fields[6] = new Field("", "NULLABLE", Types.SMALLINT, 5);
		Fields[7] = new Field("", "CASE_SENSITIVE", Types.CHAR, 3);
		Fields[8] = new Field("", "SEARCHABLE", Types.SMALLINT, 3);
		Fields[9] = new Field("", "UNSIGNED_ATTRIBUTE", Types.CHAR, 3);
		Fields[10] = new Field("", "FIXED_PREC_SCALE", Types.CHAR, 3);
		Fields[11] = new Field("", "AUTO_INCREMENT", Types.CHAR, 3);
		Fields[12] = new Field("", "LOCAL_TYPE_NAME", Types.CHAR, 32);
		Fields[13] = new Field("", "MINIMUM_SCALE", Types.SMALLINT, 5);
		Fields[14] = new Field("", "MAXIMUM_SCALE", Types.SMALLINT, 5);
		Fields[15] = new Field("", "SQL_DATA_TYPE", Types.INTEGER, 10);
		Fields[16] = new Field("", "SQL_DATETIME_SUB", Types.INTEGER, 10);
		Fields[17] = new Field("", "NUM_PREC_RADIX", Types.INTEGER, 10);

		byte[][] RowVal = null;

		Vector Tuples = new Vector();

		/*
		 * The following are ordered by java.sql.Types, and
		 * then by how closely the MySQL type matches the 
		 * JDBC Type (per spec)
		 */

		/*
		 * MySQL Type: BIT (silently converted to TINYINT(1))
		 * JDBC  Type: BIT
		 */

		RowVal = new byte[18][];
		RowVal[0] = s2b("BIT");
		RowVal[1] = Integer.toString(java.sql.Types.BIT).getBytes(); // JDBC Data type
		RowVal[2] = s2b("1"); // Precision
		RowVal[3] = s2b(""); // Literal Prefix
		RowVal[4] = s2b(""); // Literal Suffix
		RowVal[5] = s2b(""); // Create Params
		RowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();
		// Nullable
		RowVal[7] = s2b("true"); // Case Sensitive
		RowVal[8] =
			Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();
		// Searchable
		RowVal[9] = s2b("false"); // Unsignable
		RowVal[10] = s2b("false"); // Fixed Prec Scale
		RowVal[11] = s2b("false"); // Auto Increment
		RowVal[12] = s2b("BIT"); // Locale Type Name
		RowVal[13] = s2b("0"); // Minimum Scale
		RowVal[14] = s2b("0"); // Maximum Scale
		RowVal[15] = s2b("0"); // SQL Data Type (not used)
		RowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		RowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)

		Tuples.addElement(RowVal);

		/*
		 * MySQL Type: BOOL (silently converted to TINYINT(1))
		 * JDBC  Type: BIT
		 */

		RowVal = new byte[18][];
		RowVal[0] = s2b("BOOL");
		RowVal[1] = Integer.toString(java.sql.Types.BIT).getBytes(); // JDBC Data type
		RowVal[2] = s2b("1"); // Precision
		RowVal[3] = s2b(""); // Literal Prefix
		RowVal[4] = s2b(""); // Literal Suffix
		RowVal[5] = s2b(""); // Create Params
		RowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();
		// Nullable
		RowVal[7] = s2b("true"); // Case Sensitive
		RowVal[8] =
			Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();
		// Searchable
		RowVal[9] = s2b("false"); // Unsignable
		RowVal[10] = s2b("false"); // Fixed Prec Scale
		RowVal[11] = s2b("false"); // Auto Increment
		RowVal[12] = s2b("BOOL"); // Locale Type Name
		RowVal[13] = s2b("0"); // Minimum Scale
		RowVal[14] = s2b("0"); // Maximum Scale
		RowVal[15] = s2b("0"); // SQL Data Type (not used)
		RowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		RowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)

		Tuples.addElement(RowVal);

		/*
		 * MySQL Type: TINYINT
		 * JDBC  Type: TINYINT
		 */

		RowVal = new byte[18][];
		RowVal[0] = s2b("TINYINT");
		RowVal[1] = Integer.toString(java.sql.Types.TINYINT).getBytes();
		// JDBC Data type
		RowVal[2] = s2b("3"); // Precision
		RowVal[3] = s2b(""); // Literal Prefix
		RowVal[4] = s2b(""); // Literal Suffix
		RowVal[5] = s2b("[(M)] [UNSIGNED] [ZEROFILL]"); // Create Params
		RowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();
		// Nullable
		RowVal[7] = s2b("false"); // Case Sensitive
		RowVal[8] =
			Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();
		// Searchable
		RowVal[9] = s2b("true"); // Unsignable
		RowVal[10] = s2b("false"); // Fixed Prec Scale
		RowVal[11] = s2b("true"); // Auto Increment
		RowVal[12] = s2b("TINYINT"); // Locale Type Name
		RowVal[13] = s2b("0"); // Minimum Scale
		RowVal[14] = s2b("0"); // Maximum Scale
		RowVal[15] = s2b("0"); // SQL Data Type (not used)
		RowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		RowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)

		Tuples.addElement(RowVal);

		/*
		 * MySQL Type: BIGINT
		 * JDBC  Type: BIGINT
		 */

		RowVal = new byte[18][];
		RowVal[0] = s2b("BIGINT");
		RowVal[1] = Integer.toString(java.sql.Types.BIGINT).getBytes();
		// JDBC Data type
		RowVal[2] = s2b("19"); // Precision
		RowVal[3] = s2b(""); // Literal Prefix
		RowVal[4] = s2b(""); // Literal Suffix
		RowVal[5] = s2b("[(M)] [UNSIGNED] [ZEROFILL]"); // Create Params
		RowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();
		// Nullable
		RowVal[7] = s2b("false"); // Case Sensitive
		RowVal[8] =
			Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();
		// Searchable
		RowVal[9] = s2b("true"); // Unsignable
		RowVal[10] = s2b("false"); // Fixed Prec Scale
		RowVal[11] = s2b("true"); // Auto Increment
		RowVal[12] = s2b("BIGINT"); // Locale Type Name
		RowVal[13] = s2b("0"); // Minimum Scale
		RowVal[14] = s2b("0"); // Maximum Scale
		RowVal[15] = s2b("0"); // SQL Data Type (not used)
		RowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		RowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)

		Tuples.addElement(RowVal);

		/*
		 * MySQL Type: LONG VARBINARY
		 * JDBC  Type: LONGVARBINARY
		 */

		RowVal = new byte[18][];
		RowVal[0] = s2b("LONG VARBINARY");
		RowVal[1] = Integer.toString(java.sql.Types.LONGVARBINARY).getBytes();
		// JDBC Data type
		RowVal[2] = s2b("16777215"); // Precision
		RowVal[3] = s2b("'"); // Literal Prefix
		RowVal[4] = s2b("'"); // Literal Suffix
		RowVal[5] = s2b(""); // Create Params
		RowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();
		// Nullable
		RowVal[7] = s2b("true"); // Case Sensitive
		RowVal[8] =
			Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();
		// Searchable
		RowVal[9] = s2b("false"); // Unsignable
		RowVal[10] = s2b("false"); // Fixed Prec Scale
		RowVal[11] = s2b("false"); // Auto Increment
		RowVal[12] = s2b("LONG VARBINARY"); // Locale Type Name
		RowVal[13] = s2b("0"); // Minimum Scale
		RowVal[14] = s2b("0"); // Maximum Scale
		RowVal[15] = s2b("0"); // SQL Data Type (not used)
		RowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		RowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)

		Tuples.addElement(RowVal);

		/*
		 * MySQL Type: MEDIUMBLOB
		 * JDBC  Type: LONGVARBINARY
		 */

		RowVal = new byte[18][];
		RowVal[0] = s2b("MEDIUMBLOB");
		RowVal[1] = Integer.toString(java.sql.Types.LONGVARBINARY).getBytes();
		// JDBC Data type
		RowVal[2] = s2b("16777215"); // Precision
		RowVal[3] = s2b("'"); // Literal Prefix
		RowVal[4] = s2b("'"); // Literal Suffix
		RowVal[5] = s2b(""); // Create Params
		RowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();
		// Nullable
		RowVal[7] = s2b("true"); // Case Sensitive
		RowVal[8] =
			Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();
		// Searchable
		RowVal[9] = s2b("false"); // Unsignable
		RowVal[10] = s2b("false"); // Fixed Prec Scale
		RowVal[11] = s2b("false"); // Auto Increment
		RowVal[12] = s2b("MEDIUMBLOB"); // Locale Type Name
		RowVal[13] = s2b("0"); // Minimum Scale
		RowVal[14] = s2b("0"); // Maximum Scale
		RowVal[15] = s2b("0"); // SQL Data Type (not used)
		RowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		RowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)

		Tuples.addElement(RowVal);

		/*
		 * MySQL Type: LONGBLOB
		 * JDBC  Type: LONGVARBINARY
		 */

		RowVal = new byte[18][];
		RowVal[0] = s2b("LONGBLOB");
		RowVal[1] = Integer.toString(java.sql.Types.LONGVARBINARY).getBytes();
		// JDBC Data type
		RowVal[2] = Integer.toString(Integer.MAX_VALUE).getBytes(); // Precision
		RowVal[3] = s2b("'"); // Literal Prefix
		RowVal[4] = s2b("'"); // Literal Suffix
		RowVal[5] = s2b(""); // Create Params
		RowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();
		// Nullable
		RowVal[7] = s2b("true"); // Case Sensitive
		RowVal[8] =
			Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();
		// Searchable
		RowVal[9] = s2b("false"); // Unsignable
		RowVal[10] = s2b("false"); // Fixed Prec Scale
		RowVal[11] = s2b("false"); // Auto Increment
		RowVal[12] = s2b("LONGBLOB"); // Locale Type Name
		RowVal[13] = s2b("0"); // Minimum Scale
		RowVal[14] = s2b("0"); // Maximum Scale
		RowVal[15] = s2b("0"); // SQL Data Type (not used)
		RowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		RowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)

		Tuples.addElement(RowVal);

		/*
		 * MySQL Type: BLOB
		 * JDBC  Type: LONGVARBINARY
		 */

		RowVal = new byte[18][];
		RowVal[0] = s2b("BLOB");
		RowVal[1] = Integer.toString(java.sql.Types.LONGVARBINARY).getBytes();
		// JDBC Data type
		RowVal[2] = s2b("65535"); // Precision
		RowVal[3] = s2b("'"); // Literal Prefix
		RowVal[4] = s2b("'"); // Literal Suffix
		RowVal[5] = s2b(""); // Create Params
		RowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();
		// Nullable
		RowVal[7] = s2b("true"); // Case Sensitive
		RowVal[8] =
			Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();
		// Searchable
		RowVal[9] = s2b("false"); // Unsignable
		RowVal[10] = s2b("false"); // Fixed Prec Scale
		RowVal[11] = s2b("false"); // Auto Increment
		RowVal[12] = s2b("BLOB"); // Locale Type Name
		RowVal[13] = s2b("0"); // Minimum Scale
		RowVal[14] = s2b("0"); // Maximum Scale
		RowVal[15] = s2b("0"); // SQL Data Type (not used)
		RowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		RowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)

		Tuples.addElement(RowVal);

		/*
		 * MySQL Type: TINYBLOB
		 * JDBC  Type: LONGVARBINARY
		 */

		RowVal = new byte[18][];
		RowVal[0] = s2b("TINYBLOB");
		RowVal[1] = Integer.toString(java.sql.Types.LONGVARBINARY).getBytes();
		// JDBC Data type
		RowVal[2] = s2b("255"); // Precision
		RowVal[3] = s2b("'"); // Literal Prefix
		RowVal[4] = s2b("'"); // Literal Suffix
		RowVal[5] = s2b(""); // Create Params
		RowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();
		// Nullable
		RowVal[7] = s2b("true"); // Case Sensitive
		RowVal[8] =
			Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();
		// Searchable
		RowVal[9] = s2b("false"); // Unsignable
		RowVal[10] = s2b("false"); // Fixed Prec Scale
		RowVal[11] = s2b("false"); // Auto Increment
		RowVal[12] = s2b("TINYBLOB"); // Locale Type Name
		RowVal[13] = s2b("0"); // Minimum Scale
		RowVal[14] = s2b("0"); // Maximum Scale
		RowVal[15] = s2b("0"); // SQL Data Type (not used)
		RowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		RowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)

		Tuples.addElement(RowVal);

		/*
		 * MySQL Type: VARBINARY (sliently converted to VARCHAR(M) BINARY)
		 * JDBC  Type: VARBINARY
		 */

		RowVal = new byte[18][];
		RowVal[0] = s2b("VARBINARY");
		RowVal[1] = Integer.toString(java.sql.Types.VARBINARY).getBytes();
		// JDBC Data type
		RowVal[2] = s2b("255"); // Precision
		RowVal[3] = s2b("'"); // Literal Prefix
		RowVal[4] = s2b("'"); // Literal Suffix
		RowVal[5] = s2b("(M)"); // Create Params
		RowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();
		// Nullable
		RowVal[7] = s2b("true"); // Case Sensitive
		RowVal[8] =
			Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();
		// Searchable
		RowVal[9] = s2b("false"); // Unsignable
		RowVal[10] = s2b("false"); // Fixed Prec Scale
		RowVal[11] = s2b("false"); // Auto Increment
		RowVal[12] = s2b("VARBINARY"); // Locale Type Name
		RowVal[13] = s2b("0"); // Minimum Scale
		RowVal[14] = s2b("0"); // Maximum Scale
		RowVal[15] = s2b("0"); // SQL Data Type (not used)
		RowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		RowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)

		Tuples.addElement(RowVal);

		/*
		 * MySQL Type: BINARY (silently converted to CHAR(M) BINARY)
		 * JDBC  Type: BINARY
		 */

		RowVal = new byte[18][];
		RowVal[0] = s2b("BINARY");
		RowVal[1] = Integer.toString(java.sql.Types.BINARY).getBytes();
		// JDBC Data type
		RowVal[2] = s2b("255"); // Precision
		RowVal[3] = s2b("'"); // Literal Prefix
		RowVal[4] = s2b("'"); // Literal Suffix
		RowVal[5] = s2b("(M)"); // Create Params
		RowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();
		// Nullable
		RowVal[7] = s2b("true"); // Case Sensitive
		RowVal[8] =
			Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();
		// Searchable
		RowVal[9] = s2b("false"); // Unsignable
		RowVal[10] = s2b("false"); // Fixed Prec Scale
		RowVal[11] = s2b("false"); // Auto Increment
		RowVal[12] = s2b("BINARY"); // Locale Type Name
		RowVal[13] = s2b("0"); // Minimum Scale
		RowVal[14] = s2b("0"); // Maximum Scale
		RowVal[15] = s2b("0"); // SQL Data Type (not used)
		RowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		RowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)

		Tuples.addElement(RowVal);

		/*
		 * MySQL Type: LONG VARCHAR
		 * JDBC  Type: LONGVARCHAR
		 */

		RowVal = new byte[18][];
		RowVal[0] = s2b("LONG VARCHAR");
		RowVal[1] = Integer.toString(java.sql.Types.LONGVARCHAR).getBytes();
		// JDBC Data type
		RowVal[2] = s2b("16777215"); // Precision
		RowVal[3] = s2b("'"); // Literal Prefix
		RowVal[4] = s2b("'"); // Literal Suffix
		RowVal[5] = s2b(""); // Create Params
		RowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();
		// Nullable
		RowVal[7] = s2b("false"); // Case Sensitive
		RowVal[8] =
			Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();
		// Searchable
		RowVal[9] = s2b("false"); // Unsignable
		RowVal[10] = s2b("false"); // Fixed Prec Scale
		RowVal[11] = s2b("false"); // Auto Increment
		RowVal[12] = s2b("LONG VARCHAR"); // Locale Type Name
		RowVal[13] = s2b("0"); // Minimum Scale
		RowVal[14] = s2b("0"); // Maximum Scale
		RowVal[15] = s2b("0"); // SQL Data Type (not used)
		RowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		RowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)

		Tuples.addElement(RowVal);

		/*
		 * MySQL Type: MEDIUMTEXT
		 * JDBC  Type: LONGVARCHAR
		 */

		RowVal = new byte[18][];
		RowVal[0] = s2b("MEDIUMTEXT");
		RowVal[1] = Integer.toString(java.sql.Types.LONGVARCHAR).getBytes();
		// JDBC Data type
		RowVal[2] = s2b("16777215"); // Precision
		RowVal[3] = s2b("'"); // Literal Prefix
		RowVal[4] = s2b("'"); // Literal Suffix
		RowVal[5] = s2b(""); // Create Params
		RowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();
		// Nullable
		RowVal[7] = s2b("false"); // Case Sensitive
		RowVal[8] =
			Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();
		// Searchable
		RowVal[9] = s2b("false"); // Unsignable
		RowVal[10] = s2b("false"); // Fixed Prec Scale
		RowVal[11] = s2b("false"); // Auto Increment
		RowVal[12] = s2b("MEDIUMTEXT"); // Locale Type Name
		RowVal[13] = s2b("0"); // Minimum Scale
		RowVal[14] = s2b("0"); // Maximum Scale
		RowVal[15] = s2b("0"); // SQL Data Type (not used)
		RowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		RowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)

		Tuples.addElement(RowVal);

		/*
		 * MySQL Type: LONGTEXT
		 * JDBC  Type: LONGVARCHAR
		 */

		RowVal = new byte[18][];
		RowVal[0] = s2b("LONGTEXT");
		RowVal[1] = Integer.toString(java.sql.Types.LONGVARCHAR).getBytes();
		// JDBC Data type
		RowVal[2] = Integer.toString(Integer.MAX_VALUE).getBytes(); // Precision
		RowVal[3] = s2b("'"); // Literal Prefix
		RowVal[4] = s2b("'"); // Literal Suffix
		RowVal[5] = s2b(""); // Create Params
		RowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();
		// Nullable
		RowVal[7] = s2b("false"); // Case Sensitive
		RowVal[8] =
			Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();
		// Searchable
		RowVal[9] = s2b("false"); // Unsignable
		RowVal[10] = s2b("false"); // Fixed Prec Scale
		RowVal[11] = s2b("false"); // Auto Increment
		RowVal[12] = s2b("LONGTEXT"); // Locale Type Name
		RowVal[13] = s2b("0"); // Minimum Scale
		RowVal[14] = s2b("0"); // Maximum Scale
		RowVal[15] = s2b("0"); // SQL Data Type (not used)
		RowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		RowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)

		Tuples.addElement(RowVal);

		/*
		 * MySQL Type: TEXT
		 * JDBC  Type: LONGVARCHAR
		 */

		RowVal = new byte[18][];
		RowVal[0] = s2b("TEXT");
		RowVal[1] = Integer.toString(java.sql.Types.LONGVARCHAR).getBytes();
		// JDBC Data type
		RowVal[2] = s2b("65535"); // Precision
		RowVal[3] = s2b("'"); // Literal Prefix
		RowVal[4] = s2b("'"); // Literal Suffix
		RowVal[5] = s2b(""); // Create Params
		RowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();
		// Nullable
		RowVal[7] = s2b("false"); // Case Sensitive
		RowVal[8] =
			Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();
		// Searchable
		RowVal[9] = s2b("false"); // Unsignable
		RowVal[10] = s2b("false"); // Fixed Prec Scale
		RowVal[11] = s2b("false"); // Auto Increment
		RowVal[12] = s2b("TEXT"); // Locale Type Name
		RowVal[13] = s2b("0"); // Minimum Scale
		RowVal[14] = s2b("0"); // Maximum Scale
		RowVal[15] = s2b("0"); // SQL Data Type (not used)
		RowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		RowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)

		Tuples.addElement(RowVal);

		/*
		 * MySQL Type: TINYTEXT
		 * JDBC  Type: LONGVARCHAR
		 */

		RowVal = new byte[18][];
		RowVal[0] = s2b("TINYTEXT");
		RowVal[1] = Integer.toString(java.sql.Types.LONGVARCHAR).getBytes();
		// JDBC Data type
		RowVal[2] = s2b("255"); // Precision
		RowVal[3] = s2b("'"); // Literal Prefix
		RowVal[4] = s2b("'"); // Literal Suffix
		RowVal[5] = s2b(""); // Create Params
		RowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();
		// Nullable
		RowVal[7] = s2b("false"); // Case Sensitive
		RowVal[8] =
			Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();
		// Searchable
		RowVal[9] = s2b("false"); // Unsignable
		RowVal[10] = s2b("false"); // Fixed Prec Scale
		RowVal[11] = s2b("false"); // Auto Increment
		RowVal[12] = s2b("TINYTEXT"); // Locale Type Name
		RowVal[13] = s2b("0"); // Minimum Scale
		RowVal[14] = s2b("0"); // Maximum Scale
		RowVal[15] = s2b("0"); // SQL Data Type (not used)
		RowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		RowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)

		Tuples.addElement(RowVal);

		/*
		 * MySQL Type: CHAR
		 * JDBC  Type: CHAR
		 */

		RowVal = new byte[18][];
		RowVal[0] = s2b("CHAR");
		RowVal[1] = Integer.toString(java.sql.Types.CHAR).getBytes(); // JDBC Data type
		RowVal[2] = s2b("255"); // Precision
		RowVal[3] = s2b("'"); // Literal Prefix
		RowVal[4] = s2b("'"); // Literal Suffix
		RowVal[5] = s2b("(M)"); // Create Params
		RowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();
		// Nullable
		RowVal[7] = s2b("false"); // Case Sensitive
		RowVal[8] =
			Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();
		// Searchable
		RowVal[9] = s2b("false"); // Unsignable
		RowVal[10] = s2b("false"); // Fixed Prec Scale
		RowVal[11] = s2b("false"); // Auto Increment
		RowVal[12] = s2b("CHAR"); // Locale Type Name
		RowVal[13] = s2b("0"); // Minimum Scale
		RowVal[14] = s2b("0"); // Maximum Scale
		RowVal[15] = s2b("0"); // SQL Data Type (not used)
		RowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		RowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)

		Tuples.addElement(RowVal);

		/*
		 * MySQL Type: NUMERIC (silently converted to DECIMAL)
		 * JDBC  Type: NUMERIC
		 */

		RowVal = new byte[18][];
		RowVal[0] = s2b("NUMERIC");
		RowVal[1] = Integer.toString(java.sql.Types.NUMERIC).getBytes();
		// JDBC Data type
		RowVal[2] = s2b("17"); // Precision
		RowVal[3] = s2b(""); // Literal Prefix
		RowVal[4] = s2b(""); // Literal Suffix
		RowVal[5] = s2b("[(M[,D])] [ZEROFILL]"); // Create Params
		RowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();
		// Nullable
		RowVal[7] = s2b("false"); // Case Sensitive
		RowVal[8] =
			Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();
		// Searchable
		RowVal[9] = s2b("false"); // Unsignable
		RowVal[10] = s2b("false"); // Fixed Prec Scale
		RowVal[11] = s2b("true"); // Auto Increment
		RowVal[12] = s2b("NUMERIC"); // Locale Type Name
		RowVal[13] = s2b("308"); // Minimum Scale
		RowVal[14] = s2b("308"); // Maximum Scale
		RowVal[15] = s2b("0"); // SQL Data Type (not used)
		RowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		RowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)

		Tuples.addElement(RowVal);

		/*
		 * MySQL Type: DECIMAL
		 * JDBC  Type: DECIMAL
		 */

		RowVal = new byte[18][];
		RowVal[0] = s2b("DECIMAL");
		RowVal[1] = Integer.toString(java.sql.Types.DECIMAL).getBytes();
		// JDBC Data type
		RowVal[2] = s2b("17"); // Precision
		RowVal[3] = s2b(""); // Literal Prefix
		RowVal[4] = s2b(""); // Literal Suffix
		RowVal[5] = s2b("[(M[,D])] [ZEROFILL]"); // Create Params
		RowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();
		// Nullable
		RowVal[7] = s2b("false"); // Case Sensitive
		RowVal[8] =
			Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();
		// Searchable
		RowVal[9] = s2b("false"); // Unsignable
		RowVal[10] = s2b("false"); // Fixed Prec Scale
		RowVal[11] = s2b("true"); // Auto Increment
		RowVal[12] = s2b("DECIMAL"); // Locale Type Name
		RowVal[13] = s2b("-308"); // Minimum Scale
		RowVal[14] = s2b("308"); // Maximum Scale
		RowVal[15] = s2b("0"); // SQL Data Type (not used)
		RowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		RowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)

		Tuples.addElement(RowVal);

		/*
		 * MySQL Type: INTEGER
		 * JDBC  Type: INTEGER
		 */

		RowVal = new byte[18][];
		RowVal[0] = s2b("INTEGER");
		RowVal[1] = Integer.toString(java.sql.Types.INTEGER).getBytes();
		// JDBC Data type
		RowVal[2] = s2b("10"); // Precision
		RowVal[3] = s2b(""); // Literal Prefix
		RowVal[4] = s2b(""); // Literal Suffix
		RowVal[5] = s2b("[(M)] [UNSIGNED] [ZEROFILL]"); // Create Params
		RowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();
		// Nullable
		RowVal[7] = s2b("false"); // Case Sensitive
		RowVal[8] =
			Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();
		// Searchable
		RowVal[9] = s2b("true"); // Unsignable
		RowVal[10] = s2b("false"); // Fixed Prec Scale
		RowVal[11] = s2b("true"); // Auto Increment
		RowVal[12] = s2b("INTEGER"); // Locale Type Name
		RowVal[13] = s2b("0"); // Minimum Scale
		RowVal[14] = s2b("0"); // Maximum Scale
		RowVal[15] = s2b("0"); // SQL Data Type (not used)
		RowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		RowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)

		Tuples.addElement(RowVal);

		/*
		 * MySQL Type: INT
		 * JDBC  Type: INTEGER
		 */

		RowVal = new byte[18][];
		RowVal[0] = s2b("INT");
		RowVal[1] = Integer.toString(java.sql.Types.INTEGER).getBytes();
		// JDBC Data type
		RowVal[2] = s2b("10"); // Precision
		RowVal[3] = s2b(""); // Literal Prefix
		RowVal[4] = s2b(""); // Literal Suffix
		RowVal[5] = s2b("[(M)] [UNSIGNED] [ZEROFILL]"); // Create Params
		RowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();
		// Nullable
		RowVal[7] = s2b("false"); // Case Sensitive
		RowVal[8] =
			Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();
		// Searchable
		RowVal[9] = s2b("true"); // Unsignable
		RowVal[10] = s2b("false"); // Fixed Prec Scale
		RowVal[11] = s2b("true"); // Auto Increment
		RowVal[12] = s2b("INT"); // Locale Type Name
		RowVal[13] = s2b("0"); // Minimum Scale
		RowVal[14] = s2b("0"); // Maximum Scale
		RowVal[15] = s2b("0"); // SQL Data Type (not used)
		RowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		RowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)

		Tuples.addElement(RowVal);

		/*
		 * MySQL Type: MEDIUMINT
		 * JDBC  Type: INTEGER
		 */

		RowVal = new byte[18][];
		RowVal[0] = s2b("MEDIUMINT");
		RowVal[1] = Integer.toString(java.sql.Types.INTEGER).getBytes();
		// JDBC Data type
		RowVal[2] = s2b("7"); // Precision
		RowVal[3] = s2b(""); // Literal Prefix
		RowVal[4] = s2b(""); // Literal Suffix
		RowVal[5] = s2b("[(M)] [UNSIGNED] [ZEROFILL]"); // Create Params
		RowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();
		// Nullable
		RowVal[7] = s2b("false"); // Case Sensitive
		RowVal[8] =
			Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();
		// Searchable
		RowVal[9] = s2b("true"); // Unsignable
		RowVal[10] = s2b("false"); // Fixed Prec Scale
		RowVal[11] = s2b("true"); // Auto Increment
		RowVal[12] = s2b("MEDIUMINT"); // Locale Type Name
		RowVal[13] = s2b("0"); // Minimum Scale
		RowVal[14] = s2b("0"); // Maximum Scale
		RowVal[15] = s2b("0"); // SQL Data Type (not used)
		RowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		RowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10) 

		Tuples.addElement(RowVal);

		/*
		 * MySQL Type: SMALLINT
		 * JDBC  Type: SMALLINT
		 */

		RowVal = new byte[18][];
		RowVal[0] = s2b("SMALLINT");
		RowVal[1] = Integer.toString(java.sql.Types.SMALLINT).getBytes();
		// JDBC Data type
		RowVal[2] = s2b("5"); // Precision
		RowVal[3] = s2b(""); // Literal Prefix
		RowVal[4] = s2b(""); // Literal Suffix
		RowVal[5] = s2b("[(M)] [UNSIGNED] [ZEROFILL]"); // Create Params
		RowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();
		// Nullable
		RowVal[7] = s2b("false"); // Case Sensitive
		RowVal[8] =
			Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();
		// Searchable
		RowVal[9] = s2b("true"); // Unsignable
		RowVal[10] = s2b("false"); // Fixed Prec Scale
		RowVal[11] = s2b("true"); // Auto Increment
		RowVal[12] = s2b("SMALLINT"); // Locale Type Name
		RowVal[13] = s2b("0"); // Minimum Scale
		RowVal[14] = s2b("0"); // Maximum Scale
		RowVal[15] = s2b("0"); // SQL Data Type (not used)
		RowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		RowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10) 

		Tuples.addElement(RowVal);

		/*
		 * MySQL Type: DOUBLE
		 * JDBC  Type: FLOAT (is really an alias for DOUBLE from JDBC's perspective)
		 */

		RowVal = new byte[18][];
		RowVal[0] = s2b("DOUBLE");
		RowVal[1] = Integer.toString(java.sql.Types.FLOAT).getBytes();
		// JDBC Data type
		RowVal[2] = s2b("17"); // Precision
		RowVal[3] = s2b(""); // Literal Prefix
		RowVal[4] = s2b(""); // Literal Suffix
		RowVal[5] = s2b("[(M,D)] [ZEROFILL]"); // Create Params
		RowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();
		// Nullable
		RowVal[7] = s2b("false"); // Case Sensitive
		RowVal[8] =
			Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();
		// Searchable
		RowVal[9] = s2b("false"); // Unsignable
		RowVal[10] = s2b("false"); // Fixed Prec Scale
		RowVal[11] = s2b("true"); // Auto Increment
		RowVal[12] = s2b("DOUBLE"); // Locale Type Name
		RowVal[13] = s2b("-308"); // Minimum Scale
		RowVal[14] = s2b("308"); // Maximum Scale
		RowVal[15] = s2b("0"); // SQL Data Type (not used)
		RowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		RowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10) 

		Tuples.addElement(RowVal);

		/*
		 * MySQL Type: FLOAT
		 * JDBC  Type: REAL (this is the SINGLE PERCISION floating point type)
		 */

		RowVal = new byte[18][];
		RowVal[0] = s2b("FLOAT");
		RowVal[1] = Integer.toString(java.sql.Types.REAL).getBytes(); // JDBC Data type
		RowVal[2] = s2b("10"); // Precision
		RowVal[3] = s2b(""); // Literal Prefix
		RowVal[4] = s2b(""); // Literal Suffix
		RowVal[5] = s2b("[(M,D)] [ZEROFILL]"); // Create Params
		RowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();
		// Nullable
		RowVal[7] = s2b("false"); // Case Sensitive
		RowVal[8] =
			Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();
		// Searchable
		RowVal[9] = s2b("false"); // Unsignable
		RowVal[10] = s2b("false"); // Fixed Prec Scale
		RowVal[11] = s2b("true"); // Auto Increment
		RowVal[12] = s2b("FLOAT"); // Locale Type Name
		RowVal[13] = s2b("-38"); // Minimum Scale
		RowVal[14] = s2b("38"); // Maximum Scale
		RowVal[15] = s2b("0"); // SQL Data Type (not used)
		RowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		RowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10) 

		Tuples.addElement(RowVal);

		/*
		 * MySQL Type: DOUBLE
		 * JDBC  Type: DOUBLE
		 */

		RowVal = new byte[18][];
		RowVal[0] = s2b("DOUBLE");
		RowVal[1] = Integer.toString(java.sql.Types.DOUBLE).getBytes();
		// JDBC Data type
		RowVal[2] = s2b("17"); // Precision
		RowVal[3] = s2b(""); // Literal Prefix
		RowVal[4] = s2b(""); // Literal Suffix
		RowVal[5] = s2b("[(M,D)] [ZEROFILL]"); // Create Params
		RowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();
		// Nullable
		RowVal[7] = s2b("false"); // Case Sensitive
		RowVal[8] =
			Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();
		// Searchable
		RowVal[9] = s2b("false"); // Unsignable
		RowVal[10] = s2b("false"); // Fixed Prec Scale
		RowVal[11] = s2b("true"); // Auto Increment
		RowVal[12] = s2b("DOUBLE"); // Locale Type Name
		RowVal[13] = s2b("-308"); // Minimum Scale
		RowVal[14] = s2b("308"); // Maximum Scale
		RowVal[15] = s2b("0"); // SQL Data Type (not used)
		RowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		RowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10) 

		Tuples.addElement(RowVal);

		/*
		 * MySQL Type: DOUBLE PRECISION
		 * JDBC  Type: DOUBLE
		 */

		RowVal = new byte[18][];
		RowVal[0] = s2b("DOUBLE PRECISION");
		RowVal[1] = Integer.toString(java.sql.Types.DOUBLE).getBytes();
		// JDBC Data type
		RowVal[2] = s2b("17"); // Precision
		RowVal[3] = s2b(""); // Literal Prefix
		RowVal[4] = s2b(""); // Literal Suffix
		RowVal[5] = s2b("[(M,D)] [ZEROFILL]"); // Create Params
		RowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();
		// Nullable
		RowVal[7] = s2b("false"); // Case Sensitive
		RowVal[8] =
			Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();
		// Searchable
		RowVal[9] = s2b("false"); // Unsignable
		RowVal[10] = s2b("false"); // Fixed Prec Scale
		RowVal[11] = s2b("true"); // Auto Increment
		RowVal[12] = s2b("DOUBLE PRECISION"); // Locale Type Name
		RowVal[13] = s2b("-308"); // Minimum Scale
		RowVal[14] = s2b("308"); // Maximum Scale
		RowVal[15] = s2b("0"); // SQL Data Type (not used)
		RowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		RowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10) 

		Tuples.addElement(RowVal);

		/*
		 * MySQL Type: REAL (does not map to Types.REAL)
		 * JDBC  Type: DOUBLE
		 */

		RowVal = new byte[18][];
		RowVal[0] = s2b("REAL");
		RowVal[1] = Integer.toString(java.sql.Types.DOUBLE).getBytes();
		// JDBC Data type
		RowVal[2] = s2b("17"); // Precision
		RowVal[3] = s2b(""); // Literal Prefix
		RowVal[4] = s2b(""); // Literal Suffix
		RowVal[5] = s2b("[(M,D)] [ZEROFILL]"); // Create Params
		RowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();
		// Nullable
		RowVal[7] = s2b("false"); // Case Sensitive
		RowVal[8] =
			Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();
		// Searchable
		RowVal[9] = s2b("false"); // Unsignable
		RowVal[10] = s2b("false"); // Fixed Prec Scale
		RowVal[11] = s2b("true"); // Auto Increment
		RowVal[12] = s2b("REAL"); // Locale Type Name
		RowVal[13] = s2b("-308"); // Minimum Scale
		RowVal[14] = s2b("308"); // Maximum Scale
		RowVal[15] = s2b("0"); // SQL Data Type (not used)
		RowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		RowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10) 

		Tuples.addElement(RowVal);

		/*
		 * MySQL Type: VARCHAR
		 * JDBC  Type: VARCHAR
		 */

		RowVal = new byte[18][];
		RowVal[0] = s2b("VARCHAR");
		RowVal[1] = Integer.toString(java.sql.Types.VARCHAR).getBytes();
		// JDBC Data type
		RowVal[2] = s2b("255"); // Precision
		RowVal[3] = s2b("'"); // Literal Prefix
		RowVal[4] = s2b("'"); // Literal Suffix
		RowVal[5] = s2b("(M)"); // Create Params
		RowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();
		// Nullable
		RowVal[7] = s2b("false"); // Case Sensitive
		RowVal[8] =
			Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();
		// Searchable
		RowVal[9] = s2b("false"); // Unsignable
		RowVal[10] = s2b("false"); // Fixed Prec Scale
		RowVal[11] = s2b("false"); // Auto Increment
		RowVal[12] = s2b("VARCHAR"); // Locale Type Name
		RowVal[13] = s2b("0"); // Minimum Scale
		RowVal[14] = s2b("0"); // Maximum Scale
		RowVal[15] = s2b("0"); // SQL Data Type (not used)
		RowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		RowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10) 

		Tuples.addElement(RowVal);

		/*
		 * MySQL Type: ENUM
		 * JDBC  Type: VARCHAR
		 */

		RowVal = new byte[18][];
		RowVal[0] = s2b("ENUM");
		RowVal[1] = Integer.toString(java.sql.Types.VARCHAR).getBytes();
		// JDBC Data type
		RowVal[2] = s2b("65535"); // Precision
		RowVal[3] = s2b("'"); // Literal Prefix
		RowVal[4] = s2b("'"); // Literal Suffix
		RowVal[5] = s2b(""); // Create Params
		RowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();
		// Nullable
		RowVal[7] = s2b("false"); // Case Sensitive
		RowVal[8] =
			Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();
		// Searchable
		RowVal[9] = s2b("false"); // Unsignable
		RowVal[10] = s2b("false"); // Fixed Prec Scale
		RowVal[11] = s2b("false"); // Auto Increment
		RowVal[12] = s2b("ENUM"); // Locale Type Name
		RowVal[13] = s2b("0"); // Minimum Scale
		RowVal[14] = s2b("0"); // Maximum Scale
		RowVal[15] = s2b("0"); // SQL Data Type (not used)
		RowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		RowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10) 

		Tuples.addElement(RowVal);

		/*
		 * MySQL Type: SET
		 * JDBC  Type: VARCHAR
		 */

		RowVal = new byte[18][];
		RowVal[0] = s2b("SET");
		RowVal[1] = Integer.toString(java.sql.Types.VARCHAR).getBytes();
		// JDBC Data type
		RowVal[2] = s2b("64"); // Precision
		RowVal[3] = s2b("'"); // Literal Prefix
		RowVal[4] = s2b("'"); // Literal Suffix
		RowVal[5] = s2b(""); // Create Params
		RowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();
		// Nullable
		RowVal[7] = s2b("false"); // Case Sensitive
		RowVal[8] =
			Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();
		// Searchable
		RowVal[9] = s2b("false"); // Unsignable
		RowVal[10] = s2b("false"); // Fixed Prec Scale
		RowVal[11] = s2b("false"); // Auto Increment
		RowVal[12] = s2b("SET"); // Locale Type Name
		RowVal[13] = s2b("0"); // Minimum Scale
		RowVal[14] = s2b("0"); // Maximum Scale
		RowVal[15] = s2b("0"); // SQL Data Type (not used)
		RowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		RowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10) 

		Tuples.addElement(RowVal);

		/*
		 * MySQL Type: DATE
		 * JDBC  Type: DATE
		 */

		RowVal = new byte[18][];
		RowVal[0] = s2b("DATE");
		RowVal[1] = Integer.toString(java.sql.Types.DATE).getBytes(); // JDBC Data type
		RowVal[2] = s2b("0"); // Precision
		RowVal[3] = s2b("'"); // Literal Prefix
		RowVal[4] = s2b("'"); // Literal Suffix
		RowVal[5] = s2b(""); // Create Params
		RowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();
		// Nullable
		RowVal[7] = s2b("false"); // Case Sensitive
		RowVal[8] =
			Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();
		// Searchable
		RowVal[9] = s2b("false"); // Unsignable
		RowVal[10] = s2b("false"); // Fixed Prec Scale
		RowVal[11] = s2b("false"); // Auto Increment
		RowVal[12] = s2b("DATE"); // Locale Type Name
		RowVal[13] = s2b("0"); // Minimum Scale
		RowVal[14] = s2b("0"); // Maximum Scale
		RowVal[15] = s2b("0"); // SQL Data Type (not used)
		RowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		RowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10) 

		Tuples.addElement(RowVal);

		/*
		 * MySQL Type: TIME
		 * JDBC  Type: TIME
		 */

		RowVal = new byte[18][];
		RowVal[0] = s2b("TIME");
		RowVal[1] = Integer.toString(java.sql.Types.TIME).getBytes(); // JDBC Data type
		RowVal[2] = s2b("0"); // Precision
		RowVal[3] = s2b("'"); // Literal Prefix
		RowVal[4] = s2b("'"); // Literal Suffix
		RowVal[5] = s2b(""); // Create Params
		RowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();
		// Nullable
		RowVal[7] = s2b("false"); // Case Sensitive
		RowVal[8] =
			Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();
		// Searchable
		RowVal[9] = s2b("false"); // Unsignable
		RowVal[10] = s2b("false"); // Fixed Prec Scale
		RowVal[11] = s2b("false"); // Auto Increment
		RowVal[12] = s2b("TIME"); // Locale Type Name
		RowVal[13] = s2b("0"); // Minimum Scale
		RowVal[14] = s2b("0"); // Maximum Scale
		RowVal[15] = s2b("0"); // SQL Data Type (not used)
		RowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		RowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10) 

		Tuples.addElement(RowVal);

		/*
		 * MySQL Type: DATETIME
		 * JDBC  Type: TIMESTAMP
		 */

		RowVal = new byte[18][];
		RowVal[0] = s2b("DATETIME");
		RowVal[1] = Integer.toString(java.sql.Types.TIMESTAMP).getBytes();
		// JDBC Data type
		RowVal[2] = s2b("0"); // Precision
		RowVal[3] = s2b("'"); // Literal Prefix
		RowVal[4] = s2b("'"); // Literal Suffix
		RowVal[5] = s2b(""); // Create Params
		RowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();
		// Nullable
		RowVal[7] = s2b("false"); // Case Sensitive
		RowVal[8] =
			Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();
		// Searchable
		RowVal[9] = s2b("false"); // Unsignable
		RowVal[10] = s2b("false"); // Fixed Prec Scale
		RowVal[11] = s2b("false"); // Auto Increment
		RowVal[12] = s2b("DATETIME"); // Locale Type Name
		RowVal[13] = s2b("0"); // Minimum Scale
		RowVal[14] = s2b("0"); // Maximum Scale
		RowVal[15] = s2b("0"); // SQL Data Type (not used)
		RowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		RowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10) 

		Tuples.addElement(RowVal);

		/*
		 * MySQL Type: TIMESTAMP
		 * JDBC  Type: TIMESTAMP
		 */

		RowVal = new byte[18][];
		RowVal[0] = s2b("TIMESTAMP");
		RowVal[1] = Integer.toString(java.sql.Types.TIMESTAMP).getBytes();
		// JDBC Data type
		RowVal[2] = s2b("0"); // Precision
		RowVal[3] = s2b("'"); // Literal Prefix
		RowVal[4] = s2b("'"); // Literal Suffix
		RowVal[5] = s2b("[(M)]"); // Create Params
		RowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();
		// Nullable
		RowVal[7] = s2b("false"); // Case Sensitive
		RowVal[8] =
			Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();
		// Searchable
		RowVal[9] = s2b("false"); // Unsignable
		RowVal[10] = s2b("false"); // Fixed Prec Scale
		RowVal[11] = s2b("false"); // Auto Increment
		RowVal[12] = s2b("TIMESTAMP"); // Locale Type Name
		RowVal[13] = s2b("0"); // Minimum Scale
		RowVal[14] = s2b("0"); // Maximum Scale
		RowVal[15] = s2b("0"); // SQL Data Type (not used)
		RowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		RowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10) 

		Tuples.addElement(RowVal);

		return buildResultSet(Fields, Tuples, _conn);
	}

	/**
	 * Get a description of a table's indices and statistics. They are
	 * ordered by NON_UNIQUE, TYPE, INDEX_NAME, and ORDINAL_POSITION.
	 *
	 * <P>Each index column description has the following columns:
	 *  <OL>
	 *    <LI><B>TABLE_CAT</B> String => table catalog (may be null)
	 *    <LI><B>TABLE_SCHEM</B> String => table schema (may be null)
	 *    <LI><B>TABLE_NAME</B> String => table name
	 *    <LI><B>NON_UNIQUE</B> boolean => Can index values be non-unique?
	 *      false when TYPE is tableIndexStatistic
	 *    <LI><B>INDEX_QUALIFIER</B> String => index catalog (may be null);
	 *      null when TYPE is tableIndexStatistic
	 *    <LI><B>INDEX_NAME</B> String => index name; null when TYPE is
	 *      tableIndexStatistic
	 *    <LI><B>TYPE</B> short => index type:
	 *      <UL>
	 *      <LI> tableIndexStatistic - this identifies table statistics that are
	 *           returned in conjuction with a table's index descriptions
	 *      <LI> tableIndexClustered - this is a clustered index
	 *      <LI> tableIndexHashed - this is a hashed index
	 *      <LI> tableIndexOther - this is some other style of index
	 *      </UL>
	 *    <LI><B>ORDINAL_POSITION</B> short => column sequence number
	 *      within index; zero when TYPE is tableIndexStatistic
	 *    <LI><B>COLUMN_NAME</B> String => column name; null when TYPE is
	 *      tableIndexStatistic
	 *    <LI><B>ASC_OR_DESC</B> String => column sort sequence, "A" => ascending,
	 *      "D" => descending, may be null if sort sequence is not supported;
	 *      null when TYPE is tableIndexStatistic
	 *    <LI><B>CARDINALITY</B> int => When TYPE is tableIndexStatisic then
	 *      this is the number of rows in the table; otherwise it is the
	 *      number of unique values in the index.
	 *    <LI><B>PAGES</B> int => When TYPE is  tableIndexStatisic then
	 *      this is the number of pages used for the table, otherwise it
	 *      is the number of pages used for the current index.
	 *    <LI><B>FILTER_CONDITION</B> String => Filter condition, if any.
	 *      (may be null)
	 *  </OL>
	 *
	 * @param catalog a catalog name; "" retrieves those without a catalog
	 * @param schema a schema name pattern; "" retrieves those without a schema
	 * @param table a table name
	 * @param unique when true, return only indices for unique values;
	 *     when false, return indices regardless of whether unique or not
	 * @param approximate when true, result is allowed to reflect approximate
	 *     or out of data values; when false, results are requested to be
	 *     accurate
	 * @return ResultSet each row is an index column description
	 */

	public java.sql.ResultSet getIndexInfo(
		String Catalog,
		String Schema,
		String Table,
		boolean unique,
		boolean approximate)
		throws java.sql.SQLException
	{
		/*
		 * MySQL stores index information in the following fields:
		 *
		 * Table
		 * Non_unique 
		 * Key_name
		 * Seq_in_index
		 * Column_name
		 * Collation
		 * Cardinality
		 * Sub_part
		 */

		String DB_Sub = "";

		if (Catalog != null)
		{
			if (!Catalog.equals(""))
			{
				DB_Sub = " FROM " + _quotedId + Catalog + _quotedId;
			}
		}
		else
		{
			DB_Sub = " FROM " + _quotedId + _database + _quotedId;
		}

		com.mysql.jdbc.ResultSet RS =
			_conn.execSQL("SHOW INDEX FROM " + Table + DB_Sub, -1);

		RS.setConnection(_conn);

		Field[] Fields = new Field[13];

		Fields[0] = new Field("", "TABLE_CAT", Types.CHAR, 255);
		Fields[1] = new Field("", "TABLE_SCHEM", Types.CHAR, 0);
		Fields[2] = new Field("", "TABLE_NAME", Types.CHAR, 255);
		Fields[3] = new Field("", "NON_UNIQUE", Types.CHAR, 3);
		Fields[4] = new Field("", "INDEX_QUALIFIER", Types.CHAR, 1);
		Fields[5] = new Field("", "INDEX_NAME", Types.CHAR, 32);
		Fields[6] = new Field("", "TYPE", Types.CHAR, 32);
		Fields[7] = new Field("", "ORDINAL_POSITION", Types.SMALLINT, 5);
		Fields[8] = new Field("", "COLUMN_NAME", Types.CHAR, 32);
		Fields[9] = new Field("", "ASC_OR_DESC", Types.CHAR, 1);
		Fields[10] = new Field("", "CARDINALITY", Types.CHAR, 32);
		Fields[11] = new Field("", "PAGES", Types.INTEGER, 10);
		Fields[12] = new Field("", "FILTER_CONDITION", Types.CHAR, 32);

		Vector Tuples = new Vector();

		while (RS.next())
		{
			byte[][] Tuple = new byte[14][];

			Tuple[0] = (Catalog == null ? new byte[0] : Catalog.getBytes());
			Tuple[1] = new byte[0];
			Tuple[2] = RS.getBytes("Table");
			Tuple[3] = (RS.getInt("Non_unique") != 0 ? s2b("true") : s2b("false"));
			Tuple[4] = new byte[0];
			Tuple[5] = RS.getBytes("Key_name");
			Tuple[6] =
				Integer.toString(java.sql.DatabaseMetaData.tableIndexOther).getBytes();
			Tuple[7] = RS.getBytes("Seq_in_index");
			Tuple[8] = RS.getBytes("Column_name");
			Tuple[9] = RS.getBytes("Collation");
			Tuple[10] = RS.getBytes("Cardinality");
			Tuple[11] = s2b("0");
			Tuple[12] = null;

			Tuples.addElement(Tuple);
		}

		java.sql.ResultSet Results = buildResultSet(Fields, Tuples, _conn);

		return Results;
	}

	private byte[] s2b(String S)
	{
		return S.getBytes();
	}

	protected abstract java.sql.ResultSet buildResultSet(
		Field[] Fields,
		Vector Rows,
		Connection Conn);
}