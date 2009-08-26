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

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

/**
 * JDBC Interface to Mysql functions
 * <p>
 * This class provides information about the database as a whole.
 * </p>
 * <p>
 * Many of the methods here return lists of information in ResultSets. You can
 * use the normal ResultSet methods such as getString and getInt to retrieve the
 * data from these ResultSets. If a given form of metadata is not available,
 * these methods show throw a SQLException.
 * </p>
 * <p>
 * Some of these methods take arguments that are String patterns. These methods
 * all have names such as fooPattern. Within a pattern String "%" means match
 * any substring of 0 or more characters and "_" means match any one character.
 * </p>
 * 
 * @author Mark Matthews
 * @version $Id: DatabaseMetaData.java,v 1.27.4.66 2005/05/03 18:40:39 mmatthews
 *          Exp $
 */
public class DatabaseMetaData implements java.sql.DatabaseMetaData {

	protected abstract class IteratorWithCleanup {
		abstract void close() throws SQLException;

		abstract boolean hasNext() throws SQLException;

		abstract Object next() throws SQLException;
	}

	class LocalAndReferencedColumns {
		String constraintName;

		List localColumnsList;

		String referencedCatalog;

		List referencedColumnsList;

		String referencedTable;

		LocalAndReferencedColumns(List localColumns, List refColumns,
				String constName, String refCatalog, String refTable) {
			this.localColumnsList = localColumns;
			this.referencedColumnsList = refColumns;
			this.constraintName = constName;
			this.referencedTable = refTable;
			this.referencedCatalog = refCatalog;
		}
	}

	protected class ResultSetIterator extends IteratorWithCleanup {
		int colIndex;

		ResultSet resultSet;

		ResultSetIterator(ResultSet rs, int index) {
			resultSet = rs;
			colIndex = index;
		}

		void close() throws SQLException {
			resultSet.close();
		}

		boolean hasNext() throws SQLException {
			return resultSet.next();
		}

		Object next() throws SQLException {
			return resultSet.getObject(colIndex);
		}
	}

	protected class SingleStringIterator extends IteratorWithCleanup {
		boolean onFirst = true;

		String value;

		SingleStringIterator(String s) {
			value = s;
		}

		void close() throws SQLException {
			// not needed

		}

		boolean hasNext() throws SQLException {
			return onFirst;
		}

		Object next() throws SQLException {
			onFirst = false;
			return value;
		}
	}

	/**
	 * Parses and represents common data type information used by various
	 * column/parameter methods.
	 */
	class TypeDescriptor {
		int bufferLength;

		int charOctetLength;

		Integer columnSize;

		short dataType;

		Integer decimalDigits;

		String isNullable;

		int nullability;

		int numPrecRadix = 10;

		String typeName;

		TypeDescriptor(String typeInfo, String nullabilityInfo)
				throws SQLException {
			if (typeInfo == null) {
				throw SQLError.createSQLException("NULL typeinfo not supported.",
						SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
			}
			
			String mysqlType = "";
			String fullMysqlType = null;

			if (typeInfo.indexOf("(") != -1) {
				mysqlType = typeInfo.substring(0, typeInfo.indexOf("("));
			} else {
				mysqlType = typeInfo;
			}

			int indexOfUnsignedInMysqlType = StringUtils.indexOfIgnoreCase(
					mysqlType, "unsigned");

			if (indexOfUnsignedInMysqlType != -1) {
				mysqlType = mysqlType.substring(0,
						(indexOfUnsignedInMysqlType - 1));
			}

			// Add unsigned to typename reported to enduser as 'native type', if
			// present

			boolean isUnsigned = false;
			
			if (StringUtils.indexOfIgnoreCase(typeInfo, "unsigned") != -1) {
				fullMysqlType = mysqlType + " unsigned";
				isUnsigned = true;
			} else {
				fullMysqlType = mysqlType;
			}

			if (conn.getCapitalizeTypeNames()) {
				fullMysqlType = fullMysqlType.toUpperCase(Locale.ENGLISH);
			}

			this.dataType = (short) MysqlDefs.mysqlToJavaType(mysqlType);

			this.typeName = fullMysqlType;

			// Figure Out the Size
			
			if (StringUtils.startsWithIgnoreCase(typeInfo, "enum")) {
				String temp = typeInfo.substring(typeInfo.indexOf("("),
						typeInfo.lastIndexOf(")"));
				java.util.StringTokenizer tokenizer = new java.util.StringTokenizer(
						temp, ",");
				int maxLength = 0;

				while (tokenizer.hasMoreTokens()) {
					maxLength = Math.max(maxLength, (tokenizer.nextToken()
							.length() - 2));
				}

				this.columnSize = Constants.integerValueOf(maxLength);
				this.decimalDigits = null;
			} else if (StringUtils.startsWithIgnoreCase(typeInfo, "set")) {
				String temp = typeInfo.substring(typeInfo.indexOf("(") + 1,
						typeInfo.lastIndexOf(")"));
				java.util.StringTokenizer tokenizer = new java.util.StringTokenizer(
						temp, ",");
				int maxLength = 0;

				int numElements = tokenizer.countTokens();
				
				if (numElements > 0) {
					maxLength += (numElements - 1);
				}
				
				while (tokenizer.hasMoreTokens()) {
					String setMember = tokenizer.nextToken().trim();

					if (setMember.startsWith("'")
							&& setMember.endsWith("'")) {
						maxLength += setMember.length() - 2;
					} else {
						maxLength += setMember.length();
					}
				}

				this.columnSize = Constants.integerValueOf(maxLength);
				this.decimalDigits = null;
			} else if (typeInfo.indexOf(",") != -1) {
				// Numeric with decimals
				this.columnSize = Integer.valueOf(typeInfo.substring((typeInfo
						.indexOf("(") + 1), (typeInfo.indexOf(","))).trim());
				this.decimalDigits = Integer.valueOf(typeInfo.substring(
						(typeInfo.indexOf(",") + 1),
						(typeInfo.indexOf(")"))).trim());
			} else {
				this.columnSize = null;
				this.decimalDigits = null;

				/* If the size is specified with the DDL, use that */
				if ((StringUtils.indexOfIgnoreCase(typeInfo, "char") != -1
						|| StringUtils.indexOfIgnoreCase(typeInfo, "text") != -1
						|| StringUtils.indexOfIgnoreCase(typeInfo, "blob") != -1
						|| StringUtils
								.indexOfIgnoreCase(typeInfo, "binary") != -1 || StringUtils
						.indexOfIgnoreCase(typeInfo, "bit") != -1)
						&& typeInfo.indexOf("(") != -1) {
					int endParenIndex = typeInfo.indexOf(")");

					if (endParenIndex == -1) {
						endParenIndex = typeInfo.length();
					}

					this.columnSize = Integer.valueOf(typeInfo.substring(
							(typeInfo.indexOf("(") + 1), endParenIndex).trim());

					// Adjust for pseudo-boolean
					if (conn.getTinyInt1isBit()
							&& this.columnSize.intValue() == 1
							&& StringUtils.startsWithIgnoreCase(typeInfo,
									0, "tinyint")) {
						if (conn.getTransformedBitIsBoolean()) {
							this.dataType = Types.BOOLEAN;
							this.typeName = "BOOLEAN";
						} else {
							this.dataType = Types.BIT;
							this.typeName = "BIT";
						}
					}
				} else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo,
						"tinyint")) {
					if (conn.getTinyInt1isBit() && typeInfo.indexOf("(1)") != -1) {
						if (conn.getTransformedBitIsBoolean()) {
							this.dataType = Types.BOOLEAN;
							this.typeName = "BOOLEAN";
						} else {
							this.dataType = Types.BIT;
							this.typeName = "BIT";
						}
					} else {
						this.columnSize = Constants.integerValueOf(3);
						this.decimalDigits = Constants.integerValueOf(0);
					}
				} else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo,
						"smallint")) {
					this.columnSize = Constants.integerValueOf(5);
					this.decimalDigits = Constants.integerValueOf(0);
				} else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo,
						"mediumint")) {
					this.columnSize = Constants.integerValueOf(isUnsigned ? 8 : 7);
					this.decimalDigits = Constants.integerValueOf(0);
				} else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo,
						"int")) {
					this.columnSize = Constants.integerValueOf(10);
					this.decimalDigits = Constants.integerValueOf(0);
				} else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo,
						"integer")) {
					this.columnSize = Constants.integerValueOf(10);
					this.decimalDigits = Constants.integerValueOf(0);
				} else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo,
						"bigint")) {
					this.columnSize = Constants.integerValueOf(isUnsigned ? 20 : 19);
					this.decimalDigits = Constants.integerValueOf(0);
				} else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo,
						"int24")) {
					this.columnSize = Constants.integerValueOf(19);
					this.decimalDigits = Constants.integerValueOf(0);
				} else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo,
						"real")) {
					this.columnSize = Constants.integerValueOf(12);
				} else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo,
						"float")) {
					this.columnSize = Constants.integerValueOf(12);
				} else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo,
						"decimal")) {
					this.columnSize = Constants.integerValueOf(12);
				} else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo,
						"numeric")) {
					this.columnSize = Constants.integerValueOf(12);
				} else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo,
						"double")) {
					this.columnSize = Constants.integerValueOf(22);
				} else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo,
						"char")) {
					this.columnSize = Constants.integerValueOf(1);
				} else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo,
						"varchar")) {
					this.columnSize = Constants.integerValueOf(255);
				} else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo,
					"timestamp")) {
					this.columnSize = Constants.integerValueOf(19);
				} else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo,
					"datetime")) {
					this.columnSize = Constants.integerValueOf(19);
				} else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo,
						"date")) {
					this.columnSize = Constants.integerValueOf(10);
				} else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo,
						"time")) {
					this.columnSize = Constants.integerValueOf(8);
				
				} else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo,
						"tinyblob")) {
					this.columnSize = Constants.integerValueOf(255);
				} else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo,
						"blob")) {
					this.columnSize = Constants.integerValueOf(65535);
				} else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo,
						"mediumblob")) {
					this.columnSize = Constants.integerValueOf(16777215);
				} else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo,
						"longblob")) {
					this.columnSize = Constants.integerValueOf(Integer.MAX_VALUE);
				} else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo,
						"tinytext")) {
					this.columnSize = Constants.integerValueOf(255);
				} else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo,
						"text")) {
					this.columnSize = Constants.integerValueOf(65535);
				} else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo,
						"mediumtext")) {
					this.columnSize = Constants.integerValueOf(16777215);
				} else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo,
						"longtext")) {
					this.columnSize = Constants.integerValueOf(Integer.MAX_VALUE);
				} else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo,
						"enum")) {
					this.columnSize = Constants.integerValueOf(255);
				} else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo,
						"set")) {
					this.columnSize = Constants.integerValueOf(255);
				}

			}

			// BUFFER_LENGTH
			this.bufferLength = MysqlIO.getMaxBuf();

			// NUM_PREC_RADIX (is this right for char?)
			this.numPrecRadix = 10;

			// Nullable?
			if (nullabilityInfo != null) {
				if (nullabilityInfo.equals("YES")) {
					this.nullability = java.sql.DatabaseMetaData.columnNullable;
					this.isNullable = "YES";

					// IS_NULLABLE
				} else {
					this.nullability = java.sql.DatabaseMetaData.columnNoNulls;
					this.isNullable = "NO";
				}
			} else {
				this.nullability = java.sql.DatabaseMetaData.columnNoNulls;
				this.isNullable = "NO";
			}
		}
	}
       
	private static String mysqlKeywordsThatArentSQL92;
	
	protected static final int MAX_IDENTIFIER_LENGTH = 64;
	
	private static final int DEFERRABILITY = 13;

	private static final int DELETE_RULE = 10;

	private static final int FK_NAME = 11;

	private static final int FKCOLUMN_NAME = 7;

	private static final int FKTABLE_CAT = 4;

	private static final int FKTABLE_NAME = 6;

	private static final int FKTABLE_SCHEM = 5;

	private static final int KEY_SEQ = 8;

	private static final int PK_NAME = 12;

	private static final int PKCOLUMN_NAME = 3;

	//
	// Column indexes used by all DBMD foreign key
	// ResultSets
	//
	private static final int PKTABLE_CAT = 0;

	private static final int PKTABLE_NAME = 2;

	private static final int PKTABLE_SCHEM = 1;

	/** The table type for generic tables that support foreign keys. */
	private static final String SUPPORTS_FK = "SUPPORTS_FK";

	private static final byte[] TABLE_AS_BYTES = "TABLE".getBytes();

	private static final byte[] SYSTEM_TABLE_AS_BYTES = "SYSTEM TABLE".getBytes();
	
	private static final int UPDATE_RULE = 9;

	private static final byte[] VIEW_AS_BYTES = "VIEW".getBytes();
	
	private static final Constructor JDBC_4_DBMD_SHOW_CTOR;
	
	private static final Constructor JDBC_4_DBMD_IS_CTOR;
	
	static {
		if (Util.isJdbc4()) {
			try {
				JDBC_4_DBMD_SHOW_CTOR = Class.forName(
						"com.mysql.jdbc.JDBC4DatabaseMetaData").getConstructor(
						new Class[] { com.mysql.jdbc.ConnectionImpl.class,
								String.class });
				JDBC_4_DBMD_IS_CTOR = Class.forName(
						"com.mysql.jdbc.JDBC4DatabaseMetaDataUsingInfoSchema")
						.getConstructor(
								new Class[] { com.mysql.jdbc.ConnectionImpl.class,
										String.class });
			} catch (SecurityException e) {
				throw new RuntimeException(e);
			} catch (NoSuchMethodException e) {
				throw new RuntimeException(e);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		} else {
			JDBC_4_DBMD_IS_CTOR = null;
			JDBC_4_DBMD_SHOW_CTOR = null;
		}
		
		// Current as-of MySQL-5.1.16
		String[] allMySQLKeywords = new String[] { "ACCESSIBLE", "ADD", "ALL",
				"ALTER", "ANALYZE", "AND", "AS", "ASC", "ASENSITIVE", "BEFORE",
				"BETWEEN", "BIGINT", "BINARY", "BLOB", "BOTH", "BY", "CALL",
				"CASCADE", "CASE", "CHANGE", "CHAR", "CHARACTER", "CHECK",
				"COLLATE", "COLUMN", "CONDITION", "CONNECTION", "CONSTRAINT",
				"CONTINUE", "CONVERT", "CREATE", "CROSS", "CURRENT_DATE",
				"CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR",
				"DATABASE", "DATABASES", "DAY_HOUR", "DAY_MICROSECOND",
				"DAY_MINUTE", "DAY_SECOND", "DEC", "DECIMAL", "DECLARE",
				"DEFAULT", "DELAYED", "DELETE", "DESC", "DESCRIBE",
				"DETERMINISTIC", "DISTINCT", "DISTINCTROW", "DIV", "DOUBLE",
				"DROP", "DUAL", "EACH", "ELSE", "ELSEIF", "ENCLOSED",
				"ESCAPED", "EXISTS", "EXIT", "EXPLAIN", "FALSE", "FETCH",
				"FLOAT", "FLOAT4", "FLOAT8", "FOR", "FORCE", "FOREIGN", "FROM",
				"FULLTEXT", "GRANT", "GROUP", "HAVING", "HIGH_PRIORITY",
				"HOUR_MICROSECOND", "HOUR_MINUTE", "HOUR_SECOND", "IF",
				"IGNORE", "IN", "INDEX", "INFILE", "INNER", "INOUT",
				"INSENSITIVE", "INSERT", "INT", "INT1", "INT2", "INT3", "INT4",
				"INT8", "INTEGER", "INTERVAL", "INTO", "IS", "ITERATE", "JOIN",
				"KEY", "KEYS", "KILL", "LEADING", "LEAVE", "LEFT", "LIKE",
				"LIMIT", "LINEAR", "LINES", "LOAD", "LOCALTIME",
				"LOCALTIMESTAMP", "LOCK", "LONG", "LONGBLOB", "LONGTEXT",
				"LOOP", "LOW_PRIORITY", "MATCH", "MEDIUMBLOB", "MEDIUMINT",
				"MEDIUMTEXT", "MIDDLEINT", "MINUTE_MICROSECOND",
				"MINUTE_SECOND", "MOD", "MODIFIES", "NATURAL", "NOT",
				"NO_WRITE_TO_BINLOG", "NULL", "NUMERIC", "ON", "OPTIMIZE",
				"OPTION", "OPTIONALLY", "OR", "ORDER", "OUT", "OUTER",
				"OUTFILE", "PRECISION", "PRIMARY", "PROCEDURE", "PURGE",
				"RANGE", "READ", "READS", "READ_ONLY", "READ_WRITE", "REAL",
				"REFERENCES", "REGEXP", "RELEASE", "RENAME", "REPEAT",
				"REPLACE", "REQUIRE", "RESTRICT", "RETURN", "REVOKE", "RIGHT",
				"RLIKE", "SCHEMA", "SCHEMAS", "SECOND_MICROSECOND", "SELECT",
				"SENSITIVE", "SEPARATOR", "SET", "SHOW", "SMALLINT", "SPATIAL",
				"SPECIFIC", "SQL", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING",
				"SQL_BIG_RESULT", "SQL_CALC_FOUND_ROWS", "SQL_SMALL_RESULT",
				"SSL", "STARTING", "STRAIGHT_JOIN", "TABLE", "TERMINATED",
				"THEN", "TINYBLOB", "TINYINT", "TINYTEXT", "TO", "TRAILING",
				"TRIGGER", "TRUE", "UNDO", "UNION", "UNIQUE", "UNLOCK",
				"UNSIGNED", "UPDATE", "USAGE", "USE", "USING", "UTC_DATE",
				"UTC_TIME", "UTC_TIMESTAMP", "VALUES", "VARBINARY", "VARCHAR",
				"VARCHARACTER", "VARYING", "WHEN", "WHERE", "WHILE", "WITH",
				"WRITE", "X509", "XOR", "YEAR_MONTH", "ZEROFILL" };

		String[] sql92Keywords = new String[] { "ABSOLUTE", "EXEC", "OVERLAPS",
				"ACTION", "EXECUTE", "PAD", "ADA", "EXISTS", "PARTIAL", "ADD",
				"EXTERNAL", "PASCAL", "ALL", "EXTRACT", "POSITION", "ALLOCATE",
				"FALSE", "PRECISION", "ALTER", "FETCH", "PREPARE", "AND",
				"FIRST", "PRESERVE", "ANY", "FLOAT", "PRIMARY", "ARE", "FOR",
				"PRIOR", "AS", "FOREIGN", "PRIVILEGES", "ASC", "FORTRAN",
				"PROCEDURE", "ASSERTION", "FOUND", "PUBLIC", "AT", "FROM",
				"READ", "AUTHORIZATION", "FULL", "REAL", "AVG", "GET",
				"REFERENCES", "BEGIN", "GLOBAL", "RELATIVE", "BETWEEN", "GO",
				"RESTRICT", "BIT", "GOTO", "REVOKE", "BIT_LENGTH", "GRANT",
				"RIGHT", "BOTH", "GROUP", "ROLLBACK", "BY", "HAVING", "ROWS",
				"CASCADE", "HOUR", "SCHEMA", "CASCADED", "IDENTITY", "SCROLL",
				"CASE", "IMMEDIATE", "SECOND", "CAST", "IN", "SECTION",
				"CATALOG", "INCLUDE", "SELECT", "CHAR", "INDEX", "SESSION",
				"CHAR_LENGTH", "INDICATOR", "SESSION_USER", "CHARACTER",
				"INITIALLY", "SET", "CHARACTER_LENGTH", "INNER", "SIZE",
				"CHECK", "INPUT", "SMALLINT", "CLOSE", "INSENSITIVE", "SOME",
				"COALESCE", "INSERT", "SPACE", "COLLATE", "INT", "SQL",
				"COLLATION", "INTEGER", "SQLCA", "COLUMN", "INTERSECT",
				"SQLCODE", "COMMIT", "INTERVAL", "SQLERROR", "CONNECT", "INTO",
				"SQLSTATE", "CONNECTION", "IS", "SQLWARNING", "CONSTRAINT",
				"ISOLATION", "SUBSTRING", "CONSTRAINTS", "JOIN", "SUM",
				"CONTINUE", "KEY", "SYSTEM_USER", "CONVERT", "LANGUAGE",
				"TABLE", "CORRESPONDING", "LAST", "TEMPORARY", "COUNT",
				"LEADING", "THEN", "CREATE", "LEFT", "TIME", "CROSS", "LEVEL",
				"TIMESTAMP", "CURRENT", "LIKE", "TIMEZONE_HOUR",
				"CURRENT_DATE", "LOCAL", "TIMEZONE_MINUTE", "CURRENT_TIME",
				"LOWER", "TO", "CURRENT_TIMESTAMP", "MATCH", "TRAILING",
				"CURRENT_USER", "MAX", "TRANSACTION", "CURSOR", "MIN",
				"TRANSLATE", "DATE", "MINUTE", "TRANSLATION", "DAY", "MODULE",
				"TRIM", "DEALLOCATE", "MONTH", "TRUE", "DEC", "NAMES", "UNION",
				"DECIMAL", "NATIONAL", "UNIQUE", "DECLARE", "NATURAL",
				"UNKNOWN", "DEFAULT", "NCHAR", "UPDATE", "DEFERRABLE", "NEXT",
				"UPPER", "DEFERRED", "NO", "USAGE", "DELETE", "NONE", "USER",
				"DESC", "NOT", "USING", "DESCRIBE", "NULL", "VALUE",
				"DESCRIPTOR", "NULLIF", "VALUES", "DIAGNOSTICS", "NUMERIC",
				"VARCHAR", "DISCONNECT", "OCTET_LENGTH", "VARYING", "DISTINCT",
				"OF", "VIEW", "DOMAIN", "ON", "WHEN", "DOUBLE", "ONLY",
				"WHENEVER", "DROP", "OPEN", "WHERE", "ELSE", "OPTION", "WITH",
				"END", "OR", "WORK", "END-EXEC", "ORDER", "WRITE", "ESCAPE",
				"OUTER", "YEAR", "EXCEPT", "OUTPUT", "ZONE", "EXCEPTION" };
		
		TreeMap mySQLKeywordMap = new TreeMap();
		
		for (int i = 0; i < allMySQLKeywords.length; i++) {
			mySQLKeywordMap.put(allMySQLKeywords[i], null);
		}
		
		HashMap sql92KeywordMap = new HashMap(sql92Keywords.length);
		
		for (int i = 0; i < sql92Keywords.length; i++) {
			sql92KeywordMap.put(sql92Keywords[i], null);
		}
		
		Iterator it = sql92KeywordMap.keySet().iterator();
		
		while (it.hasNext()) {
			mySQLKeywordMap.remove(it.next());
		}
		
		StringBuffer keywordBuf = new StringBuffer();
		
		it = mySQLKeywordMap.keySet().iterator();
		
		if (it.hasNext()) {
			keywordBuf.append(it.next().toString());
		}
		
		while (it.hasNext()) {
			keywordBuf.append(",");
			keywordBuf.append(it.next().toString());
		}
	
		mysqlKeywordsThatArentSQL92 = keywordBuf.toString();
	}
	
	/** The connection to the database */
	protected ConnectionImpl conn;

	/** The 'current' database name being used */
	protected String database = null;

	/** What character to use when quoting identifiers */
	protected String quotedId = null;

	// We need to provide factory-style methods so we can support both JDBC3 (and older)
	// and JDBC4 runtimes, otherwise the class verifier complains...
	
	protected static DatabaseMetaData getInstance(
			ConnectionImpl connToSet, String databaseToSet, boolean checkForInfoSchema)
			throws SQLException {
		if (!Util.isJdbc4()) {
			if (checkForInfoSchema && connToSet != null 
					&& connToSet.getUseInformationSchema()
					&& connToSet.versionMeetsMinimum(5, 0, 7)) {
				return new DatabaseMetaDataUsingInfoSchema(connToSet,
						databaseToSet);
			}

			return new DatabaseMetaData(connToSet, databaseToSet);
		}

		if (checkForInfoSchema && connToSet != null 
				&& connToSet.getUseInformationSchema()
				&& connToSet.versionMeetsMinimum(5, 0, 7)) {

			return (DatabaseMetaData) Util.handleNewInstance(
					JDBC_4_DBMD_IS_CTOR, new Object[] { connToSet,
							databaseToSet }, connToSet.getExceptionInterceptor());
		}

		return (DatabaseMetaData) Util.handleNewInstance(JDBC_4_DBMD_SHOW_CTOR,
				new Object[] { connToSet, databaseToSet }, connToSet.getExceptionInterceptor());
	}
	
	/**
	 * Creates a new DatabaseMetaData object.
	 * 
	 * @param connToSet
	 *            DOCUMENT ME!
	 * @param databaseToSet
	 *            DOCUMENT ME!
	 */
	protected DatabaseMetaData(ConnectionImpl connToSet, String databaseToSet) {
		this.conn = connToSet;
		this.database = databaseToSet;
		this.exceptionInterceptor = this.conn.getExceptionInterceptor();
		
		try {
			this.quotedId = this.conn.supportsQuotedIdentifiers() ? getIdentifierQuoteString()
					: "";
		} catch (SQLException sqlEx) {
			// Forced by API, never thrown from getIdentifierQuoteString() in
			// this
			// implementation.
			AssertionFailedException.shouldNotHappen(sqlEx);
		}
	}

	/**
	 * Can all the procedures returned by getProcedures be called by the current
	 * user?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean allProceduresAreCallable() throws SQLException {
		return false;
	}

	/**
	 * Can all the tables returned by getTable be SELECTed by the current user?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean allTablesAreSelectable() throws SQLException {
		return false;
	}

	private java.sql.ResultSet buildResultSet(com.mysql.jdbc.Field[] fields,
			java.util.ArrayList rows) throws SQLException {
		return buildResultSet(fields, rows, this.conn);
	}
	
	static java.sql.ResultSet buildResultSet(com.mysql.jdbc.Field[] fields,
			java.util.ArrayList rows, ConnectionImpl c) throws SQLException {
		int fieldsLength = fields.length;

		for (int i = 0; i < fieldsLength; i++) {
			int jdbcType = fields[i].getSQLType();
			
			switch (jdbcType) {
			case Types.CHAR:
			case Types.VARCHAR:
			case Types.LONGVARCHAR:
				fields[i].setCharacterSet(c.getCharacterSetMetadata());
				break;
			default:
				// do nothing
			}
			
			fields[i].setConnection(c);
			fields[i].setUseOldNameMetadata(true);
		}

		return com.mysql.jdbc.ResultSetImpl.getInstance(c.getCatalog(), fields,
				new RowDataStatic(rows), c, null, false);
	}

	private void convertToJdbcFunctionList(String catalog,
			ResultSet proceduresRs, boolean needsClientFiltering, String db,
			Map procedureRowsOrderedByName, int nameIndex,
			Field[] fields) throws SQLException {
		while (proceduresRs.next()) {
			boolean shouldAdd = true;

			if (needsClientFiltering) {
				shouldAdd = false;

				String procDb = proceduresRs.getString(1);

				if (db == null && procDb == null) {
					shouldAdd = true;
				} else if (db != null && db.equals(procDb)) {
					shouldAdd = true;
				}
			}

			if (shouldAdd) {
				String functionName = proceduresRs.getString(nameIndex);
				
				byte[][] rowData = null;
				
				if (fields != null && fields.length == 9) {
					
					rowData = new byte[9][];
					rowData[0] = catalog == null ? null : s2b(catalog);         // PROCEDURE_CAT
					rowData[1] = null;                                          // PROCEDURE_SCHEM
					rowData[2] = s2b(functionName);                             // PROCEDURE_NAME
					rowData[3] = null;                                          // reserved1
					rowData[4] = null;                                          // reserved2
					rowData[5] = null;                                          // reserved3
					rowData[6] = s2b(proceduresRs.getString("comment"));        // REMARKS
					rowData[7] = s2b(Integer.toString(procedureReturnsResult)); // PROCEDURE_TYPE
					rowData[8] = s2b(functionName);
				} else {
					
					rowData = new byte[6][];
					
					rowData[0] = catalog == null ? null : s2b(catalog);  // FUNCTION_CAT
					rowData[1] = null;                                   // FUNCTION_SCHEM
					rowData[2] = s2b(functionName);                      // FUNCTION_NAME
					rowData[3] = s2b(proceduresRs.getString("comment")); // REMARKS
					rowData[4] = s2b(Integer.toString(getJDBC4FunctionNoTableConstant())); // FUNCTION_TYPE
					rowData[5] = s2b(functionName);                      // SPECFIC NAME
				}

				procedureRowsOrderedByName.put(functionName, new ByteArrayRow(rowData, getExceptionInterceptor()));
			}
		}
	}
	
	protected int getJDBC4FunctionNoTableConstant() {
		return 0;
	}
	
	private void convertToJdbcProcedureList(boolean fromSelect, String catalog,
			ResultSet proceduresRs, boolean needsClientFiltering, String db,
			Map procedureRowsOrderedByName, int nameIndex) throws SQLException {
		while (proceduresRs.next()) {
			boolean shouldAdd = true;

			if (needsClientFiltering) {
				shouldAdd = false;

				String procDb = proceduresRs.getString(1);

				if (db == null && procDb == null) {
					shouldAdd = true;
				} else if (db != null && db.equals(procDb)) {
					shouldAdd = true;
				}
			}

			if (shouldAdd) {
				String procedureName = proceduresRs.getString(nameIndex);
				byte[][] rowData = new byte[9][];
				rowData[0] = catalog == null ? null : s2b(catalog);
				rowData[1] = null;
				rowData[2] = s2b(procedureName);
				rowData[3] = null;
				rowData[4] = null;
				rowData[5] = null;
				rowData[6] = null;

				boolean isFunction = fromSelect ? "FUNCTION"
						.equalsIgnoreCase(proceduresRs.getString("type"))
						: false;
				rowData[7] = s2b(isFunction ? Integer
						.toString(procedureReturnsResult) : Integer
						.toString(procedureResultUnknown));

				rowData[8] = s2b(procedureName);
				
				procedureRowsOrderedByName.put(procedureName, new ByteArrayRow(rowData, getExceptionInterceptor()));
			}
		}
	}

	private ResultSetRow convertTypeDescriptorToProcedureRow(
			byte[] procNameAsBytes, String paramName, boolean isOutParam,
			boolean isInParam, boolean isReturnParam, TypeDescriptor typeDesc,
			boolean forGetFunctionColumns,
			int ordinal)
			throws SQLException {
		byte[][] row = forGetFunctionColumns ? new byte[17][] : new byte[14][];
		row[0] = null; // PROCEDURE_CAT
		row[1] = null; // PROCEDURE_SCHEM
		row[2] = procNameAsBytes; // PROCEDURE/NAME
		row[3] = s2b(paramName); // COLUMN_NAME
		// COLUMN_TYPE
		
		// NOTE: For JDBC-4.0, we luck out here for functions
		// because the values are the same for functionColumn....
		// and they're not using Enumerations....
		
		if (isInParam && isOutParam) {
			row[4] = s2b(String.valueOf(procedureColumnInOut));
		} else if (isInParam) {
			row[4] = s2b(String.valueOf(procedureColumnIn));
		} else if (isOutParam) {
			row[4] = s2b(String.valueOf(procedureColumnOut));
		} else if (isReturnParam) {
			row[4] = s2b(String.valueOf(procedureColumnReturn));
		} else {
			row[4] = s2b(String.valueOf(procedureColumnUnknown));
		}
		row[5] = s2b(Short.toString(typeDesc.dataType)); // DATA_TYPE
		row[6] = s2b(typeDesc.typeName); // TYPE_NAME
		row[7] = typeDesc.columnSize == null ? null : s2b(typeDesc.columnSize.toString()); // PRECISION
		row[8] = row[7]; // LENGTH
		row[9] = typeDesc.decimalDigits == null ? null : s2b(typeDesc.decimalDigits.toString()); // SCALE
		row[10] = s2b(Integer.toString(typeDesc.numPrecRadix)); // RADIX
		// Map 'column****' to 'procedure****'
		switch (typeDesc.nullability) {
		case columnNoNulls:
			row[11] = s2b(String.valueOf(procedureNoNulls)); // NULLABLE

			break;

		case columnNullable:
			row[11] = s2b(String.valueOf(procedureNullable)); // NULLABLE

			break;

		case columnNullableUnknown:
			row[11] = s2b(String.valueOf(procedureNullableUnknown)); // nullable

			break;

		default:
			throw SQLError.createSQLException(
					"Internal error while parsing callable statement metadata (unknown nullability value fount)",
					SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
		}
		
		row[12] = null;
		
		if (forGetFunctionColumns) {
			// CHAR_OCTECT_LENGTH
			row[13] = null;
			
			// ORDINAL_POSITION
			row[14] = s2b(String.valueOf(ordinal));
			
			// IS_NULLABLE
			row[15] = Constants.EMPTY_BYTE_ARRAY;
			
			row[16] = s2b(paramName);
		}
		
		return new ByteArrayRow(row, getExceptionInterceptor());
	}

	private ExceptionInterceptor exceptionInterceptor;
	
	protected ExceptionInterceptor getExceptionInterceptor() {
		return this.exceptionInterceptor;
	}

	/**
	 * Does a data definition statement within a transaction force the
	 * transaction to commit?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
		return true;
	}

	/**
	 * Is a data definition statement within a transaction ignored?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
		return false;
	}

	/**
	 * JDBC 2.0 Determine whether or not a visible row delete can be detected by
	 * calling ResultSet.rowDeleted(). If deletesAreDetected() returns false,
	 * then deleted rows are removed from the result set.
	 * 
	 * @param type
	 *            set type, i.e. ResultSet.TYPE_XXX
	 * @return true if changes are detected by the resultset type
	 * @exception SQLException
	 *                if a database-access error occurs.
	 */
	public boolean deletesAreDetected(int type) throws SQLException {
		return false;
	}

	// ----------------------------------------------------------------------

	/**
	 * Did getMaxRowSize() include LONGVARCHAR and LONGVARBINARY blobs?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
		return true;
	}

	/**
	 * Extracts foreign key info for one table.
	 * 
	 * @param rows
	 *            the list of rows to add to
	 * @param rs
	 *            the result set from 'SHOW CREATE TABLE'
	 * @param catalog
	 *            the database name
	 * @return the list of rows with new rows added
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	public List extractForeignKeyForTable(ArrayList rows,
			java.sql.ResultSet rs, String catalog) throws SQLException {
		byte[][] row = new byte[3][];
		row[0] = rs.getBytes(1);
		row[1] = s2b(SUPPORTS_FK);
	
		String createTableString = rs.getString(2);
		StringTokenizer lineTokenizer = new StringTokenizer(createTableString,
				"\n");
		StringBuffer commentBuf = new StringBuffer("comment; ");
		boolean firstTime = true;
	
		String quoteChar = getIdentifierQuoteString();
	
		if (quoteChar == null) {
			quoteChar = "`";
		}
	
		while (lineTokenizer.hasMoreTokens()) {
			String line = lineTokenizer.nextToken().trim();
	
			String constraintName = null;
	
			if (StringUtils.startsWithIgnoreCase(line, "CONSTRAINT")) {
				boolean usingBackTicks = true;
				int beginPos = line.indexOf(quoteChar);
	
				if (beginPos == -1) {
					beginPos = line.indexOf("\"");
					usingBackTicks = false;
				}
	
				if (beginPos != -1) {
					int endPos = -1;
	
					if (usingBackTicks) {
						endPos = line.indexOf(quoteChar, beginPos + 1);
					} else {
						endPos = line.indexOf("\"", beginPos + 1);
					}
	
					if (endPos != -1) {
						constraintName = line.substring(beginPos + 1, endPos);
						line = line.substring(endPos + 1, line.length()).trim();
					}
				}
			}
	
			
			if (line.startsWith("FOREIGN KEY")) {
				if (line.endsWith(",")) {
					line = line.substring(0, line.length() - 1);
				}
	
				char quote = this.quotedId.charAt(0);
				
				int indexOfFK = line.indexOf("FOREIGN KEY");
				
				String localColumnName = null;
				String referencedCatalogName = this.quotedId + catalog + this.quotedId;
				String referencedTableName = null;
				String referencedColumnName = null;
				
				
				if (indexOfFK != -1) {
					int afterFk = indexOfFK + "FOREIGN KEY".length();
					
					int indexOfRef = StringUtils.indexOfIgnoreCaseRespectQuotes(afterFk, line, "REFERENCES", quote, true);
					
					if (indexOfRef != -1) {
						
						int indexOfParenOpen = line.indexOf('(', afterFk);
						int indexOfParenClose = StringUtils.indexOfIgnoreCaseRespectQuotes(indexOfParenOpen, line, ")", quote, true);
						
						if (indexOfParenOpen == -1 || indexOfParenClose == -1) {
							// throw SQLError.createSQLException();
						}
						
						localColumnName = line.substring(indexOfParenOpen + 1, indexOfParenClose);
						
						int afterRef = indexOfRef + "REFERENCES".length();
						
						int referencedColumnBegin = StringUtils.indexOfIgnoreCaseRespectQuotes(afterRef, line, "(", quote, true);
						
						if (referencedColumnBegin != -1) {
							referencedTableName = line.substring(afterRef, referencedColumnBegin);
	
							int referencedColumnEnd = StringUtils.indexOfIgnoreCaseRespectQuotes(referencedColumnBegin + 1, line, ")", quote, true);
							
							if (referencedColumnEnd != -1) {
								referencedColumnName = line.substring(referencedColumnBegin + 1, referencedColumnEnd);
							}
							
							int indexOfCatalogSep = StringUtils.indexOfIgnoreCaseRespectQuotes(0, referencedTableName, ".", quote, true);
							
							if (indexOfCatalogSep != -1) {
								referencedCatalogName = referencedTableName.substring(0, indexOfCatalogSep);
								referencedTableName = referencedTableName.substring(indexOfCatalogSep + 1);
							}
						}
					}
				}
				
				
				if (!firstTime) {
					commentBuf.append("; ");
				} else {
					firstTime = false;
				}
	
				if (constraintName != null) {
					commentBuf.append(constraintName);
				} else {
					commentBuf.append("not_available");
				}
	
				commentBuf.append("(");
				commentBuf.append(localColumnName);
				commentBuf.append(") REFER ");
				commentBuf.append(referencedCatalogName);
				commentBuf.append("/");
				commentBuf.append(referencedTableName);
				commentBuf.append("(");
				commentBuf.append(referencedColumnName);
				commentBuf.append(")");
	
				int lastParenIndex = line.lastIndexOf(")");
	
				if (lastParenIndex != (line.length() - 1)) {
					String cascadeOptions = line
							.substring(lastParenIndex + 1);
					commentBuf.append(" ");
					commentBuf.append(cascadeOptions);
				}
			}
		}
	
		row[2] = s2b(commentBuf.toString());
		rows.add(new ByteArrayRow(row, getExceptionInterceptor()));
	
		return rows;
	}

	/**
	 * Creates a result set similar enough to 'SHOW TABLE STATUS' to allow the
	 * same code to work on extracting the foreign key data
	 * 
	 * @param connToUse
	 *            the database connection to use
	 * @param metadata
	 *            the DatabaseMetaData instance calling this method
	 * @param catalog
	 *            the database name to extract foreign key info for
	 * @param tableName
	 *            the table to extract foreign key info for
	 * @return A result set that has the structure of 'show table status'
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public ResultSet extractForeignKeyFromCreateTable(String catalog,
			String tableName) throws SQLException {
		ArrayList tableList = new ArrayList();
		java.sql.ResultSet rs = null;
		java.sql.Statement stmt = null;

		if (tableName != null) {
			tableList.add(tableName);
		} else {
			try {
				rs = getTables(catalog, "", "%", new String[] { "TABLE" });

				while (rs.next()) {
					tableList.add(rs.getString("TABLE_NAME"));
				}
			} finally {
				if (rs != null) {
					rs.close();
				}

				rs = null;
			}
		}

		ArrayList rows = new ArrayList();
		Field[] fields = new Field[3];
		fields[0] = new Field("", "Name", Types.CHAR, Integer.MAX_VALUE);
		fields[1] = new Field("", "Type", Types.CHAR, 255);
		fields[2] = new Field("", "Comment", Types.CHAR, Integer.MAX_VALUE);

		int numTables = tableList.size();
		stmt = this.conn.getMetadataSafeStatement();

		String quoteChar = getIdentifierQuoteString();

		if (quoteChar == null) {
			quoteChar = "`";
		}

		try {
			for (int i = 0; i < numTables; i++) {
				String tableToExtract = (String) tableList.get(i);

				String query = new StringBuffer("SHOW CREATE TABLE ").append(
						quoteChar).append(catalog).append(quoteChar)
						.append(".").append(quoteChar).append(tableToExtract)
						.append(quoteChar).toString();
				
				try {
					rs = stmt.executeQuery(query);
				} catch (SQLException sqlEx) {
					// Table might've disappeared on us, not really an error
					String sqlState = sqlEx.getSQLState();
					
					if (!"42S02".equals(sqlState) && 
							sqlEx.getErrorCode() != MysqlErrorNumbers.ER_NO_SUCH_TABLE) {
						throw sqlEx;
					}
					
					continue;
				}

				while (rs.next()) {
					extractForeignKeyForTable(rows, rs, catalog);
				}
			}
		} finally {
			if (rs != null) {
				rs.close();
			}

			rs = null;

			if (stmt != null) {
				stmt.close();
			}

			stmt = null;
		}

		return buildResultSet(fields, rows);
	}

	/**
	 * @see DatabaseMetaData#getAttributes(String, String, String, String)
	 */
	public java.sql.ResultSet getAttributes(String arg0, String arg1,
			String arg2, String arg3) throws SQLException {
		Field[] fields = new Field[21];
		fields[0] = new Field("", "TYPE_CAT", Types.CHAR, 32);
		fields[1] = new Field("", "TYPE_SCHEM", Types.CHAR, 32);
		fields[2] = new Field("", "TYPE_NAME", Types.CHAR, 32);
		fields[3] = new Field("", "ATTR_NAME", Types.CHAR, 32);
		fields[4] = new Field("", "DATA_TYPE", Types.SMALLINT, 32);
		fields[5] = new Field("", "ATTR_TYPE_NAME", Types.CHAR, 32);
		fields[6] = new Field("", "ATTR_SIZE", Types.INTEGER, 32);
		fields[7] = new Field("", "DECIMAL_DIGITS", Types.INTEGER, 32);
		fields[8] = new Field("", "NUM_PREC_RADIX", Types.INTEGER, 32);
		fields[9] = new Field("", "NULLABLE ", Types.INTEGER, 32);
		fields[10] = new Field("", "REMARKS", Types.CHAR, 32);
		fields[11] = new Field("", "ATTR_DEF", Types.CHAR, 32);
		fields[12] = new Field("", "SQL_DATA_TYPE", Types.INTEGER, 32);
		fields[13] = new Field("", "SQL_DATETIME_SUB", Types.INTEGER, 32);
		fields[14] = new Field("", "CHAR_OCTET_LENGTH", Types.INTEGER, 32);
		fields[15] = new Field("", "ORDINAL_POSITION", Types.INTEGER, 32);
		fields[16] = new Field("", "IS_NULLABLE", Types.CHAR, 32);
		fields[17] = new Field("", "SCOPE_CATALOG", Types.CHAR, 32);
		fields[18] = new Field("", "SCOPE_SCHEMA", Types.CHAR, 32);
		fields[19] = new Field("", "SCOPE_TABLE", Types.CHAR, 32);
		fields[20] = new Field("", "SOURCE_DATA_TYPE", Types.SMALLINT, 32);

		return buildResultSet(fields, new ArrayList());
	}

	/**
	 * Get a description of a table's optimal set of columns that uniquely
	 * identifies a row. They are ordered by SCOPE.
	 * <P>
	 * Each column description has the following columns:
	 * <OL>
	 * <li> <B>SCOPE</B> short => actual scope of result
	 * <UL>
	 * <li> bestRowTemporary - very temporary, while using row </li>
	 * <li> bestRowTransaction - valid for remainder of current transaction
	 * </li>
	 * <li> bestRowSession - valid for remainder of current session </li>
	 * </ul>
	 * </li>
	 * <li> <B>COLUMN_NAME</B> String => column name </li>
	 * <li> <B>DATA_TYPE</B> short => SQL data type from java.sql.Types </li>
	 * <li> <B>TYPE_NAME</B> String => Data source dependent type name </li>
	 * <li> <B>COLUMN_SIZE</B> int => precision </li>
	 * <li> <B>BUFFER_LENGTH</B> int => not used </li>
	 * <li> <B>DECIMAL_DIGITS</B> short => scale </li>
	 * <li> <B>PSEUDO_COLUMN</B> short => is this a pseudo column like an
	 * Oracle ROWID
	 * <UL>
	 * <li> bestRowUnknown - may or may not be pseudo column </li>
	 * <li> bestRowNotPseudo - is NOT a pseudo column </li>
	 * <li> bestRowPseudo - is a pseudo column </li>
	 * </ul>
	 * </li>
	 * </ol>
	 * </p>
	 * 
	 * @param catalog
	 *            a catalog name; "" retrieves those without a catalog
	 * @param schema
	 *            a schema name; "" retrieves those without a schema
	 * @param table
	 *            a table name
	 * @param scope
	 *            the scope of interest; use same values as SCOPE
	 * @param nullable
	 *            include columns that are nullable?
	 * @return ResultSet each row is a column description
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public java.sql.ResultSet getBestRowIdentifier(String catalog,
			String schema, final String table, int scope, boolean nullable)
			throws SQLException {
		if (table == null) {
			throw SQLError.createSQLException("Table not specified.",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
		}

		Field[] fields = new Field[8];
		fields[0] = new Field("", "SCOPE", Types.SMALLINT, 5);
		fields[1] = new Field("", "COLUMN_NAME", Types.CHAR, 32);
		fields[2] = new Field("", "DATA_TYPE", Types.INTEGER, 32);
		fields[3] = new Field("", "TYPE_NAME", Types.CHAR, 32);
		fields[4] = new Field("", "COLUMN_SIZE", Types.INTEGER, 10);
		fields[5] = new Field("", "BUFFER_LENGTH", Types.INTEGER, 10);
		fields[6] = new Field("", "DECIMAL_DIGITS", Types.SMALLINT, 10);
		fields[7] = new Field("", "PSEUDO_COLUMN", Types.SMALLINT, 5);

		final ArrayList rows = new ArrayList();
		final Statement stmt = this.conn.getMetadataSafeStatement();

		try {

			new IterateBlock(getCatalogIterator(catalog)) {
				void forEach(Object catalogStr) throws SQLException {
					ResultSet results = null;

					try {
						StringBuffer queryBuf = new StringBuffer(
								"SHOW COLUMNS FROM ");
						queryBuf.append(quotedId);
						queryBuf.append(table);
						queryBuf.append(quotedId);
						queryBuf.append(" FROM ");
						queryBuf.append(quotedId);
						queryBuf.append(catalogStr.toString());
						queryBuf.append(quotedId);

						results = stmt.executeQuery(queryBuf.toString());

						while (results.next()) {
							String keyType = results.getString("Key");

							if (keyType != null) {
								if (StringUtils.startsWithIgnoreCase(keyType,
										"PRI")) {
									byte[][] rowVal = new byte[8][];
									rowVal[0] = Integer
											.toString(
													java.sql.DatabaseMetaData.bestRowSession)
											.getBytes();
									rowVal[1] = results.getBytes("Field");

									String type = results.getString("Type");
									int size = MysqlIO.getMaxBuf();
									int decimals = 0;

									/*
									 * Parse the Type column from MySQL
									 */
									if (type.indexOf("enum") != -1) {
										String temp = type.substring(type
												.indexOf("("), type
												.indexOf(")"));
										java.util.StringTokenizer tokenizer = new java.util.StringTokenizer(
												temp, ",");
										int maxLength = 0;

										while (tokenizer.hasMoreTokens()) {
											maxLength = Math.max(maxLength,
													(tokenizer.nextToken()
															.length() - 2));
										}

										size = maxLength;
										decimals = 0;
										type = "enum";
									} else if (type.indexOf("(") != -1) {
										if (type.indexOf(",") != -1) {
											size = Integer.parseInt(type
													.substring(type
															.indexOf("(") + 1,
															type.indexOf(",")));
											decimals = Integer.parseInt(type
													.substring(type
															.indexOf(",") + 1,
															type.indexOf(")")));
										} else {
											size = Integer.parseInt(type
													.substring(type
															.indexOf("(") + 1,
															type.indexOf(")")));
										}

										type = type.substring(0, type
												.indexOf("("));
									}

									rowVal[2] = s2b(String.valueOf(MysqlDefs
											.mysqlToJavaType(type)));
									rowVal[3] = s2b(type);
									rowVal[4] = Integer.toString(
											size + decimals).getBytes();
									rowVal[5] = Integer.toString(
											size + decimals).getBytes();
									rowVal[6] = Integer.toString(decimals)
											.getBytes();
									rowVal[7] = Integer
											.toString(
													java.sql.DatabaseMetaData.bestRowNotPseudo)
											.getBytes();

									rows.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));
								}
							}
						}
					} catch (SQLException sqlEx) {
						if (!SQLError.SQL_STATE_BASE_TABLE_OR_VIEW_NOT_FOUND.equals(sqlEx.getSQLState())) {
							throw sqlEx;
						}
					} finally {
						if (results != null) {
							try {
								results.close();
							} catch (Exception ex) {
								;
							}

							results = null;
						}
					}
				}
			}.doForAll();
		} finally {
			if (stmt != null) {
				stmt.close();
			}
		}

		java.sql.ResultSet results = buildResultSet(fields, rows);

		return results;

	}

	/*
	 * * Each row in the ResultSet is a parameter desription or column
	 * description with the following fields: <OL> <li> <B>PROCEDURE_CAT</B>
	 * String => procedure catalog (may be null) </li> <li> <B>PROCEDURE_SCHEM</B>
	 * String => procedure schema (may be null) </li> <li> <B>PROCEDURE_NAME</B>
	 * String => procedure name </li> <li> <B>COLUMN_NAME</B> String =>
	 * column/parameter name </li> <li> <B>COLUMN_TYPE</B> Short => kind of
	 * column/parameter: <UL> <li> procedureColumnUnknown - nobody knows </li>
	 * <li> procedureColumnIn - IN parameter </li> <li> procedureColumnInOut -
	 * INOUT parameter </li> <li> procedureColumnOut - OUT parameter </li> <li>
	 * procedureColumnReturn - procedure return value </li> <li>
	 * procedureColumnResult - result column in ResultSet </li> </ul> </li> <li>
	 * <B>DATA_TYPE</B> short => SQL type from java.sql.Types </li> <li>
	 * <B>TYPE_NAME</B> String => SQL type name </li> <li> <B>PRECISION</B>
	 * int => precision </li> <li> <B>LENGTH</B> int => length in bytes of data
	 * </li> <li> <B>SCALE</B> short => scale </li> <li> <B>RADIX</B> short =>
	 * radix </li> <li> <B>NULLABLE</B> short => can it contain NULL? <UL> <li>
	 * procedureNoNulls - does not allow NULL values </li> <li>
	 * procedureNullable - allows NULL values </li> <li>
	 * procedureNullableUnknown - nullability unknown </li> </ul> </li> <li>
	 * <B>REMARKS</B> String => comment describing parameter/column </li> </ol>
	 * </p> <P> <B>Note:</B> Some databases may not return the column
	 * descriptions for a procedure. Additional columns beyond REMARKS can be
	 * defined by the database. </p> @param catalog a catalog name; "" retrieves
	 * those without a catalog @param schemaPattern a schema name pattern; ""
	 * retrieves those without a schema @param procedureNamePattern a procedure
	 * name pattern @param columnNamePattern a column name pattern @return
	 * ResultSet each row is a stored procedure parameter or column description
	 * @throws SQLException if a database access error occurs
	 * 
	 * @see #getSearchStringEscape
	 */
	private void getCallStmtParameterTypes(String catalog, String procName,
			String parameterNamePattern, List resultRows) throws SQLException {
		getCallStmtParameterTypes(catalog, procName, 
				parameterNamePattern, resultRows, false);
	}
	
	private void getCallStmtParameterTypes(String catalog, String procName,
			String parameterNamePattern, List resultRows, 
			boolean forGetFunctionColumns) throws SQLException {
		java.sql.Statement paramRetrievalStmt = null;
		java.sql.ResultSet paramRetrievalRs = null;

		if (parameterNamePattern == null) {
			if (this.conn.getNullNamePatternMatchesAll()) {
				parameterNamePattern = "%";
			} else {
				throw SQLError.createSQLException(
						"Parameter/Column name pattern can not be NULL or empty.",
						SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
			}
		}

		byte[] procNameAsBytes = null;

		try {
			procNameAsBytes = procName.getBytes("UTF-8");
		} catch (UnsupportedEncodingException ueEx) {
			procNameAsBytes = s2b(procName);

			// Set all fields to connection encoding
		}

		String quoteChar = getIdentifierQuoteString();

		String parameterDef = null;
	
		boolean isProcedureInAnsiMode = false;
		String storageDefnDelims = null;
		String storageDefnClosures = null;
		
		try {
			paramRetrievalStmt = this.conn.getMetadataSafeStatement();
			
			if (this.conn.lowerCaseTableNames() && catalog != null 
					&& catalog.length() != 0) {
				// Workaround for bug in server wrt. to 
				// SHOW CREATE PROCEDURE not respecting
				// lower-case table names
				
				String oldCatalog = this.conn.getCatalog();
				ResultSet rs = null;
				
				try {
					this.conn.setCatalog(catalog);
					rs = paramRetrievalStmt.executeQuery("SELECT DATABASE()");
					rs.next();
					
					catalog = rs.getString(1);
					
				} finally {
					
					this.conn.setCatalog(oldCatalog);
					
					if (rs != null) {
						rs.close();
					}
				}
			}
			
			if (paramRetrievalStmt.getMaxRows() != 0) {
				paramRetrievalStmt.setMaxRows(0);
			}

			int dotIndex = -1;

			if (!" ".equals(quoteChar)) {
				dotIndex = StringUtils.indexOfIgnoreCaseRespectQuotes(0,
						procName, ".", quoteChar.charAt(0), !this.conn
								.isNoBackslashEscapesSet());
			} else {
				dotIndex = procName.indexOf(".");
			}

			String dbName = null;

			if (dotIndex != -1 && (dotIndex + 1) < procName.length()) {
				dbName = procName.substring(0, dotIndex);
				procName = procName.substring(dotIndex + 1);
			} else {
				dbName = catalog;
			}

			StringBuffer procNameBuf = new StringBuffer();

			if (dbName != null) {
				if (!" ".equals(quoteChar) && !dbName.startsWith(quoteChar)) {
					procNameBuf.append(quoteChar);
				}

				procNameBuf.append(dbName);

				if (!" ".equals(quoteChar) && !dbName.startsWith(quoteChar)) {
					procNameBuf.append(quoteChar);
				}

				procNameBuf.append(".");
			}

			boolean procNameIsNotQuoted = !procName.startsWith(quoteChar);

			if (!" ".equals(quoteChar) && procNameIsNotQuoted) {
				procNameBuf.append(quoteChar);
			}

			procNameBuf.append(procName);

			if (!" ".equals(quoteChar) && procNameIsNotQuoted) {
				procNameBuf.append(quoteChar);
			}

			boolean parsingFunction = false;

			try {
				paramRetrievalRs = paramRetrievalStmt
						.executeQuery("SHOW CREATE PROCEDURE "
								+ procNameBuf.toString());
				parsingFunction = false;
			} catch (SQLException sqlEx) {
				paramRetrievalRs = paramRetrievalStmt
						.executeQuery("SHOW CREATE FUNCTION "
								+ procNameBuf.toString());
				parsingFunction = true;
			}

			if (paramRetrievalRs.next()) {
				String procedureDef = parsingFunction ? paramRetrievalRs
						.getString("Create Function") : paramRetrievalRs
						.getString("Create Procedure");
						
				if (procedureDef == null || procedureDef.length() == 0) {
					throw SQLError.createSQLException("User does not have access to metadata required to determine " +
							"stored procedure parameter types. If rights can not be granted, configure connection with \"noAccessToProcedureBodies=true\" " +
							"to have driver generate parameters that represent INOUT strings irregardless of actual parameter types.",
							SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());		
				}

				try {
					String sqlMode = paramRetrievalRs.getString("sql_mode");
					
					if (StringUtils.indexOfIgnoreCase(sqlMode, "ANSI") != -1) {
						isProcedureInAnsiMode = true;
					}
				} catch (SQLException sqlEx) {
					// doesn't exist
				}

				String identifierMarkers = isProcedureInAnsiMode ? "`\"" : "`";
				String identifierAndStringMarkers = "'" + identifierMarkers;
				storageDefnDelims = "(" + identifierMarkers;
				storageDefnClosures = ")" + identifierMarkers;
				
				// sanitize/normalize by stripping out comments
				procedureDef = StringUtils.stripComments(procedureDef, 
						identifierAndStringMarkers, identifierAndStringMarkers, true, false, true, true);
				
				int openParenIndex = StringUtils
				.indexOfIgnoreCaseRespectQuotes(0, procedureDef, "(",
						quoteChar.charAt(0), !this.conn
						.isNoBackslashEscapesSet());
				int endOfParamDeclarationIndex = 0;

				endOfParamDeclarationIndex = endPositionOfParameterDeclaration(
						openParenIndex, procedureDef, quoteChar);

				if (parsingFunction) {

					// Grab the return column since it needs
					// to go first in the output result set
					int returnsIndex = StringUtils
					.indexOfIgnoreCaseRespectQuotes(0, procedureDef,
							" RETURNS ", quoteChar.charAt(0),
							!this.conn.isNoBackslashEscapesSet());

					int endReturnsDef = findEndOfReturnsClause(procedureDef,
							quoteChar, returnsIndex);

					// Trim off whitespace after "RETURNS"

					int declarationStart = returnsIndex + "RETURNS ".length();

					while (declarationStart < procedureDef.length()) {
						if (Character.isWhitespace(procedureDef.charAt(declarationStart))) {
							declarationStart++;
						} else {
							break;
						}
					}

					String returnsDefn = procedureDef.substring(declarationStart, endReturnsDef).trim();
					TypeDescriptor returnDescriptor = new TypeDescriptor(
							returnsDefn, null);

					resultRows.add(convertTypeDescriptorToProcedureRow(
							procNameAsBytes, "", false, false, true,
							returnDescriptor, forGetFunctionColumns, 0));
				}

				if ((openParenIndex == -1)
						|| (endOfParamDeclarationIndex == -1)) {
					// parse error?
					throw SQLError
					.createSQLException(
							"Internal error when parsing callable statement metadata",
							SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
				}

				parameterDef = procedureDef.substring(openParenIndex + 1,
						endOfParamDeclarationIndex);
			}
		} finally {
			SQLException sqlExRethrow = null;

			if (paramRetrievalRs != null) {
				try {
					paramRetrievalRs.close();
				} catch (SQLException sqlEx) {
					sqlExRethrow = sqlEx;
				}

				paramRetrievalRs = null;
			}

			if (paramRetrievalStmt != null) {
				try {
					paramRetrievalStmt.close();
				} catch (SQLException sqlEx) {
					sqlExRethrow = sqlEx;
				}

				paramRetrievalStmt = null;
			}

			if (sqlExRethrow != null) {
				throw sqlExRethrow;
			}
		}

		if (parameterDef != null) {
			int ordinal = 1;
			
			List parseList = StringUtils.split(parameterDef, ",",
					storageDefnDelims, storageDefnClosures, true);

			int parseListLen = parseList.size();

			for (int i = 0; i < parseListLen; i++) {
				String declaration = (String) parseList.get(i);

				if (declaration.trim().length() == 0) {
					break; // no parameters actually declared, but whitespace spans lines
				}
				
				StringTokenizer declarationTok = new StringTokenizer(
						declaration, " \t");

				String paramName = null;
				boolean isOutParam = false;
				boolean isInParam = false;

				if (declarationTok.hasMoreTokens()) {
					String possibleParamName = declarationTok.nextToken();

					if (possibleParamName.equalsIgnoreCase("OUT")) {
						isOutParam = true;

						if (declarationTok.hasMoreTokens()) {
							paramName = declarationTok.nextToken();
						} else {
							throw SQLError.createSQLException(
									"Internal error when parsing callable statement metadata (missing parameter name)",
									SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
						}
					} else if (possibleParamName.equalsIgnoreCase("INOUT")) {
						isOutParam = true;
						isInParam = true;

						if (declarationTok.hasMoreTokens()) {
							paramName = declarationTok.nextToken();
						} else {
							throw SQLError.createSQLException(
									"Internal error when parsing callable statement metadata (missing parameter name)",
									SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
						}
					} else if (possibleParamName.equalsIgnoreCase("IN")) {
						isOutParam = false;
						isInParam = true;

						if (declarationTok.hasMoreTokens()) {
							paramName = declarationTok.nextToken();
						} else {
							throw SQLError.createSQLException(
									"Internal error when parsing callable statement metadata (missing parameter name)",
									SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
						}
					} else {
						isOutParam = false;
						isInParam = true;

						paramName = possibleParamName;
					}

					TypeDescriptor typeDesc = null;

					if (declarationTok.hasMoreTokens()) {
						StringBuffer typeInfoBuf = new StringBuffer(
								declarationTok.nextToken());

						while (declarationTok.hasMoreTokens()) {
							typeInfoBuf.append(" ");
							typeInfoBuf.append(declarationTok.nextToken());
						}

						String typeInfo = typeInfoBuf.toString();

						typeDesc = new TypeDescriptor(typeInfo, null);
					} else {
						throw SQLError.createSQLException(
								"Internal error when parsing callable statement metadata (missing parameter type)",
								SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
					}

					if ((paramName.startsWith("`") && paramName.endsWith("`")) || 
							(isProcedureInAnsiMode && paramName.startsWith("\"") && paramName.endsWith("\""))) {
						paramName = paramName.substring(1, paramName.length() - 1);
					}

					int wildCompareRes = StringUtils.wildCompare(paramName,
							parameterNamePattern);

					if (wildCompareRes != StringUtils.WILD_COMPARE_NO_MATCH) {
						ResultSetRow row = convertTypeDescriptorToProcedureRow(
								procNameAsBytes, paramName, isOutParam,
								isInParam, false, typeDesc, forGetFunctionColumns,
								ordinal++);

						resultRows.add(row);
					}
				} else {
					throw SQLError.createSQLException(
							"Internal error when parsing callable statement metadata (unknown output from 'SHOW CREATE PROCEDURE')",
							SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
				}
			}
		} else {
			// Is this an error? JDBC spec doesn't make it clear if stored
			// procedure doesn't
			// exist, is it an error....
		}
	}
	/**
	 * Finds the end of the parameter declaration from the output of "SHOW
	 * CREATE PROCEDURE".
	 * 
	 * @param beginIndex
	 *            should be the index of the procedure body that contains the
	 *            first "(".
	 * @param procedureDef
	 *            the procedure body
	 * @param quoteChar
	 *            the identifier quote character in use
	 * @return the ending index of the parameter declaration, not including the
	 *         closing ")"
	 * @throws SQLException
	 *             if a parse error occurs.
	 */
	private int endPositionOfParameterDeclaration(int beginIndex,
			String procedureDef, String quoteChar) throws SQLException {
		int currentPos = beginIndex + 1;
		int parenDepth = 1; // counting the first openParen

		while (parenDepth > 0 && currentPos < procedureDef.length()) {
			int closedParenIndex = StringUtils.indexOfIgnoreCaseRespectQuotes(
					currentPos, procedureDef, ")", quoteChar.charAt(0),
					!this.conn.isNoBackslashEscapesSet());

			if (closedParenIndex != -1) {
				int nextOpenParenIndex = StringUtils
						.indexOfIgnoreCaseRespectQuotes(currentPos,
								procedureDef, "(", quoteChar.charAt(0),
								!this.conn.isNoBackslashEscapesSet());

				if (nextOpenParenIndex != -1
						&& nextOpenParenIndex < closedParenIndex) {
					parenDepth++;
					currentPos = closedParenIndex + 1; // set after closed
														// paren that increases
														// depth
				} else {
					parenDepth--;
					currentPos = closedParenIndex; // start search from same
													// position
				}
			} else {
				// we should always get closed paren of some sort
				throw SQLError
						.createSQLException(
								"Internal error when parsing callable statement metadata",
								SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
			}
		}

		return currentPos;
	}

	/**
	 * Finds the end of the RETURNS clause for SQL Functions by using any of the
	 * keywords allowed after the RETURNS clause, or a label.
	 * 
	 * @param procedureDefn
	 *            the function body containing the definition of the function
	 * @param quoteChar
	 *            the identifier quote string in use
	 * @param positionOfReturnKeyword
	 *            the position of "RETRUNS" in the definition
	 * @return the end of the returns clause
	 * @throws SQLException
	 *             if a parse error occurs
	 */
	private int findEndOfReturnsClause(String procedureDefn, String quoteChar,
			int positionOfReturnKeyword) throws SQLException {
		/*
		 * characteristic: LANGUAGE SQL | [NOT] DETERMINISTIC | { CONTAINS SQL |
		 * NO SQL | READS SQL DATA | MODIFIES SQL DATA } | SQL SECURITY {
		 * DEFINER | INVOKER } | COMMENT 'string'
		 */

		String[] tokens = new String[] { "LANGUAGE", "NOT", "DETERMINISTIC",
				"CONTAINS", "NO", "READ", "MODIFIES", "SQL", "COMMENT", "BEGIN", 
				"RETURN" };

		int startLookingAt = positionOfReturnKeyword + "RETURNS".length() + 1;

		int endOfReturn = -1;
		
		for (int i = 0; i < tokens.length; i++) {
			int nextEndOfReturn = StringUtils.indexOfIgnoreCaseRespectQuotes(
					startLookingAt, procedureDefn, tokens[i], quoteChar
							.charAt(0), !this.conn.isNoBackslashEscapesSet());

			if (nextEndOfReturn != -1) {
				if (endOfReturn == -1 || (nextEndOfReturn < endOfReturn)) {
					endOfReturn = nextEndOfReturn;
				}
			}
		}
		
		if (endOfReturn != -1) {
			return endOfReturn;
		}

		// Label?
		endOfReturn = StringUtils.indexOfIgnoreCaseRespectQuotes(
				startLookingAt, procedureDefn, ":", quoteChar.charAt(0),
				!this.conn.isNoBackslashEscapesSet());

		if (endOfReturn != -1) {
			// seek back until whitespace
			for (int i = endOfReturn; i > 0; i--) {
				if (Character.isWhitespace(procedureDefn.charAt(i))) {
					return i;
				}
			}
		}

		// We can't parse it.

		throw SQLError.createSQLException(
				"Internal error when parsing callable statement metadata",
				SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
	}
	
	/**
	 * Parses the cascade option string and returns the DBMD constant that
	 * represents it (for deletes)
	 * 
	 * @param cascadeOptions
	 *            the comment from 'SHOW TABLE STATUS'
	 * @return the DBMD constant that represents the cascade option
	 */
	private int getCascadeDeleteOption(String cascadeOptions) {
		int onDeletePos = cascadeOptions.indexOf("ON DELETE");

		if (onDeletePos != -1) {
			String deleteOptions = cascadeOptions.substring(onDeletePos,
					cascadeOptions.length());

			if (deleteOptions.startsWith("ON DELETE CASCADE")) {
				return java.sql.DatabaseMetaData.importedKeyCascade;
			} else if (deleteOptions.startsWith("ON DELETE SET NULL")) {
				return java.sql.DatabaseMetaData.importedKeySetNull;
			} else if (deleteOptions.startsWith("ON DELETE RESTRICT")) {
				return java.sql.DatabaseMetaData.importedKeyRestrict;
			} else if (deleteOptions.startsWith("ON DELETE NO ACTION")) {
				return java.sql.DatabaseMetaData.importedKeyNoAction;
			}
		}

		return java.sql.DatabaseMetaData.importedKeyNoAction;
	}

	/**
	 * Parses the cascade option string and returns the DBMD constant that
	 * represents it (for Updates)
	 * 
	 * @param cascadeOptions
	 *            the comment from 'SHOW TABLE STATUS'
	 * @return the DBMD constant that represents the cascade option
	 */
	private int getCascadeUpdateOption(String cascadeOptions) {
		int onUpdatePos = cascadeOptions.indexOf("ON UPDATE");

		if (onUpdatePos != -1) {
			String updateOptions = cascadeOptions.substring(onUpdatePos,
					cascadeOptions.length());

			if (updateOptions.startsWith("ON UPDATE CASCADE")) {
				return java.sql.DatabaseMetaData.importedKeyCascade;
			} else if (updateOptions.startsWith("ON UPDATE SET NULL")) {
				return java.sql.DatabaseMetaData.importedKeySetNull;
			} else if (updateOptions.startsWith("ON UPDATE RESTRICT")) {
				return java.sql.DatabaseMetaData.importedKeyRestrict;
			} else if (updateOptions.startsWith("ON UPDATE NO ACTION")) {
				return java.sql.DatabaseMetaData.importedKeyNoAction;
			}
		}

		return java.sql.DatabaseMetaData.importedKeyNoAction;
	}

	protected IteratorWithCleanup getCatalogIterator(String catalogSpec)
			throws SQLException {
		IteratorWithCleanup allCatalogsIter;
		if (catalogSpec != null) {
			if (!catalogSpec.equals("")) {
				allCatalogsIter = new SingleStringIterator(catalogSpec);
			} else {
				// legacy mode of operation
				allCatalogsIter = new SingleStringIterator(this.database);
			}
		} else if (this.conn.getNullCatalogMeansCurrent()) {
			allCatalogsIter = new SingleStringIterator(this.database);
		} else {
			allCatalogsIter = new ResultSetIterator(getCatalogs(), 1);
		}

		return allCatalogsIter;
	}

	/**
	 * Get the catalog names available in this database. The results are ordered
	 * by catalog name.
	 * <P>
	 * The catalog column is:
	 * <OL>
	 * <li> <B>TABLE_CAT</B> String => catalog name </li>
	 * </ol>
	 * </p>
	 * 
	 * @return ResultSet each row has a single String column that is a catalog
	 *         name
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public java.sql.ResultSet getCatalogs() throws SQLException {
		java.sql.ResultSet results = null;
		java.sql.Statement stmt = null;

		try {
			stmt = this.conn.createStatement();
			stmt.setEscapeProcessing(false);
			results = stmt.executeQuery("SHOW DATABASES");

			java.sql.ResultSetMetaData resultsMD = results.getMetaData();
			Field[] fields = new Field[1];
			fields[0] = new Field("", "TABLE_CAT", Types.VARCHAR, resultsMD
					.getColumnDisplaySize(1));

			ArrayList tuples = new ArrayList();

			while (results.next()) {
				byte[][] rowVal = new byte[1][];
				rowVal[0] = results.getBytes(1);
				tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));
			}

			return buildResultSet(fields, tuples);
		} finally {
			if (results != null) {
				try {
					results.close();
				} catch (SQLException sqlEx) {
					AssertionFailedException.shouldNotHappen(sqlEx);
				}

				results = null;
			}

			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException sqlEx) {
					AssertionFailedException.shouldNotHappen(sqlEx);
				}

				stmt = null;
			}
		}
	}

	/**
	 * What's the separator between catalog and table name?
	 * 
	 * @return the separator string
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public String getCatalogSeparator() throws SQLException {
		return ".";
	}

	// ----------------------------------------------------------------------
	// The following group of methods exposes various limitations
	// based on the target database with the current driver.
	// Unless otherwise specified, a result of zero means there is no
	// limit, or the limit is not known.

	/**
	 * What's the database vendor's preferred term for "catalog"?
	 * 
	 * @return the vendor term
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public String getCatalogTerm() throws SQLException {
		return "database";
	}

	/**
	 * Get a description of the access rights for a table's columns.
	 * <P>
	 * Only privileges matching the column name criteria are returned. They are
	 * ordered by COLUMN_NAME and PRIVILEGE.
	 * </p>
	 * <P>
	 * Each privilige description has the following columns:
	 * <OL>
	 * <li> <B>TABLE_CAT</B> String => table catalog (may be null) </li>
	 * <li> <B>TABLE_SCHEM</B> String => table schema (may be null) </li>
	 * <li> <B>TABLE_NAME</B> String => table name </li>
	 * <li> <B>COLUMN_NAME</B> String => column name </li>
	 * <li> <B>GRANTOR</B> => grantor of access (may be null) </li>
	 * <li> <B>GRANTEE</B> String => grantee of access </li>
	 * <li> <B>PRIVILEGE</B> String => name of access (SELECT, INSERT, UPDATE,
	 * REFRENCES, ...) </li>
	 * <li> <B>IS_GRANTABLE</B> String => "YES" if grantee is permitted to
	 * grant to others; "NO" if not; null if unknown </li>
	 * </ol>
	 * </p>
	 * 
	 * @param catalog
	 *            a catalog name; "" retrieves those without a catalog
	 * @param schema
	 *            a schema name; "" retrieves those without a schema
	 * @param table
	 *            a table name
	 * @param columnNamePattern
	 *            a column name pattern
	 * @return ResultSet each row is a column privilege description
	 * @throws SQLException
	 *             if a database access error occurs
	 * @see #getSearchStringEscape
	 */
	public java.sql.ResultSet getColumnPrivileges(String catalog,
			String schema, String table, String columnNamePattern)
			throws SQLException {
		Field[] fields = new Field[8];
		fields[0] = new Field("", "TABLE_CAT", Types.CHAR, 64);
		fields[1] = new Field("", "TABLE_SCHEM", Types.CHAR, 1);
		fields[2] = new Field("", "TABLE_NAME", Types.CHAR, 64);
		fields[3] = new Field("", "COLUMN_NAME", Types.CHAR, 64);
		fields[4] = new Field("", "GRANTOR", Types.CHAR, 77);
		fields[5] = new Field("", "GRANTEE", Types.CHAR, 77);
		fields[6] = new Field("", "PRIVILEGE", Types.CHAR, 64);
		fields[7] = new Field("", "IS_GRANTABLE", Types.CHAR, 3);

		StringBuffer grantQuery = new StringBuffer(
				"SELECT c.host, c.db, t.grantor, c.user, "
						+ "c.table_name, c.column_name, c.column_priv "
						+ "from mysql.columns_priv c, mysql.tables_priv t "
						+ "where c.host = t.host and c.db = t.db and "
						+ "c.table_name = t.table_name ");

		if ((catalog != null) && (catalog.length() != 0)) {
			grantQuery.append(" AND c.db='");
			grantQuery.append(catalog);
			grantQuery.append("' ");
			;
		}

		grantQuery.append(" AND c.table_name ='");
		grantQuery.append(table);
		grantQuery.append("' AND c.column_name like '");
		grantQuery.append(columnNamePattern);
		grantQuery.append("'");

		Statement stmt = null;
		ResultSet results = null;
		ArrayList grantRows = new ArrayList();

		try {
			stmt = this.conn.createStatement();
			stmt.setEscapeProcessing(false);
			results = stmt.executeQuery(grantQuery.toString());

			while (results.next()) {
				String host = results.getString(1);
				String db = results.getString(2);
				String grantor = results.getString(3);
				String user = results.getString(4);

				if ((user == null) || (user.length() == 0)) {
					user = "%";
				}

				StringBuffer fullUser = new StringBuffer(user);

				if ((host != null) && this.conn.getUseHostsInPrivileges()) {
					fullUser.append("@");
					fullUser.append(host);
				}

				String columnName = results.getString(6);
				String allPrivileges = results.getString(7);

				if (allPrivileges != null) {
					allPrivileges = allPrivileges.toUpperCase(Locale.ENGLISH);

					StringTokenizer st = new StringTokenizer(allPrivileges, ",");

					while (st.hasMoreTokens()) {
						String privilege = st.nextToken().trim();
						byte[][] tuple = new byte[8][];
						tuple[0] = s2b(db);
						tuple[1] = null;
						tuple[2] = s2b(table);
						tuple[3] = s2b(columnName);

						if (grantor != null) {
							tuple[4] = s2b(grantor);
						} else {
							tuple[4] = null;
						}

						tuple[5] = s2b(fullUser.toString());
						tuple[6] = s2b(privilege);
						tuple[7] = null;
						grantRows.add(new ByteArrayRow(tuple, getExceptionInterceptor()));
					}
				}
			}
		} finally {
			if (results != null) {
				try {
					results.close();
				} catch (Exception ex) {
					;
				}

				results = null;
			}

			if (stmt != null) {
				try {
					stmt.close();
				} catch (Exception ex) {
					;
				}

				stmt = null;
			}
		}

		return buildResultSet(fields, grantRows);
	}

	/**
	 * Get a description of table columns available in a catalog.
	 * <P>
	 * Only column descriptions matching the catalog, schema, table and column
	 * name criteria are returned. They are ordered by TABLE_SCHEM, TABLE_NAME
	 * and ORDINAL_POSITION.
	 * </p>
	 * <P>
	 * Each column description has the following columns:
	 * <OL>
	 * <li> <B>TABLE_CAT</B> String => table catalog (may be null) </li>
	 * <li> <B>TABLE_SCHEM</B> String => table schema (may be null) </li>
	 * <li> <B>TABLE_NAME</B> String => table name </li>
	 * <li> <B>COLUMN_NAME</B> String => column name </li>
	 * <li> <B>DATA_TYPE</B> short => SQL type from java.sql.Types </li>
	 * <li> <B>TYPE_NAME</B> String => Data source dependent type name </li>
	 * <li> <B>COLUMN_SIZE</B> int => column size. For char or date types this
	 * is the maximum number of characters, for numeric or decimal types this is
	 * precision. </li>
	 * <li> <B>BUFFER_LENGTH</B> is not used. </li>
	 * <li> <B>DECIMAL_DIGITS</B> int => the number of fractional digits </li>
	 * <li> <B>NUM_PREC_RADIX</B> int => Radix (typically either 10 or 2) </li>
	 * <li> <B>NULLABLE</B> int => is NULL allowed?
	 * <UL>
	 * <li> columnNoNulls - might not allow NULL values </li>
	 * <li> columnNullable - definitely allows NULL values </li>
	 * <li> columnNullableUnknown - nullability unknown </li>
	 * </ul>
	 * </li>
	 * <li> <B>REMARKS</B> String => comment describing column (may be null)
	 * </li>
	 * <li> <B>COLUMN_DEF</B> String => default value (may be null) </li>
	 * <li> <B>SQL_DATA_TYPE</B> int => unused </li>
	 * <li> <B>SQL_DATETIME_SUB</B> int => unused </li>
	 * <li> <B>CHAR_OCTET_LENGTH</B> int => for char types the maximum number
	 * of bytes in the column </li>
	 * <li> <B>ORDINAL_POSITION</B> int => index of column in table (starting
	 * at 1) </li>
	 * <li> <B>IS_NULLABLE</B> String => "NO" means column definitely does not
	 * allow NULL values; "YES" means the column might allow NULL values. An
	 * empty string means nobody knows. </li>
	 * </ol>
	 * </p>
	 * 
	 * @param catalog
	 *            a catalog name; "" retrieves those without a catalog
	 * @param schemaPattern
	 *            a schema name pattern; "" retrieves those without a schema
	 * @param tableNamePattern
	 *            a table name pattern
	 * @param columnNamePattern
	 *            a column name pattern
	 * @return ResultSet each row is a column description
	 * @throws SQLException
	 *             if a database access error occurs
	 * @see #getSearchStringEscape
	 */
	public java.sql.ResultSet getColumns(final String catalog,
			final String schemaPattern, final String tableNamePattern,
			String columnNamePattern) throws SQLException {

		if (columnNamePattern == null) {
			if (this.conn.getNullNamePatternMatchesAll()) {
				columnNamePattern = "%";
			} else {
				throw SQLError.createSQLException(
						"Column name pattern can not be NULL or empty.",
						SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
			}
		}

		final String colPattern = columnNamePattern;

		Field[] fields = createColumnsFields();

		final ArrayList rows = new ArrayList();
		final Statement stmt = this.conn.getMetadataSafeStatement();

		try {

			new IterateBlock(getCatalogIterator(catalog)) {
				void forEach(Object catalogStr) throws SQLException {

					ArrayList tableNameList = new ArrayList();

					if (tableNamePattern == null) {
						// Select from all tables
						java.sql.ResultSet tables = null;

						try {
							tables = getTables((String)catalogStr, schemaPattern, "%",
									new String[0]);

							while (tables.next()) {
								String tableNameFromList = tables
										.getString("TABLE_NAME");
								tableNameList.add(tableNameFromList);
							}
						} finally {
							if (tables != null) {
								try {
									tables.close();
								} catch (Exception sqlEx) {
									AssertionFailedException
											.shouldNotHappen(sqlEx);
								}

								tables = null;
							}
						}
					} else {
						java.sql.ResultSet tables = null;

						try {
							tables = getTables((String)catalogStr, schemaPattern,
									tableNamePattern, new String[0]);

							while (tables.next()) {
								String tableNameFromList = tables
										.getString("TABLE_NAME");
								tableNameList.add(tableNameFromList);
							}
						} finally {
							if (tables != null) {
								try {
									tables.close();
								} catch (SQLException sqlEx) {
									AssertionFailedException
											.shouldNotHappen(sqlEx);
								}

								tables = null;
							}
						}
					}

					java.util.Iterator tableNames = tableNameList.iterator();

					while (tableNames.hasNext()) {
						String tableName = (String) tableNames.next();

						ResultSet results = null;

						try {
							StringBuffer queryBuf = new StringBuffer("SHOW ");

							if (conn.versionMeetsMinimum(4, 1, 0)) {
								queryBuf.append("FULL ");
							}

							queryBuf.append("COLUMNS FROM ");
							queryBuf.append(quotedId);
							queryBuf.append(tableName);
							queryBuf.append(quotedId);
							queryBuf.append(" FROM ");
							queryBuf.append(quotedId);
							queryBuf.append((String)catalogStr);
							queryBuf.append(quotedId);
							queryBuf.append(" LIKE '");
							queryBuf.append(colPattern);
							queryBuf.append("'");

							// Return correct ordinals if column name pattern is
							// not '%'
							// Currently, MySQL doesn't show enough data to do
							// this, so we do it the 'hard' way...Once _SYSTEM
							// tables are in, this should be much easier
							boolean fixUpOrdinalsRequired = false;
							Map ordinalFixUpMap = null;

							if (!colPattern.equals("%")) {
								fixUpOrdinalsRequired = true;

								StringBuffer fullColumnQueryBuf = new StringBuffer(
										"SHOW ");

								if (conn.versionMeetsMinimum(4, 1, 0)) {
									fullColumnQueryBuf.append("FULL ");
								}

								fullColumnQueryBuf.append("COLUMNS FROM ");
								fullColumnQueryBuf.append(quotedId);
								fullColumnQueryBuf.append(tableName);
								fullColumnQueryBuf.append(quotedId);
								fullColumnQueryBuf.append(" FROM ");
								fullColumnQueryBuf.append(quotedId);
								fullColumnQueryBuf
										.append((String)catalogStr);
								fullColumnQueryBuf.append(quotedId);

								results = stmt.executeQuery(fullColumnQueryBuf
										.toString());

								ordinalFixUpMap = new HashMap();

								int fullOrdinalPos = 1;

								while (results.next()) {
									String fullOrdColName = results
											.getString("Field");

									ordinalFixUpMap.put(fullOrdColName,
											Constants.integerValueOf(fullOrdinalPos++));
								}
							}

							results = stmt.executeQuery(queryBuf.toString());

							int ordPos = 1;

							while (results.next()) {
								byte[][] rowVal = new byte[23][];
								rowVal[0] = s2b((String)catalogStr); // TABLE_CAT
								rowVal[1] = null; // TABLE_SCHEM (No schemas
								// in MySQL)

								rowVal[2] = s2b(tableName); // TABLE_NAME
								rowVal[3] = results.getBytes("Field");

								TypeDescriptor typeDesc = new TypeDescriptor(
										results.getString("Type"), results
												.getString("Null"));

								rowVal[4] = Short.toString(typeDesc.dataType)
										.getBytes();

								// DATA_TYPE (jdbc)
								rowVal[5] = s2b(typeDesc.typeName); // TYPE_NAME
								// (native)
								rowVal[6] = typeDesc.columnSize == null ? null : s2b(typeDesc.columnSize.toString());
								rowVal[7] = s2b(Integer.toString(typeDesc.bufferLength));
								rowVal[8] = typeDesc.decimalDigits == null ? null : s2b(typeDesc.decimalDigits.toString());
								rowVal[9] = s2b(Integer
										.toString(typeDesc.numPrecRadix));
								rowVal[10] = s2b(Integer
										.toString(typeDesc.nullability));

								//
								// Doesn't always have this field, depending on
								// version
								//
								//
								// REMARK column
								//
								try {
									if (conn.versionMeetsMinimum(4, 1, 0)) {
										rowVal[11] = results
												.getBytes("Comment");
									} else {
										rowVal[11] = results.getBytes("Extra");
									}
								} catch (Exception E) {
									rowVal[11] = new byte[0];
								}

								// COLUMN_DEF
								rowVal[12] = results.getBytes("Default");

								rowVal[13] = new byte[] { (byte) '0' }; // SQL_DATA_TYPE
								rowVal[14] = new byte[] { (byte) '0' }; // SQL_DATE_TIME_SUB
								
								if (StringUtils.indexOfIgnoreCase(typeDesc.typeName, "CHAR") != -1 ||
										StringUtils.indexOfIgnoreCase(typeDesc.typeName, "BLOB") != -1 ||
										StringUtils.indexOfIgnoreCase(typeDesc.typeName, "TEXT") != -1 ||
										StringUtils.indexOfIgnoreCase(typeDesc.typeName, "BINARY") != -1) {
									rowVal[15] = rowVal[6]; // CHAR_OCTET_LENGTH
								} else {
									rowVal[15] = null;
								}

								// ORDINAL_POSITION
								if (!fixUpOrdinalsRequired) {
									rowVal[16] = Integer.toString(ordPos++)
											.getBytes();
								} else {
									String origColName = results
											.getString("Field");
									Integer realOrdinal = (Integer) ordinalFixUpMap
											.get(origColName);

									if (realOrdinal != null) {
										rowVal[16] = realOrdinal.toString()
												.getBytes();
									} else {
										throw SQLError.createSQLException(
												"Can not find column in full column list to determine true ordinal position.",
												SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
									}
								}

								rowVal[17] = s2b(typeDesc.isNullable);
								
								// We don't support REF or DISTINCT types
								rowVal[18] = null;
								rowVal[19] = null;
								rowVal[20] = null;
								rowVal[21] = null;
								
								rowVal[22] = s2b("");
								
								String extra = results.getString("Extra");
								
								if (extra != null) {
									rowVal[22] = s2b(StringUtils
											.indexOfIgnoreCase(extra,
													"auto_increment") != -1 ? "YES"
											: "NO");
								}
								
								rows.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));
							}
						} finally {
							if (results != null) {
								try {
									results.close();
								} catch (Exception ex) {
									;
								}

								results = null;
							}
						}
					}
				}
			}.doForAll();
		} finally {
			if (stmt != null) {
				stmt.close();
			}
		}

		java.sql.ResultSet results = buildResultSet(fields, rows);

		return results;
	}

	protected Field[] createColumnsFields() {
		Field[] fields = new Field[23];
		fields[0] = new Field("", "TABLE_CAT", Types.CHAR, 255);
		fields[1] = new Field("", "TABLE_SCHEM", Types.CHAR, 0);
		fields[2] = new Field("", "TABLE_NAME", Types.CHAR, 255);
		fields[3] = new Field("", "COLUMN_NAME", Types.CHAR, 32);
		fields[4] = new Field("", "DATA_TYPE", Types.INTEGER, 5);
		fields[5] = new Field("", "TYPE_NAME", Types.CHAR, 16);
		fields[6] = new Field("", "COLUMN_SIZE", Types.INTEGER, Integer
				.toString(Integer.MAX_VALUE).length());
		fields[7] = new Field("", "BUFFER_LENGTH", Types.INTEGER, 10);
		fields[8] = new Field("", "DECIMAL_DIGITS", Types.INTEGER, 10);
		fields[9] = new Field("", "NUM_PREC_RADIX", Types.INTEGER, 10);
		fields[10] = new Field("", "NULLABLE", Types.INTEGER, 10);
		fields[11] = new Field("", "REMARKS", Types.CHAR, 0);
		fields[12] = new Field("", "COLUMN_DEF", Types.CHAR, 0);
		fields[13] = new Field("", "SQL_DATA_TYPE", Types.INTEGER, 10);
		fields[14] = new Field("", "SQL_DATETIME_SUB", Types.INTEGER, 10);
		fields[15] = new Field("", "CHAR_OCTET_LENGTH", Types.INTEGER, Integer
				.toString(Integer.MAX_VALUE).length());
		fields[16] = new Field("", "ORDINAL_POSITION", Types.INTEGER, 10);
		fields[17] = new Field("", "IS_NULLABLE", Types.CHAR, 3);
		fields[18] = new Field("", "SCOPE_CATALOG", Types.CHAR, 255);
		fields[19] = new Field("", "SCOPE_SCHEMA", Types.CHAR, 255);
		fields[20] = new Field("", "SCOPE_TABLE", Types.CHAR, 255);
		fields[21] = new Field("", "SOURCE_DATA_TYPE", Types.SMALLINT, 10);
		fields[22] = new Field("", "IS_AUTOINCREMENT", Types.CHAR, 3);
		return fields;
	}

	/**
	 * JDBC 2.0 Return the connection that produced this metadata object.
	 * 
	 * @return the connection that produced this metadata object.
	 * @throws SQLException
	 *             if a database error occurs
	 */
	public java.sql.Connection getConnection() throws SQLException {
		return this.conn;
	}

	/**
	 * Get a description of the foreign key columns in the foreign key table
	 * that reference the primary key columns of the primary key table (describe
	 * how one table imports another's key.) This should normally return a
	 * single foreign key/primary key pair (most tables only import a foreign
	 * key from a table once.) They are ordered by FKTABLE_CAT, FKTABLE_SCHEM,
	 * FKTABLE_NAME, and KEY_SEQ.
	 * <P>
	 * Each foreign key column description has the following columns:
	 * <OL>
	 * <li> <B>PKTABLE_CAT</B> String => primary key table catalog (may be
	 * null) </li>
	 * <li> <B>PKTABLE_SCHEM</B> String => primary key table schema (may be
	 * null) </li>
	 * <li> <B>PKTABLE_NAME</B> String => primary key table name </li>
	 * <li> <B>PKCOLUMN_NAME</B> String => primary key column name </li>
	 * <li> <B>FKTABLE_CAT</B> String => foreign key table catalog (may be
	 * null) being exported (may be null) </li>
	 * <li> <B>FKTABLE_SCHEM</B> String => foreign key table schema (may be
	 * null) being exported (may be null) </li>
	 * <li> <B>FKTABLE_NAME</B> String => foreign key table name being exported
	 * </li>
	 * <li> <B>FKCOLUMN_NAME</B> String => foreign key column name being
	 * exported </li>
	 * <li> <B>KEY_SEQ</B> short => sequence number within foreign key </li>
	 * <li> <B>UPDATE_RULE</B> short => What happens to foreign key when
	 * primary is updated:
	 * <UL>
	 * <li> importedKeyCascade - change imported key to agree with primary key
	 * update </li>
	 * <li> importedKeyRestrict - do not allow update of primary key if it has
	 * been imported </li>
	 * <li> importedKeySetNull - change imported key to NULL if its primary key
	 * has been updated </li>
	 * </ul>
	 * </li>
	 * <li> <B>DELETE_RULE</B> short => What happens to the foreign key when
	 * primary is deleted.
	 * <UL>
	 * <li> importedKeyCascade - delete rows that import a deleted key </li>
	 * <li> importedKeyRestrict - do not allow delete of primary key if it has
	 * been imported </li>
	 * <li> importedKeySetNull - change imported key to NULL if its primary key
	 * has been deleted </li>
	 * </ul>
	 * </li>
	 * <li> <B>FK_NAME</B> String => foreign key identifier (may be null) </li>
	 * <li> <B>PK_NAME</B> String => primary key identifier (may be null) </li>
	 * </ol>
	 * </p>
	 * 
	 * @param primaryCatalog
	 *            a catalog name; "" retrieves those without a catalog
	 * @param primarySchema
	 *            a schema name pattern; "" retrieves those without a schema
	 * @param primaryTable
	 *            a table name
	 * @param foreignCatalog
	 *            a catalog name; "" retrieves those without a catalog
	 * @param foreignSchema
	 *            a schema name pattern; "" retrieves those without a schema
	 * @param foreignTable
	 *            a table name
	 * @return ResultSet each row is a foreign key column description
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	public java.sql.ResultSet getCrossReference(final String primaryCatalog,
			final String primarySchema, final String primaryTable,
			final String foreignCatalog, final String foreignSchema,
			final String foreignTable) throws SQLException {
		if (primaryTable == null) {
			throw SQLError.createSQLException("Table not specified.",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
		}

		Field[] fields = createFkMetadataFields();

		final ArrayList tuples = new ArrayList();

		if (this.conn.versionMeetsMinimum(3, 23, 0)) {

			final Statement stmt = this.conn.getMetadataSafeStatement();

			try {

				new IterateBlock(getCatalogIterator(foreignCatalog)) {
					void forEach(Object catalogStr) throws SQLException {

						ResultSet fkresults = null;

						try {

							/*
							 * Get foreign key information for table
							 */
							if (conn.versionMeetsMinimum(3, 23, 50)) {
								fkresults = extractForeignKeyFromCreateTable(
										catalogStr.toString(), null);
							} else {
								StringBuffer queryBuf = new StringBuffer(
										"SHOW TABLE STATUS FROM ");
								queryBuf.append(quotedId);
								queryBuf.append(catalogStr.toString());
								queryBuf.append(quotedId);

								fkresults = stmt.executeQuery(queryBuf
										.toString());
							}

							String foreignTableWithCase = getTableNameWithCase(foreignTable);
							String primaryTableWithCase = getTableNameWithCase(primaryTable);

							/*
							 * Parse imported foreign key information
							 */

							String dummy;

							while (fkresults.next()) {
								String tableType = fkresults.getString("Type");

								if ((tableType != null)
										&& (tableType
												.equalsIgnoreCase("innodb") || tableType
												.equalsIgnoreCase(SUPPORTS_FK))) {
									String comment = fkresults.getString(
											"Comment").trim();

									if (comment != null) {
										StringTokenizer commentTokens = new StringTokenizer(
												comment, ";", false);

										if (commentTokens.hasMoreTokens()) {
											dummy = commentTokens.nextToken();

											// Skip InnoDB comment
										}

										while (commentTokens.hasMoreTokens()) {
											String keys = commentTokens
													.nextToken();
											LocalAndReferencedColumns parsedInfo = parseTableStatusIntoLocalAndReferencedColumns(keys);

											int keySeq = 0;

											Iterator referencingColumns = parsedInfo.localColumnsList
													.iterator();
											Iterator referencedColumns = parsedInfo.referencedColumnsList
													.iterator();

											while (referencingColumns.hasNext()) {
												String referencingColumn = removeQuotedId(referencingColumns
														.next().toString());

												// one tuple for each table
												// between
												// parenthesis
												byte[][] tuple = new byte[14][];
												tuple[4] = ((foreignCatalog == null) ? null
														: s2b(foreignCatalog));
												tuple[5] = ((foreignSchema == null) ? null
														: s2b(foreignSchema));
												dummy = fkresults
														.getString("Name"); // FKTABLE_NAME

												if (dummy
														.compareTo(foreignTableWithCase) != 0) {
													continue;
												}

												tuple[6] = s2b(dummy);

												tuple[7] = s2b(referencingColumn); // FKCOLUMN_NAME
												tuple[0] = ((primaryCatalog == null) ? null
														: s2b(primaryCatalog));
												tuple[1] = ((primarySchema == null) ? null
														: s2b(primarySchema));

												// Skip foreign key if it
												// doesn't refer to
												// the right table
												if (parsedInfo.referencedTable
														.compareTo(primaryTableWithCase) != 0) {
													continue;
												}

												tuple[2] = s2b(parsedInfo.referencedTable); // PKTABLE_NAME
												tuple[3] = s2b(removeQuotedId(referencedColumns
														.next().toString())); // PKCOLUMN_NAME
												tuple[8] = Integer.toString(
														keySeq).getBytes(); // KEY_SEQ

												int[] actions = getForeignKeyActions(keys);

												tuple[9] = Integer.toString(
														actions[1]).getBytes();
												tuple[10] = Integer.toString(
														actions[0]).getBytes();
												tuple[11] = null; // FK_NAME
												tuple[12] = null; // PK_NAME
												tuple[13] = Integer
														.toString(
																java.sql.DatabaseMetaData.importedKeyNotDeferrable)
														.getBytes();
												tuples.add(new ByteArrayRow(tuple, getExceptionInterceptor()));
												keySeq++;
											}
										}
									}
								}
							}

						} finally {
							if (fkresults != null) {
								try {
									fkresults.close();
								} catch (Exception sqlEx) {
									AssertionFailedException
											.shouldNotHappen(sqlEx);
								}

								fkresults = null;
							}
						}
					}
				}.doForAll();
			} finally {
				if (stmt != null) {
					stmt.close();
				}
			}
		}

		java.sql.ResultSet results = buildResultSet(fields, tuples);

		return results;
	}

	protected Field[] createFkMetadataFields() {
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
		fields[13] = new Field("", "DEFERRABILITY", Types.SMALLINT, 2);
		return fields;
	}

	/**
	 * @see DatabaseMetaData#getDatabaseMajorVersion()
	 */
	public int getDatabaseMajorVersion() throws SQLException {
		return this.conn.getServerMajorVersion();
	}

	/**
	 * @see DatabaseMetaData#getDatabaseMinorVersion()
	 */
	public int getDatabaseMinorVersion() throws SQLException {
		return this.conn.getServerMinorVersion();
	}

	/**
	 * What's the name of this database product?
	 * 
	 * @return database product name
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public String getDatabaseProductName() throws SQLException {
		return "MySQL";
	}

	/**
	 * What's the version of this database product?
	 * 
	 * @return database version
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public String getDatabaseProductVersion() throws SQLException {
		return this.conn.getServerVersion();
	}

	/**
	 * What's the database's default transaction isolation level? The values are
	 * defined in java.sql.Connection.
	 * 
	 * @return the default isolation level
	 * @throws SQLException
	 *             if a database access error occurs
	 * @see Connection
	 */
	public int getDefaultTransactionIsolation() throws SQLException {
		if (this.conn.supportsIsolationLevel()) {
			return java.sql.Connection.TRANSACTION_READ_COMMITTED;
		}

		return java.sql.Connection.TRANSACTION_NONE;
	}

	/**
	 * What's this JDBC driver's major version number?
	 * 
	 * @return JDBC driver major version
	 */
	public int getDriverMajorVersion() {
		return NonRegisteringDriver.getMajorVersionInternal();
	}

	/**
	 * What's this JDBC driver's minor version number?
	 * 
	 * @return JDBC driver minor version number
	 */
	public int getDriverMinorVersion() {
		return NonRegisteringDriver.getMinorVersionInternal();
	}

	/**
	 * What's the name of this JDBC driver?
	 * 
	 * @return JDBC driver name
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public String getDriverName() throws SQLException {
		return "MySQL-AB JDBC Driver";
	}

	/**
	 * What's the version of this JDBC driver?
	 * 
	 * @return JDBC driver version
	 * @throws java.sql.SQLException
	 *             DOCUMENT ME!
	 */
	public String getDriverVersion() throws java.sql.SQLException {
		return "@MYSQL_CJ_FULL_PROD_NAME@ ( Revision: @MYSQL_CJ_REVISION@ )";
	}

	/**
	 * Get a description of a foreign key columns that reference a table's
	 * primary key columns (the foreign keys exported by a table). They are
	 * ordered by FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, and KEY_SEQ.
	 * <P>
	 * Each foreign key column description has the following columns:
	 * <OL>
	 * <li> <B>PKTABLE_CAT</B> String => primary key table catalog (may be
	 * null) </li>
	 * <li> <B>PKTABLE_SCHEM</B> String => primary key table schema (may be
	 * null) </li>
	 * <li> <B>PKTABLE_NAME</B> String => primary key table name </li>
	 * <li> <B>PKCOLUMN_NAME</B> String => primary key column name </li>
	 * <li> <B>FKTABLE_CAT</B> String => foreign key table catalog (may be
	 * null) being exported (may be null) </li>
	 * <li> <B>FKTABLE_SCHEM</B> String => foreign key table schema (may be
	 * null) being exported (may be null) </li>
	 * <li> <B>FKTABLE_NAME</B> String => foreign key table name being exported
	 * </li>
	 * <li> <B>FKCOLUMN_NAME</B> String => foreign key column name being
	 * exported </li>
	 * <li> <B>KEY_SEQ</B> short => sequence number within foreign key </li>
	 * <li> <B>UPDATE_RULE</B> short => What happens to foreign key when
	 * primary is updated:
	 * <UL>
	 * <li> importedKeyCascade - change imported key to agree with primary key
	 * update </li>
	 * <li> importedKeyRestrict - do not allow update of primary key if it has
	 * been imported </li>
	 * <li> importedKeySetNull - change imported key to NULL if its primary key
	 * has been updated </li>
	 * </ul>
	 * </li>
	 * <li> <B>DELETE_RULE</B> short => What happens to the foreign key when
	 * primary is deleted.
	 * <UL>
	 * <li> importedKeyCascade - delete rows that import a deleted key </li>
	 * <li> importedKeyRestrict - do not allow delete of primary key if it has
	 * been imported </li>
	 * <li> importedKeySetNull - change imported key to NULL if its primary key
	 * has been deleted </li>
	 * </ul>
	 * </li>
	 * <li> <B>FK_NAME</B> String => foreign key identifier (may be null) </li>
	 * <li> <B>PK_NAME</B> String => primary key identifier (may be null) </li>
	 * </ol>
	 * </p>
	 * 
	 * @param catalog
	 *            a catalog name; "" retrieves those without a catalog
	 * @param schema
	 *            a schema name pattern; "" retrieves those without a schema
	 * @param table
	 *            a table name
	 * @return ResultSet each row is a foreign key column description
	 * @throws SQLException
	 *             if a database access error occurs
	 * @see #getImportedKeys
	 */
	public java.sql.ResultSet getExportedKeys(String catalog, String schema,
			final String table) throws SQLException {
		if (table == null) {
			throw SQLError.createSQLException("Table not specified.",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
		}

		Field[] fields = createFkMetadataFields();

		final ArrayList rows = new ArrayList();

		if (this.conn.versionMeetsMinimum(3, 23, 0)) {

			final Statement stmt = this.conn.getMetadataSafeStatement();

			try {

				new IterateBlock(getCatalogIterator(catalog)) {
					void forEach(Object catalogStr) throws SQLException {
						ResultSet fkresults = null;

						try {

							/*
							 * Get foreign key information for table
							 */
							if (conn.versionMeetsMinimum(3, 23, 50)) {
								// we can use 'SHOW CREATE TABLE'

								fkresults = extractForeignKeyFromCreateTable(
										catalogStr.toString(), null);
							} else {
								StringBuffer queryBuf = new StringBuffer(
										"SHOW TABLE STATUS FROM ");
								queryBuf.append(quotedId);
								queryBuf.append(catalogStr.toString());
								queryBuf.append(quotedId);

								fkresults = stmt.executeQuery(queryBuf
										.toString());
							}

							// lower-case table name might be turned on
							String tableNameWithCase = getTableNameWithCase(table);

							/*
							 * Parse imported foreign key information
							 */

							while (fkresults.next()) {
								String tableType = fkresults.getString("Type");

								if ((tableType != null)
										&& (tableType
												.equalsIgnoreCase("innodb") || tableType
												.equalsIgnoreCase(SUPPORTS_FK))) {
									String comment = fkresults.getString(
											"Comment").trim();

									if (comment != null) {
										StringTokenizer commentTokens = new StringTokenizer(
												comment, ";", false);

										if (commentTokens.hasMoreTokens()) {
											commentTokens.nextToken(); // Skip
											// InnoDB
											// comment

											while (commentTokens
													.hasMoreTokens()) {
												String keys = commentTokens
														.nextToken();
												getExportKeyResults(
														catalogStr.toString(),
														tableNameWithCase,
														keys,
														rows,
														fkresults
																.getString("Name"));
											}
										}
									}
								}
							}

						} finally {
							if (fkresults != null) {
								try {
									fkresults.close();
								} catch (SQLException sqlEx) {
									AssertionFailedException
											.shouldNotHappen(sqlEx);
								}

								fkresults = null;
							}
						}
					}
				}.doForAll();
			} finally {
				if (stmt != null) {
					stmt.close();
				}
			}
		}

		java.sql.ResultSet results = buildResultSet(fields, rows);

		return results;
	}

	/**
	 * Adds to the tuples list the exported keys of exportingTable based on the
	 * keysComment from the 'show table status' sql command. KeysComment is that
	 * part of the comment field that follows the "InnoDB free ...;" prefix.
	 * 
	 * @param catalog
	 *            the database to use
	 * @param exportingTable
	 *            the table keys are being exported from
	 * @param keysComment
	 *            the comment from 'show table status'
	 * @param tuples
	 *            the rows to add results to
	 * @param fkTableName
	 *            the foreign key table name
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	private void getExportKeyResults(String catalog, String exportingTable,
			String keysComment, List tuples, String fkTableName)
			throws SQLException {
		getResultsImpl(catalog, exportingTable, keysComment, tuples,
				fkTableName, true);
	}

	/**
	 * Get all the "extra" characters that can be used in unquoted identifier
	 * names (those beyond a-z, 0-9 and _).
	 * 
	 * @return the string containing the extra characters
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public String getExtraNameCharacters() throws SQLException {
		return "#@";
	}

	/**
	 * Returns the DELETE and UPDATE foreign key actions from the given 'SHOW
	 * TABLE STATUS' string, with the DELETE action being the first item in the
	 * array, and the UPDATE action being the second.
	 * 
	 * @param commentString
	 *            the comment from 'SHOW TABLE STATUS'
	 * @return int[] [0] = delete action, [1] = update action
	 */
	private int[] getForeignKeyActions(String commentString) {
		int[] actions = new int[] {
				java.sql.DatabaseMetaData.importedKeyNoAction,
				java.sql.DatabaseMetaData.importedKeyNoAction };

		int lastParenIndex = commentString.lastIndexOf(")");

		if (lastParenIndex != (commentString.length() - 1)) {
			String cascadeOptions = commentString.substring(lastParenIndex + 1)
					.trim().toUpperCase(Locale.ENGLISH);

			actions[0] = getCascadeDeleteOption(cascadeOptions);
			actions[1] = getCascadeUpdateOption(cascadeOptions);
		}

		return actions;
	}

	/**
	 * What's the string used to quote SQL identifiers? This returns a space " "
	 * if identifier quoting isn't supported. A JDBC compliant driver always
	 * uses a double quote character.
	 * 
	 * @return the quoting string
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public String getIdentifierQuoteString() throws SQLException {
		if (this.conn.supportsQuotedIdentifiers()) {
			if (!this.conn.useAnsiQuotedIdentifiers()) {
				return "`";
			}

			return "\"";
		}

		return " ";
	}

	/**
	 * Get a description of the primary key columns that are referenced by a
	 * table's foreign key columns (the primary keys imported by a table). They
	 * are ordered by PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, and KEY_SEQ.
	 * <P>
	 * Each primary key column description has the following columns:
	 * <OL>
	 * <li> <B>PKTABLE_CAT</B> String => primary key table catalog being
	 * imported (may be null) </li>
	 * <li> <B>PKTABLE_SCHEM</B> String => primary key table schema being
	 * imported (may be null) </li>
	 * <li> <B>PKTABLE_NAME</B> String => primary key table name being imported
	 * </li>
	 * <li> <B>PKCOLUMN_NAME</B> String => primary key column name being
	 * imported </li>
	 * <li> <B>FKTABLE_CAT</B> String => foreign key table catalog (may be
	 * null) </li>
	 * <li> <B>FKTABLE_SCHEM</B> String => foreign key table schema (may be
	 * null) </li>
	 * <li> <B>FKTABLE_NAME</B> String => foreign key table name </li>
	 * <li> <B>FKCOLUMN_NAME</B> String => foreign key column name </li>
	 * <li> <B>KEY_SEQ</B> short => sequence number within foreign key </li>
	 * <li> <B>UPDATE_RULE</B> short => What happens to foreign key when
	 * primary is updated:
	 * <UL>
	 * <li> importedKeyCascade - change imported key to agree with primary key
	 * update </li>
	 * <li> importedKeyRestrict - do not allow update of primary key if it has
	 * been imported </li>
	 * <li> importedKeySetNull - change imported key to NULL if its primary key
	 * has been updated </li>
	 * </ul>
	 * </li>
	 * <li> <B>DELETE_RULE</B> short => What happens to the foreign key when
	 * primary is deleted.
	 * <UL>
	 * <li> importedKeyCascade - delete rows that import a deleted key </li>
	 * <li> importedKeyRestrict - do not allow delete of primary key if it has
	 * been imported </li>
	 * <li> importedKeySetNull - change imported key to NULL if its primary key
	 * has been deleted </li>
	 * </ul>
	 * </li>
	 * <li> <B>FK_NAME</B> String => foreign key name (may be null) </li>
	 * <li> <B>PK_NAME</B> String => primary key name (may be null) </li>
	 * </ol>
	 * </p>
	 * 
	 * @param catalog
	 *            a catalog name; "" retrieves those without a catalog
	 * @param schema
	 *            a schema name pattern; "" retrieves those without a schema
	 * @param table
	 *            a table name
	 * @return ResultSet each row is a primary key column description
	 * @throws SQLException
	 *             if a database access error occurs
	 * @see #getExportedKeys
	 */
	public java.sql.ResultSet getImportedKeys(String catalog, String schema,
			final String table) throws SQLException {
		if (table == null) {
			throw SQLError.createSQLException("Table not specified.",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
		}

		Field[] fields = createFkMetadataFields();
		
		final ArrayList rows = new ArrayList();

		if (this.conn.versionMeetsMinimum(3, 23, 0)) {

			final Statement stmt = this.conn.getMetadataSafeStatement();

			try {

				new IterateBlock(getCatalogIterator(catalog)) {
					void forEach(Object catalogStr) throws SQLException {
						ResultSet fkresults = null;

						try {

							/*
							 * Get foreign key information for table
							 */
							if (conn.versionMeetsMinimum(3, 23, 50)) {
								// we can use 'SHOW CREATE TABLE'

								fkresults = extractForeignKeyFromCreateTable(
										catalogStr.toString(), table);
							} else {
								StringBuffer queryBuf = new StringBuffer(
										"SHOW TABLE STATUS ");
								queryBuf.append(" FROM ");
								queryBuf.append(quotedId);
								queryBuf.append(catalogStr.toString());
								queryBuf.append(quotedId);
								queryBuf.append(" LIKE '");
								queryBuf.append(table);
								queryBuf.append("'");

								fkresults = stmt.executeQuery(queryBuf
										.toString());
							}

							/*
							 * Parse imported foreign key information
							 */

							while (fkresults.next()) {
								String tableType = fkresults.getString("Type");

								if ((tableType != null)
										&& (tableType
												.equalsIgnoreCase("innodb") || tableType
												.equalsIgnoreCase(SUPPORTS_FK))) {
									String comment = fkresults.getString(
											"Comment").trim();

									if (comment != null) {
										StringTokenizer commentTokens = new StringTokenizer(
												comment, ";", false);

										if (commentTokens.hasMoreTokens()) {
											commentTokens.nextToken(); // Skip
											// InnoDB
											// comment

											while (commentTokens
													.hasMoreTokens()) {
												String keys = commentTokens
														.nextToken();
												getImportKeyResults(catalogStr
														.toString(), table,
														keys, rows);
											}
										}
									}
								}
							}
						} finally {
							if (fkresults != null) {
								try {
									fkresults.close();
								} catch (SQLException sqlEx) {
									AssertionFailedException
											.shouldNotHappen(sqlEx);
								}

								fkresults = null;
							}
						}
					}
				}.doForAll();
			} finally {
				if (stmt != null) {
					stmt.close();
				}
			}
		}

		java.sql.ResultSet results = buildResultSet(fields, rows);

		return results;
	}

	/**
	 * Populates the tuples list with the imported keys of importingTable based
	 * on the keysComment from the 'show table status' sql command. KeysComment
	 * is that part of the comment field that follows the "InnoDB free ...;"
	 * prefix.
	 * 
	 * @param catalog
	 *            the database to use
	 * @param importingTable
	 *            the table keys are being imported to
	 * @param keysComment
	 *            the comment from 'show table status'
	 * @param tuples
	 *            the rows to add results to
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	private void getImportKeyResults(String catalog, String importingTable,
			String keysComment, List tuples) throws SQLException {
		getResultsImpl(catalog, importingTable, keysComment, tuples, null,
				false);
	}

	/**
	 * Get a description of a table's indices and statistics. They are ordered
	 * by NON_UNIQUE, TYPE, INDEX_NAME, and ORDINAL_POSITION.
	 * <P>
	 * Each index column description has the following columns:
	 * <OL>
	 * <li> <B>TABLE_CAT</B> String => table catalog (may be null) </li>
	 * <li> <B>TABLE_SCHEM</B> String => table schema (may be null) </li>
	 * <li> <B>TABLE_NAME</B> String => table name </li>
	 * <li> <B>NON_UNIQUE</B> boolean => Can index values be non-unique? false
	 * when TYPE is tableIndexStatistic </li>
	 * <li> <B>INDEX_QUALIFIER</B> String => index catalog (may be null); null
	 * when TYPE is tableIndexStatistic </li>
	 * <li> <B>INDEX_NAME</B> String => index name; null when TYPE is
	 * tableIndexStatistic </li>
	 * <li> <B>TYPE</B> short => index type:
	 * <UL>
	 * <li> tableIndexStatistic - this identifies table statistics that are
	 * returned in conjuction with a table's index descriptions </li>
	 * <li> tableIndexClustered - this is a clustered index </li>
	 * <li> tableIndexHashed - this is a hashed index </li>
	 * <li> tableIndexOther - this is some other style of index </li>
	 * </ul>
	 * </li>
	 * <li> <B>ORDINAL_POSITION</B> short => column sequence number within
	 * index; zero when TYPE is tableIndexStatistic </li>
	 * <li> <B>COLUMN_NAME</B> String => column name; null when TYPE is
	 * tableIndexStatistic </li>
	 * <li> <B>ASC_OR_DESC</B> String => column sort sequence, "A" =>
	 * ascending, "D" => descending, may be null if sort sequence is not
	 * supported; null when TYPE is tableIndexStatistic </li>
	 * <li> <B>CARDINALITY</B> int => When TYPE is tableIndexStatisic then this
	 * is the number of rows in the table; otherwise it is the number of unique
	 * values in the index. </li>
	 * <li> <B>PAGES</B> int => When TYPE is tableIndexStatisic then this is
	 * the number of pages used for the table, otherwise it is the number of
	 * pages used for the current index. </li>
	 * <li> <B>FILTER_CONDITION</B> String => Filter condition, if any. (may be
	 * null) </li>
	 * </ol>
	 * </p>
	 * 
	 * @param catalog
	 *            a catalog name; "" retrieves those without a catalog
	 * @param schema
	 *            a schema name pattern; "" retrieves those without a schema
	 * @param table
	 *            a table name
	 * @param unique
	 *            when true, return only indices for unique values; when false,
	 *            return indices regardless of whether unique or not
	 * @param approximate
	 *            when true, result is allowed to reflect approximate or out of
	 *            data values; when false, results are requested to be accurate
	 * @return ResultSet each row is an index column description
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public java.sql.ResultSet getIndexInfo(String catalog, String schema,
			final String table, final boolean unique, boolean approximate)
			throws SQLException {
		/*
		 * MySQL stores index information in the following fields: Table
		 * Non_unique Key_name Seq_in_index Column_name Collation Cardinality
		 * Sub_part
		 */

		Field[] fields = createIndexInfoFields();

		final ArrayList rows = new ArrayList();
		final Statement stmt = this.conn.getMetadataSafeStatement();

		try {

			new IterateBlock(getCatalogIterator(catalog)) {
				void forEach(Object catalogStr) throws SQLException {

					ResultSet results = null;

					try {
						StringBuffer queryBuf = new StringBuffer(
								"SHOW INDEX FROM ");
						queryBuf.append(quotedId);
						queryBuf.append(table);
						queryBuf.append(quotedId);
						queryBuf.append(" FROM ");
						queryBuf.append(quotedId);
						queryBuf.append(catalogStr.toString());
						queryBuf.append(quotedId);

						try {
							results = stmt.executeQuery(queryBuf.toString());
						} catch (SQLException sqlEx) {
							int errorCode = sqlEx.getErrorCode();

							// If SQLState is 42S02, ignore this SQLException
							// it means the table doesn't exist....
							if (!"42S02".equals(sqlEx.getSQLState())) {
								// Sometimes not mapped correctly for pre-4.1
								// so use error code instead.
								if (errorCode != MysqlErrorNumbers.ER_NO_SUCH_TABLE) {
									throw sqlEx;
								}
							}
						}

						while (results != null && results.next()) {
							byte[][] row = new byte[14][];
							row[0] = ((catalogStr.toString() == null) ? new byte[0]
									: s2b(catalogStr.toString()));
							;
							row[1] = null;
							row[2] = results.getBytes("Table");

							boolean indexIsUnique = results
									.getInt("Non_unique") == 0;

							row[3] = (!indexIsUnique ? s2b("true")
									: s2b("false"));
							row[4] = new byte[0];
							row[5] = results.getBytes("Key_name");
							row[6] = Integer.toString(
									java.sql.DatabaseMetaData.tableIndexOther)
									.getBytes();
							row[7] = results.getBytes("Seq_in_index");
							row[8] = results.getBytes("Column_name");
							row[9] = results.getBytes("Collation");
							row[10] = results.getBytes("Cardinality");
							row[11] = s2b("0");
							row[12] = null;

							if (unique) {
								if (indexIsUnique) {
									rows.add(new ByteArrayRow(row, getExceptionInterceptor()));
								}
							} else {
								// All rows match
								rows.add(new ByteArrayRow(row, getExceptionInterceptor()));
							}
						}
					} finally {
						if (results != null) {
							try {
								results.close();
							} catch (Exception ex) {
								;
							}

							results = null;
						}
					}
				}
			}.doForAll();

			java.sql.ResultSet indexInfo = buildResultSet(fields, rows);

			return indexInfo;
		} finally {
			if (stmt != null) {
				stmt.close();
			}
		}
	}

	protected Field[] createIndexInfoFields() {
		Field[] fields = new Field[13];
		fields[0] = new Field("", "TABLE_CAT", Types.CHAR, 255);
		fields[1] = new Field("", "TABLE_SCHEM", Types.CHAR, 0);
		fields[2] = new Field("", "TABLE_NAME", Types.CHAR, 255);
		fields[3] = new Field("", "NON_UNIQUE", Types.BOOLEAN, 4);
		fields[4] = new Field("", "INDEX_QUALIFIER", Types.CHAR, 1);
		fields[5] = new Field("", "INDEX_NAME", Types.CHAR, 32);
		fields[6] = new Field("", "TYPE", Types.SMALLINT, 32);
		fields[7] = new Field("", "ORDINAL_POSITION", Types.SMALLINT, 5);
		fields[8] = new Field("", "COLUMN_NAME", Types.CHAR, 32);
		fields[9] = new Field("", "ASC_OR_DESC", Types.CHAR, 1);
		fields[10] = new Field("", "CARDINALITY", Types.INTEGER, 10);
		fields[11] = new Field("", "PAGES", Types.INTEGER, 10);
		fields[12] = new Field("", "FILTER_CONDITION", Types.CHAR, 32);
		return fields;
	}

	/**
	 * @see DatabaseMetaData#getJDBCMajorVersion()
	 */
	public int getJDBCMajorVersion() throws SQLException {
		return 3;
	}

	/**
	 * @see DatabaseMetaData#getJDBCMinorVersion()
	 */
	public int getJDBCMinorVersion() throws SQLException {
		return 0;
	}

	/**
	 * How many hex characters can you have in an inline binary literal?
	 * 
	 * @return max literal length
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public int getMaxBinaryLiteralLength() throws SQLException {
		return 16777208;
	}

	/**
	 * What's the maximum length of a catalog name?
	 * 
	 * @return max name length in bytes
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public int getMaxCatalogNameLength() throws SQLException {
		return 32;
	}

	/**
	 * What's the max length for a character literal?
	 * 
	 * @return max literal length
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public int getMaxCharLiteralLength() throws SQLException {
		return 16777208;
	}

	/**
	 * What's the limit on column name length?
	 * 
	 * @return max literal length
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public int getMaxColumnNameLength() throws SQLException {
		return 64;
	}

	/**
	 * What's the maximum number of columns in a "GROUP BY" clause?
	 * 
	 * @return max number of columns
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public int getMaxColumnsInGroupBy() throws SQLException {
		return 64;
	}

	/**
	 * What's the maximum number of columns allowed in an index?
	 * 
	 * @return max columns
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public int getMaxColumnsInIndex() throws SQLException {
		return 16;
	}

	/**
	 * What's the maximum number of columns in an "ORDER BY" clause?
	 * 
	 * @return max columns
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public int getMaxColumnsInOrderBy() throws SQLException {
		return 64;
	}

	/**
	 * What's the maximum number of columns in a "SELECT" list?
	 * 
	 * @return max columns
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public int getMaxColumnsInSelect() throws SQLException {
		return 256;
	}

	/**
	 * What's maximum number of columns in a table?
	 * 
	 * @return max columns
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public int getMaxColumnsInTable() throws SQLException {
		return 512;
	}

	/**
	 * How many active connections can we have at a time to this database?
	 * 
	 * @return max connections
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public int getMaxConnections() throws SQLException {
		return 0;
	}

	/**
	 * What's the maximum cursor name length?
	 * 
	 * @return max cursor name length in bytes
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public int getMaxCursorNameLength() throws SQLException {
		return 64;
	}

	/**
	 * What's the maximum length of an index (in bytes)?
	 * 
	 * @return max index length in bytes
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public int getMaxIndexLength() throws SQLException {
		return 256;
	}

	/**
	 * What's the maximum length of a procedure name?
	 * 
	 * @return max name length in bytes
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public int getMaxProcedureNameLength() throws SQLException {
		return 0;
	}

	/**
	 * What's the maximum length of a single row?
	 * 
	 * @return max row size in bytes
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public int getMaxRowSize() throws SQLException {
		return Integer.MAX_VALUE - 8; // Max buffer size - HEADER
	}

	/**
	 * What's the maximum length allowed for a schema name?
	 * 
	 * @return max name length in bytes
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public int getMaxSchemaNameLength() throws SQLException {
		return 0;
	}

	/**
	 * What's the maximum length of a SQL statement?
	 * 
	 * @return max length in bytes
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public int getMaxStatementLength() throws SQLException {
		return MysqlIO.getMaxBuf() - 4; // Max buffer - header
	}

	/**
	 * How many active statements can we have open at one time to this database?
	 * 
	 * @return the maximum
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public int getMaxStatements() throws SQLException {
		return 0;
	}

	/**
	 * What's the maximum length of a table name?
	 * 
	 * @return max name length in bytes
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public int getMaxTableNameLength() throws SQLException {
		return 64;
	}

	/**
	 * What's the maximum number of tables in a SELECT?
	 * 
	 * @return the maximum
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public int getMaxTablesInSelect() throws SQLException {
		return 256;
	}

	/**
	 * What's the maximum length of a user name?
	 * 
	 * @return max name length in bytes
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public int getMaxUserNameLength() throws SQLException {
		return 16;
	}

	/**
	 * Get a comma separated list of math functions.
	 * 
	 * @return the list
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public String getNumericFunctions() throws SQLException {
		return "ABS,ACOS,ASIN,ATAN,ATAN2,BIT_COUNT,CEILING,COS,"
				+ "COT,DEGREES,EXP,FLOOR,LOG,LOG10,MAX,MIN,MOD,PI,POW,"
				+ "POWER,RADIANS,RAND,ROUND,SIN,SQRT,TAN,TRUNCATE";
	}

	/**
	 * Get a description of a table's primary key columns. They are ordered by
	 * COLUMN_NAME.
	 * <P>
	 * Each column description has the following columns:
	 * <OL>
	 * <li> <B>TABLE_CAT</B> String => table catalog (may be null) </li>
	 * <li> <B>TABLE_SCHEM</B> String => table schema (may be null) </li>
	 * <li> <B>TABLE_NAME</B> String => table name </li>
	 * <li> <B>COLUMN_NAME</B> String => column name </li>
	 * <li> <B>KEY_SEQ</B> short => sequence number within primary key </li>
	 * <li> <B>PK_NAME</B> String => primary key name (may be null) </li>
	 * </ol>
	 * </p>
	 * 
	 * @param catalog
	 *            a catalog name; "" retrieves those without a catalog
	 * @param schema
	 *            a schema name pattern; "" retrieves those without a schema
	 * @param table
	 *            a table name
	 * @return ResultSet each row is a primary key column description
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public java.sql.ResultSet getPrimaryKeys(String catalog, String schema,
			final String table) throws SQLException {
		Field[] fields = new Field[6];
		fields[0] = new Field("", "TABLE_CAT", Types.CHAR, 255);
		fields[1] = new Field("", "TABLE_SCHEM", Types.CHAR, 0);
		fields[2] = new Field("", "TABLE_NAME", Types.CHAR, 255);
		fields[3] = new Field("", "COLUMN_NAME", Types.CHAR, 32);
		fields[4] = new Field("", "KEY_SEQ", Types.SMALLINT, 5);
		fields[5] = new Field("", "PK_NAME", Types.CHAR, 32);

		if (table == null) {
			throw SQLError.createSQLException("Table not specified.",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
		}

		final ArrayList rows = new ArrayList();
		final Statement stmt = this.conn.getMetadataSafeStatement();

		try {

			new IterateBlock(getCatalogIterator(catalog)) {
				void forEach(Object catalogStr) throws SQLException {
					ResultSet rs = null;

					try {

						StringBuffer queryBuf = new StringBuffer(
								"SHOW KEYS FROM ");
						queryBuf.append(quotedId);
						queryBuf.append(table);
						queryBuf.append(quotedId);
						queryBuf.append(" FROM ");
						queryBuf.append(quotedId);
						queryBuf.append(catalogStr.toString());
						queryBuf.append(quotedId);

						rs = stmt.executeQuery(queryBuf.toString());

						TreeMap sortMap = new TreeMap();

						while (rs.next()) {
							String keyType = rs.getString("Key_name");

							if (keyType != null) {
								if (keyType.equalsIgnoreCase("PRIMARY")
										|| keyType.equalsIgnoreCase("PRI")) {
									byte[][] tuple = new byte[6][];
									tuple[0] = ((catalogStr.toString() == null) ? new byte[0]
											: s2b(catalogStr.toString()));
									tuple[1] = null;
									tuple[2] = s2b(table);

									String columnName = rs
											.getString("Column_name");
									tuple[3] = s2b(columnName);
									tuple[4] = s2b(rs.getString("Seq_in_index"));
									tuple[5] = s2b(keyType);
									sortMap.put(columnName, tuple);
								}
							}
						}

						// Now pull out in column name sorted order
						Iterator sortedIterator = sortMap.values().iterator();

						while (sortedIterator.hasNext()) {
							rows.add(new ByteArrayRow((byte[][])sortedIterator.next(), getExceptionInterceptor()));
						}

					} finally {
						if (rs != null) {
							try {
								rs.close();
							} catch (Exception ex) {
								;
							}

							rs = null;
						}
					}
				}
			}.doForAll();
		} finally {
			if (stmt != null) {
				stmt.close();
			}
		}

		java.sql.ResultSet results = buildResultSet(fields, rows);

		return results;
	}

	/**
	 * Get a description of a catalog's stored procedure parameters and result
	 * columns.
	 * <P>
	 * Only descriptions matching the schema, procedure and parameter name
	 * criteria are returned. They are ordered by PROCEDURE_SCHEM and
	 * PROCEDURE_NAME. Within this, the return value, if any, is first. Next are
	 * the parameter descriptions in call order. The column descriptions follow
	 * in column number order.
	 * </p>
	 * <P>
	 * Each row in the ResultSet is a parameter desription or column description
	 * with the following fields:
	 * <OL>
	 * <li> <B>PROCEDURE_CAT</B> String => procedure catalog (may be null)
	 * </li>
	 * <li> <B>PROCEDURE_SCHEM</B> String => procedure schema (may be null)
	 * </li>
	 * <li> <B>PROCEDURE_NAME</B> String => procedure name </li>
	 * <li> <B>COLUMN_NAME</B> String => column/parameter name </li>
	 * <li> <B>COLUMN_TYPE</B> Short => kind of column/parameter:
	 * <UL>
	 * <li> procedureColumnUnknown - nobody knows </li>
	 * <li> procedureColumnIn - IN parameter </li>
	 * <li> procedureColumnInOut - INOUT parameter </li>
	 * <li> procedureColumnOut - OUT parameter </li>
	 * <li> procedureColumnReturn - procedure return value </li>
	 * <li> procedureColumnResult - result column in ResultSet </li>
	 * </ul>
	 * </li>
	 * <li> <B>DATA_TYPE</B> short => SQL type from java.sql.Types </li>
	 * <li> <B>TYPE_NAME</B> String => SQL type name </li>
	 * <li> <B>PRECISION</B> int => precision </li>
	 * <li> <B>LENGTH</B> int => length in bytes of data </li>
	 * <li> <B>SCALE</B> short => scale </li>
	 * <li> <B>RADIX</B> short => radix </li>
	 * <li> <B>NULLABLE</B> short => can it contain NULL?
	 * <UL>
	 * <li> procedureNoNulls - does not allow NULL values </li>
	 * <li> procedureNullable - allows NULL values </li>
	 * <li> procedureNullableUnknown - nullability unknown </li>
	 * </ul>
	 * </li>
	 * <li> <B>REMARKS</B> String => comment describing parameter/column </li>
	 * </ol>
	 * </p>
	 * <P>
	 * <B>Note:</B> Some databases may not return the column descriptions for a
	 * procedure. Additional columns beyond REMARKS can be defined by the
	 * database.
	 * </p>
	 * 
	 * @param catalog
	 *            a catalog name; "" retrieves those without a catalog
	 * @param schemaPattern
	 *            a schema name pattern; "" retrieves those without a schema
	 * @param procedureNamePattern
	 *            a procedure name pattern
	 * @param columnNamePattern
	 *            a column name pattern
	 * @return ResultSet each row is a stored procedure parameter or column
	 *         description
	 * @throws SQLException
	 *             if a database access error occurs
	 * @see #getSearchStringEscape
	 */
	public java.sql.ResultSet getProcedureColumns(String catalog,
			String schemaPattern, String procedureNamePattern,
			String columnNamePattern) throws SQLException {
		Field[] fields = createProcedureColumnsFields();
		
		return getProcedureOrFunctionColumns(
				fields, catalog, schemaPattern,
				procedureNamePattern, columnNamePattern,
				true, true);
	}

	protected Field[] createProcedureColumnsFields() {
		Field[] fields = new Field[13];

		fields[0] = new Field("", "PROCEDURE_CAT", Types.CHAR, 0);
		fields[1] = new Field("", "PROCEDURE_SCHEM", Types.CHAR, 0);
		fields[2] = new Field("", "PROCEDURE_NAME", Types.CHAR, 0);
		fields[3] = new Field("", "COLUMN_NAME", Types.CHAR, 0);
		fields[4] = new Field("", "COLUMN_TYPE", Types.CHAR, 0);
		fields[5] = new Field("", "DATA_TYPE", Types.SMALLINT, 0);
		fields[6] = new Field("", "TYPE_NAME", Types.CHAR, 0);
		fields[7] = new Field("", "PRECISION", Types.INTEGER, 0);
		fields[8] = new Field("", "LENGTH", Types.INTEGER, 0);
		fields[9] = new Field("", "SCALE", Types.SMALLINT, 0);
		fields[10] = new Field("", "RADIX", Types.SMALLINT, 0);
		fields[11] = new Field("", "NULLABLE", Types.SMALLINT, 0);
		fields[12] = new Field("", "REMARKS", Types.CHAR, 0);
		return fields;
	}
	
	protected java.sql.ResultSet getProcedureOrFunctionColumns(
			Field[] fields, String catalog, String schemaPattern,
			String procedureOrFunctionNamePattern,
			String columnNamePattern, boolean returnProcedures,
			boolean returnFunctions) throws SQLException {

		List proceduresToExtractList = new ArrayList();

		if (supportsStoredProcedures()) {
			if ((procedureOrFunctionNamePattern.indexOf("%") == -1)
					&& (procedureOrFunctionNamePattern.indexOf("?") == -1)) {
				proceduresToExtractList.add(procedureOrFunctionNamePattern);
			} else {
				
				ResultSet procedureNameRs = null;

				try {

					procedureNameRs = getProceduresAndOrFunctions(
							createFieldMetadataForGetProcedures(),
							catalog, schemaPattern,
							procedureOrFunctionNamePattern, returnProcedures,
							returnFunctions);

					while (procedureNameRs.next()) {
						proceduresToExtractList.add(procedureNameRs
								.getString(3));
					}

					// Required to be sorted in name-order by JDBC spec,
					// in 'normal' case getProcedures takes care of this for us,
					// but if system tables are inaccessible, we need to sort...
					// so just do this to be safe...
					Collections.sort(proceduresToExtractList);
				} finally {
					SQLException rethrowSqlEx = null;

					if (procedureNameRs != null) {
						try {
							procedureNameRs.close();
						} catch (SQLException sqlEx) {
							rethrowSqlEx = sqlEx;
						}
					}
					
					if (rethrowSqlEx != null) {
						throw rethrowSqlEx;
					}
				}
			}
		}

		ArrayList resultRows = new ArrayList();

		for (Iterator iter = proceduresToExtractList.iterator(); iter.hasNext();) {
			String procName = (String) iter.next();

			getCallStmtParameterTypes(catalog, procName, columnNamePattern,
					resultRows, 
					fields.length == 17 /* for getFunctionColumns */);
		}

		return buildResultSet(fields, resultRows);
	}

	/**
	 * Get a description of stored procedures available in a catalog.
	 * <P>
	 * Only procedure descriptions matching the schema and procedure name
	 * criteria are returned. They are ordered by PROCEDURE_SCHEM, and
	 * PROCEDURE_NAME.
	 * </p>
	 * <P>
	 * Each procedure description has the the following columns:
	 * <OL>
	 * <li> <B>PROCEDURE_CAT</B> String => procedure catalog (may be null)
	 * </li>
	 * <li> <B>PROCEDURE_SCHEM</B> String => procedure schema (may be null)
	 * </li>
	 * <li> <B>PROCEDURE_NAME</B> String => procedure name </li>
	 * <li> reserved for future use </li>
	 * <li> reserved for future use </li>
	 * <li> reserved for future use </li>
	 * <li> <B>REMARKS</B> String => explanatory comment on the procedure </li>
	 * <li> <B>PROCEDURE_TYPE</B> short => kind of procedure:
	 * <UL>
	 * <li> procedureResultUnknown - May return a result </li>
	 * <li> procedureNoResult - Does not return a result </li>
	 * <li> procedureReturnsResult - Returns a result </li>
	 * </ul>
	 * </li>
	 * </ol>
	 * </p>
	 * 
	 * @param catalog
	 *            a catalog name; "" retrieves those without a catalog
	 * @param schemaPattern
	 *            a schema name pattern; "" retrieves those without a schema
	 * @param procedureNamePattern
	 *            a procedure name pattern
	 * @return ResultSet each row is a procedure description
	 * @throws SQLException
	 *             if a database access error occurs
	 * @see #getSearchStringEscape
	 */
	public java.sql.ResultSet getProcedures(String catalog,
			String schemaPattern, String procedureNamePattern)
			throws SQLException {
		Field[] fields = createFieldMetadataForGetProcedures();
		
		return getProceduresAndOrFunctions(fields, catalog, schemaPattern,
				procedureNamePattern, true, true);
	}

	private Field[] createFieldMetadataForGetProcedures() {
		Field[] fields = new Field[9];
		fields[0] = new Field("", "PROCEDURE_CAT", Types.CHAR, 255);
		fields[1] = new Field("", "PROCEDURE_SCHEM", Types.CHAR, 255);
		fields[2] = new Field("", "PROCEDURE_NAME", Types.CHAR, 255);
		fields[3] = new Field("", "reserved1", Types.CHAR, 0);
		fields[4] = new Field("", "reserved2", Types.CHAR, 0);
		fields[5] = new Field("", "reserved3", Types.CHAR, 0);
		fields[6] = new Field("", "REMARKS", Types.CHAR, 255);
		fields[7] = new Field("", "PROCEDURE_TYPE", Types.SMALLINT, 6);
		fields[8] = new Field("", "SPECIFIC_NAME", Types.CHAR, 255);
		
		return fields;
	}
	
	protected java.sql.ResultSet getProceduresAndOrFunctions(
			final Field[] fields,
			String catalog,
			String schemaPattern,
			String procedureNamePattern,
			final boolean returnProcedures,
			final boolean returnFunctions) throws SQLException {
		if ((procedureNamePattern == null)
				|| (procedureNamePattern.length() == 0)) {
			if (this.conn.getNullNamePatternMatchesAll()) {
				procedureNamePattern = "%";
			} else {
				throw SQLError.createSQLException(
						"Procedure name pattern can not be NULL or empty.",
						SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
			}
		}

		final ArrayList procedureRows = new ArrayList();

		if (supportsStoredProcedures()) {
			final String procNamePattern = procedureNamePattern;

			final Map procedureRowsOrderedByName = new TreeMap();

			new IterateBlock(getCatalogIterator(catalog)) {
				void forEach(Object catalogStr) throws SQLException {
					String db = catalogStr.toString();

					boolean fromSelect = false;
					ResultSet proceduresRs = null;
					boolean needsClientFiltering = true;
					PreparedStatement proceduresStmt = (PreparedStatement) conn
							.clientPrepareStatement("SELECT name, type, comment FROM mysql.proc WHERE name like ? and db <=> ? ORDER BY name");

					try {
						//
						// Try using system tables first, as this is a little
						// bit more efficient....
						//

						boolean hasTypeColumn = false;

						if (db != null) {
							proceduresStmt.setString(2, db);
						} else {
							proceduresStmt.setNull(2, Types.VARCHAR);
						}

						int nameIndex = 1;

						if (proceduresStmt.getMaxRows() != 0) {
							proceduresStmt.setMaxRows(0);
						}

						proceduresStmt.setString(1, procNamePattern);

						try {
							proceduresRs = proceduresStmt.executeQuery();
							fromSelect = true;
							needsClientFiltering = false;
							hasTypeColumn = true;
						} catch (SQLException sqlEx) {

							//
							// Okay, system tables aren't accessible, so use
							// 'SHOW
							// ....'....
							//
							proceduresStmt.close();

							fromSelect = false;

							if (conn.versionMeetsMinimum(5, 0, 1)) {
								nameIndex = 2;
							} else {
								nameIndex = 1;
							}

							proceduresStmt = (PreparedStatement) conn
									.clientPrepareStatement("SHOW PROCEDURE STATUS LIKE ?");

							if (proceduresStmt.getMaxRows() != 0) {
								proceduresStmt.setMaxRows(0);
							}

							proceduresStmt.setString(1, procNamePattern);

							proceduresRs = proceduresStmt.executeQuery();
						}

						if (returnProcedures) {
							convertToJdbcProcedureList(fromSelect, db,
								proceduresRs, needsClientFiltering, db,
								procedureRowsOrderedByName, nameIndex);
						}

						if (!hasTypeColumn) {
							// need to go after functions too...
							if (proceduresStmt != null) {
								proceduresStmt.close();
							}

							proceduresStmt = (PreparedStatement) conn
									.clientPrepareStatement("SHOW FUNCTION STATUS LIKE ?");

							if (proceduresStmt.getMaxRows() != 0) {
								proceduresStmt.setMaxRows(0);
							}

							proceduresStmt.setString(1, procNamePattern);

							proceduresRs = proceduresStmt.executeQuery();

							if (returnFunctions) {
								convertToJdbcFunctionList(db, proceduresRs,
									needsClientFiltering, db,
									procedureRowsOrderedByName, nameIndex,
									fields);
							}
						}

						// Now, sort them

						Iterator proceduresIter = procedureRowsOrderedByName
								.values().iterator();

						while (proceduresIter.hasNext()) {
							procedureRows.add(proceduresIter.next());
						}
					} finally {
						SQLException rethrowSqlEx = null;

						if (proceduresRs != null) {
							try {
								proceduresRs.close();
							} catch (SQLException sqlEx) {
								rethrowSqlEx = sqlEx;
							}
						}

						if (proceduresStmt != null) {
							try {
								proceduresStmt.close();
							} catch (SQLException sqlEx) {
								rethrowSqlEx = sqlEx;
							}
						}

						if (rethrowSqlEx != null) {
							throw rethrowSqlEx;
						}
					}
				}
			}.doForAll();
		}

		return buildResultSet(fields, procedureRows);
	}


	/**
	 * What's the database vendor's preferred term for "procedure"?
	 * 
	 * @return the vendor term
	 * @throws SQLException
	 *             if an error occurs (don't know why it would in this case...)
	 */
	public String getProcedureTerm() throws SQLException {
		return "PROCEDURE";
	}

	/**
	 * @see DatabaseMetaData#getResultSetHoldability()
	 */
	public int getResultSetHoldability() throws SQLException {
		return ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}

	private void getResultsImpl(String catalog, String table,
			String keysComment, List tuples, String fkTableName,
			boolean isExport) throws SQLException {

		LocalAndReferencedColumns parsedInfo = parseTableStatusIntoLocalAndReferencedColumns(keysComment);

		if (isExport && !parsedInfo.referencedTable.equals(table)) {
			return;
		}

		if (parsedInfo.localColumnsList.size() != parsedInfo.referencedColumnsList
				.size()) {
			throw SQLError.createSQLException(
					"Error parsing foreign keys definition,"
							+ "number of local and referenced columns is not the same.",
					SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
		}

		Iterator localColumnNames = parsedInfo.localColumnsList.iterator();
		Iterator referColumnNames = parsedInfo.referencedColumnsList.iterator();

		int keySeqIndex = 1;

		while (localColumnNames.hasNext()) {
			byte[][] tuple = new byte[14][];
			String lColumnName = removeQuotedId(localColumnNames.next()
					.toString());
			String rColumnName = removeQuotedId(referColumnNames.next()
					.toString());
			tuple[FKTABLE_CAT] = ((catalog == null) ? new byte[0]
					: s2b(catalog));
			tuple[FKTABLE_SCHEM] = null;
			tuple[FKTABLE_NAME] = s2b((isExport) ? fkTableName : table);
			tuple[FKCOLUMN_NAME] = s2b(lColumnName);
			tuple[PKTABLE_CAT] = s2b(parsedInfo.referencedCatalog);
			tuple[PKTABLE_SCHEM] = null;
			tuple[PKTABLE_NAME] = s2b((isExport) ? table
					: parsedInfo.referencedTable);
			tuple[PKCOLUMN_NAME] = s2b(rColumnName);
			tuple[KEY_SEQ] = s2b(Integer.toString(keySeqIndex++));

			int[] actions = getForeignKeyActions(keysComment);

			tuple[UPDATE_RULE] = s2b(Integer.toString(actions[1]));
			tuple[DELETE_RULE] = s2b(Integer.toString(actions[0]));
			tuple[FK_NAME] = s2b(parsedInfo.constraintName);
			tuple[PK_NAME] = null; // not available from show table status
			tuple[DEFERRABILITY] = s2b(Integer
					.toString(java.sql.DatabaseMetaData.importedKeyNotDeferrable));
			tuples.add(new ByteArrayRow(tuple, getExceptionInterceptor()));
		}
	}

	/**
	 * Get the schema names available in this database. The results are ordered
	 * by schema name.
	 * <P>
	 * The schema column is:
	 * <OL>
	 * <li> <B>TABLE_SCHEM</B> String => schema name </li>
	 * </ol>
	 * </p>
	 * 
	 * @return ResultSet each row has a single String column that is a schema
	 *         name
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public java.sql.ResultSet getSchemas() throws SQLException {
		Field[] fields = new Field[2];
	    fields[0] = new Field("", "TABLE_SCHEM", java.sql.Types.CHAR, 0);
	    fields[1] = new Field("", "TABLE_CATALOG", java.sql.Types.CHAR, 0);

		ArrayList tuples = new ArrayList();
		java.sql.ResultSet results = buildResultSet(fields, tuples);

		return results;
	}

	/**
	 * What's the database vendor's preferred term for "schema"?
	 * 
	 * @return the vendor term
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public String getSchemaTerm() throws SQLException {
		return "";
	}

	/**
	 * This is the string that can be used to escape '_' or '%' in the string
	 * pattern style catalog search parameters.
	 * <P>
	 * The '_' character represents any single character.
	 * </p>
	 * <P>
	 * The '%' character represents any sequence of zero or more characters.
	 * </p>
	 * 
	 * @return the string used to escape wildcard characters
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public String getSearchStringEscape() throws SQLException {
		return "\\";
	}

	/**
	 * Get a comma separated list of all a database's SQL keywords that are NOT
	 * also SQL92 keywords.
	 * 
	 * @return the list
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public String getSQLKeywords() throws SQLException {
		return mysqlKeywordsThatArentSQL92;
	}

	/**
	 * @see DatabaseMetaData#getSQLStateType()
	 */
	public int getSQLStateType() throws SQLException {
		if (this.conn.versionMeetsMinimum(4, 1, 0)) {
			return DatabaseMetaData.sqlStateSQL99;
		}

		if (this.conn.getUseSqlStateCodes()) {
			return DatabaseMetaData.sqlStateSQL99;
		}

		return DatabaseMetaData.sqlStateXOpen;
	}

	/**
	 * Get a comma separated list of string functions.
	 * 
	 * @return the list
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public String getStringFunctions() throws SQLException {
		return "ASCII,BIN,BIT_LENGTH,CHAR,CHARACTER_LENGTH,CHAR_LENGTH,CONCAT,"
				+ "CONCAT_WS,CONV,ELT,EXPORT_SET,FIELD,FIND_IN_SET,HEX,INSERT,"
				+ "INSTR,LCASE,LEFT,LENGTH,LOAD_FILE,LOCATE,LOCATE,LOWER,LPAD,"
				+ "LTRIM,MAKE_SET,MATCH,MID,OCT,OCTET_LENGTH,ORD,POSITION,"
				+ "QUOTE,REPEAT,REPLACE,REVERSE,RIGHT,RPAD,RTRIM,SOUNDEX,"
				+ "SPACE,STRCMP,SUBSTRING,SUBSTRING,SUBSTRING,SUBSTRING,"
				+ "SUBSTRING_INDEX,TRIM,UCASE,UPPER";
	}

	/**
	 * @see DatabaseMetaData#getSuperTables(String, String, String)
	 */
	public java.sql.ResultSet getSuperTables(String arg0, String arg1,
			String arg2) throws SQLException {
		Field[] fields = new Field[4];
		fields[0] = new Field("", "TABLE_CAT", Types.CHAR, 32);
		fields[1] = new Field("", "TABLE_SCHEM", Types.CHAR, 32);
		fields[2] = new Field("", "TABLE_NAME", Types.CHAR, 32);
		fields[3] = new Field("", "SUPERTABLE_NAME", Types.CHAR, 32);

		return buildResultSet(fields, new ArrayList());
	}

	/**
	 * @see DatabaseMetaData#getSuperTypes(String, String, String)
	 */
	public java.sql.ResultSet getSuperTypes(String arg0, String arg1,
			String arg2) throws SQLException {
		Field[] fields = new Field[6];
		fields[0] = new Field("", "TYPE_CAT", Types.CHAR, 32);
		fields[1] = new Field("", "TYPE_SCHEM", Types.CHAR, 32);
		fields[2] = new Field("", "TYPE_NAME", Types.CHAR, 32);
		fields[3] = new Field("", "SUPERTYPE_CAT", Types.CHAR, 32);
		fields[4] = new Field("", "SUPERTYPE_SCHEM", Types.CHAR, 32);
		fields[5] = new Field("", "SUPERTYPE_NAME", Types.CHAR, 32);

		return buildResultSet(fields, new ArrayList());
	}

	/**
	 * Get a comma separated list of system functions.
	 * 
	 * @return the list
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public String getSystemFunctions() throws SQLException {
		return "DATABASE,USER,SYSTEM_USER,SESSION_USER,PASSWORD,ENCRYPT,LAST_INSERT_ID,VERSION";
	}

	private String getTableNameWithCase(String table) {
		String tableNameWithCase = (this.conn.lowerCaseTableNames() ? table
				.toLowerCase() : table);

		return tableNameWithCase;
	}

	/**
	 * Get a description of the access rights for each table available in a
	 * catalog.
	 * <P>
	 * Only privileges matching the schema and table name criteria are returned.
	 * They are ordered by TABLE_SCHEM, TABLE_NAME, and PRIVILEGE.
	 * </p>
	 * <P>
	 * Each privilige description has the following columns:
	 * <OL>
	 * <li> <B>TABLE_CAT</B> String => table catalog (may be null) </li>
	 * <li> <B>TABLE_SCHEM</B> String => table schema (may be null) </li>
	 * <li> <B>TABLE_NAME</B> String => table name </li>
	 * <li> <B>COLUMN_NAME</B> String => column name </li>
	 * <li> <B>GRANTOR</B> => grantor of access (may be null) </li>
	 * <li> <B>GRANTEE</B> String => grantee of access </li>
	 * <li> <B>PRIVILEGE</B> String => name of access (SELECT, INSERT, UPDATE,
	 * REFRENCES, ...) </li>
	 * <li> <B>IS_GRANTABLE</B> String => "YES" if grantee is permitted to
	 * grant to others; "NO" if not; null if unknown </li>
	 * </ol>
	 * </p>
	 * 
	 * @param catalog
	 *            a catalog name; "" retrieves those without a catalog
	 * @param schemaPattern
	 *            a schema name pattern; "" retrieves those without a schema
	 * @param tableNamePattern
	 *            a table name pattern
	 * @return ResultSet each row is a table privilege description
	 * @throws SQLException
	 *             if a database access error occurs
	 * @see #getSearchStringEscape
	 */
	public java.sql.ResultSet getTablePrivileges(String catalog,
			String schemaPattern, String tableNamePattern) throws SQLException {

		if (tableNamePattern == null) {
			if (this.conn.getNullNamePatternMatchesAll()) {
				tableNamePattern = "%";
			} else {
				throw SQLError.createSQLException(
						"Table name pattern can not be NULL or empty.",
						SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
			}
		}

		Field[] fields = new Field[7];
		fields[0] = new Field("", "TABLE_CAT", Types.CHAR, 64);
		fields[1] = new Field("", "TABLE_SCHEM", Types.CHAR, 1);
		fields[2] = new Field("", "TABLE_NAME", Types.CHAR, 64);
		fields[3] = new Field("", "GRANTOR", Types.CHAR, 77);
		fields[4] = new Field("", "GRANTEE", Types.CHAR, 77);
		fields[5] = new Field("", "PRIVILEGE", Types.CHAR, 64);
		fields[6] = new Field("", "IS_GRANTABLE", Types.CHAR, 3);

		StringBuffer grantQuery = new StringBuffer(
				"SELECT host,db,table_name,grantor,user,table_priv from mysql.tables_priv ");
		grantQuery.append(" WHERE ");

		if ((catalog != null) && (catalog.length() != 0)) {
			grantQuery.append(" db='");
			grantQuery.append(catalog);
			grantQuery.append("' AND ");
		}

		grantQuery.append("table_name like '");
		grantQuery.append(tableNamePattern);
		grantQuery.append("'");

		ResultSet results = null;
		ArrayList grantRows = new ArrayList();
		Statement stmt = null;

		try {
			stmt = this.conn.createStatement();
			stmt.setEscapeProcessing(false);

			results = stmt.executeQuery(grantQuery.toString());

			while (results.next()) {
				String host = results.getString(1);
				String db = results.getString(2);
				String table = results.getString(3);
				String grantor = results.getString(4);
				String user = results.getString(5);

				if ((user == null) || (user.length() == 0)) {
					user = "%";
				}

				StringBuffer fullUser = new StringBuffer(user);

				if ((host != null) && this.conn.getUseHostsInPrivileges()) {
					fullUser.append("@");
					fullUser.append(host);
				}

				String allPrivileges = results.getString(6);

				if (allPrivileges != null) {
					allPrivileges = allPrivileges.toUpperCase(Locale.ENGLISH);

					StringTokenizer st = new StringTokenizer(allPrivileges, ",");

					while (st.hasMoreTokens()) {
						String privilege = st.nextToken().trim();

						// Loop through every column in the table
						java.sql.ResultSet columnResults = null;

						try {
							columnResults = getColumns(catalog, schemaPattern,
									table, "%");

							while (columnResults.next()) {
								byte[][] tuple = new byte[8][];
								tuple[0] = s2b(db);
								tuple[1] = null;
								tuple[2] = s2b(table);

								if (grantor != null) {
									tuple[3] = s2b(grantor);
								} else {
									tuple[3] = null;
								}

								tuple[4] = s2b(fullUser.toString());
								tuple[5] = s2b(privilege);
								tuple[6] = null;
								grantRows.add(new ByteArrayRow(tuple, getExceptionInterceptor()));
							}
						} finally {
							if (columnResults != null) {
								try {
									columnResults.close();
								} catch (Exception ex) {
									;
								}
							}
						}
					}
				}
			}
		} finally {
			if (results != null) {
				try {
					results.close();
				} catch (Exception ex) {
					;
				}

				results = null;
			}

			if (stmt != null) {
				try {
					stmt.close();
				} catch (Exception ex) {
					;
				}

				stmt = null;
			}
		}

		return buildResultSet(fields, grantRows);
	}

	/**
	 * Get a description of tables available in a catalog.
	 * <P>
	 * Only table descriptions matching the catalog, schema, table name and type
	 * criteria are returned. They are ordered by TABLE_TYPE, TABLE_SCHEM and
	 * TABLE_NAME.
	 * </p>
	 * <P>
	 * Each table description has the following columns:
	 * <OL>
	 * <li> <B>TABLE_CAT</B> String => table catalog (may be null) </li>
	 * <li> <B>TABLE_SCHEM</B> String => table schema (may be null) </li>
	 * <li> <B>TABLE_NAME</B> String => table name </li>
	 * <li> <B>TABLE_TYPE</B> String => table type. Typical types are "TABLE",
	 * "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS",
	 * "SYNONYM". </li>
	 * <li> <B>REMARKS</B> String => explanatory comment on the table </li>
	 * </ol>
	 * </p>
	 * <P>
	 * <B>Note:</B> Some databases may not return information for all tables.
	 * </p>
	 * 
	 * @param catalog
	 *            a catalog name; "" retrieves those without a catalog
	 * @param schemaPattern
	 *            a schema name pattern; "" retrieves those without a schema
	 * @param tableNamePattern
	 *            a table name pattern
	 * @param types
	 *            a list of table types to include; null returns all types
	 * @return ResultSet each row is a table description
	 * @throws SQLException
	 *             DOCUMENT ME!
	 * @see #getSearchStringEscape
	 */
	public java.sql.ResultSet getTables(String catalog, String schemaPattern,
			String tableNamePattern, final String[] types) throws SQLException {

		if (tableNamePattern == null) {
			if (this.conn.getNullNamePatternMatchesAll()) {
				tableNamePattern = "%";
			} else {
				throw SQLError.createSQLException(
						"Table name pattern can not be NULL or empty.",
						SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
			}
		}

		Field[] fields = new Field[5];
		fields[0] = new Field("", "TABLE_CAT", java.sql.Types.VARCHAR, 255);
		fields[1] = new Field("", "TABLE_SCHEM", java.sql.Types.VARCHAR, 0);
		fields[2] = new Field("", "TABLE_NAME", java.sql.Types.VARCHAR, 255);
		fields[3] = new Field("", "TABLE_TYPE", java.sql.Types.VARCHAR, 5);
		fields[4] = new Field("", "REMARKS", java.sql.Types.VARCHAR, 0);

		final ArrayList tuples = new ArrayList();

		final Statement stmt = this.conn.getMetadataSafeStatement();

		final String tableNamePat = tableNamePattern;

		final boolean operatingOnInformationSchema = "information_schema".equalsIgnoreCase(catalog);
		
		try {

			new IterateBlock(getCatalogIterator(catalog)) {
				void forEach(Object catalogStr) throws SQLException {
					ResultSet results = null;

					try {

						if (!conn.versionMeetsMinimum(5, 0, 2)) {
							try {
								results = stmt
									.executeQuery("SHOW TABLES FROM "
											+ quotedId + catalogStr.toString()
											+ quotedId + " LIKE '"
											+ tableNamePat + "'");
							} catch (SQLException sqlEx) {
								if (SQLError.SQL_STATE_COMMUNICATION_LINK_FAILURE.equals(sqlEx.getSQLState())) {
									throw sqlEx;
								}
								
								return;
							}
						} else {
							try {
								results = stmt
									.executeQuery("SHOW FULL TABLES FROM "
											+ quotedId + catalogStr.toString()
											+ quotedId + " LIKE '"
											+ tableNamePat + "'");
							} catch (SQLException sqlEx) {
								if (SQLError.SQL_STATE_COMMUNICATION_LINK_FAILURE.equals(sqlEx.getSQLState())) {
									throw sqlEx;
								}
								
								return;
							}
						}

						boolean shouldReportTables = false;
						boolean shouldReportViews = false;
						boolean shouldReportSystemTables = false;
						
						if (types == null || types.length == 0) {
							shouldReportTables = true;
							shouldReportViews = true;
							shouldReportSystemTables = true;
						} else {
							for (int i = 0; i < types.length; i++) {
								if ("TABLE".equalsIgnoreCase(types[i])) {
									shouldReportTables = true;
								}

								if ("VIEW".equalsIgnoreCase(types[i])) {
									shouldReportViews = true;
								}
								
								if ("SYSTEM TABLE".equalsIgnoreCase(types[i])) {
									shouldReportSystemTables = true;
								}
							}
						}

						int typeColumnIndex = 0;
						boolean hasTableTypes = false;

						if (conn.versionMeetsMinimum(5, 0, 2)) {
							try {
								// Both column names have been in use in the
								// source tree
								// so far....
								typeColumnIndex = results
										.findColumn("table_type");
								hasTableTypes = true;
							} catch (SQLException sqlEx) {

								// We should probably check SQLState here, but
								// that
								// can change depending on the server version
								// and
								// user properties, however, we'll get a 'true'
								// SQLException when we actually try to find the
								// 'Type' column
								// 
								try {
									typeColumnIndex = results
											.findColumn("Type");
									hasTableTypes = true;
								} catch (SQLException sqlEx2) {
									hasTableTypes = false;
								}
							}
						}

						TreeMap tablesOrderedByName = null;
						TreeMap viewsOrderedByName = null;

						while (results.next()) {
							byte[][] row = new byte[5][];
							row[0] = (catalogStr.toString() == null) ? null
									: s2b(catalogStr.toString());
							row[1] = null;
							row[2] = results.getBytes(1);
							row[4] = new byte[0];

							if (hasTableTypes) {
								String tableType = results
										.getString(typeColumnIndex);

								if (("table".equalsIgnoreCase(tableType) || "base table"
										.equalsIgnoreCase(tableType))
										&& shouldReportTables) {
									boolean reportTable = false;
									
									if (!operatingOnInformationSchema && shouldReportTables) {
										row[3] = TABLE_AS_BYTES;
										reportTable = true;
									} else if (operatingOnInformationSchema && shouldReportSystemTables) {
										row[3] = SYSTEM_TABLE_AS_BYTES;
										reportTable = true;
									}
									
									if (reportTable) {
										if (tablesOrderedByName == null) {
											tablesOrderedByName = new TreeMap();
										}
	
										tablesOrderedByName.put(results
												.getString(1), row);
									}
								} else if ("system view".equalsIgnoreCase(tableType) && shouldReportSystemTables) {
									row[3] = SYSTEM_TABLE_AS_BYTES;

									if (tablesOrderedByName == null) {
										tablesOrderedByName = new TreeMap();
									}

									tablesOrderedByName.put(results
											.getString(1), row);
								} else if ("view".equalsIgnoreCase(tableType)
										&& shouldReportViews) {
									row[3] = VIEW_AS_BYTES;

									if (viewsOrderedByName == null) {
										viewsOrderedByName = new TreeMap();
									}

									viewsOrderedByName.put(
											results.getString(1), row);
								} else if (!hasTableTypes) {
									// punt?
									row[3] = TABLE_AS_BYTES;

									if (tablesOrderedByName == null) {
										tablesOrderedByName = new TreeMap();
									}

									tablesOrderedByName.put(results
											.getString(1), row);
								}
							} else {
								if (shouldReportTables) {
									// Pre-MySQL-5.0.1, tables only
									row[3] = TABLE_AS_BYTES;

									if (tablesOrderedByName == null) {
										tablesOrderedByName = new TreeMap();
									}

									tablesOrderedByName.put(results
											.getString(1), row);
								}
							}
						}

						// They are ordered by TABLE_TYPE,
						// * TABLE_SCHEM and TABLE_NAME.

						if (tablesOrderedByName != null) {
							Iterator tablesIter = tablesOrderedByName.values()
									.iterator();

							while (tablesIter.hasNext()) {
								tuples.add(new ByteArrayRow((byte[][])tablesIter.next(), getExceptionInterceptor()));
							}
						}

						if (viewsOrderedByName != null) {
							Iterator viewsIter = viewsOrderedByName.values()
									.iterator();

							while (viewsIter.hasNext()) {
								tuples.add(new ByteArrayRow((byte[][])viewsIter.next(), getExceptionInterceptor()));
							}
						}

					} finally {
						if (results != null) {
							try {
								results.close();
							} catch (Exception ex) {
								;
							}

							results = null;
						}

					}
				}
			}.doForAll();
		} finally {
			if (stmt != null) {
				stmt.close();
			}
		}

		java.sql.ResultSet tables = buildResultSet(fields, tuples);

		return tables;
	}

	/**
	 * Get the table types available in this database. The results are ordered
	 * by table type.
	 * <P>
	 * The table type is:
	 * <OL>
	 * <li> <B>TABLE_TYPE</B> String => table type. Typical types are "TABLE",
	 * "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS",
	 * "SYNONYM". </li>
	 * </ol>
	 * </p>
	 * 
	 * @return ResultSet each row has a single String column that is a table
	 *         type
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public java.sql.ResultSet getTableTypes() throws SQLException {
		ArrayList tuples = new ArrayList();
		Field[] fields = new Field[1];
		fields[0] = new Field("", "TABLE_TYPE", Types.VARCHAR, 5);

		byte[][] tableTypeRow = new byte[1][];
		tableTypeRow[0] = TABLE_AS_BYTES;
		tuples.add(new ByteArrayRow(tableTypeRow, getExceptionInterceptor()));

		if (this.conn.versionMeetsMinimum(5, 0, 1)) {
			byte[][] viewTypeRow = new byte[1][];
			viewTypeRow[0] = VIEW_AS_BYTES;
			tuples.add(new ByteArrayRow(viewTypeRow, getExceptionInterceptor()));
		}

		byte[][] tempTypeRow = new byte[1][];
		tempTypeRow[0] = s2b("LOCAL TEMPORARY");
		tuples.add(new ByteArrayRow(tempTypeRow, getExceptionInterceptor()));

		return buildResultSet(fields, tuples);
	}

	/**
	 * Get a comma separated list of time and date functions.
	 * 
	 * @return the list
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public String getTimeDateFunctions() throws SQLException {
		return "DAYOFWEEK,WEEKDAY,DAYOFMONTH,DAYOFYEAR,MONTH,DAYNAME,"
				+ "MONTHNAME,QUARTER,WEEK,YEAR,HOUR,MINUTE,SECOND,PERIOD_ADD,"
				+ "PERIOD_DIFF,TO_DAYS,FROM_DAYS,DATE_FORMAT,TIME_FORMAT,"
				+ "CURDATE,CURRENT_DATE,CURTIME,CURRENT_TIME,NOW,SYSDATE,"
				+ "CURRENT_TIMESTAMP,UNIX_TIMESTAMP,FROM_UNIXTIME,"
				+ "SEC_TO_TIME,TIME_TO_SEC";
	}

	/**
	 * Get a description of all the standard SQL types supported by this
	 * database. They are ordered by DATA_TYPE and then by how closely the data
	 * type maps to the corresponding JDBC SQL type.
	 * <P>
	 * Each type description has the following columns:
	 * <OL>
	 * <li> <B>TYPE_NAME</B> String => Type name </li>
	 * <li> <B>DATA_TYPE</B> short => SQL data type from java.sql.Types </li>
	 * <li> <B>PRECISION</B> int => maximum precision </li>
	 * <li> <B>LITERAL_PREFIX</B> String => prefix used to quote a literal (may
	 * be null) </li>
	 * <li> <B>LITERAL_SUFFIX</B> String => suffix used to quote a literal (may
	 * be null) </li>
	 * <li> <B>CREATE_PARAMS</B> String => parameters used in creating the type
	 * (may be null) </li>
	 * <li> <B>NULLABLE</B> short => can you use NULL for this type?
	 * <UL>
	 * <li> typeNoNulls - does not allow NULL values </li>
	 * <li> typeNullable - allows NULL values </li>
	 * <li> typeNullableUnknown - nullability unknown </li>
	 * </ul>
	 * </li>
	 * <li> <B>CASE_SENSITIVE</B> boolean=> is it case sensitive? </li>
	 * <li> <B>SEARCHABLE</B> short => can you use "WHERE" based on this type:
	 * <UL>
	 * <li> typePredNone - No support </li>
	 * <li> typePredChar - Only supported with WHERE .. LIKE </li>
	 * <li> typePredBasic - Supported except for WHERE .. LIKE </li>
	 * <li> typeSearchable - Supported for all WHERE .. </li>
	 * </ul>
	 * </li>
	 * <li> <B>UNSIGNED_ATTRIBUTE</B> boolean => is it unsigned? </li>
	 * <li> <B>FIXED_PREC_SCALE</B> boolean => can it be a money value? </li>
	 * <li> <B>AUTO_INCREMENT</B> boolean => can it be used for an
	 * auto-increment value? </li>
	 * <li> <B>LOCAL_TYPE_NAME</B> String => localized version of type name
	 * (may be null) </li>
	 * <li> <B>MINIMUM_SCALE</B> short => minimum scale supported </li>
	 * <li> <B>MAXIMUM_SCALE</B> short => maximum scale supported </li>
	 * <li> <B>SQL_DATA_TYPE</B> int => unused </li>
	 * <li> <B>SQL_DATETIME_SUB</B> int => unused </li>
	 * <li> <B>NUM_PREC_RADIX</B> int => usually 2 or 10 </li>
	 * </ol>
	 * </p>
	 * 
	 * @return ResultSet each row is a SQL type description
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	/**
	 * Get a description of all the standard SQL types supported by this
	 * database. They are ordered by DATA_TYPE and then by how closely the data
	 * type maps to the corresponding JDBC SQL type.
	 * <P>
	 * Each type description has the following columns:
	 * <OL>
	 * <li> <B>TYPE_NAME</B> String => Type name </li>
	 * <li> <B>DATA_TYPE</B> short => SQL data type from java.sql.Types </li>
	 * <li> <B>PRECISION</B> int => maximum precision </li>
	 * <li> <B>LITERAL_PREFIX</B> String => prefix used to quote a literal (may
	 * be null) </li>
	 * <li> <B>LITERAL_SUFFIX</B> String => suffix used to quote a literal (may
	 * be null) </li>
	 * <li> <B>CREATE_PARAMS</B> String => parameters used in creating the type
	 * (may be null) </li>
	 * <li> <B>NULLABLE</B> short => can you use NULL for this type?
	 * <UL>
	 * <li> typeNoNulls - does not allow NULL values </li>
	 * <li> typeNullable - allows NULL values </li>
	 * <li> typeNullableUnknown - nullability unknown </li>
	 * </ul>
	 * </li>
	 * <li> <B>CASE_SENSITIVE</B> boolean=> is it case sensitive? </li>
	 * <li> <B>SEARCHABLE</B> short => can you use "WHERE" based on this type:
	 * <UL>
	 * <li> typePredNone - No support </li>
	 * <li> typePredChar - Only supported with WHERE .. LIKE </li>
	 * <li> typePredBasic - Supported except for WHERE .. LIKE </li>
	 * <li> typeSearchable - Supported for all WHERE .. </li>
	 * </ul>
	 * </li>
	 * <li> <B>UNSIGNED_ATTRIBUTE</B> boolean => is it unsigned? </li>
	 * <li> <B>FIXED_PREC_SCALE</B> boolean => can it be a money value? </li>
	 * <li> <B>AUTO_INCREMENT</B> boolean => can it be used for an
	 * auto-increment value? </li>
	 * <li> <B>LOCAL_TYPE_NAME</B> String => localized version of type name
	 * (may be null) </li>
	 * <li> <B>MINIMUM_SCALE</B> short => minimum scale supported </li>
	 * <li> <B>MAXIMUM_SCALE</B> short => maximum scale supported </li>
	 * <li> <B>SQL_DATA_TYPE</B> int => unused </li>
	 * <li> <B>SQL_DATETIME_SUB</B> int => unused </li>
	 * <li> <B>NUM_PREC_RADIX</B> int => usually 2 or 10 </li>
	 * </ol>
	 * </p>
	 * 
	 * @return ResultSet each row is a SQL type description
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public java.sql.ResultSet getTypeInfo() throws SQLException {
		Field[] fields = new Field[18];
		fields[0] = new Field("", "TYPE_NAME", Types.CHAR, 32);
		fields[1] = new Field("", "DATA_TYPE", Types.INTEGER, 5);
		fields[2] = new Field("", "PRECISION", Types.INTEGER, 10);
		fields[3] = new Field("", "LITERAL_PREFIX", Types.CHAR, 4);
		fields[4] = new Field("", "LITERAL_SUFFIX", Types.CHAR, 4);
		fields[5] = new Field("", "CREATE_PARAMS", Types.CHAR, 32);
		fields[6] = new Field("", "NULLABLE", Types.SMALLINT, 5);
		fields[7] = new Field("", "CASE_SENSITIVE", Types.BOOLEAN, 3);
		fields[8] = new Field("", "SEARCHABLE", Types.SMALLINT, 3);
		fields[9] = new Field("", "UNSIGNED_ATTRIBUTE", Types.BOOLEAN, 3);
		fields[10] = new Field("", "FIXED_PREC_SCALE", Types.BOOLEAN, 3);
		fields[11] = new Field("", "AUTO_INCREMENT", Types.BOOLEAN, 3);
		fields[12] = new Field("", "LOCAL_TYPE_NAME", Types.CHAR, 32);
		fields[13] = new Field("", "MINIMUM_SCALE", Types.SMALLINT, 5);
		fields[14] = new Field("", "MAXIMUM_SCALE", Types.SMALLINT, 5);
		fields[15] = new Field("", "SQL_DATA_TYPE", Types.INTEGER, 10);
		fields[16] = new Field("", "SQL_DATETIME_SUB", Types.INTEGER, 10);
		fields[17] = new Field("", "NUM_PREC_RADIX", Types.INTEGER, 10);

		byte[][] rowVal = null;
		ArrayList tuples = new ArrayList();

		/*
		 * The following are ordered by java.sql.Types, and then by how closely
		 * the MySQL type matches the JDBC Type (per spec)
		 */
		/*
		 * MySQL Type: BIT (silently converted to TINYINT(1)) JDBC Type: BIT
		 */
		rowVal = new byte[18][];
		rowVal[0] = s2b("BIT");
		rowVal[1] = Integer.toString(java.sql.Types.BIT).getBytes();

		// JDBC Data type
		rowVal[2] = s2b("1"); // Precision
		rowVal[3] = s2b(""); // Literal Prefix
		rowVal[4] = s2b(""); // Literal Suffix
		rowVal[5] = s2b(""); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("true"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("false"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("false"); // Auto Increment
		rowVal[12] = s2b("BIT"); // Locale Type Name
		rowVal[13] = s2b("0"); // Minimum Scale
		rowVal[14] = s2b("0"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

		/*
		 * MySQL Type: BOOL (silently converted to TINYINT(1)) JDBC Type: BIT
		 */
		rowVal = new byte[18][];
		rowVal[0] = s2b("BOOL");
		rowVal[1] = Integer.toString(java.sql.Types.BIT).getBytes();

		// JDBC Data type
		rowVal[2] = s2b("1"); // Precision
		rowVal[3] = s2b(""); // Literal Prefix
		rowVal[4] = s2b(""); // Literal Suffix
		rowVal[5] = s2b(""); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("true"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("false"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("false"); // Auto Increment
		rowVal[12] = s2b("BOOL"); // Locale Type Name
		rowVal[13] = s2b("0"); // Minimum Scale
		rowVal[14] = s2b("0"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

		/*
		 * MySQL Type: TINYINT JDBC Type: TINYINT
		 */
		rowVal = new byte[18][];
		rowVal[0] = s2b("TINYINT");
		rowVal[1] = Integer.toString(java.sql.Types.TINYINT).getBytes();

		// JDBC Data type
		rowVal[2] = s2b("3"); // Precision
		rowVal[3] = s2b(""); // Literal Prefix
		rowVal[4] = s2b(""); // Literal Suffix
		rowVal[5] = s2b("[(M)] [UNSIGNED] [ZEROFILL]"); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("false"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("true"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("true"); // Auto Increment
		rowVal[12] = s2b("TINYINT"); // Locale Type Name
		rowVal[13] = s2b("0"); // Minimum Scale
		rowVal[14] = s2b("0"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));
		
		rowVal = new byte[18][];
		rowVal[0] = s2b("TINYINT UNSIGNED");
		rowVal[1] = Integer.toString(java.sql.Types.TINYINT).getBytes();

		// JDBC Data type
		rowVal[2] = s2b("3"); // Precision
		rowVal[3] = s2b(""); // Literal Prefix
		rowVal[4] = s2b(""); // Literal Suffix
		rowVal[5] = s2b("[(M)] [UNSIGNED] [ZEROFILL]"); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("false"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("true"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("true"); // Auto Increment
		rowVal[12] = s2b("TINYINT UNSIGNED"); // Locale Type Name
		rowVal[13] = s2b("0"); // Minimum Scale
		rowVal[14] = s2b("0"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

		/*
		 * MySQL Type: BIGINT JDBC Type: BIGINT
		 */
		rowVal = new byte[18][];
		rowVal[0] = s2b("BIGINT");
		rowVal[1] = Integer.toString(java.sql.Types.BIGINT).getBytes();

		// JDBC Data type
		rowVal[2] = s2b("19"); // Precision
		rowVal[3] = s2b(""); // Literal Prefix
		rowVal[4] = s2b(""); // Literal Suffix
		rowVal[5] = s2b("[(M)] [UNSIGNED] [ZEROFILL]"); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("false"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("true"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("true"); // Auto Increment
		rowVal[12] = s2b("BIGINT"); // Locale Type Name
		rowVal[13] = s2b("0"); // Minimum Scale
		rowVal[14] = s2b("0"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));
		
		rowVal = new byte[18][];
		rowVal[0] = s2b("BIGINT UNSIGNED");
		rowVal[1] = Integer.toString(java.sql.Types.BIGINT).getBytes();

		// JDBC Data type
		rowVal[2] = s2b("20"); // Precision
		rowVal[3] = s2b(""); // Literal Prefix
		rowVal[4] = s2b(""); // Literal Suffix
		rowVal[5] = s2b("[(M)] [ZEROFILL]"); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("false"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("true"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("true"); // Auto Increment
		rowVal[12] = s2b("BIGINT UNSIGNED"); // Locale Type Name
		rowVal[13] = s2b("0"); // Minimum Scale
		rowVal[14] = s2b("0"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

		/*
		 * MySQL Type: LONG VARBINARY JDBC Type: LONGVARBINARY
		 */
		rowVal = new byte[18][];
		rowVal[0] = s2b("LONG VARBINARY");
		rowVal[1] = Integer.toString(java.sql.Types.LONGVARBINARY).getBytes();

		// JDBC Data type
		rowVal[2] = s2b("16777215"); // Precision
		rowVal[3] = s2b("'"); // Literal Prefix
		rowVal[4] = s2b("'"); // Literal Suffix
		rowVal[5] = s2b(""); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("true"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("false"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("false"); // Auto Increment
		rowVal[12] = s2b("LONG VARBINARY"); // Locale Type Name
		rowVal[13] = s2b("0"); // Minimum Scale
		rowVal[14] = s2b("0"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

		/*
		 * MySQL Type: MEDIUMBLOB JDBC Type: LONGVARBINARY
		 */
		rowVal = new byte[18][];
		rowVal[0] = s2b("MEDIUMBLOB");
		rowVal[1] = Integer.toString(java.sql.Types.LONGVARBINARY).getBytes();

		// JDBC Data type
		rowVal[2] = s2b("16777215"); // Precision
		rowVal[3] = s2b("'"); // Literal Prefix
		rowVal[4] = s2b("'"); // Literal Suffix
		rowVal[5] = s2b(""); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("true"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("false"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("false"); // Auto Increment
		rowVal[12] = s2b("MEDIUMBLOB"); // Locale Type Name
		rowVal[13] = s2b("0"); // Minimum Scale
		rowVal[14] = s2b("0"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

		/*
		 * MySQL Type: LONGBLOB JDBC Type: LONGVARBINARY
		 */
		rowVal = new byte[18][];
		rowVal[0] = s2b("LONGBLOB");
		rowVal[1] = Integer.toString(java.sql.Types.LONGVARBINARY).getBytes();

		// JDBC Data type
		rowVal[2] = Integer.toString(Integer.MAX_VALUE).getBytes();

		// Precision
		rowVal[3] = s2b("'"); // Literal Prefix
		rowVal[4] = s2b("'"); // Literal Suffix
		rowVal[5] = s2b(""); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("true"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("false"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("false"); // Auto Increment
		rowVal[12] = s2b("LONGBLOB"); // Locale Type Name
		rowVal[13] = s2b("0"); // Minimum Scale
		rowVal[14] = s2b("0"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

		/*
		 * MySQL Type: BLOB JDBC Type: LONGVARBINARY
		 */
		rowVal = new byte[18][];
		rowVal[0] = s2b("BLOB");
		rowVal[1] = Integer.toString(java.sql.Types.LONGVARBINARY).getBytes();

		// JDBC Data type
		rowVal[2] = s2b("65535"); // Precision
		rowVal[3] = s2b("'"); // Literal Prefix
		rowVal[4] = s2b("'"); // Literal Suffix
		rowVal[5] = s2b(""); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("true"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("false"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("false"); // Auto Increment
		rowVal[12] = s2b("BLOB"); // Locale Type Name
		rowVal[13] = s2b("0"); // Minimum Scale
		rowVal[14] = s2b("0"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

		/*
		 * MySQL Type: TINYBLOB JDBC Type: LONGVARBINARY
		 */
		rowVal = new byte[18][];
		rowVal[0] = s2b("TINYBLOB");
		rowVal[1] = Integer.toString(java.sql.Types.LONGVARBINARY).getBytes();

		// JDBC Data type
		rowVal[2] = s2b("255"); // Precision
		rowVal[3] = s2b("'"); // Literal Prefix
		rowVal[4] = s2b("'"); // Literal Suffix
		rowVal[5] = s2b(""); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("true"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("false"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("false"); // Auto Increment
		rowVal[12] = s2b("TINYBLOB"); // Locale Type Name
		rowVal[13] = s2b("0"); // Minimum Scale
		rowVal[14] = s2b("0"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

		/*
		 * MySQL Type: VARBINARY (sliently converted to VARCHAR(M) BINARY) JDBC
		 * Type: VARBINARY
		 */
		rowVal = new byte[18][];
		rowVal[0] = s2b("VARBINARY");
		rowVal[1] = Integer.toString(java.sql.Types.VARBINARY).getBytes();

		// JDBC Data type
		rowVal[2] = s2b("255"); // Precision
		rowVal[3] = s2b("'"); // Literal Prefix
		rowVal[4] = s2b("'"); // Literal Suffix
		rowVal[5] = s2b("(M)"); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("true"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("false"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("false"); // Auto Increment
		rowVal[12] = s2b("VARBINARY"); // Locale Type Name
		rowVal[13] = s2b("0"); // Minimum Scale
		rowVal[14] = s2b("0"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

		/*
		 * MySQL Type: BINARY (silently converted to CHAR(M) BINARY) JDBC Type:
		 * BINARY
		 */
		rowVal = new byte[18][];
		rowVal[0] = s2b("BINARY");
		rowVal[1] = Integer.toString(java.sql.Types.BINARY).getBytes();

		// JDBC Data type
		rowVal[2] = s2b("255"); // Precision
		rowVal[3] = s2b("'"); // Literal Prefix
		rowVal[4] = s2b("'"); // Literal Suffix
		rowVal[5] = s2b("(M)"); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("true"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("false"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("false"); // Auto Increment
		rowVal[12] = s2b("BINARY"); // Locale Type Name
		rowVal[13] = s2b("0"); // Minimum Scale
		rowVal[14] = s2b("0"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

		/*
		 * MySQL Type: LONG VARCHAR JDBC Type: LONGVARCHAR
		 */
		rowVal = new byte[18][];
		rowVal[0] = s2b("LONG VARCHAR");
		rowVal[1] = Integer.toString(java.sql.Types.LONGVARCHAR).getBytes();

		// JDBC Data type
		rowVal[2] = s2b("16777215"); // Precision
		rowVal[3] = s2b("'"); // Literal Prefix
		rowVal[4] = s2b("'"); // Literal Suffix
		rowVal[5] = s2b(""); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("false"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("false"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("false"); // Auto Increment
		rowVal[12] = s2b("LONG VARCHAR"); // Locale Type Name
		rowVal[13] = s2b("0"); // Minimum Scale
		rowVal[14] = s2b("0"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

		/*
		 * MySQL Type: MEDIUMTEXT JDBC Type: LONGVARCHAR
		 */
		rowVal = new byte[18][];
		rowVal[0] = s2b("MEDIUMTEXT");
		rowVal[1] = Integer.toString(java.sql.Types.LONGVARCHAR).getBytes();

		// JDBC Data type
		rowVal[2] = s2b("16777215"); // Precision
		rowVal[3] = s2b("'"); // Literal Prefix
		rowVal[4] = s2b("'"); // Literal Suffix
		rowVal[5] = s2b(""); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("false"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("false"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("false"); // Auto Increment
		rowVal[12] = s2b("MEDIUMTEXT"); // Locale Type Name
		rowVal[13] = s2b("0"); // Minimum Scale
		rowVal[14] = s2b("0"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

		/*
		 * MySQL Type: LONGTEXT JDBC Type: LONGVARCHAR
		 */
		rowVal = new byte[18][];
		rowVal[0] = s2b("LONGTEXT");
		rowVal[1] = Integer.toString(java.sql.Types.LONGVARCHAR).getBytes();

		// JDBC Data type
		rowVal[2] = Integer.toString(Integer.MAX_VALUE).getBytes();

		// Precision
		rowVal[3] = s2b("'"); // Literal Prefix
		rowVal[4] = s2b("'"); // Literal Suffix
		rowVal[5] = s2b(""); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("false"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("false"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("false"); // Auto Increment
		rowVal[12] = s2b("LONGTEXT"); // Locale Type Name
		rowVal[13] = s2b("0"); // Minimum Scale
		rowVal[14] = s2b("0"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

		/*
		 * MySQL Type: TEXT JDBC Type: LONGVARCHAR
		 */
		rowVal = new byte[18][];
		rowVal[0] = s2b("TEXT");
		rowVal[1] = Integer.toString(java.sql.Types.LONGVARCHAR).getBytes();

		// JDBC Data type
		rowVal[2] = s2b("65535"); // Precision
		rowVal[3] = s2b("'"); // Literal Prefix
		rowVal[4] = s2b("'"); // Literal Suffix
		rowVal[5] = s2b(""); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("false"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("false"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("false"); // Auto Increment
		rowVal[12] = s2b("TEXT"); // Locale Type Name
		rowVal[13] = s2b("0"); // Minimum Scale
		rowVal[14] = s2b("0"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

		/*
		 * MySQL Type: TINYTEXT JDBC Type: LONGVARCHAR
		 */
		rowVal = new byte[18][];
		rowVal[0] = s2b("TINYTEXT");
		rowVal[1] = Integer.toString(java.sql.Types.LONGVARCHAR).getBytes();

		// JDBC Data type
		rowVal[2] = s2b("255"); // Precision
		rowVal[3] = s2b("'"); // Literal Prefix
		rowVal[4] = s2b("'"); // Literal Suffix
		rowVal[5] = s2b(""); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("false"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("false"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("false"); // Auto Increment
		rowVal[12] = s2b("TINYTEXT"); // Locale Type Name
		rowVal[13] = s2b("0"); // Minimum Scale
		rowVal[14] = s2b("0"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

		/*
		 * MySQL Type: CHAR JDBC Type: CHAR
		 */
		rowVal = new byte[18][];
		rowVal[0] = s2b("CHAR");
		rowVal[1] = Integer.toString(java.sql.Types.CHAR).getBytes();

		// JDBC Data type
		rowVal[2] = s2b("255"); // Precision
		rowVal[3] = s2b("'"); // Literal Prefix
		rowVal[4] = s2b("'"); // Literal Suffix
		rowVal[5] = s2b("(M)"); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("false"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("false"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("false"); // Auto Increment
		rowVal[12] = s2b("CHAR"); // Locale Type Name
		rowVal[13] = s2b("0"); // Minimum Scale
		rowVal[14] = s2b("0"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

		// The maximum number of digits for DECIMAL or NUMERIC is 65 (64 from MySQL 5.0.3 to 5.0.5). 
		
		int decimalPrecision = 254;
		
		if (this.conn.versionMeetsMinimum(5,0,3)) {
			if (this.conn.versionMeetsMinimum(5, 0, 6)) {
				decimalPrecision = 65;
			} else {
				decimalPrecision = 64;
			}
		}
		
		/*
		 * MySQL Type: NUMERIC (silently converted to DECIMAL) JDBC Type:
		 * NUMERIC
		 */
		rowVal = new byte[18][];
		rowVal[0] = s2b("NUMERIC");
		rowVal[1] = Integer.toString(java.sql.Types.NUMERIC).getBytes();

		// JDBC Data type
		rowVal[2] = s2b(String.valueOf(decimalPrecision)); // Precision
		rowVal[3] = s2b(""); // Literal Prefix
		rowVal[4] = s2b(""); // Literal Suffix
		rowVal[5] = s2b("[(M[,D])] [ZEROFILL]"); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("false"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("false"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("true"); // Auto Increment
		rowVal[12] = s2b("NUMERIC"); // Locale Type Name
		rowVal[13] = s2b("-308"); // Minimum Scale
		rowVal[14] = s2b("308"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

		/*
		 * MySQL Type: DECIMAL JDBC Type: DECIMAL
		 */
		rowVal = new byte[18][];
		rowVal[0] = s2b("DECIMAL");
		rowVal[1] = Integer.toString(java.sql.Types.DECIMAL).getBytes();

		// JDBC Data type
		rowVal[2] = s2b(String.valueOf(decimalPrecision)); // Precision
		rowVal[3] = s2b(""); // Literal Prefix
		rowVal[4] = s2b(""); // Literal Suffix
		rowVal[5] = s2b("[(M[,D])] [ZEROFILL]"); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("false"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("false"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("true"); // Auto Increment
		rowVal[12] = s2b("DECIMAL"); // Locale Type Name
		rowVal[13] = s2b("-308"); // Minimum Scale
		rowVal[14] = s2b("308"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

		/*
		 * MySQL Type: INTEGER JDBC Type: INTEGER
		 */
		rowVal = new byte[18][];
		rowVal[0] = s2b("INTEGER");
		rowVal[1] = Integer.toString(java.sql.Types.INTEGER).getBytes();

		// JDBC Data type
		rowVal[2] = s2b("10"); // Precision
		rowVal[3] = s2b(""); // Literal Prefix
		rowVal[4] = s2b(""); // Literal Suffix
		rowVal[5] = s2b("[(M)] [UNSIGNED] [ZEROFILL]"); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("false"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("true"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("true"); // Auto Increment
		rowVal[12] = s2b("INTEGER"); // Locale Type Name
		rowVal[13] = s2b("0"); // Minimum Scale
		rowVal[14] = s2b("0"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));
		
		rowVal = new byte[18][];
		rowVal[0] = s2b("INTEGER UNSIGNED");
		rowVal[1] = Integer.toString(java.sql.Types.INTEGER).getBytes();

		// JDBC Data type
		rowVal[2] = s2b("10"); // Precision
		rowVal[3] = s2b(""); // Literal Prefix
		rowVal[4] = s2b(""); // Literal Suffix
		rowVal[5] = s2b("[(M)] [ZEROFILL]"); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("false"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("true"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("true"); // Auto Increment
		rowVal[12] = s2b("INTEGER UNSIGNED"); // Locale Type Name
		rowVal[13] = s2b("0"); // Minimum Scale
		rowVal[14] = s2b("0"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

		/*
		 * MySQL Type: INT JDBC Type: INTEGER
		 */
		rowVal = new byte[18][];
		rowVal[0] = s2b("INT");
		rowVal[1] = Integer.toString(java.sql.Types.INTEGER).getBytes();

		// JDBC Data type
		rowVal[2] = s2b("10"); // Precision
		rowVal[3] = s2b(""); // Literal Prefix
		rowVal[4] = s2b(""); // Literal Suffix
		rowVal[5] = s2b("[(M)] [UNSIGNED] [ZEROFILL]"); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("false"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("true"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("true"); // Auto Increment
		rowVal[12] = s2b("INT"); // Locale Type Name
		rowVal[13] = s2b("0"); // Minimum Scale
		rowVal[14] = s2b("0"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));
		
		rowVal = new byte[18][];
		rowVal[0] = s2b("INT UNSIGNED");
		rowVal[1] = Integer.toString(java.sql.Types.INTEGER).getBytes();

		// JDBC Data type
		rowVal[2] = s2b("10"); // Precision
		rowVal[3] = s2b(""); // Literal Prefix
		rowVal[4] = s2b(""); // Literal Suffix
		rowVal[5] = s2b("[(M)] [ZEROFILL]"); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("false"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("true"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("true"); // Auto Increment
		rowVal[12] = s2b("INT UNSIGNED"); // Locale Type Name
		rowVal[13] = s2b("0"); // Minimum Scale
		rowVal[14] = s2b("0"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

		/*
		 * MySQL Type: MEDIUMINT JDBC Type: INTEGER
		 */
		rowVal = new byte[18][];
		rowVal[0] = s2b("MEDIUMINT");
		rowVal[1] = Integer.toString(java.sql.Types.INTEGER).getBytes();

		// JDBC Data type
		rowVal[2] = s2b("7"); // Precision
		rowVal[3] = s2b(""); // Literal Prefix
		rowVal[4] = s2b(""); // Literal Suffix
		rowVal[5] = s2b("[(M)] [UNSIGNED] [ZEROFILL]"); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("false"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("true"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("true"); // Auto Increment
		rowVal[12] = s2b("MEDIUMINT"); // Locale Type Name
		rowVal[13] = s2b("0"); // Minimum Scale
		rowVal[14] = s2b("0"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

		rowVal = new byte[18][];
		rowVal[0] = s2b("MEDIUMINT UNSIGNED");
		rowVal[1] = Integer.toString(java.sql.Types.INTEGER).getBytes();

		// JDBC Data type
		rowVal[2] = s2b("8"); // Precision
		rowVal[3] = s2b(""); // Literal Prefix
		rowVal[4] = s2b(""); // Literal Suffix
		rowVal[5] = s2b("[(M)] [ZEROFILL]"); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("false"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("true"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("true"); // Auto Increment
		rowVal[12] = s2b("MEDIUMINT UNSIGNED"); // Locale Type Name
		rowVal[13] = s2b("0"); // Minimum Scale
		rowVal[14] = s2b("0"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));
		
		/*
		 * MySQL Type: SMALLINT JDBC Type: SMALLINT
		 */
		rowVal = new byte[18][];
		rowVal[0] = s2b("SMALLINT");
		rowVal[1] = Integer.toString(java.sql.Types.SMALLINT).getBytes();

		// JDBC Data type
		rowVal[2] = s2b("5"); // Precision
		rowVal[3] = s2b(""); // Literal Prefix
		rowVal[4] = s2b(""); // Literal Suffix
		rowVal[5] = s2b("[(M)] [UNSIGNED] [ZEROFILL]"); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("false"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("true"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("true"); // Auto Increment
		rowVal[12] = s2b("SMALLINT"); // Locale Type Name
		rowVal[13] = s2b("0"); // Minimum Scale
		rowVal[14] = s2b("0"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));
		
		rowVal = new byte[18][];
		rowVal[0] = s2b("SMALLINT UNSIGNED");
		rowVal[1] = Integer.toString(java.sql.Types.SMALLINT).getBytes();

		// JDBC Data type
		rowVal[2] = s2b("5"); // Precision
		rowVal[3] = s2b(""); // Literal Prefix
		rowVal[4] = s2b(""); // Literal Suffix
		rowVal[5] = s2b("[(M)] [ZEROFILL]"); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("false"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("true"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("true"); // Auto Increment
		rowVal[12] = s2b("SMALLINT UNSIGNED"); // Locale Type Name
		rowVal[13] = s2b("0"); // Minimum Scale
		rowVal[14] = s2b("0"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

		/*
		 * MySQL Type: FLOAT JDBC Type: REAL (this is the SINGLE PERCISION
		 * floating point type)
		 */
		rowVal = new byte[18][];
		rowVal[0] = s2b("FLOAT");
		rowVal[1] = Integer.toString(java.sql.Types.REAL).getBytes();

		// JDBC Data type
		rowVal[2] = s2b("10"); // Precision
		rowVal[3] = s2b(""); // Literal Prefix
		rowVal[4] = s2b(""); // Literal Suffix
		rowVal[5] = s2b("[(M,D)] [ZEROFILL]"); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("false"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("false"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("true"); // Auto Increment
		rowVal[12] = s2b("FLOAT"); // Locale Type Name
		rowVal[13] = s2b("-38"); // Minimum Scale
		rowVal[14] = s2b("38"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

		/*
		 * MySQL Type: DOUBLE JDBC Type: DOUBLE
		 */
		rowVal = new byte[18][];
		rowVal[0] = s2b("DOUBLE");
		rowVal[1] = Integer.toString(java.sql.Types.DOUBLE).getBytes();

		// JDBC Data type
		rowVal[2] = s2b("17"); // Precision
		rowVal[3] = s2b(""); // Literal Prefix
		rowVal[4] = s2b(""); // Literal Suffix
		rowVal[5] = s2b("[(M,D)] [ZEROFILL]"); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("false"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("false"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("true"); // Auto Increment
		rowVal[12] = s2b("DOUBLE"); // Locale Type Name
		rowVal[13] = s2b("-308"); // Minimum Scale
		rowVal[14] = s2b("308"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

		/*
		 * MySQL Type: DOUBLE PRECISION JDBC Type: DOUBLE
		 */
		rowVal = new byte[18][];
		rowVal[0] = s2b("DOUBLE PRECISION");
		rowVal[1] = Integer.toString(java.sql.Types.DOUBLE).getBytes();

		// JDBC Data type
		rowVal[2] = s2b("17"); // Precision
		rowVal[3] = s2b(""); // Literal Prefix
		rowVal[4] = s2b(""); // Literal Suffix
		rowVal[5] = s2b("[(M,D)] [ZEROFILL]"); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("false"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("false"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("true"); // Auto Increment
		rowVal[12] = s2b("DOUBLE PRECISION"); // Locale Type Name
		rowVal[13] = s2b("-308"); // Minimum Scale
		rowVal[14] = s2b("308"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

		/*
		 * MySQL Type: REAL (does not map to Types.REAL) JDBC Type: DOUBLE
		 */
		rowVal = new byte[18][];
		rowVal[0] = s2b("REAL");
		rowVal[1] = Integer.toString(java.sql.Types.DOUBLE).getBytes();

		// JDBC Data type
		rowVal[2] = s2b("17"); // Precision
		rowVal[3] = s2b(""); // Literal Prefix
		rowVal[4] = s2b(""); // Literal Suffix
		rowVal[5] = s2b("[(M,D)] [ZEROFILL]"); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("false"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("false"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("true"); // Auto Increment
		rowVal[12] = s2b("REAL"); // Locale Type Name
		rowVal[13] = s2b("-308"); // Minimum Scale
		rowVal[14] = s2b("308"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

		/*
		 * MySQL Type: VARCHAR JDBC Type: VARCHAR
		 */
		rowVal = new byte[18][];
		rowVal[0] = s2b("VARCHAR");
		rowVal[1] = Integer.toString(java.sql.Types.VARCHAR).getBytes();

		// JDBC Data type
		rowVal[2] = s2b("255"); // Precision
		rowVal[3] = s2b("'"); // Literal Prefix
		rowVal[4] = s2b("'"); // Literal Suffix
		rowVal[5] = s2b("(M)"); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("false"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("false"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("false"); // Auto Increment
		rowVal[12] = s2b("VARCHAR"); // Locale Type Name
		rowVal[13] = s2b("0"); // Minimum Scale
		rowVal[14] = s2b("0"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

		/*
		 * MySQL Type: ENUM JDBC Type: VARCHAR
		 */
		rowVal = new byte[18][];
		rowVal[0] = s2b("ENUM");
		rowVal[1] = Integer.toString(java.sql.Types.VARCHAR).getBytes();

		// JDBC Data type
		rowVal[2] = s2b("65535"); // Precision
		rowVal[3] = s2b("'"); // Literal Prefix
		rowVal[4] = s2b("'"); // Literal Suffix
		rowVal[5] = s2b(""); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("false"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("false"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("false"); // Auto Increment
		rowVal[12] = s2b("ENUM"); // Locale Type Name
		rowVal[13] = s2b("0"); // Minimum Scale
		rowVal[14] = s2b("0"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

		/*
		 * MySQL Type: SET JDBC Type: VARCHAR
		 */
		rowVal = new byte[18][];
		rowVal[0] = s2b("SET");
		rowVal[1] = Integer.toString(java.sql.Types.VARCHAR).getBytes();

		// JDBC Data type
		rowVal[2] = s2b("64"); // Precision
		rowVal[3] = s2b("'"); // Literal Prefix
		rowVal[4] = s2b("'"); // Literal Suffix
		rowVal[5] = s2b(""); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("false"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("false"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("false"); // Auto Increment
		rowVal[12] = s2b("SET"); // Locale Type Name
		rowVal[13] = s2b("0"); // Minimum Scale
		rowVal[14] = s2b("0"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

		/*
		 * MySQL Type: DATE JDBC Type: DATE
		 */
		rowVal = new byte[18][];
		rowVal[0] = s2b("DATE");
		rowVal[1] = Integer.toString(java.sql.Types.DATE).getBytes();

		// JDBC Data type
		rowVal[2] = s2b("0"); // Precision
		rowVal[3] = s2b("'"); // Literal Prefix
		rowVal[4] = s2b("'"); // Literal Suffix
		rowVal[5] = s2b(""); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("false"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("false"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("false"); // Auto Increment
		rowVal[12] = s2b("DATE"); // Locale Type Name
		rowVal[13] = s2b("0"); // Minimum Scale
		rowVal[14] = s2b("0"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

		/*
		 * MySQL Type: TIME JDBC Type: TIME
		 */
		rowVal = new byte[18][];
		rowVal[0] = s2b("TIME");
		rowVal[1] = Integer.toString(java.sql.Types.TIME).getBytes();

		// JDBC Data type
		rowVal[2] = s2b("0"); // Precision
		rowVal[3] = s2b("'"); // Literal Prefix
		rowVal[4] = s2b("'"); // Literal Suffix
		rowVal[5] = s2b(""); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("false"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("false"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("false"); // Auto Increment
		rowVal[12] = s2b("TIME"); // Locale Type Name
		rowVal[13] = s2b("0"); // Minimum Scale
		rowVal[14] = s2b("0"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

		/*
		 * MySQL Type: DATETIME JDBC Type: TIMESTAMP
		 */
		rowVal = new byte[18][];
		rowVal[0] = s2b("DATETIME");
		rowVal[1] = Integer.toString(java.sql.Types.TIMESTAMP).getBytes();

		// JDBC Data type
		rowVal[2] = s2b("0"); // Precision
		rowVal[3] = s2b("'"); // Literal Prefix
		rowVal[4] = s2b("'"); // Literal Suffix
		rowVal[5] = s2b(""); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("false"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("false"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("false"); // Auto Increment
		rowVal[12] = s2b("DATETIME"); // Locale Type Name
		rowVal[13] = s2b("0"); // Minimum Scale
		rowVal[14] = s2b("0"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

		/*
		 * MySQL Type: TIMESTAMP JDBC Type: TIMESTAMP
		 */
		rowVal = new byte[18][];
		rowVal[0] = s2b("TIMESTAMP");
		rowVal[1] = Integer.toString(java.sql.Types.TIMESTAMP).getBytes();

		// JDBC Data type
		rowVal[2] = s2b("0"); // Precision
		rowVal[3] = s2b("'"); // Literal Prefix
		rowVal[4] = s2b("'"); // Literal Suffix
		rowVal[5] = s2b("[(M)]"); // Create Params
		rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
				.getBytes();

		// Nullable
		rowVal[7] = s2b("false"); // Case Sensitive
		rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
				.getBytes();

		// Searchable
		rowVal[9] = s2b("false"); // Unsignable
		rowVal[10] = s2b("false"); // Fixed Prec Scale
		rowVal[11] = s2b("false"); // Auto Increment
		rowVal[12] = s2b("TIMESTAMP"); // Locale Type Name
		rowVal[13] = s2b("0"); // Minimum Scale
		rowVal[14] = s2b("0"); // Maximum Scale
		rowVal[15] = s2b("0"); // SQL Data Type (not used)
		rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
		rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
		tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

		return buildResultSet(fields, tuples);
	}

	/**
	 * JDBC 2.0 Get a description of the user-defined types defined in a
	 * particular schema. Schema specific UDTs may have type JAVA_OBJECT,
	 * STRUCT, or DISTINCT.
	 * <P>
	 * Only types matching the catalog, schema, type name and type criteria are
	 * returned. They are ordered by DATA_TYPE, TYPE_SCHEM and TYPE_NAME. The
	 * type name parameter may be a fully qualified name. In this case, the
	 * catalog and schemaPattern parameters are ignored.
	 * </p>
	 * <P>
	 * Each type description has the following columns:
	 * <OL>
	 * <li> <B>TYPE_CAT</B> String => the type's catalog (may be null) </li>
	 * <li> <B>TYPE_SCHEM</B> String => type's schema (may be null) </li>
	 * <li> <B>TYPE_NAME</B> String => type name </li>
	 * <li> <B>CLASS_NAME</B> String => Java class name </li>
	 * <li> <B>DATA_TYPE</B> String => type value defined in java.sql.Types.
	 * One of JAVA_OBJECT, STRUCT, or DISTINCT </li>
	 * <li> <B>REMARKS</B> String => explanatory comment on the type </li>
	 * </ol>
	 * </p>
	 * <P>
	 * <B>Note:</B> If the driver does not support UDTs then an empty result
	 * set is returned.
	 * </p>
	 * 
	 * @param catalog
	 *            a catalog name; "" retrieves those without a catalog; null
	 *            means drop catalog name from the selection criteria
	 * @param schemaPattern
	 *            a schema name pattern; "" retrieves those without a schema
	 * @param typeNamePattern
	 *            a type name pattern; may be a fully qualified name
	 * @param types
	 *            a list of user-named types to include (JAVA_OBJECT, STRUCT, or
	 *            DISTINCT); null returns all types
	 * @return ResultSet - each row is a type description
	 * @exception SQLException
	 *                if a database-access error occurs.
	 */
	public java.sql.ResultSet getUDTs(String catalog, String schemaPattern,
			String typeNamePattern, int[] types) throws SQLException {
		Field[] fields = new Field[6];
		fields[0] = new Field("", "TYPE_CAT", Types.VARCHAR, 32);
		fields[1] = new Field("", "TYPE_SCHEM", Types.VARCHAR, 32);
		fields[2] = new Field("", "TYPE_NAME", Types.VARCHAR, 32);
		fields[3] = new Field("", "CLASS_NAME", Types.VARCHAR, 32);
		fields[4] = new Field("", "DATA_TYPE", Types.VARCHAR, 32);
		fields[5] = new Field("", "REMARKS", Types.VARCHAR, 32);

		ArrayList tuples = new ArrayList();

		return buildResultSet(fields, tuples);
	}

	/**
	 * What's the url for this database?
	 * 
	 * @return the url or null if it can't be generated
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public String getURL() throws SQLException {
		return this.conn.getURL();
	}

	/**
	 * What's our user name as known to the database?
	 * 
	 * @return our database user name
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public String getUserName() throws SQLException {
		if (this.conn.getUseHostsInPrivileges()) {
			Statement stmt = null;
			ResultSet rs = null;

			try {
				stmt = this.conn.createStatement();
				stmt.setEscapeProcessing(false);

				rs = stmt.executeQuery("SELECT USER()");
				rs.next();

				return rs.getString(1);
			} finally {
				if (rs != null) {
					try {
						rs.close();
					} catch (Exception ex) {
						AssertionFailedException.shouldNotHappen(ex);
					}

					rs = null;
				}

				if (stmt != null) {
					try {
						stmt.close();
					} catch (Exception ex) {
						AssertionFailedException.shouldNotHappen(ex);
					}

					stmt = null;
				}
			}
		}

		return this.conn.getUser();
	}

	/**
	 * Get a description of a table's columns that are automatically updated
	 * when any value in a row is updated. They are unordered.
	 * <P>
	 * Each column description has the following columns:
	 * <OL>
	 * <li> <B>SCOPE</B> short => is not used </li>
	 * <li> <B>COLUMN_NAME</B> String => column name </li>
	 * <li> <B>DATA_TYPE</B> short => SQL data type from java.sql.Types </li>
	 * <li> <B>TYPE_NAME</B> String => Data source dependent type name </li>
	 * <li> <B>COLUMN_SIZE</B> int => precision </li>
	 * <li> <B>BUFFER_LENGTH</B> int => length of column value in bytes </li>
	 * <li> <B>DECIMAL_DIGITS</B> short => scale </li>
	 * <li> <B>PSEUDO_COLUMN</B> short => is this a pseudo column like an
	 * Oracle ROWID
	 * <UL>
	 * <li> versionColumnUnknown - may or may not be pseudo column </li>
	 * <li> versionColumnNotPseudo - is NOT a pseudo column </li>
	 * <li> versionColumnPseudo - is a pseudo column </li>
	 * </ul>
	 * </li>
	 * </ol>
	 * </p>
	 * 
	 * @param catalog
	 *            a catalog name; "" retrieves those without a catalog
	 * @param schema
	 *            a schema name; "" retrieves those without a schema
	 * @param table
	 *            a table name
	 * @return ResultSet each row is a column description
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public java.sql.ResultSet getVersionColumns(String catalog, String schema,
			String table) throws SQLException {
		Field[] fields = new Field[8];
		fields[0] = new Field("", "SCOPE", Types.SMALLINT, 5);
		fields[1] = new Field("", "COLUMN_NAME", Types.CHAR, 32);
		fields[2] = new Field("", "DATA_TYPE", Types.INTEGER, 5);
		fields[3] = new Field("", "TYPE_NAME", Types.CHAR, 16);
		fields[4] = new Field("", "COLUMN_SIZE", Types.INTEGER, 16);
		fields[5] = new Field("", "BUFFER_LENGTH", Types.INTEGER, 16);
		fields[6] = new Field("", "DECIMAL_DIGITS", Types.SMALLINT, 16);
		fields[7] = new Field("", "PSEUDO_COLUMN", Types.SMALLINT, 5);

		return buildResultSet(fields, new ArrayList());

		// do TIMESTAMP columns count?
	}

	/**
	 * JDBC 2.0 Determine whether or not a visible row insert can be detected by
	 * calling ResultSet.rowInserted().
	 * 
	 * @param type
	 *            set type, i.e. ResultSet.TYPE_XXX
	 * @return true if changes are detected by the resultset type
	 * @exception SQLException
	 *                if a database-access error occurs.
	 */
	public boolean insertsAreDetected(int type) throws SQLException {
		return false;
	}

	/**
	 * Does a catalog appear at the start of a qualified table name? (Otherwise
	 * it appears at the end)
	 * 
	 * @return true if it appears at the start
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean isCatalogAtStart() throws SQLException {
		return true;
	}

	/**
	 * Is the database in read-only mode?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean isReadOnly() throws SQLException {
		return false;
	}

	/**
	 * @see DatabaseMetaData#locatorsUpdateCopy()
	 */
	public boolean locatorsUpdateCopy() throws SQLException {
		return !this.conn.getEmulateLocators();
	}

	/**
	 * Are concatenations between NULL and non-NULL values NULL? A JDBC
	 * compliant driver always returns true.
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean nullPlusNonNullIsNull() throws SQLException {
		return true;
	}

	/**
	 * Are NULL values sorted at the end regardless of sort order?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean nullsAreSortedAtEnd() throws SQLException {
		return false;
	}

	/**
	 * Are NULL values sorted at the start regardless of sort order?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean nullsAreSortedAtStart() throws SQLException {
		return (this.conn.versionMeetsMinimum(4, 0, 2) && !this.conn
				.versionMeetsMinimum(4, 0, 11));
	}

	/**
	 * Are NULL values sorted high?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean nullsAreSortedHigh() throws SQLException {
		return false;
	}

	/**
	 * Are NULL values sorted low?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean nullsAreSortedLow() throws SQLException {
		return !nullsAreSortedHigh();
	}

	/**
	 * DOCUMENT ME!
	 * 
	 * @param type
	 *            DOCUMENT ME!
	 * @return DOCUMENT ME!
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean othersDeletesAreVisible(int type) throws SQLException {
		return false;
	}

	/**
	 * DOCUMENT ME!
	 * 
	 * @param type
	 *            DOCUMENT ME!
	 * @return DOCUMENT ME!
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean othersInsertsAreVisible(int type) throws SQLException {
		return false;
	}

	/**
	 * JDBC 2.0 Determine whether changes made by others are visible.
	 * 
	 * @param type
	 *            set type, i.e. ResultSet.TYPE_XXX
	 * @return true if changes are visible for the result set type
	 * @exception SQLException
	 *                if a database-access error occurs.
	 */
	public boolean othersUpdatesAreVisible(int type) throws SQLException {
		return false;
	}

	/**
	 * DOCUMENT ME!
	 * 
	 * @param type
	 *            DOCUMENT ME!
	 * @return DOCUMENT ME!
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean ownDeletesAreVisible(int type) throws SQLException {
		return false;
	}

	/**
	 * DOCUMENT ME!
	 * 
	 * @param type
	 *            DOCUMENT ME!
	 * @return DOCUMENT ME!
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean ownInsertsAreVisible(int type) throws SQLException {
		return false;
	}

	/**
	 * JDBC 2.0 Determine whether a result set's own changes visible.
	 * 
	 * @param type
	 *            set type, i.e. ResultSet.TYPE_XXX
	 * @return true if changes are visible for the result set type
	 * @exception SQLException
	 *                if a database-access error occurs.
	 */
	public boolean ownUpdatesAreVisible(int type) throws SQLException {
		return false;
	}

	private LocalAndReferencedColumns parseTableStatusIntoLocalAndReferencedColumns(
			String keysComment) throws SQLException {
		// keys will equal something like this:
		// (parent_service_id child_service_id) REFER
		// ds/subservices(parent_service_id child_service_id)
		//
		// simple-columned keys: (m) REFER
		// airline/tt(a)
		//
		// multi-columned keys : (m n) REFER
		// airline/vv(a b)
		//
		// parse of the string into three phases:
		// 1: parse the opening parentheses to determine how many results there
		// will be
		// 2: read in the schema name/table name
		// 3: parse the closing parentheses

		String columnsDelimitter = ","; // what version did this change in?

		char quoteChar = this.quotedId.length() == 0 ? 0 : this.quotedId
				.charAt(0);

		int indexOfOpenParenLocalColumns = StringUtils
				.indexOfIgnoreCaseRespectQuotes(0, keysComment, "(", quoteChar,
						true);

		if (indexOfOpenParenLocalColumns == -1) {
			throw SQLError.createSQLException("Error parsing foreign keys definition,"
					+ " couldn't find start of local columns list.",
					SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
		}

		String constraintName = removeQuotedId(keysComment.substring(0,
				indexOfOpenParenLocalColumns).trim());
		keysComment = keysComment.substring(indexOfOpenParenLocalColumns,
				keysComment.length());

		String keysCommentTrimmed = keysComment.trim();

		int indexOfCloseParenLocalColumns = StringUtils
				.indexOfIgnoreCaseRespectQuotes(0, keysCommentTrimmed, ")",
						quoteChar, true);

		if (indexOfCloseParenLocalColumns == -1) {
			throw SQLError.createSQLException("Error parsing foreign keys definition,"
					+ " couldn't find end of local columns list.",
					SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
		}

		String localColumnNamesString = keysCommentTrimmed.substring(1,
				indexOfCloseParenLocalColumns);

		int indexOfRefer = StringUtils.indexOfIgnoreCaseRespectQuotes(0,
				keysCommentTrimmed, "REFER ", this.quotedId.charAt(0), true);

		if (indexOfRefer == -1) {
			throw SQLError.createSQLException("Error parsing foreign keys definition,"
					+ " couldn't find start of referenced tables list.",
					SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
		}

		int indexOfOpenParenReferCol = StringUtils
				.indexOfIgnoreCaseRespectQuotes(indexOfRefer,
						keysCommentTrimmed, "(", quoteChar, false);

		if (indexOfOpenParenReferCol == -1) {
			throw SQLError.createSQLException("Error parsing foreign keys definition,"
					+ " couldn't find start of referenced columns list.",
					SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
		}

		String referCatalogTableString = keysCommentTrimmed.substring(
				indexOfRefer + "REFER ".length(), indexOfOpenParenReferCol);

		int indexOfSlash = StringUtils.indexOfIgnoreCaseRespectQuotes(0,
				referCatalogTableString, "/", this.quotedId.charAt(0), false);

		if (indexOfSlash == -1) {
			throw SQLError.createSQLException("Error parsing foreign keys definition,"
					+ " couldn't find name of referenced catalog.",
					SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
		}

		String referCatalog = removeQuotedId(referCatalogTableString.substring(
				0, indexOfSlash));
		String referTable = removeQuotedId(referCatalogTableString.substring(
				indexOfSlash + 1).trim());

		int indexOfCloseParenRefer = StringUtils
				.indexOfIgnoreCaseRespectQuotes(indexOfOpenParenReferCol,
						keysCommentTrimmed, ")", quoteChar, true);

		if (indexOfCloseParenRefer == -1) {
			throw SQLError.createSQLException("Error parsing foreign keys definition,"
					+ " couldn't find end of referenced columns list.",
					SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
		}

		String referColumnNamesString = keysCommentTrimmed.substring(
				indexOfOpenParenReferCol + 1, indexOfCloseParenRefer);

		List referColumnsList = StringUtils.split(referColumnNamesString,
				columnsDelimitter, this.quotedId, this.quotedId, false);
		List localColumnsList = StringUtils.split(localColumnNamesString,
				columnsDelimitter, this.quotedId, this.quotedId, false);

		return new LocalAndReferencedColumns(localColumnsList,
				referColumnsList, constraintName, referCatalog, referTable);
	}

	private String removeQuotedId(String s) {
		if (s == null) {
			return null;
		}

		if (this.quotedId.equals("")) {
			return s;
		}

		s = s.trim();

		int frontOffset = 0;
		int backOffset = s.length();
		int quoteLength = this.quotedId.length();

		if (s.startsWith(this.quotedId)) {
			frontOffset = quoteLength;
		}

		if (s.endsWith(this.quotedId)) {
			backOffset -= quoteLength;
		}

		return s.substring(frontOffset, backOffset);
	}

	/**
	 * Converts the given string to bytes, using the connection's character
	 * encoding, or if not available, the JVM default encoding.
	 * 
	 * @param s
	 *            DOCUMENT ME!
	 * @return DOCUMENT ME!
	 */
	protected byte[] s2b(String s) throws SQLException {
		if (s == null) {
			return null;
		}
		
		return StringUtils.getBytes(s, this.conn.getCharacterSetMetadata(),
				this.conn.getServerCharacterEncoding(), this.conn
						.parserKnowsUnicode(), this.conn, getExceptionInterceptor());
	}

	/**
	 * Does the database store mixed case unquoted SQL identifiers in lower
	 * case?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean storesLowerCaseIdentifiers() throws SQLException {
		return this.conn.storesLowerCaseTableName();
	}

	/**
	 * Does the database store mixed case quoted SQL identifiers in lower case?
	 * A JDBC compliant driver will always return false.
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
		return this.conn.storesLowerCaseTableName();
	}

	/**
	 * Does the database store mixed case unquoted SQL identifiers in mixed
	 * case?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean storesMixedCaseIdentifiers() throws SQLException {
		return !this.conn.storesLowerCaseTableName();
	}
	/**
	 * Does the database store mixed case quoted SQL identifiers in mixed case?
	 * A JDBC compliant driver will always return false.
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
		return !this.conn.storesLowerCaseTableName();
	}

	/**
	 * Does the database store mixed case unquoted SQL identifiers in upper
	 * case?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean storesUpperCaseIdentifiers() throws SQLException {
		return false;
	}

	/**
	 * Does the database store mixed case quoted SQL identifiers in upper case?
	 * A JDBC compliant driver will always return true.
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
		return true; // not actually true, but required by JDBC spec!?
	}

	/**
	 * Is "ALTER TABLE" with add column supported?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsAlterTableWithAddColumn() throws SQLException {
		return true;
	}

	/**
	 * Is "ALTER TABLE" with drop column supported?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsAlterTableWithDropColumn() throws SQLException {
		return true;
	}

	/**
	 * Is the ANSI92 entry level SQL grammar supported? All JDBC compliant
	 * drivers must return true.
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsANSI92EntryLevelSQL() throws SQLException {
		return true;
	}

	/**
	 * Is the ANSI92 full SQL grammar supported?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsANSI92FullSQL() throws SQLException {
		return false;
	}

	/**
	 * Is the ANSI92 intermediate SQL grammar supported?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsANSI92IntermediateSQL() throws SQLException {
		return false;
	}

	/**
	 * JDBC 2.0 Return true if the driver supports batch updates, else return
	 * false.
	 * 
	 * @return DOCUMENT ME!
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsBatchUpdates() throws SQLException {
		return true;
	}

	/**
	 * Can a catalog name be used in a data manipulation statement?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsCatalogsInDataManipulation() throws SQLException {
		// Servers before 3.22 could not do this
		return this.conn.versionMeetsMinimum(3, 22, 0);
	}

	/**
	 * Can a catalog name be used in a index definition statement?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
		// Servers before 3.22 could not do this
		return this.conn.versionMeetsMinimum(3, 22, 0);
	}

	/**
	 * Can a catalog name be used in a privilege definition statement?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
		// Servers before 3.22 could not do this
		return this.conn.versionMeetsMinimum(3, 22, 0);
	}

	/**
	 * Can a catalog name be used in a procedure call statement?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsCatalogsInProcedureCalls() throws SQLException {
		// Servers before 3.22 could not do this
		return this.conn.versionMeetsMinimum(3, 22, 0);
	}

	/**
	 * Can a catalog name be used in a table definition statement?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsCatalogsInTableDefinitions() throws SQLException {
		// Servers before 3.22 could not do this
		return this.conn.versionMeetsMinimum(3, 22, 0);
	}

	/**
	 * Is column aliasing supported?
	 * <P>
	 * If so, the SQL AS clause can be used to provide names for computed
	 * columns or to provide alias names for columns as required. A JDBC
	 * compliant driver always returns true.
	 * </p>
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsColumnAliasing() throws SQLException {
		return true;
	}

	/**
	 * Is the CONVERT function between SQL types supported?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsConvert() throws SQLException {
		return false;
	}

	/**
	 * Is CONVERT between the given SQL types supported?
	 * 
	 * @param fromType
	 *            the type to convert from
	 * @param toType
	 *            the type to convert to
	 * @return true if so
	 * @throws SQLException
	 *             if an error occurs
	 * @see Types
	 */
	public boolean supportsConvert(int fromType, int toType)
			throws SQLException {
		switch (fromType) {
		/*
		 * The char/binary types can be converted to pretty much anything.
		 */
		case java.sql.Types.CHAR:
		case java.sql.Types.VARCHAR:
		case java.sql.Types.LONGVARCHAR:
		case java.sql.Types.BINARY:
		case java.sql.Types.VARBINARY:
		case java.sql.Types.LONGVARBINARY:

			switch (toType) {
			case java.sql.Types.DECIMAL:
			case java.sql.Types.NUMERIC:
			case java.sql.Types.REAL:
			case java.sql.Types.TINYINT:
			case java.sql.Types.SMALLINT:
			case java.sql.Types.INTEGER:
			case java.sql.Types.BIGINT:
			case java.sql.Types.FLOAT:
			case java.sql.Types.DOUBLE:
			case java.sql.Types.CHAR:
			case java.sql.Types.VARCHAR:
			case java.sql.Types.LONGVARCHAR:
			case java.sql.Types.BINARY:
			case java.sql.Types.VARBINARY:
			case java.sql.Types.LONGVARBINARY:
			case java.sql.Types.OTHER:
			case java.sql.Types.DATE:
			case java.sql.Types.TIME:
			case java.sql.Types.TIMESTAMP:
				return true;

			default:
				return false;
			}

		/*
		 * We don't handle the BIT type yet.
		 */
		case java.sql.Types.BIT:
			return false;

		/*
		 * The numeric types. Basically they can convert among themselves, and
		 * with char/binary types.
		 */
		case java.sql.Types.DECIMAL:
		case java.sql.Types.NUMERIC:
		case java.sql.Types.REAL:
		case java.sql.Types.TINYINT:
		case java.sql.Types.SMALLINT:
		case java.sql.Types.INTEGER:
		case java.sql.Types.BIGINT:
		case java.sql.Types.FLOAT:
		case java.sql.Types.DOUBLE:

			switch (toType) {
			case java.sql.Types.DECIMAL:
			case java.sql.Types.NUMERIC:
			case java.sql.Types.REAL:
			case java.sql.Types.TINYINT:
			case java.sql.Types.SMALLINT:
			case java.sql.Types.INTEGER:
			case java.sql.Types.BIGINT:
			case java.sql.Types.FLOAT:
			case java.sql.Types.DOUBLE:
			case java.sql.Types.CHAR:
			case java.sql.Types.VARCHAR:
			case java.sql.Types.LONGVARCHAR:
			case java.sql.Types.BINARY:
			case java.sql.Types.VARBINARY:
			case java.sql.Types.LONGVARBINARY:
				return true;

			default:
				return false;
			}

		/* MySQL doesn't support a NULL type. */
		case java.sql.Types.NULL:
			return false;

		/*
		 * With this driver, this will always be a serialized object, so the
		 * char/binary types will work.
		 */
		case java.sql.Types.OTHER:

			switch (toType) {
			case java.sql.Types.CHAR:
			case java.sql.Types.VARCHAR:
			case java.sql.Types.LONGVARCHAR:
			case java.sql.Types.BINARY:
			case java.sql.Types.VARBINARY:
			case java.sql.Types.LONGVARBINARY:
				return true;

			default:
				return false;
			}

		/* Dates can be converted to char/binary types. */
		case java.sql.Types.DATE:

			switch (toType) {
			case java.sql.Types.CHAR:
			case java.sql.Types.VARCHAR:
			case java.sql.Types.LONGVARCHAR:
			case java.sql.Types.BINARY:
			case java.sql.Types.VARBINARY:
			case java.sql.Types.LONGVARBINARY:
				return true;

			default:
				return false;
			}

		/* Time can be converted to char/binary types */
		case java.sql.Types.TIME:

			switch (toType) {
			case java.sql.Types.CHAR:
			case java.sql.Types.VARCHAR:
			case java.sql.Types.LONGVARCHAR:
			case java.sql.Types.BINARY:
			case java.sql.Types.VARBINARY:
			case java.sql.Types.LONGVARBINARY:
				return true;

			default:
				return false;
			}

		/*
		 * Timestamp can be converted to char/binary types and date/time types
		 * (with loss of precision).
		 */
		case java.sql.Types.TIMESTAMP:

			switch (toType) {
			case java.sql.Types.CHAR:
			case java.sql.Types.VARCHAR:
			case java.sql.Types.LONGVARCHAR:
			case java.sql.Types.BINARY:
			case java.sql.Types.VARBINARY:
			case java.sql.Types.LONGVARBINARY:
			case java.sql.Types.TIME:
			case java.sql.Types.DATE:
				return true;

			default:
				return false;
			}

		/* We shouldn't get here! */
		default:
			return false; // not sure
		}
	}

	/**
	 * Is the ODBC Core SQL grammar supported?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsCoreSQLGrammar() throws SQLException {
		return true;
	}

	/**
	 * Are correlated subqueries supported? A JDBC compliant driver always
	 * returns true.
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsCorrelatedSubqueries() throws SQLException {
		return this.conn.versionMeetsMinimum(4, 1, 0);
	}

	/**
	 * Are both data definition and data manipulation statements within a
	 * transaction supported?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsDataDefinitionAndDataManipulationTransactions()
			throws SQLException {
		return false;
	}

	/**
	 * Are only data manipulation statements within a transaction supported?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsDataManipulationTransactionsOnly()
			throws SQLException {
		return false;
	}

	/**
	 * If table correlation names are supported, are they restricted to be
	 * different from the names of the tables? A JDBC compliant driver always
	 * returns true.
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsDifferentTableCorrelationNames() throws SQLException {
		return true;
	}

	/**
	 * Are expressions in "ORDER BY" lists supported?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsExpressionsInOrderBy() throws SQLException {
		return true;
	}

	/**
	 * Is the ODBC Extended SQL grammar supported?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsExtendedSQLGrammar() throws SQLException {
		return false;
	}

	/**
	 * Are full nested outer joins supported?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsFullOuterJoins() throws SQLException {
		return false;
	}

	/**
	 * JDBC 3.0
	 * 
	 * @return DOCUMENT ME!
	 */
	public boolean supportsGetGeneratedKeys() {
		return true;
	}

	/**
	 * Is some form of "GROUP BY" clause supported?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsGroupBy() throws SQLException {
		return true;
	}

	/**
	 * Can a "GROUP BY" clause add columns not in the SELECT provided it
	 * specifies all the columns in the SELECT?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsGroupByBeyondSelect() throws SQLException {
		return true;
	}

	/**
	 * Can a "GROUP BY" clause use columns not in the SELECT?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsGroupByUnrelated() throws SQLException {
		return true;
	}

	/**
	 * Is the SQL Integrity Enhancement Facility supported?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsIntegrityEnhancementFacility() throws SQLException {
		if (!this.conn.getOverrideSupportsIntegrityEnhancementFacility()) {
			return false;
		} 
		
		return true;
	}

	/**
	 * Is the escape character in "LIKE" clauses supported? A JDBC compliant
	 * driver always returns true.
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsLikeEscapeClause() throws SQLException {
		return true;
	}

	/**
	 * Is there limited support for outer joins? (This will be true if
	 * supportFullOuterJoins is true.)
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsLimitedOuterJoins() throws SQLException {
		return true;
	}

	/**
	 * Is the ODBC Minimum SQL grammar supported? All JDBC compliant drivers
	 * must return true.
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsMinimumSQLGrammar() throws SQLException {
		return true;
	}

	/**
	 * Does the database support mixed case unquoted SQL identifiers?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsMixedCaseIdentifiers() throws SQLException {
		return !this.conn.lowerCaseTableNames();
	}

	/**
	 * Does the database support mixed case quoted SQL identifiers? A JDBC
	 * compliant driver will always return true.
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
		return !this.conn.lowerCaseTableNames();
	}

	/**
	 * @see DatabaseMetaData#supportsMultipleOpenResults()
	 */
	public boolean supportsMultipleOpenResults() throws SQLException {
		return true;
	}

	/**
	 * Are multiple ResultSets from a single execute supported?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsMultipleResultSets() throws SQLException {
		return false;
	}

	/**
	 * Can we have multiple transactions open at once (on different
	 * connections)?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsMultipleTransactions() throws SQLException {
		return true;
	}

	/**
	 * @see DatabaseMetaData#supportsNamedParameters()
	 */
	public boolean supportsNamedParameters() throws SQLException {
		return false;
	}

	/**
	 * Can columns be defined as non-nullable? A JDBC compliant driver always
	 * returns true.
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsNonNullableColumns() throws SQLException {
		return true;
	}

	/**
	 * Can cursors remain open across commits?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             if a database access error occurs
	 * @see Connection#disableAutoClose
	 */
	public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
		return false;
	}

	/**
	 * Can cursors remain open across rollbacks?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             if an error occurs
	 * @see Connection#disableAutoClose
	 */
	public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
		return false;
	}

	/**
	 * Can statements remain open across commits?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             if an error occurs
	 * @see Connection#disableAutoClose
	 */
	public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
		return false;
	}

	/**
	 * Can statements remain open across rollbacks?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             if an error occurs
	 * @see Connection#disableAutoClose
	 */
	public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
		return false;
	}

	/**
	 * Can an "ORDER BY" clause use columns not in the SELECT?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsOrderByUnrelated() throws SQLException {
		return false;
	}

	/**
	 * Is some form of outer join supported?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsOuterJoins() throws SQLException {
		return true;
	}

	/**
	 * Is positioned DELETE supported?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsPositionedDelete() throws SQLException {
		return false;
	}

	/**
	 * Is positioned UPDATE supported?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsPositionedUpdate() throws SQLException {
		return false;
	}

	/**
	 * JDBC 2.0 Does the database support the concurrency type in combination
	 * with the given result set type?
	 * 
	 * @param type
	 *            defined in java.sql.ResultSet
	 * @param concurrency
	 *            type defined in java.sql.ResultSet
	 * @return true if so
	 * @exception SQLException
	 *                if a database-access error occurs.
	 * @see Connection
	 */
	public boolean supportsResultSetConcurrency(int type, int concurrency)
			throws SQLException {
		switch (type) {
		case ResultSet.TYPE_SCROLL_INSENSITIVE:
			if ((concurrency == ResultSet.CONCUR_READ_ONLY)
					|| (concurrency == ResultSet.CONCUR_UPDATABLE)) {
				return true;
			} else {
				throw SQLError.createSQLException(
						"Illegal arguments to supportsResultSetConcurrency()",
						SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
			}
		case ResultSet.TYPE_FORWARD_ONLY:
			if ((concurrency == ResultSet.CONCUR_READ_ONLY)
					|| (concurrency == ResultSet.CONCUR_UPDATABLE)) {
				return true;
			} else {
				throw SQLError.createSQLException(
						"Illegal arguments to supportsResultSetConcurrency()",
						SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
			}
		case ResultSet.TYPE_SCROLL_SENSITIVE:
			return false;
		default:
			throw SQLError.createSQLException(
					"Illegal arguments to supportsResultSetConcurrency()",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
		}

	}

	/**
	 * @see DatabaseMetaData#supportsResultSetHoldability(int)
	 */
	public boolean supportsResultSetHoldability(int holdability)
			throws SQLException {
		return (holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT);
	}

	/**
	 * JDBC 2.0 Does the database support the given result set type?
	 * 
	 * @param type
	 *            defined in java.sql.ResultSet
	 * @return true if so
	 * @exception SQLException
	 *                if a database-access error occurs.
	 * @see Connection
	 */
	public boolean supportsResultSetType(int type) throws SQLException {
		return (type == ResultSet.TYPE_SCROLL_INSENSITIVE);
	}

	/**
	 * @see DatabaseMetaData#supportsSavepoints()
	 */
	public boolean supportsSavepoints() throws SQLException {

		return (this.conn.versionMeetsMinimum(4, 0, 14) || this.conn
				.versionMeetsMinimum(4, 1, 1));
	}

	/**
	 * Can a schema name be used in a data manipulation statement?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsSchemasInDataManipulation() throws SQLException {
		return false;
	}

	/**
	 * Can a schema name be used in an index definition statement?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsSchemasInIndexDefinitions() throws SQLException {
		return false;
	}

	/**
	 * Can a schema name be used in a privilege definition statement?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
		return false;
	}

	/**
	 * Can a schema name be used in a procedure call statement?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsSchemasInProcedureCalls() throws SQLException {
		return false;
	}

	/**
	 * Can a schema name be used in a table definition statement?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsSchemasInTableDefinitions() throws SQLException {
		return false;
	}

	/**
	 * Is SELECT for UPDATE supported?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsSelectForUpdate() throws SQLException {
		return this.conn.versionMeetsMinimum(4, 0, 0);
	}

	/**
	 * @see DatabaseMetaData#supportsStatementPooling()
	 */
	public boolean supportsStatementPooling() throws SQLException {
		return false;
	}

	/**
	 * Are stored procedure calls using the stored procedure escape syntax
	 * supported?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsStoredProcedures() throws SQLException {
		return this.conn.versionMeetsMinimum(5, 0, 0);
	}

	/**
	 * Are subqueries in comparison expressions supported? A JDBC compliant
	 * driver always returns true.
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsSubqueriesInComparisons() throws SQLException {
		return this.conn.versionMeetsMinimum(4, 1, 0);
	}

	/**
	 * Are subqueries in exists expressions supported? A JDBC compliant driver
	 * always returns true.
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsSubqueriesInExists() throws SQLException {
		return this.conn.versionMeetsMinimum(4, 1, 0);
	}

	/**
	 * Are subqueries in "in" statements supported? A JDBC compliant driver
	 * always returns true.
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsSubqueriesInIns() throws SQLException {
		return this.conn.versionMeetsMinimum(4, 1, 0);
	}

	/**
	 * Are subqueries in quantified expressions supported? A JDBC compliant
	 * driver always returns true.
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsSubqueriesInQuantifieds() throws SQLException {
		return this.conn.versionMeetsMinimum(4, 1, 0);
	}

	/**
	 * Are table correlation names supported? A JDBC compliant driver always
	 * returns true.
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsTableCorrelationNames() throws SQLException {
		return true;
	}

	/**
	 * Does the database support the given transaction isolation level?
	 * 
	 * @param level
	 *            the values are defined in java.sql.Connection
	 * @return true if so
	 * @throws SQLException
	 *             if a database access error occurs
	 * @see Connection
	 */
	public boolean supportsTransactionIsolationLevel(int level)
			throws SQLException {
		if (this.conn.supportsIsolationLevel()) {
			switch (level) {
			case java.sql.Connection.TRANSACTION_READ_COMMITTED:
			case java.sql.Connection.TRANSACTION_READ_UNCOMMITTED:
			case java.sql.Connection.TRANSACTION_REPEATABLE_READ:
			case java.sql.Connection.TRANSACTION_SERIALIZABLE:
				return true;

			default:
				return false;
			}
		}

		return false;
	}

	/**
	 * Are transactions supported? If not, commit is a noop and the isolation
	 * level is TRANSACTION_NONE.
	 * 
	 * @return true if transactions are supported
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsTransactions() throws SQLException {
		return this.conn.supportsTransactions();
	}

	/**
	 * Is SQL UNION supported? A JDBC compliant driver always returns true.
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsUnion() throws SQLException {
		return this.conn.versionMeetsMinimum(4, 0, 0);
	}

	/**
	 * Is SQL UNION ALL supported? A JDBC compliant driver always returns true.
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean supportsUnionAll() throws SQLException {
		return this.conn.versionMeetsMinimum(4, 0, 0);
	}

	/**
	 * JDBC 2.0 Determine whether or not a visible row update can be detected by
	 * calling ResultSet.rowUpdated().
	 * 
	 * @param type
	 *            set type, i.e. ResultSet.TYPE_XXX
	 * @return true if changes are detected by the resultset type
	 * @exception SQLException
	 *                if a database-access error occurs.
	 */
	public boolean updatesAreDetected(int type) throws SQLException {
		return false;
	}

	/**
	 * Does the database use a file for each table?
	 * 
	 * @return true if the database uses a local file for each table
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean usesLocalFilePerTable() throws SQLException {
		return false;
	}

	/**
	 * Does the database store tables in a local file?
	 * 
	 * @return true if so
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public boolean usesLocalFiles() throws SQLException {
		return false;
	}
	
	//
	// JDBC-4.0 functions that aren't reliant on Java6
    /**
     * Retrieves a description of the given catalog's system or user 
     * function parameters and return type.
     *
 	 * @see java.sql.DatabaseMetaData#getFunctionColumns(String, String, String, String)
     * @since 1.6
     */
    public ResultSet getFunctionColumns(String catalog, 
    		String schemaPattern, 
    		String functionNamePattern, 
    		String columnNamePattern) throws SQLException {
    	Field[] fields = createFunctionColumnsFields();

		return getProcedureOrFunctionColumns(
				fields, catalog, schemaPattern,
				functionNamePattern, columnNamePattern,
				false, true);
	}

	protected Field[] createFunctionColumnsFields() {
		Field[] fields = {
    			new Field("", "FUNCTION_CAT", Types.VARCHAR, 0),
    			new Field("", "FUNCTION_SCHEM", Types.VARCHAR, 0),
    			new Field("", "FUNCTION_NAME", Types.VARCHAR, 0),
    			new Field("", "COLUMN_NAME", Types.VARCHAR, 0),
    			new Field("", "COLUMN_TYPE", Types.VARCHAR, 0),
    			new Field("", "DATA_TYPE", Types.SMALLINT, 0),
    			new Field("", "TYPE_NAME", Types.VARCHAR, 0),
    			new Field("", "PRECISION", Types.INTEGER, 0),
    			new Field("", "LENGTH", Types.INTEGER, 0),
    			new Field("", "SCALE", Types.SMALLINT, 0),
    			new Field("", "RADIX", Types.SMALLINT, 0),
    			new Field("", "NULLABLE", Types.SMALLINT, 0),
    			new Field("", "REMARKS", Types.VARCHAR, 0),
    			new Field("", "CHAR_OCTET_LENGTH", Types.INTEGER, 0),
    			new Field("", "ORDINAL_POSITION", Types.INTEGER, 0),
    			new Field("", "IS_NULLABLE", Types.VARCHAR, 3),
    			new Field("", "SPECIFIC_NAME", Types.VARCHAR, 0)};
		return fields;
	}

	public boolean providesQueryObjectGenerator() throws SQLException {
		return false;
	}
	
	public ResultSet getSchemas(String catalog, 
			String schemaPattern) throws SQLException {
		Field[] fields = {
				new Field("", "TABLE_SCHEM", Types.VARCHAR, 255),
				new Field("", "TABLE_CATALOG", Types.VARCHAR, 255)
		};
		
		return buildResultSet(fields, new ArrayList());
	}

	public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
		return true;
	}
	
	/**
	 * Get a prepared statement to query information_schema tables.
	 * 
	 * @return PreparedStatement
	 * @throws SQLException
	 */
	protected PreparedStatement prepareMetaDataSafeStatement(String sql)
			throws SQLException {
		// Can't use server-side here as we coerce a lot of types to match
		// the spec.
		PreparedStatement pStmt = (PreparedStatement) this.conn
				.clientPrepareStatement(sql);

		if (pStmt.getMaxRows() != 0) {
			pStmt.setMaxRows(0);
		}

		pStmt.setHoldResultsOpenOverClose(true);

		return pStmt;
	}
}
