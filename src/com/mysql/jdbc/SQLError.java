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

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.BindException;
import java.sql.DataTruncation;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import com.mysql.jdbc.exceptions.MySQLDataException;
import com.mysql.jdbc.exceptions.MySQLIntegrityConstraintViolationException;
import com.mysql.jdbc.exceptions.MySQLNonTransientConnectionException;
import com.mysql.jdbc.exceptions.MySQLSyntaxErrorException;
import com.mysql.jdbc.exceptions.MySQLTransactionRollbackException;
import com.mysql.jdbc.exceptions.MySQLTransientConnectionException;

/**
 * SQLError is a utility class that maps MySQL error codes to X/Open error codes
 * as is required by the JDBC spec.
 * 
 * @author Mark Matthews <mmatthew_at_worldserver.com>
 * @version $Id: SQLError.java 5122 2006-04-03 15:37:11 +0000 (Mon, 03 Apr 2006)
 *          mmatthews $
 */
public class SQLError {
	static final int ER_WARNING_NOT_COMPLETE_ROLLBACK = 1196;

	private static Map mysqlToSql99State;

	private static Map mysqlToSqlState;

	public static final String SQL_STATE_BASE_TABLE_NOT_FOUND = "S0002"; //$NON-NLS-1$

	public static final String SQL_STATE_BASE_TABLE_OR_VIEW_ALREADY_EXISTS = "S0001"; //$NON-NLS-1$

	public static final String SQL_STATE_BASE_TABLE_OR_VIEW_NOT_FOUND = "42S02"; //$NON-NLS-1$

	public static final String SQL_STATE_COLUMN_ALREADY_EXISTS = "S0021"; //$NON-NLS-1$

	public static final String SQL_STATE_COLUMN_NOT_FOUND = "S0022"; //$NON-NLS-1$

	public static final String SQL_STATE_COMMUNICATION_LINK_FAILURE = "08S01"; //$NON-NLS-1$

	public static final String SQL_STATE_CONNECTION_FAIL_DURING_TX = "08007"; //$NON-NLS-1$

	public static final String SQL_STATE_CONNECTION_IN_USE = "08002"; //$NON-NLS-1$

	public static final String SQL_STATE_CONNECTION_NOT_OPEN = "08003"; //$NON-NLS-1$

	public static final String SQL_STATE_CONNECTION_REJECTED = "08004"; //$NON-NLS-1$

	public static final String SQL_STATE_DATE_TRUNCATED = "01004"; //$NON-NLS-1$

	public static final String SQL_STATE_DATETIME_FIELD_OVERFLOW = "22008"; //$NON-NLS-1$

	public static final String SQL_STATE_DEADLOCK = "41000"; //$NON-NLS-1$

	public static final String SQL_STATE_DISCONNECT_ERROR = "01002"; //$NON-NLS-1$

	public static final String SQL_STATE_DIVISION_BY_ZERO = "22012"; //$NON-NLS-1$

	public static final String SQL_STATE_DRIVER_NOT_CAPABLE = "S1C00"; //$NON-NLS-1$

	public static final String SQL_STATE_ERROR_IN_ROW = "01S01"; //$NON-NLS-1$

	public static final String SQL_STATE_GENERAL_ERROR = "S1000"; //$NON-NLS-1$

	public static final String SQL_STATE_ILLEGAL_ARGUMENT = "S1009"; //$NON-NLS-1$

	public static final String SQL_STATE_INDEX_ALREADY_EXISTS = "S0011"; //$NON-NLS-1$

	public static final String SQL_STATE_INDEX_NOT_FOUND = "S0012"; //$NON-NLS-1$

	public static final String SQL_STATE_INSERT_VALUE_LIST_NO_MATCH_COL_LIST = "21S01"; //$NON-NLS-1$

	public static final String SQL_STATE_INVALID_AUTH_SPEC = "28000"; //$NON-NLS-1$

	public static final String SQL_STATE_INVALID_CHARACTER_VALUE_FOR_CAST = "22018"; // $NON_NLS-1$

	public static final String SQL_STATE_INVALID_COLUMN_NUMBER = "S1002"; //$NON-NLS-1$

	public static final String SQL_STATE_INVALID_CONNECTION_ATTRIBUTE = "01S00"; //$NON-NLS-1$

	public static final String SQL_STATE_MEMORY_ALLOCATION_FAILURE = "S1001"; //$NON-NLS-1$

	public static final String SQL_STATE_MORE_THAN_ONE_ROW_UPDATED_OR_DELETED = "01S04"; //$NON-NLS-1$

	public static final String SQL_STATE_NO_DEFAULT_FOR_COLUMN = "S0023"; //$NON-NLS-1$

	public static final String SQL_STATE_NO_ROWS_UPDATED_OR_DELETED = "01S03"; //$NON-NLS-1$

	public static final String SQL_STATE_NUMERIC_VALUE_OUT_OF_RANGE = "22003"; //$NON-NLS-1$

	public static final String SQL_STATE_PRIVILEGE_NOT_REVOKED = "01006"; //$NON-NLS-1$

	public static final String SQL_STATE_SYNTAX_ERROR = "42000"; //$NON-NLS-1$

	public static final String SQL_STATE_TIMEOUT_EXPIRED = "S1T00"; //$NON-NLS-1$

	public static final String SQL_STATE_TRANSACTION_RESOLUTION_UNKNOWN = "08007"; // $NON_NLS-1$

	public static final String SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE = "08001"; //$NON-NLS-1$

	public static final String SQL_STATE_WRONG_NO_OF_PARAMETERS = "07001"; //$NON-NLS-1$

	public static final String SQL_STATE_INVALID_TRANSACTION_TERMINATION = "2D000"; // $NON_NLS-1$

	private static Map sqlStateMessages;

	private static final long DEFAULT_WAIT_TIMEOUT_SECONDS = 28800;

	private static final int DUE_TO_TIMEOUT_FALSE = 0;

	private static final int DUE_TO_TIMEOUT_MAYBE = 2;

	private static final int DUE_TO_TIMEOUT_TRUE = 1;
	
	private static final Constructor JDBC_4_COMMUNICATIONS_EXCEPTION_CTOR;
	
	private static Method THROWABLE_INIT_CAUSE_METHOD;
	
	static {
		if (Util.isJdbc4()) {
			try {
				JDBC_4_COMMUNICATIONS_EXCEPTION_CTOR = Class.forName(
						"com.mysql.jdbc.exceptions.jdbc4.CommunicationsException")
						.getConstructor(
								new Class[] { ConnectionImpl.class, Long.TYPE, Long.TYPE, Exception.class });
			} catch (SecurityException e) {
				throw new RuntimeException(e);
			} catch (NoSuchMethodException e) {
				throw new RuntimeException(e);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		} else {
			JDBC_4_COMMUNICATIONS_EXCEPTION_CTOR = null;
		}
		
		try {
			THROWABLE_INIT_CAUSE_METHOD = Throwable.class.getMethod("initCause", new Class[] {Throwable.class});
		} catch (Throwable t) {
			// we're not on a VM that has it
			THROWABLE_INIT_CAUSE_METHOD = null;
		}
		
		sqlStateMessages = new HashMap();
		sqlStateMessages.put(SQL_STATE_DISCONNECT_ERROR, Messages
				.getString("SQLError.35")); //$NON-NLS-1$
		sqlStateMessages.put(SQL_STATE_DATE_TRUNCATED, Messages
				.getString("SQLError.36")); //$NON-NLS-1$
		sqlStateMessages.put(SQL_STATE_PRIVILEGE_NOT_REVOKED, Messages
				.getString("SQLError.37")); //$NON-NLS-1$
		sqlStateMessages.put(SQL_STATE_INVALID_CONNECTION_ATTRIBUTE, Messages
				.getString("SQLError.38")); //$NON-NLS-1$
		sqlStateMessages.put(SQL_STATE_ERROR_IN_ROW, Messages
				.getString("SQLError.39")); //$NON-NLS-1$
		sqlStateMessages.put(SQL_STATE_NO_ROWS_UPDATED_OR_DELETED, Messages
				.getString("SQLError.40")); //$NON-NLS-1$
		sqlStateMessages.put(SQL_STATE_MORE_THAN_ONE_ROW_UPDATED_OR_DELETED,
				Messages.getString("SQLError.41")); //$NON-NLS-1$
		sqlStateMessages.put(SQL_STATE_WRONG_NO_OF_PARAMETERS, Messages
				.getString("SQLError.42")); //$NON-NLS-1$
		sqlStateMessages.put(SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE,
				Messages.getString("SQLError.43")); //$NON-NLS-1$
		sqlStateMessages.put(SQL_STATE_CONNECTION_IN_USE, Messages
				.getString("SQLError.44")); //$NON-NLS-1$
		sqlStateMessages.put(SQL_STATE_CONNECTION_NOT_OPEN, Messages
				.getString("SQLError.45")); //$NON-NLS-1$
		sqlStateMessages.put(SQL_STATE_CONNECTION_REJECTED, Messages
				.getString("SQLError.46")); //$NON-NLS-1$
		sqlStateMessages.put(SQL_STATE_CONNECTION_FAIL_DURING_TX, Messages
				.getString("SQLError.47")); //$NON-NLS-1$
		sqlStateMessages.put(SQL_STATE_COMMUNICATION_LINK_FAILURE, Messages
				.getString("SQLError.48")); //$NON-NLS-1$
		sqlStateMessages.put(SQL_STATE_INSERT_VALUE_LIST_NO_MATCH_COL_LIST,
				Messages.getString("SQLError.49")); //$NON-NLS-1$
		sqlStateMessages.put(SQL_STATE_NUMERIC_VALUE_OUT_OF_RANGE, Messages
				.getString("SQLError.50")); //$NON-NLS-1$
		sqlStateMessages.put(SQL_STATE_DATETIME_FIELD_OVERFLOW, Messages
				.getString("SQLError.51")); //$NON-NLS-1$
		sqlStateMessages.put(SQL_STATE_DIVISION_BY_ZERO, Messages
				.getString("SQLError.52")); //$NON-NLS-1$
		sqlStateMessages.put(SQL_STATE_DEADLOCK, Messages
				.getString("SQLError.53")); //$NON-NLS-1$
		sqlStateMessages.put(SQL_STATE_INVALID_AUTH_SPEC, Messages
				.getString("SQLError.54")); //$NON-NLS-1$
		sqlStateMessages.put(SQL_STATE_SYNTAX_ERROR, Messages
				.getString("SQLError.55")); //$NON-NLS-1$
		sqlStateMessages.put(SQL_STATE_BASE_TABLE_OR_VIEW_NOT_FOUND, Messages
				.getString("SQLError.56")); //$NON-NLS-1$
		sqlStateMessages.put(SQL_STATE_BASE_TABLE_OR_VIEW_ALREADY_EXISTS,
				Messages.getString("SQLError.57")); //$NON-NLS-1$
		sqlStateMessages.put(SQL_STATE_BASE_TABLE_NOT_FOUND, Messages
				.getString("SQLError.58")); //$NON-NLS-1$
		sqlStateMessages.put(SQL_STATE_INDEX_ALREADY_EXISTS, Messages
				.getString("SQLError.59")); //$NON-NLS-1$
		sqlStateMessages.put(SQL_STATE_INDEX_NOT_FOUND, Messages
				.getString("SQLError.60")); //$NON-NLS-1$
		sqlStateMessages.put(SQL_STATE_COLUMN_ALREADY_EXISTS, Messages
				.getString("SQLError.61")); //$NON-NLS-1$
		sqlStateMessages.put(SQL_STATE_COLUMN_NOT_FOUND, Messages
				.getString("SQLError.62")); //$NON-NLS-1$
		sqlStateMessages.put(SQL_STATE_NO_DEFAULT_FOR_COLUMN, Messages
				.getString("SQLError.63")); //$NON-NLS-1$
		sqlStateMessages.put(SQL_STATE_GENERAL_ERROR, Messages
				.getString("SQLError.64")); //$NON-NLS-1$
		sqlStateMessages.put(SQL_STATE_MEMORY_ALLOCATION_FAILURE, Messages
				.getString("SQLError.65")); //$NON-NLS-1$
		sqlStateMessages.put(SQL_STATE_INVALID_COLUMN_NUMBER, Messages
				.getString("SQLError.66")); //$NON-NLS-1$
		sqlStateMessages.put(SQL_STATE_ILLEGAL_ARGUMENT, Messages
				.getString("SQLError.67")); //$NON-NLS-1$
		sqlStateMessages.put(SQL_STATE_DRIVER_NOT_CAPABLE, Messages
				.getString("SQLError.68")); //$NON-NLS-1$
		sqlStateMessages.put(SQL_STATE_TIMEOUT_EXPIRED, Messages
				.getString("SQLError.69")); //$NON-NLS-1$

		mysqlToSqlState = new Hashtable();

		//
		// Communications Errors
		//
		// ER_CON_COUNT_ERROR 1040
		// ER_BAD_HOST_ERROR 1042
		// ER_HANDSHAKE_ERROR 1043
		// ER_UNKNOWN_COM_ERROR 1047
		// ER_IPSOCK_ERROR 1081
		//
		mysqlToSqlState.put(Constants.integerValueOf(1040), SQL_STATE_CONNECTION_REJECTED);
		mysqlToSqlState.put(Constants.integerValueOf(1042), SQL_STATE_CONNECTION_REJECTED);
		mysqlToSqlState.put(Constants.integerValueOf(1043), SQL_STATE_CONNECTION_REJECTED);
		mysqlToSqlState.put(Constants.integerValueOf(1047),
				SQL_STATE_COMMUNICATION_LINK_FAILURE);
		mysqlToSqlState.put(Constants.integerValueOf(1081),
				SQL_STATE_COMMUNICATION_LINK_FAILURE);

		// ER_HOST_IS_BLOCKED 1129
		// ER_HOST_NOT_PRIVILEGED 1130
		mysqlToSqlState.put(Constants.integerValueOf(1129), SQL_STATE_CONNECTION_REJECTED);
		mysqlToSqlState.put(Constants.integerValueOf(1130), SQL_STATE_CONNECTION_REJECTED);

		//
		// Authentication Errors
		//
		// ER_ACCESS_DENIED_ERROR 1045
		//
		mysqlToSqlState.put(Constants.integerValueOf(1045), SQL_STATE_INVALID_AUTH_SPEC);

		//
		// Resource errors
		//
		// ER_CANT_CREATE_FILE 1004
		// ER_CANT_CREATE_TABLE 1005
		// ER_CANT_LOCK 1015
		// ER_DISK_FULL 1021
		// ER_CON_COUNT_ERROR 1040
		// ER_OUT_OF_RESOURCES 1041
		//
		// Out-of-memory errors
		//
		// ER_OUTOFMEMORY 1037
		// ER_OUT_OF_SORTMEMORY 1038
		//
		mysqlToSqlState.put(Constants.integerValueOf(1037),
				SQL_STATE_MEMORY_ALLOCATION_FAILURE);
		mysqlToSqlState.put(Constants.integerValueOf(1038),
				SQL_STATE_MEMORY_ALLOCATION_FAILURE);

		//
		// Syntax Errors
		//
		// ER_PARSE_ERROR 1064
		// ER_EMPTY_QUERY 1065
		//
		mysqlToSqlState.put(Constants.integerValueOf(1064), SQL_STATE_SYNTAX_ERROR);
		mysqlToSqlState.put(Constants.integerValueOf(1065), SQL_STATE_SYNTAX_ERROR);

		//
		// Invalid argument errors
		//
		// ER_WRONG_FIELD_WITH_GROUP 1055
		// ER_WRONG_GROUP_FIELD 1056
		// ER_WRONG_SUM_SELECT 1057
		// ER_TOO_LONG_IDENT 1059
		// ER_DUP_FIELDNAME 1060
		// ER_DUP_KEYNAME 1061
		// ER_DUP_ENTRY 1062
		// ER_WRONG_FIELD_SPEC 1063
		// ER_NONUNIQ_TABLE 1066
		// ER_INVALID_DEFAULT 1067
		// ER_MULTIPLE_PRI_KEY 1068
		// ER_TOO_MANY_KEYS 1069
		// ER_TOO_MANY_KEY_PARTS 1070
		// ER_TOO_LONG_KEY 1071
		// ER_KEY_COLUMN_DOES_NOT_EXIST 1072
		// ER_BLOB_USED_AS_KEY 1073
		// ER_TOO_BIG_FIELDLENGTH 1074
		// ER_WRONG_AUTO_KEY 1075
		// ER_NO_SUCH_INDEX 1082
		// ER_WRONG_FIELD_TERMINATORS 1083
		// ER_BLOBS_AND_NO_TERMINATED 1084
		//
		mysqlToSqlState.put(Constants.integerValueOf(1055), SQL_STATE_ILLEGAL_ARGUMENT);
		mysqlToSqlState.put(Constants.integerValueOf(1056), SQL_STATE_ILLEGAL_ARGUMENT);
		mysqlToSqlState.put(Constants.integerValueOf(1057), SQL_STATE_ILLEGAL_ARGUMENT);
		mysqlToSqlState.put(Constants.integerValueOf(1059), SQL_STATE_ILLEGAL_ARGUMENT);
		mysqlToSqlState.put(Constants.integerValueOf(1060), SQL_STATE_ILLEGAL_ARGUMENT);
		mysqlToSqlState.put(Constants.integerValueOf(1061), SQL_STATE_ILLEGAL_ARGUMENT);
		mysqlToSqlState.put(Constants.integerValueOf(1062), SQL_STATE_ILLEGAL_ARGUMENT);
		mysqlToSqlState.put(Constants.integerValueOf(1063), SQL_STATE_ILLEGAL_ARGUMENT);
		mysqlToSqlState.put(Constants.integerValueOf(1066), SQL_STATE_ILLEGAL_ARGUMENT);
		mysqlToSqlState.put(Constants.integerValueOf(1067), SQL_STATE_ILLEGAL_ARGUMENT);
		mysqlToSqlState.put(Constants.integerValueOf(1068), SQL_STATE_ILLEGAL_ARGUMENT);
		mysqlToSqlState.put(Constants.integerValueOf(1069), SQL_STATE_ILLEGAL_ARGUMENT);
		mysqlToSqlState.put(Constants.integerValueOf(1070), SQL_STATE_ILLEGAL_ARGUMENT);
		mysqlToSqlState.put(Constants.integerValueOf(1071), SQL_STATE_ILLEGAL_ARGUMENT);
		mysqlToSqlState.put(Constants.integerValueOf(1072), SQL_STATE_ILLEGAL_ARGUMENT);
		mysqlToSqlState.put(Constants.integerValueOf(1073), SQL_STATE_ILLEGAL_ARGUMENT);
		mysqlToSqlState.put(Constants.integerValueOf(1074), SQL_STATE_ILLEGAL_ARGUMENT);
		mysqlToSqlState.put(Constants.integerValueOf(1075), SQL_STATE_ILLEGAL_ARGUMENT);
		mysqlToSqlState.put(Constants.integerValueOf(1082), SQL_STATE_ILLEGAL_ARGUMENT);
		mysqlToSqlState.put(Constants.integerValueOf(1083), SQL_STATE_ILLEGAL_ARGUMENT);
		mysqlToSqlState.put(Constants.integerValueOf(1084), SQL_STATE_ILLEGAL_ARGUMENT);

		//
		// ER_WRONG_VALUE_COUNT 1058
		//
		mysqlToSqlState.put(Constants.integerValueOf(1058),
				SQL_STATE_INSERT_VALUE_LIST_NO_MATCH_COL_LIST);

		// ER_CANT_CREATE_DB 1006
		// ER_DB_CREATE_EXISTS 1007
		// ER_DB_DROP_EXISTS 1008
		// ER_DB_DROP_DELETE 1009
		// ER_DB_DROP_RMDIR 1010
		// ER_CANT_DELETE_FILE 1011
		// ER_CANT_FIND_SYSTEM_REC 1012
		// ER_CANT_GET_STAT 1013
		// ER_CANT_GET_WD 1014
		// ER_UNEXPECTED_EOF 1039
		// ER_CANT_OPEN_FILE 1016
		// ER_FILE_NOT_FOUND 1017
		// ER_CANT_READ_DIR 1018
		// ER_CANT_SET_WD 1019
		// ER_CHECKREAD 1020
		// ER_DUP_KEY 1022
		// ER_ERROR_ON_CLOSE 1023
		// ER_ERROR_ON_READ 1024
		// ER_ERROR_ON_RENAME 1025
		// ER_ERROR_ON_WRITE 1026
		// ER_FILE_USED 1027
		// ER_FILSORT_ABORT 1028
		// ER_FORM_NOT_FOUND 1029
		// ER_GET_ERRNO 1030
		// ER_ILLEGAL_HA 1031
		// ER_KEY_NOT_FOUND 1032
		// ER_NOT_FORM_FILE 1033
		// ER_DBACCESS_DENIED_ERROR 1044
		// ER_NO_DB_ERROR 1046
		// ER_BAD_NULL_ERROR 1048
		// ER_BAD_DB_ERROR 1049
		// ER_TABLE_EXISTS_ERROR 1050
		// ER_BAD_TABLE_ERROR 1051
		mysqlToSqlState.put(Constants.integerValueOf(1051),
				SQL_STATE_BASE_TABLE_OR_VIEW_NOT_FOUND);

		// ER_NON_UNIQ_ERROR 1052
		// ER_BAD_FIELD_ERROR 1054
		mysqlToSqlState.put(Constants.integerValueOf(1054), SQL_STATE_COLUMN_NOT_FOUND);

		// ER_TEXTFILE_NOT_READABLE 1085
		// ER_FILE_EXISTS_ERROR 1086
		// ER_LOAD_INFO 1087
		// ER_ALTER_INFO 1088
		// ER_WRONG_SUB_KEY 1089
		// ER_CANT_REMOVE_ALL_FIELDS 1090
		// ER_CANT_DROP_FIELD_OR_KEY 1091
		// ER_INSERT_INFO 1092
		// ER_INSERT_TABLE_USED 1093
		// ER_LOCK_DEADLOCK 1213
		mysqlToSqlState.put(Constants.integerValueOf(1205), SQL_STATE_DEADLOCK);
		mysqlToSqlState.put(Constants.integerValueOf(1213), SQL_STATE_DEADLOCK);

		mysqlToSql99State = new HashMap();

		mysqlToSql99State.put(Constants.integerValueOf(1205), SQL_STATE_DEADLOCK);
		mysqlToSql99State.put(Constants.integerValueOf(1213), SQL_STATE_DEADLOCK);
		mysqlToSql99State.put(Constants.integerValueOf(MysqlErrorNumbers.ER_DUP_KEY),
				"23000");
		mysqlToSql99State.put(Constants.integerValueOf(MysqlErrorNumbers.ER_OUTOFMEMORY),
				"HY001");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_OUT_OF_SORTMEMORY), "HY001");
		mysqlToSql99State.put(
				Constants.integerValueOf(MysqlErrorNumbers.ER_CON_COUNT_ERROR), "08004");
		mysqlToSql99State.put(Constants.integerValueOf(MysqlErrorNumbers.ER_BAD_HOST_ERROR),
				"08S01");
		mysqlToSql99State.put(
				Constants.integerValueOf(MysqlErrorNumbers.ER_HANDSHAKE_ERROR), "08S01");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_DBACCESS_DENIED_ERROR), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_ACCESS_DENIED_ERROR), "28000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_TABLE_EXISTS_ERROR), "42S01");
		mysqlToSql99State.put(
				Constants.integerValueOf(MysqlErrorNumbers.ER_BAD_TABLE_ERROR), "42S02");
		mysqlToSql99State.put(Constants.integerValueOf(MysqlErrorNumbers.ER_NON_UNIQ_ERROR),
				"23000");
		mysqlToSql99State.put(
				Constants.integerValueOf(MysqlErrorNumbers.ER_SERVER_SHUTDOWN), "08S01");
		mysqlToSql99State.put(
				Constants.integerValueOf(MysqlErrorNumbers.ER_BAD_FIELD_ERROR), "42S22");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_WRONG_FIELD_WITH_GROUP), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_WRONG_GROUP_FIELD), "42000");
		mysqlToSql99State.put(
				Constants.integerValueOf(MysqlErrorNumbers.ER_WRONG_SUM_SELECT), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_WRONG_VALUE_COUNT), "21S01");
		mysqlToSql99State.put(Constants.integerValueOf(MysqlErrorNumbers.ER_TOO_LONG_IDENT),
				"42000");
		mysqlToSql99State.put(Constants.integerValueOf(MysqlErrorNumbers.ER_DUP_FIELDNAME),
				"42S21");
		mysqlToSql99State.put(Constants.integerValueOf(MysqlErrorNumbers.ER_DUP_KEYNAME),
				"42000");
		mysqlToSql99State.put(Constants.integerValueOf(MysqlErrorNumbers.ER_DUP_ENTRY),
				"23000");
		mysqlToSql99State.put(
				Constants.integerValueOf(MysqlErrorNumbers.ER_WRONG_FIELD_SPEC), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(MysqlErrorNumbers.ER_PARSE_ERROR),
				"42000");
		mysqlToSql99State.put(Constants.integerValueOf(MysqlErrorNumbers.ER_EMPTY_QUERY),
				"42000");
		mysqlToSql99State.put(Constants.integerValueOf(MysqlErrorNumbers.ER_NONUNIQ_TABLE),
				"42000");
		mysqlToSql99State.put(
				Constants.integerValueOf(MysqlErrorNumbers.ER_INVALID_DEFAULT), "42000");
		mysqlToSql99State.put(
				Constants.integerValueOf(MysqlErrorNumbers.ER_MULTIPLE_PRI_KEY), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(MysqlErrorNumbers.ER_TOO_MANY_KEYS),
				"42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_TOO_MANY_KEY_PARTS), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(MysqlErrorNumbers.ER_TOO_LONG_KEY),
				"42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_KEY_COLUMN_DOES_NOT_EXITS), "42000");
		mysqlToSql99State.put(
				Constants.integerValueOf(MysqlErrorNumbers.ER_BLOB_USED_AS_KEY), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_TOO_BIG_FIELDLENGTH), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(MysqlErrorNumbers.ER_WRONG_AUTO_KEY),
				"42000");
		mysqlToSql99State.put(Constants.integerValueOf(MysqlErrorNumbers.ER_FORCING_CLOSE),
				"08S01");
		mysqlToSql99State.put(Constants.integerValueOf(MysqlErrorNumbers.ER_IPSOCK_ERROR),
				"08S01");
		mysqlToSql99State.put(Constants.integerValueOf(MysqlErrorNumbers.ER_NO_SUCH_INDEX),
				"42S12");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_WRONG_FIELD_TERMINATORS), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_BLOBS_AND_NO_TERMINATED), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_CANT_REMOVE_ALL_FIELDS), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_CANT_DROP_FIELD_OR_KEY), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_BLOB_CANT_HAVE_DEFAULT), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(MysqlErrorNumbers.ER_WRONG_DB_NAME),
				"42000");
		mysqlToSql99State.put(
				Constants.integerValueOf(MysqlErrorNumbers.ER_WRONG_TABLE_NAME), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(MysqlErrorNumbers.ER_TOO_BIG_SELECT),
				"42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_UNKNOWN_PROCEDURE), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_WRONG_PARAMCOUNT_TO_PROCEDURE), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(MysqlErrorNumbers.ER_UNKNOWN_TABLE),
				"42S02");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_FIELD_SPECIFIED_TWICE), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_UNSUPPORTED_EXTENSION), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_TABLE_MUST_HAVE_COLUMNS), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_UNKNOWN_CHARACTER_SET), "42000");
		mysqlToSql99State.put(
				Constants.integerValueOf(MysqlErrorNumbers.ER_TOO_BIG_ROWSIZE), "42000");
		mysqlToSql99State.put(
				Constants.integerValueOf(MysqlErrorNumbers.ER_WRONG_OUTER_JOIN), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_NULL_COLUMN_IN_INDEX), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_PASSWORD_ANONYMOUS_USER), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_PASSWORD_NOT_ALLOWED), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_PASSWORD_NO_MATCH), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_WRONG_VALUE_COUNT_ON_ROW), "21S01");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_INVALID_USE_OF_NULL), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(MysqlErrorNumbers.ER_REGEXP_ERROR),
				"42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_MIX_OF_GROUP_FUNC_AND_FIELDS), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_NONEXISTING_GRANT), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_TABLEACCESS_DENIED_ERROR), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_COLUMNACCESS_DENIED_ERROR), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_ILLEGAL_GRANT_FOR_TABLE), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_GRANT_WRONG_HOST_OR_USER), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(MysqlErrorNumbers.ER_NO_SUCH_TABLE),
				"42S02");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_NONEXISTING_TABLE_GRANT), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_NOT_ALLOWED_COMMAND), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(MysqlErrorNumbers.ER_SYNTAX_ERROR),
				"42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_ABORTING_CONNECTION), "08S01");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_NET_PACKET_TOO_LARGE), "08S01");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_NET_READ_ERROR_FROM_PIPE), "08S01");
		mysqlToSql99State.put(
				Constants.integerValueOf(MysqlErrorNumbers.ER_NET_FCNTL_ERROR), "08S01");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_NET_PACKETS_OUT_OF_ORDER), "08S01");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_NET_UNCOMPRESS_ERROR), "08S01");
		mysqlToSql99State.put(Constants.integerValueOf(MysqlErrorNumbers.ER_NET_READ_ERROR),
				"08S01");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_NET_READ_INTERRUPTED), "08S01");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_NET_ERROR_ON_WRITE), "08S01");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_NET_WRITE_INTERRUPTED), "08S01");
		mysqlToSql99State.put(
				Constants.integerValueOf(MysqlErrorNumbers.ER_TOO_LONG_STRING), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_TABLE_CANT_HANDLE_BLOB), "42000");
		mysqlToSql99State
				.put(Constants.integerValueOf(
						MysqlErrorNumbers.ER_TABLE_CANT_HANDLE_AUTO_INCREMENT),
						"42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_WRONG_COLUMN_NAME), "42000");
		mysqlToSql99State.put(
				Constants.integerValueOf(MysqlErrorNumbers.ER_WRONG_KEY_COLUMN), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(MysqlErrorNumbers.ER_DUP_UNIQUE),
				"23000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_BLOB_KEY_WITHOUT_LENGTH), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_PRIMARY_CANT_HAVE_NULL), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(MysqlErrorNumbers.ER_TOO_MANY_ROWS),
				"42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_REQUIRES_PRIMARY_KEY), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_CHECK_NO_SUCH_TABLE), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_CHECK_NOT_IMPLEMENTED), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_CANT_DO_THIS_DURING_AN_TRANSACTION),
				"25000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_NEW_ABORTING_CONNECTION), "08S01");
		mysqlToSql99State.put(
				Constants.integerValueOf(MysqlErrorNumbers.ER_MASTER_NET_READ), "08S01");
		mysqlToSql99State.put(
				Constants.integerValueOf(MysqlErrorNumbers.ER_MASTER_NET_WRITE), "08S01");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_TOO_MANY_USER_CONNECTIONS), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_READ_ONLY_TRANSACTION), "25000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_NO_PERMISSION_TO_CREATE_USER), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(MysqlErrorNumbers.ER_LOCK_DEADLOCK),
				"40001");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_NO_REFERENCED_ROW), "23000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_ROW_IS_REFERENCED), "23000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_CONNECT_TO_MASTER), "08S01");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT),
				"21000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_USER_LIMIT_REACHED), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(MysqlErrorNumbers.ER_NO_DEFAULT),
				"42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_WRONG_VALUE_FOR_VAR), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_WRONG_TYPE_FOR_VAR), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_CANT_USE_OPTION_HERE), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_NOT_SUPPORTED_YET), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(MysqlErrorNumbers.ER_WRONG_FK_DEF),
				"42000");
		mysqlToSql99State.put(
				Constants.integerValueOf(MysqlErrorNumbers.ER_OPERAND_COLUMNS), "21000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_SUBQUERY_NO_1_ROW), "21000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_ILLEGAL_REFERENCE), "42S22");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_DERIVED_MUST_HAVE_ALIAS), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(MysqlErrorNumbers.ER_SELECT_REDUCED),
				"01000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_TABLENAME_NOT_ALLOWED_HERE), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_NOT_SUPPORTED_AUTH_MODE), "08004");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_SPATIAL_CANT_HAVE_NULL), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_COLLATION_CHARSET_MISMATCH), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_WARN_TOO_FEW_RECORDS), "01000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_WARN_TOO_MANY_RECORDS), "01000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_WARN_NULL_TO_NOTNULL), "01000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_WARN_DATA_OUT_OF_RANGE), "01000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_WARN_DATA_TRUNCATED), "01000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_WRONG_NAME_FOR_INDEX), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_WRONG_NAME_FOR_CATALOG), "42000");
		mysqlToSql99State.put(Constants.integerValueOf(
				MysqlErrorNumbers.ER_UNKNOWN_STORAGE_ENGINE), "42000");
	}

	/**
	 * Turns output of 'SHOW WARNINGS' into JDBC SQLWarning instances.
	 * 
	 * If 'forTruncationOnly' is true, only looks for truncation warnings, and
	 * actually throws DataTruncation as an exception.
	 * 
	 * @param connection
	 *            the connection to use for getting warnings.
	 * 
	 * @return the SQLWarning chain (or null if no warnings)
	 * 
	 * @throws SQLException
	 *             if the warnings could not be retrieved
	 */
	static SQLWarning convertShowWarningsToSQLWarnings(Connection connection)
			throws SQLException {
		return convertShowWarningsToSQLWarnings(connection, 0, false);
	}

	/**
	 * Turns output of 'SHOW WARNINGS' into JDBC SQLWarning instances.
	 * 
	 * If 'forTruncationOnly' is true, only looks for truncation warnings, and
	 * actually throws DataTruncation as an exception.
	 * 
	 * @param connection
	 *            the connection to use for getting warnings.
	 * @param warningCountIfKnown
	 *            the warning count (if known), otherwise set it to 0.
	 * @param forTruncationOnly
	 *            if this method should only scan for data truncation warnings
	 * 
	 * @return the SQLWarning chain (or null if no warnings)
	 * 
	 * @throws SQLException
	 *             if the warnings could not be retrieved, or if data truncation
	 *             is being scanned for and truncations were found.
	 */
	static SQLWarning convertShowWarningsToSQLWarnings(Connection connection,
			int warningCountIfKnown, boolean forTruncationOnly)
			throws SQLException {
		java.sql.Statement stmt = null;
		java.sql.ResultSet warnRs = null;

		SQLWarning currentWarning = null;

		try {
			if (warningCountIfKnown < 100) {
				stmt = connection.createStatement();

				if (stmt.getMaxRows() != 0) {
					stmt.setMaxRows(0);
				}
			} else {
				// stream large warning counts
				stmt = connection.createStatement(
						java.sql.ResultSet.TYPE_FORWARD_ONLY,
						java.sql.ResultSet.CONCUR_READ_ONLY);
				stmt.setFetchSize(Integer.MIN_VALUE);
			}

			/*
			 * +---------+------+---------------------------------------------+ |
			 * Level | Code | Message |
			 * +---------+------+---------------------------------------------+ |
			 * Warning | 1265 | Data truncated for column 'field1' at row 1 |
			 * +---------+------+---------------------------------------------+
			 */
			warnRs = stmt.executeQuery("SHOW WARNINGS"); //$NON-NLS-1$

			while (warnRs.next()) {
				int code = warnRs.getInt("Code"); //$NON-NLS-1$

				if (forTruncationOnly) {
					if (code == 1265 || code == 1264) {
						DataTruncation newTruncation = new MysqlDataTruncation(
								warnRs.getString("Message"), 0, false, false, 0, 0, code); //$NON-NLS-1$

						if (currentWarning == null) {
							currentWarning = newTruncation;
						} else {
							currentWarning.setNextWarning(newTruncation);
						}
					}
				} else {
					String level = warnRs.getString("Level"); //$NON-NLS-1$
					String message = warnRs.getString("Message"); //$NON-NLS-1$

					SQLWarning newWarning = new SQLWarning(message, SQLError
							.mysqlToSqlState(code, connection
									.getUseSqlStateCodes()), code);

					if (currentWarning == null) {
						currentWarning = newWarning;
					} else {
						currentWarning.setNextWarning(newWarning);
					}
				}
			}

			if (forTruncationOnly && (currentWarning != null)) {
				throw currentWarning;
			}

			return currentWarning;
		} finally {
			SQLException reThrow = null;

			if (warnRs != null) {
				try {
					warnRs.close();
				} catch (SQLException sqlEx) {
					reThrow = sqlEx;
				}
			}

			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException sqlEx) {
					// ideally, we'd use chained exceptions here,
					// but we still support JDK-1.2.x with this driver
					// which doesn't have them....
					reThrow = sqlEx;
				}
			}

			if (reThrow != null) {
				throw reThrow;
			}
		}
	}

	public static void dumpSqlStatesMappingsAsXml() throws Exception {
		TreeMap allErrorNumbers = new TreeMap();
		Map mysqlErrorNumbersToNames = new HashMap();

		Integer errorNumber = null;

		// 
		// First create a list of all 'known' error numbers that
		// are mapped.
		//
		for (Iterator mysqlErrorNumbers = mysqlToSql99State.keySet().iterator(); mysqlErrorNumbers
				.hasNext();) {
			errorNumber = (Integer) mysqlErrorNumbers.next();
			allErrorNumbers.put(errorNumber, errorNumber);
		}

		for (Iterator mysqlErrorNumbers = mysqlToSqlState.keySet().iterator(); mysqlErrorNumbers
				.hasNext();) {
			errorNumber = (Integer) mysqlErrorNumbers.next();
			allErrorNumbers.put(errorNumber, errorNumber);
		}

		//
		// Now create a list of the actual MySQL error numbers we know about
		//
		java.lang.reflect.Field[] possibleFields = MysqlErrorNumbers.class
				.getDeclaredFields();

		for (int i = 0; i < possibleFields.length; i++) {
			String fieldName = possibleFields[i].getName();

			if (fieldName.startsWith("ER_")) {
				mysqlErrorNumbersToNames.put(possibleFields[i].get(null),
						fieldName);
			}
		}

		System.out.println("<ErrorMappings>");

		for (Iterator allErrorNumbersIter = allErrorNumbers.keySet().iterator(); allErrorNumbersIter
				.hasNext();) {
			errorNumber = (Integer) allErrorNumbersIter.next();

			String sql92State = mysqlToSql99(errorNumber.intValue());
			String oldSqlState = mysqlToXOpen(errorNumber.intValue());

			System.out.println("   <ErrorMapping mysqlErrorNumber=\""
					+ errorNumber + "\" mysqlErrorName=\""
					+ mysqlErrorNumbersToNames.get(errorNumber)
					+ "\" legacySqlState=\""
					+ ((oldSqlState == null) ? "" : oldSqlState)
					+ "\" sql92SqlState=\""
					+ ((sql92State == null) ? "" : sql92State) + "\"/>");
		}

		System.out.println("</ErrorMappings>");
	}

	static String get(String stateCode) {
		return (String) sqlStateMessages.get(stateCode);
	}

	private static String mysqlToSql99(int errno) {
		Integer err = Constants.integerValueOf(errno);

		if (mysqlToSql99State.containsKey(err)) {
			return (String) mysqlToSql99State.get(err);
		}

		return "HY000";
	}

	/**
	 * Map MySQL error codes to X/Open or SQL-92 error codes
	 * 
	 * @param errno
	 *            the MySQL error code
	 * 
	 * @return the corresponding X/Open or SQL-92 error code
	 */
	static String mysqlToSqlState(int errno, boolean useSql92States) {
		if (useSql92States) {
			return mysqlToSql99(errno);
		}

		return mysqlToXOpen(errno);
	}

	private static String mysqlToXOpen(int errno) {
		Integer err = Constants.integerValueOf(errno);

		if (mysqlToSqlState.containsKey(err)) {
			return (String) mysqlToSqlState.get(err);
		}

		return SQL_STATE_GENERAL_ERROR;
	}

	/*
	 * SQL State Class SQLNonTransientException Subclass 08
	 * SQLNonTransientConnectionException 22 SQLDataException 23
	 * SQLIntegrityConstraintViolationException N/A
	 * SQLInvalidAuthorizationException 42 SQLSyntaxErrorException
	 * 
	 * SQL State Class SQLTransientException Subclass 08
	 * SQLTransientConnectionException 40 SQLTransactionRollbackException N/A
	 * SQLTimeoutException
	 */

	public static SQLException createSQLException(String message,
			String sqlState, ExceptionInterceptor interceptor) {
		return createSQLException(message, sqlState, 0, interceptor);
	}

	public static SQLException createSQLException(String message, ExceptionInterceptor interceptor) {
		return createSQLException(message, interceptor, null);
	}
	public static SQLException createSQLException(String message, ExceptionInterceptor interceptor, Connection conn) {
		SQLException sqlEx = new SQLException(message);
		
		if (interceptor != null) {
			SQLException interceptedEx = interceptor.interceptException(sqlEx, conn);
			
			if (interceptedEx != null) {
				return interceptedEx;
			}
		}
		
		return sqlEx;
	}

	public static SQLException createSQLException(String message, String sqlState, Throwable cause, ExceptionInterceptor interceptor) {
		return createSQLException(message, sqlState, cause, interceptor, null);
	}
	public static SQLException createSQLException(String message, String sqlState, Throwable cause, ExceptionInterceptor interceptor,
			Connection conn) {
		if (THROWABLE_INIT_CAUSE_METHOD == null) {
			if (cause != null) {
				message = message + " due to " + cause.toString();
			}
		}
		
		SQLException sqlEx = createSQLException(message, sqlState, interceptor);
		
		if (cause != null && THROWABLE_INIT_CAUSE_METHOD != null) {
			try {
				THROWABLE_INIT_CAUSE_METHOD.invoke(sqlEx, new Object[] {cause});
			} catch (Throwable t) {
				// we're not going to muck with that here, since it's
				// an error condition anyway!
			}
		}
		
		if (interceptor != null) {
			SQLException interceptedEx = interceptor.interceptException(sqlEx, conn);
			
			if (interceptedEx != null) {
				return interceptedEx;
			}
		}
		
		return sqlEx;
	}
	
	public static SQLException createSQLException(String message,
			String sqlState, int vendorErrorCode, ExceptionInterceptor interceptor) {
		return createSQLException(message, sqlState, vendorErrorCode, false, interceptor);
	}
	
	public static SQLException createSQLException(String message,
			String sqlState, int vendorErrorCode, boolean isTransient, ExceptionInterceptor interceptor) {
		return createSQLException(message, sqlState, vendorErrorCode, false, interceptor, null);
	}
	public static SQLException createSQLException(String message,
			String sqlState, int vendorErrorCode, boolean isTransient, ExceptionInterceptor interceptor, Connection conn) {
		try {
			SQLException sqlEx = null;
			
			if (sqlState != null) {
				if (sqlState.startsWith("08")) {
					if (isTransient) {
						if (!Util.isJdbc4()) {
							sqlEx = new MySQLTransientConnectionException(
									message, sqlState, vendorErrorCode);
						} else {
							sqlEx = (SQLException) Util
								.getInstance(
										"com.mysql.jdbc.exceptions.jdbc4.MySQLTransientConnectionException",
										new Class[] { String.class,
												String.class, Integer.TYPE },
										new Object[] { message, sqlState,
												Constants.integerValueOf(vendorErrorCode) }, interceptor);
						}
					} else if (!Util.isJdbc4()) {
						sqlEx = new MySQLNonTransientConnectionException(
								message, sqlState, vendorErrorCode);
					} else {
						sqlEx = (SQLException) Util.getInstance(
									"com.mysql.jdbc.exceptions.jdbc4.MySQLNonTransientConnectionException",
									new Class[] { String.class, String.class,
											Integer.TYPE  }, new Object[] {
											message, sqlState,
											Constants.integerValueOf(vendorErrorCode) }, interceptor);
					}
				} else if (sqlState.startsWith("22")) {
					if (!Util.isJdbc4()) {
						sqlEx = new MySQLDataException(message, sqlState,
								vendorErrorCode);
					} else {
						sqlEx = (SQLException) Util
							.getInstance(
									"com.mysql.jdbc.exceptions.jdbc4.MySQLDataException",
									new Class[] { String.class, String.class,
											Integer.TYPE  }, new Object[] {
											message, sqlState,
											Constants.integerValueOf(vendorErrorCode) }, interceptor);
					}
				} else if (sqlState.startsWith("23")) {

					if (!Util.isJdbc4()) {
						sqlEx = new MySQLIntegrityConstraintViolationException(
								message, sqlState, vendorErrorCode);
					} else {
						sqlEx =  (SQLException) Util
							.getInstance(
									"com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException",
									new Class[] { String.class, String.class,
											Integer.TYPE  }, new Object[] {
											message, sqlState,
											Constants.integerValueOf(vendorErrorCode) }, interceptor);
					}
				} else if (sqlState.startsWith("42")) {
					if (!Util.isJdbc4()) {
						sqlEx = new MySQLSyntaxErrorException(message, sqlState,
								vendorErrorCode);
					} else {
						sqlEx = (SQLException) Util.getInstance(
									"com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException",
									new Class[] { String.class, String.class,
											Integer.TYPE  }, new Object[] {
											message, sqlState,
											Constants.integerValueOf(vendorErrorCode) }, interceptor);
					}
				} else if (sqlState.startsWith("40")) {
					if (!Util.isJdbc4()) {
						sqlEx = new MySQLTransactionRollbackException(message,
								sqlState, vendorErrorCode);
					} else {
						sqlEx = (SQLException) Util
							.getInstance(
									"com.mysql.jdbc.exceptions.jdbc4.MySQLTransactionRollbackException",
									new Class[] { String.class, String.class,
											Integer.TYPE  }, new Object[] {
											message, sqlState,
											Constants.integerValueOf(vendorErrorCode) }, interceptor);
					}
				} else {
					sqlEx = new SQLException(message, sqlState, vendorErrorCode);
				}
			} else {
				sqlEx = new SQLException(message, sqlState, vendorErrorCode);
			}
			
			if (interceptor != null) {
				SQLException interceptedEx = interceptor.interceptException(sqlEx, conn);
				
				if (interceptedEx != null) {
					return interceptedEx;
				}
			}
			
			if (sqlEx == null) {
				System.out.println("!");
			}
			
			return sqlEx;
		} catch (SQLException sqlEx) {
			SQLException unexpectedEx = new SQLException(
					"Unable to create correct SQLException class instance, error class/codes may be incorrect. Reason: "
							+ Util.stackTraceToString(sqlEx),
					SQL_STATE_GENERAL_ERROR);
			
			if (interceptor != null) {
				SQLException interceptedEx = interceptor.interceptException(unexpectedEx, conn);
				
				if (interceptedEx != null) {
					return interceptedEx;
				}
			}
			
			return unexpectedEx;
		}
	}
	
	public static SQLException createCommunicationsException(ConnectionImpl conn, long lastPacketSentTimeMs, 
			long lastPacketReceivedTimeMs,
			Exception underlyingException, ExceptionInterceptor interceptor) {
		SQLException exToReturn = null;
		
		if (!Util.isJdbc4()) {
			exToReturn = new CommunicationsException(conn, lastPacketSentTimeMs, lastPacketReceivedTimeMs, underlyingException);
		} else {
		
			try {
				exToReturn = (SQLException) Util.handleNewInstance(JDBC_4_COMMUNICATIONS_EXCEPTION_CTOR, new Object[] {
					conn, Constants.longValueOf(lastPacketSentTimeMs), Constants.longValueOf(lastPacketReceivedTimeMs), underlyingException}, interceptor);
			} catch (SQLException sqlEx) {
				// We should _never_ get this, but let's not swallow it either
				
				return sqlEx;
			}
		}
		
		if (THROWABLE_INIT_CAUSE_METHOD != null && underlyingException != null) {
			try {
				THROWABLE_INIT_CAUSE_METHOD.invoke(exToReturn, new Object[] {underlyingException});
			} catch (Throwable t) {
				// we're not going to muck with that here, since it's
				// an error condition anyway!
			}
		}
		
		if (interceptor != null) {
			SQLException interceptedEx = interceptor.interceptException(exToReturn, conn);
			
			if (interceptedEx != null) {
				return interceptedEx;
			}
		}
		
		return exToReturn;
	}
	
	/**
	 * Creates a communications link failure message to be used
	 * in CommunicationsException that (hopefully) has some better
	 * information and suggestions based on heuristics.
	 *  
	 * @param conn
	 * @param lastPacketSentTimeMs
	 * @param underlyingException
	 * @param streamingResultSetInPlay
	 * @return
	 */
	public static String createLinkFailureMessageBasedOnHeuristics(
			ConnectionImpl conn,
			long lastPacketSentTimeMs, 
			long lastPacketReceivedTimeMs,
			Exception underlyingException,
			boolean streamingResultSetInPlay) {
		long serverTimeoutSeconds = 0;
		boolean isInteractiveClient = false;

		if (conn != null) {
			isInteractiveClient = conn.getInteractiveClient();

			String serverTimeoutSecondsStr = null;

			if (isInteractiveClient) {
				serverTimeoutSecondsStr = conn
						.getServerVariable("interactive_timeout"); //$NON-NLS-1$
			} else {
				serverTimeoutSecondsStr = conn
						.getServerVariable("wait_timeout"); //$NON-NLS-1$
			}

			if (serverTimeoutSecondsStr != null) {
				try {
					serverTimeoutSeconds = Long
							.parseLong(serverTimeoutSecondsStr);
				} catch (NumberFormatException nfe) {
					serverTimeoutSeconds = 0;
				}
			}
		}

		StringBuffer exceptionMessageBuf = new StringBuffer();

		if (lastPacketSentTimeMs == 0) {
			lastPacketSentTimeMs = System.currentTimeMillis();
		}

		long timeSinceLastPacket = (System.currentTimeMillis() - lastPacketSentTimeMs) / 1000;
		long timeSinceLastPacketMs = (System.currentTimeMillis() - lastPacketSentTimeMs);
		long timeSinceLastPacketReceivedMs = (System.currentTimeMillis() - lastPacketReceivedTimeMs);
		
		int dueToTimeout = DUE_TO_TIMEOUT_FALSE;

		StringBuffer timeoutMessageBuf = null;

		if (streamingResultSetInPlay) {
			exceptionMessageBuf.append(Messages
					.getString("CommunicationsException.ClientWasStreaming")); //$NON-NLS-1$
		} else {
			if (serverTimeoutSeconds != 0) {
				if (timeSinceLastPacket > serverTimeoutSeconds) {
					dueToTimeout = DUE_TO_TIMEOUT_TRUE;

					timeoutMessageBuf = new StringBuffer();

					timeoutMessageBuf.append(Messages
							.getString("CommunicationsException.2")); //$NON-NLS-1$

					if (!isInteractiveClient) {
						timeoutMessageBuf.append(Messages
								.getString("CommunicationsException.3")); //$NON-NLS-1$
					} else {
						timeoutMessageBuf.append(Messages
								.getString("CommunicationsException.4")); //$NON-NLS-1$
					}

				}
			} else if (timeSinceLastPacket > DEFAULT_WAIT_TIMEOUT_SECONDS) {
				dueToTimeout = DUE_TO_TIMEOUT_MAYBE;

				timeoutMessageBuf = new StringBuffer();

				timeoutMessageBuf.append(Messages
						.getString("CommunicationsException.5")); //$NON-NLS-1$
				timeoutMessageBuf.append(Messages
						.getString("CommunicationsException.6")); //$NON-NLS-1$
				timeoutMessageBuf.append(Messages
						.getString("CommunicationsException.7")); //$NON-NLS-1$
				timeoutMessageBuf.append(Messages
						.getString("CommunicationsException.8")); //$NON-NLS-1$
			}

			if (dueToTimeout == DUE_TO_TIMEOUT_TRUE
					|| dueToTimeout == DUE_TO_TIMEOUT_MAYBE) {

				if (lastPacketReceivedTimeMs != 0) {
					Object[] timingInfo = {
							new Long(timeSinceLastPacketReceivedMs),
							new Long(timeSinceLastPacketMs)
					};
					exceptionMessageBuf.append(Messages
							.getString("CommunicationsException.ServerPacketTimingInfo", //$NON-NLS-1$
									timingInfo));
				} else {
					exceptionMessageBuf.append(Messages
							.getString("CommunicationsException.ServerPacketTimingInfoNoRecv", //$NON-NLS-1$
									new Object[] { new Long(timeSinceLastPacketMs)}));
				}

				if (timeoutMessageBuf != null) {
					exceptionMessageBuf.append(timeoutMessageBuf);
				}

				exceptionMessageBuf.append(Messages
						.getString("CommunicationsException.11")); //$NON-NLS-1$
				exceptionMessageBuf.append(Messages
						.getString("CommunicationsException.12")); //$NON-NLS-1$
				exceptionMessageBuf.append(Messages
						.getString("CommunicationsException.13")); //$NON-NLS-1$

			} else {
				//
				// Attempt to determine the reason for the underlying exception
				// (we can only make a best-guess here)
				//

				if (underlyingException instanceof BindException) {
					if (conn.getLocalSocketAddress() != null
							&& !Util.interfaceExists(conn
									.getLocalSocketAddress())) {
						exceptionMessageBuf.append(Messages
								.getString("CommunicationsException.LocalSocketAddressNotAvailable")); //$NON-NLS-1$
					} else {
						// too many client connections???
						exceptionMessageBuf.append(Messages
								.getString("CommunicationsException.TooManyClientConnections")); //$NON-NLS-1$
					}
				}
			}
		}

		if (exceptionMessageBuf.length() == 0) {
			// We haven't figured out a good reason, so copy it.
			exceptionMessageBuf.append(Messages
					.getString("CommunicationsException.20")); //$NON-NLS-1$

			if (THROWABLE_INIT_CAUSE_METHOD == null && 
					underlyingException != null) {
				exceptionMessageBuf.append(Messages
						.getString("CommunicationsException.21")); //$NON-NLS-1$
				exceptionMessageBuf.append(Util
						.stackTraceToString(underlyingException));
			}

			if (conn != null && conn.getMaintainTimeStats()
					&& !conn.getParanoid()) {
				exceptionMessageBuf.append("\n\n"); //$NON-NLS-1$
				if (lastPacketReceivedTimeMs != 0) {
					Object[] timingInfo = {
							new Long(timeSinceLastPacketReceivedMs),
							new Long(timeSinceLastPacketMs)
					};
					exceptionMessageBuf.append(Messages
							.getString("CommunicationsException.ServerPacketTimingInfo", //$NON-NLS-1$
									timingInfo));
				} else {
					exceptionMessageBuf.append(Messages
							.getString("CommunicationsException.ServerPacketTimingInfoNoRecv", //$NON-NLS-1$
									new Object[] { new Long(timeSinceLastPacketMs)}));
				}
			}
		}
		
		return exceptionMessageBuf.toString();
	}
	
	public static SQLException notImplemented() {
		if (Util.isJdbc4()) {
			try {
				return (SQLException) Class.forName(
						"java.sql.SQLFeatureNotSupportedException")
						.newInstance();
			} catch (Throwable t) {
				// proceed
			}
		}

		return new NotImplemented();
	}
}
