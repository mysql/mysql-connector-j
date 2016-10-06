/*
  Copyright (c) 2002, 2016, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FOSS License Exception
  <http://www.mysql.com/about/legal/licensing/foss-exception.html>.

  This program is free software; you can redistribute it and/or modify it under the terms
  of the GNU General Public License as published by the Free Software Foundation; version 2
  of the License.

  This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
  See the GNU General Public License for more details.

  You should have received a copy of the GNU General Public License along with this
  program; if not, write to the Free Software Foundation, Inc., 51 Franklin St, Fifth
  Floor, Boston, MA 02110-1301  USA

 */

package com.mysql.cj.jdbc.exceptions;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTransientConnectionException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.api.jdbc.JdbcConnection;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.exceptions.MysqlErrorNumbers;
import com.mysql.cj.core.util.Util;

/**
 * SQLError is a utility class that maps MySQL error codes to SQL error codes as is required by the JDBC spec.
 */
public class SQLError {
    public static final int ER_WARNING_NOT_COMPLETE_ROLLBACK = 1196;
    public static final String SQL_STATE_BAD_SSL_PARAMS = "08000";

    private static Map<Integer, String> mysqlToSql99State;

    // SQL-92
    public static final String SQL_STATE_WARNING = "01000";
    public static final String SQL_STATE_DISCONNECT_ERROR = "01002";
    public static final String SQL_STATE_DATE_TRUNCATED = "01004";
    public static final String SQL_STATE_PRIVILEGE_NOT_REVOKED = "01006";
    public static final String SQL_STATE_NO_DATA = "02000";
    public static final String SQL_STATE_WRONG_NO_OF_PARAMETERS = "07001";
    public static final String SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE = "08001";
    public static final String SQL_STATE_CONNECTION_IN_USE = "08002";
    public static final String SQL_STATE_CONNECTION_NOT_OPEN = "08003";
    public static final String SQL_STATE_CONNECTION_REJECTED = "08004";
    public static final String SQL_STATE_CONNECTION_FAILURE = "08006";
    public static final String SQL_STATE_TRANSACTION_RESOLUTION_UNKNOWN = "08007";
    public static final String SQL_STATE_COMMUNICATION_LINK_FAILURE = "08S01";
    public static final String SQL_STATE_FEATURE_NOT_SUPPORTED = "0A000";
    public static final String SQL_STATE_CARDINALITY_VIOLATION = "21000";
    public static final String SQL_STATE_INSERT_VALUE_LIST_NO_MATCH_COL_LIST = "21S01";
    public static final String SQL_STATE_STRING_DATA_RIGHT_TRUNCATION = "22001";
    public static final String SQL_STATE_NUMERIC_VALUE_OUT_OF_RANGE = "22003";
    public static final String SQL_STATE_INVALID_DATETIME_FORMAT = "22007";
    public static final String SQL_STATE_DATETIME_FIELD_OVERFLOW = "22008";
    public static final String SQL_STATE_DIVISION_BY_ZERO = "22012";
    public static final String SQL_STATE_INVALID_CHARACTER_VALUE_FOR_CAST = "22018";
    public static final String SQL_STATE_INTEGRITY_CONSTRAINT_VIOLATION = "23000";
    public static final String SQL_STATE_INVALID_CURSOR_STATE = "24000";
    public static final String SQL_STATE_INVALID_TRANSACTION_STATE = "25000";
    public static final String SQL_STATE_INVALID_AUTH_SPEC = "28000";
    public static final String SQL_STATE_INVALID_TRANSACTION_TERMINATION = "2D000";
    public static final String SQL_STATE_INVALID_CONDITION_NUMBER = "35000";
    public static final String SQL_STATE_INVALID_CATALOG_NAME = "3D000";
    public static final String SQL_STATE_ROLLBACK_SERIALIZATION_FAILURE = "40001";
    public static final String SQL_STATE_SYNTAX_ERROR = "42000";
    public static final String SQL_STATE_ER_TABLE_EXISTS_ERROR = "42S01";
    public static final String SQL_STATE_BASE_TABLE_OR_VIEW_NOT_FOUND = "42S02";
    public static final String SQL_STATE_ER_NO_SUCH_INDEX = "42S12";
    public static final String SQL_STATE_ER_DUP_FIELDNAME = "42S21";
    public static final String SQL_STATE_ER_BAD_FIELD_ERROR = "42S22";

    // SQL-99
    public static final String SQL_STATE_INVALID_CONNECTION_ATTRIBUTE = "01S00";
    public static final String SQL_STATE_ERROR_IN_ROW = "01S01";
    public static final String SQL_STATE_NO_ROWS_UPDATED_OR_DELETED = "01S03";
    public static final String SQL_STATE_MORE_THAN_ONE_ROW_UPDATED_OR_DELETED = "01S04";
    public static final String SQL_STATE_RESIGNAL_WHEN_HANDLER_NOT_ACTIVE = "0K000";
    public static final String SQL_STATE_STACKED_DIAGNOSTICS_ACCESSED_WITHOUT_ACTIVE_HANDLER = "0Z002";
    public static final String SQL_STATE_CASE_NOT_FOUND_FOR_CASE_STATEMENT = "20000";
    public static final String SQL_STATE_NULL_VALUE_NOT_ALLOWED = "22004";
    public static final String SQL_STATE_INVALID_LOGARITHM_ARGUMENT = "2201E";
    public static final String SQL_STATE_ACTIVE_SQL_TRANSACTION = "25001";
    public static final String SQL_STATE_READ_ONLY_SQL_TRANSACTION = "25006";
    public static final String SQL_STATE_SRE_PROHIBITED_SQL_STATEMENT_ATTEMPTED = "2F003";
    public static final String SQL_STATE_SRE_FUNCTION_EXECUTED_NO_RETURN_STATEMENT = "2F005";
    public static final String SQL_STATE_ER_QUERY_INTERRUPTED = "70100"; // non-standard ?
    public static final String SQL_STATE_BASE_TABLE_OR_VIEW_ALREADY_EXISTS = "S0001";
    public static final String SQL_STATE_BASE_TABLE_NOT_FOUND = "S0002";
    public static final String SQL_STATE_INDEX_ALREADY_EXISTS = "S0011";
    public static final String SQL_STATE_INDEX_NOT_FOUND = "S0012";
    public static final String SQL_STATE_COLUMN_ALREADY_EXISTS = "S0021";
    public static final String SQL_STATE_COLUMN_NOT_FOUND = "S0022";
    public static final String SQL_STATE_NO_DEFAULT_FOR_COLUMN = "S0023";
    public static final String SQL_STATE_GENERAL_ERROR = "S1000";
    public static final String SQL_STATE_MEMORY_ALLOCATION_FAILURE = "S1001";
    public static final String SQL_STATE_INVALID_COLUMN_NUMBER = "S1002";
    public static final String SQL_STATE_ILLEGAL_ARGUMENT = "S1009";
    public static final String SQL_STATE_DRIVER_NOT_CAPABLE = "S1C00";
    public static final String SQL_STATE_TIMEOUT_EXPIRED = "S1T00";
    public static final String SQL_STATE_CLI_SPECIFIC_CONDITION = "HY000";
    public static final String SQL_STATE_MEMORY_ALLOCATION_ERROR = "HY001";
    public static final String SQL_STATE_XA_RBROLLBACK = "XA100";
    public static final String SQL_STATE_XA_RBDEADLOCK = "XA102";
    public static final String SQL_STATE_XA_RBTIMEOUT = "XA106";
    public static final String SQL_STATE_XA_RMERR = "XAE03";
    public static final String SQL_STATE_XAER_NOTA = "XAE04";
    public static final String SQL_STATE_XAER_INVAL = "XAE05";
    public static final String SQL_STATE_XAER_RMFAIL = "XAE07";
    public static final String SQL_STATE_XAER_DUPID = "XAE08";
    public static final String SQL_STATE_XAER_OUTSIDE = "XAE09";

    private static Map<String, String> sqlStateMessages;

    static {

        sqlStateMessages = new HashMap<String, String>();
        sqlStateMessages.put(SQL_STATE_DISCONNECT_ERROR, Messages.getString("SQLError.35"));
        sqlStateMessages.put(SQL_STATE_DATE_TRUNCATED, Messages.getString("SQLError.36"));
        sqlStateMessages.put(SQL_STATE_PRIVILEGE_NOT_REVOKED, Messages.getString("SQLError.37"));
        sqlStateMessages.put(SQL_STATE_INVALID_CONNECTION_ATTRIBUTE, Messages.getString("SQLError.38"));
        sqlStateMessages.put(SQL_STATE_ERROR_IN_ROW, Messages.getString("SQLError.39"));
        sqlStateMessages.put(SQL_STATE_NO_ROWS_UPDATED_OR_DELETED, Messages.getString("SQLError.40"));
        sqlStateMessages.put(SQL_STATE_MORE_THAN_ONE_ROW_UPDATED_OR_DELETED, Messages.getString("SQLError.41"));
        sqlStateMessages.put(SQL_STATE_WRONG_NO_OF_PARAMETERS, Messages.getString("SQLError.42"));
        sqlStateMessages.put(SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, Messages.getString("SQLError.43"));
        sqlStateMessages.put(SQL_STATE_CONNECTION_IN_USE, Messages.getString("SQLError.44"));
        sqlStateMessages.put(SQL_STATE_CONNECTION_NOT_OPEN, Messages.getString("SQLError.45"));
        sqlStateMessages.put(SQL_STATE_CONNECTION_REJECTED, Messages.getString("SQLError.46"));
        sqlStateMessages.put(SQL_STATE_TRANSACTION_RESOLUTION_UNKNOWN, Messages.getString("SQLError.47"));
        sqlStateMessages.put(SQL_STATE_COMMUNICATION_LINK_FAILURE, Messages.getString("SQLError.48"));
        sqlStateMessages.put(SQL_STATE_INSERT_VALUE_LIST_NO_MATCH_COL_LIST, Messages.getString("SQLError.49"));
        sqlStateMessages.put(SQL_STATE_NUMERIC_VALUE_OUT_OF_RANGE, Messages.getString("SQLError.50"));
        sqlStateMessages.put(SQL_STATE_DATETIME_FIELD_OVERFLOW, Messages.getString("SQLError.51"));
        sqlStateMessages.put(SQL_STATE_DIVISION_BY_ZERO, Messages.getString("SQLError.52"));
        sqlStateMessages.put(SQL_STATE_ROLLBACK_SERIALIZATION_FAILURE, Messages.getString("SQLError.53"));
        sqlStateMessages.put(SQL_STATE_INVALID_AUTH_SPEC, Messages.getString("SQLError.54"));
        sqlStateMessages.put(SQL_STATE_SYNTAX_ERROR, Messages.getString("SQLError.55"));
        sqlStateMessages.put(SQL_STATE_BASE_TABLE_OR_VIEW_NOT_FOUND, Messages.getString("SQLError.56"));
        sqlStateMessages.put(SQL_STATE_BASE_TABLE_OR_VIEW_ALREADY_EXISTS, Messages.getString("SQLError.57"));
        sqlStateMessages.put(SQL_STATE_BASE_TABLE_NOT_FOUND, Messages.getString("SQLError.58"));
        sqlStateMessages.put(SQL_STATE_INDEX_ALREADY_EXISTS, Messages.getString("SQLError.59"));
        sqlStateMessages.put(SQL_STATE_INDEX_NOT_FOUND, Messages.getString("SQLError.60"));
        sqlStateMessages.put(SQL_STATE_COLUMN_ALREADY_EXISTS, Messages.getString("SQLError.61"));
        sqlStateMessages.put(SQL_STATE_COLUMN_NOT_FOUND, Messages.getString("SQLError.62"));
        sqlStateMessages.put(SQL_STATE_NO_DEFAULT_FOR_COLUMN, Messages.getString("SQLError.63"));
        sqlStateMessages.put(SQL_STATE_GENERAL_ERROR, Messages.getString("SQLError.64"));
        sqlStateMessages.put(SQL_STATE_MEMORY_ALLOCATION_FAILURE, Messages.getString("SQLError.65"));
        sqlStateMessages.put(SQL_STATE_INVALID_COLUMN_NUMBER, Messages.getString("SQLError.66"));
        sqlStateMessages.put(SQL_STATE_ILLEGAL_ARGUMENT, Messages.getString("SQLError.67"));
        sqlStateMessages.put(SQL_STATE_DRIVER_NOT_CAPABLE, Messages.getString("SQLError.68"));
        sqlStateMessages.put(SQL_STATE_TIMEOUT_EXPIRED, Messages.getString("SQLError.69"));

        mysqlToSql99State = new HashMap<Integer, String>();

        mysqlToSql99State.put(MysqlErrorNumbers.ER_SELECT_REDUCED, SQL_STATE_WARNING);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_WARN_TOO_FEW_RECORDS, SQL_STATE_WARNING);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_WARN_TOO_MANY_RECORDS, SQL_STATE_WARNING);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_WARN_DATA_TRUNCATED, SQL_STATE_WARNING);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_WARN_NULL_TO_NOTNULL, SQL_STATE_NULL_VALUE_NOT_ALLOWED);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_WARN_DATA_OUT_OF_RANGE, SQL_STATE_NUMERIC_VALUE_OUT_OF_RANGE);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SP_UNINIT_VAR, SQL_STATE_WARNING);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SIGNAL_WARN, SQL_STATE_WARNING);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SP_FETCH_NO_DATA, SQL_STATE_NO_DATA);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SIGNAL_NOT_FOUND, SQL_STATE_NO_DATA);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_CON_COUNT_ERROR, SQL_STATE_CONNECTION_REJECTED);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_NOT_SUPPORTED_AUTH_MODE, SQL_STATE_CONNECTION_REJECTED);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_BAD_HOST_ERROR, SQL_STATE_COMMUNICATION_LINK_FAILURE);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_HANDSHAKE_ERROR, SQL_STATE_COMMUNICATION_LINK_FAILURE);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_UNKNOWN_COM_ERROR, SQL_STATE_COMMUNICATION_LINK_FAILURE);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SERVER_SHUTDOWN, SQL_STATE_COMMUNICATION_LINK_FAILURE);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_FORCING_CLOSE, SQL_STATE_COMMUNICATION_LINK_FAILURE);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_IPSOCK_ERROR, SQL_STATE_COMMUNICATION_LINK_FAILURE);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_ABORTING_CONNECTION, SQL_STATE_COMMUNICATION_LINK_FAILURE);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_NET_PACKET_TOO_LARGE, SQL_STATE_COMMUNICATION_LINK_FAILURE);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_NET_READ_ERROR_FROM_PIPE, SQL_STATE_COMMUNICATION_LINK_FAILURE);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_NET_FCNTL_ERROR, SQL_STATE_COMMUNICATION_LINK_FAILURE);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_NET_PACKETS_OUT_OF_ORDER, SQL_STATE_COMMUNICATION_LINK_FAILURE);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_NET_UNCOMPRESS_ERROR, SQL_STATE_COMMUNICATION_LINK_FAILURE);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_NET_READ_ERROR, SQL_STATE_COMMUNICATION_LINK_FAILURE);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_NET_READ_INTERRUPTED, SQL_STATE_COMMUNICATION_LINK_FAILURE);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_NET_ERROR_ON_WRITE, SQL_STATE_COMMUNICATION_LINK_FAILURE);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_NET_WRITE_INTERRUPTED, SQL_STATE_COMMUNICATION_LINK_FAILURE);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_NEW_ABORTING_CONNECTION, SQL_STATE_COMMUNICATION_LINK_FAILURE);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_MASTER_NET_READ, SQL_STATE_COMMUNICATION_LINK_FAILURE);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_MASTER_NET_WRITE, SQL_STATE_COMMUNICATION_LINK_FAILURE);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_CONNECT_TO_MASTER, SQL_STATE_COMMUNICATION_LINK_FAILURE);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SP_BADSELECT, SQL_STATE_FEATURE_NOT_SUPPORTED);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SP_BADSTATEMENT, SQL_STATE_FEATURE_NOT_SUPPORTED);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SP_SUBSELECT_NYI, SQL_STATE_FEATURE_NOT_SUPPORTED);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_STMT_NOT_ALLOWED_IN_SF_OR_TRG, SQL_STATE_FEATURE_NOT_SUPPORTED);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SP_NO_RETSET, SQL_STATE_FEATURE_NOT_SUPPORTED);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_ALTER_OPERATION_NOT_SUPPORTED, SQL_STATE_FEATURE_NOT_SUPPORTED);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_ALTER_OPERATION_NOT_SUPPORTED_REASON, SQL_STATE_FEATURE_NOT_SUPPORTED);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_DBACCESS_DENIED_ERROR, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_BAD_DB_ERROR, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_WRONG_FIELD_WITH_GROUP, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_WRONG_GROUP_FIELD, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_WRONG_SUM_SELECT, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_TOO_LONG_IDENT, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_DUP_KEYNAME, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_WRONG_FIELD_SPEC, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_PARSE_ERROR, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_EMPTY_QUERY, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_NONUNIQ_TABLE, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_INVALID_DEFAULT, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_MULTIPLE_PRI_KEY, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_TOO_MANY_KEYS, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_TOO_MANY_KEY_PARTS, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_TOO_LONG_KEY, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_KEY_COLUMN_DOES_NOT_EXITS, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_BLOB_USED_AS_KEY, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_TOO_BIG_FIELDLENGTH, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_WRONG_AUTO_KEY, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_WRONG_FIELD_TERMINATORS, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_BLOBS_AND_NO_TERMINATED, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_CANT_REMOVE_ALL_FIELDS, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_CANT_DROP_FIELD_OR_KEY, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_BLOB_CANT_HAVE_DEFAULT, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_WRONG_DB_NAME, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_WRONG_TABLE_NAME, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_TOO_BIG_SELECT, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_UNKNOWN_PROCEDURE, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_WRONG_PARAMCOUNT_TO_PROCEDURE, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_FIELD_SPECIFIED_TWICE, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_UNSUPPORTED_EXTENSION, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_TABLE_MUST_HAVE_COLUMNS, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_UNKNOWN_CHARACTER_SET, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_TOO_BIG_ROWSIZE, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_WRONG_OUTER_JOIN, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_NULL_COLUMN_IN_INDEX, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_PASSWORD_ANONYMOUS_USER, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_PASSWORD_NOT_ALLOWED, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_PASSWORD_NO_MATCH, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_REGEXP_ERROR, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_MIX_OF_GROUP_FUNC_AND_FIELDS, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_NONEXISTING_GRANT, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_TABLEACCESS_DENIED_ERROR, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_COLUMNACCESS_DENIED_ERROR, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_ILLEGAL_GRANT_FOR_TABLE, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_GRANT_WRONG_HOST_OR_USER, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_NONEXISTING_TABLE_GRANT, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_NOT_ALLOWED_COMMAND, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SYNTAX_ERROR, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_TOO_LONG_STRING, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_TABLE_CANT_HANDLE_BLOB, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_TABLE_CANT_HANDLE_AUTO_INCREMENT, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_WRONG_COLUMN_NAME, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_WRONG_KEY_COLUMN, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_BLOB_KEY_WITHOUT_LENGTH, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_PRIMARY_CANT_HAVE_NULL, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_TOO_MANY_ROWS, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_REQUIRES_PRIMARY_KEY, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_KEY_DOES_NOT_EXITS, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_CHECK_NO_SUCH_TABLE, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_CHECK_NOT_IMPLEMENTED, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_TOO_MANY_USER_CONNECTIONS, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_NO_PERMISSION_TO_CREATE_USER, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_USER_LIMIT_REACHED, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SPECIFIC_ACCESS_DENIED_ERROR, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_NO_DEFAULT, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_WRONG_VALUE_FOR_VAR, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_WRONG_TYPE_FOR_VAR, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_CANT_USE_OPTION_HERE, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_NOT_SUPPORTED_YET, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_WRONG_FK_DEF, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_DERIVED_MUST_HAVE_ALIAS, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_TABLENAME_NOT_ALLOWED_HERE, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SPATIAL_CANT_HAVE_NULL, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_COLLATION_CHARSET_MISMATCH, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_WRONG_NAME_FOR_INDEX, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_WRONG_NAME_FOR_CATALOG, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_UNKNOWN_STORAGE_ENGINE, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SP_ALREADY_EXISTS, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SP_DOES_NOT_EXIST, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SP_LILABEL_MISMATCH, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SP_LABEL_REDEFINE, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SP_LABEL_MISMATCH, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SP_BADRETURN, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_UPDATE_LOG_DEPRECATED_IGNORED, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_UPDATE_LOG_DEPRECATED_TRANSLATED, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SP_WRONG_NO_OF_ARGS, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SP_COND_MISMATCH, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SP_NORETURN, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SP_BAD_CURSOR_QUERY, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SP_BAD_CURSOR_SELECT, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SP_CURSOR_MISMATCH, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SP_UNDECLARED_VAR, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SP_DUP_PARAM, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SP_DUP_VAR, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SP_DUP_COND, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SP_DUP_CURS, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SP_VARCOND_AFTER_CURSHNDLR, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SP_CURSOR_AFTER_HANDLER, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_PROCACCESS_DENIED_ERROR, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_NONEXISTING_PROC_GRANT, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SP_BAD_SQLSTATE, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_CANT_CREATE_USER_WITH_GRANT, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SP_DUP_HANDLER, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SP_NOT_VAR_ARG, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_TOO_BIG_SCALE, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_TOO_BIG_PRECISION, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_M_BIGGER_THAN_D, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_TOO_LONG_BODY, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_TOO_BIG_DISPLAYWIDTH, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SP_BAD_VAR_SHADOW, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SP_WRONG_NAME, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SP_NO_AGGREGATE, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_MAX_PREPARED_STMT_COUNT_REACHED, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_NON_GROUPING_FIELD_USED, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_WRONG_PARAMETERS_TO_NATIVE_FCT, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_WRONG_PARAMETERS_TO_STORED_FCT, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_FUNC_INEXISTENT_NAME_COLLISION, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_DUP_SIGNAL_SET, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SPATIAL_MUST_HAVE_GEOM_COL, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_TRUNCATE_ILLEGAL_FK, SQL_STATE_SYNTAX_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT, SQL_STATE_CARDINALITY_VIOLATION);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_OPERAND_COLUMNS, SQL_STATE_CARDINALITY_VIOLATION);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SUBQUERY_NO_1_ROW, SQL_STATE_CARDINALITY_VIOLATION);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_DUP_KEY, SQL_STATE_INTEGRITY_CONSTRAINT_VIOLATION);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_BAD_NULL_ERROR, SQL_STATE_INTEGRITY_CONSTRAINT_VIOLATION);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_NON_UNIQ_ERROR, SQL_STATE_INTEGRITY_CONSTRAINT_VIOLATION);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_DUP_ENTRY, SQL_STATE_INTEGRITY_CONSTRAINT_VIOLATION);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_DUP_UNIQUE, SQL_STATE_INTEGRITY_CONSTRAINT_VIOLATION);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_NO_REFERENCED_ROW, SQL_STATE_INTEGRITY_CONSTRAINT_VIOLATION);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_ROW_IS_REFERENCED, SQL_STATE_INTEGRITY_CONSTRAINT_VIOLATION);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_ROW_IS_REFERENCED_2, SQL_STATE_INTEGRITY_CONSTRAINT_VIOLATION);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_NO_REFERENCED_ROW_2, SQL_STATE_INTEGRITY_CONSTRAINT_VIOLATION);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_FOREIGN_DUPLICATE_KEY, SQL_STATE_INTEGRITY_CONSTRAINT_VIOLATION);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_DUP_ENTRY_WITH_KEY_NAME, SQL_STATE_INTEGRITY_CONSTRAINT_VIOLATION);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_FOREIGN_DUPLICATE_KEY_WITH_CHILD_INFO, SQL_STATE_INTEGRITY_CONSTRAINT_VIOLATION);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_FOREIGN_DUPLICATE_KEY_WITHOUT_CHILD_INFO, SQL_STATE_INTEGRITY_CONSTRAINT_VIOLATION);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_DUP_UNKNOWN_IN_INDEX, SQL_STATE_INTEGRITY_CONSTRAINT_VIOLATION);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_DATA_TOO_LONG, SQL_STATE_STRING_DATA_RIGHT_TRUNCATION);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_CANT_CREATE_GEOMETRY_OBJECT, SQL_STATE_NUMERIC_VALUE_OUT_OF_RANGE);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_DATA_OUT_OF_RANGE, SQL_STATE_NUMERIC_VALUE_OUT_OF_RANGE);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_TRUNCATED_WRONG_VALUE, SQL_STATE_INVALID_DATETIME_FORMAT);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_ILLEGAL_VALUE_FOR_TYPE, SQL_STATE_INVALID_DATETIME_FORMAT);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_DATETIME_FUNCTION_OVERFLOW, SQL_STATE_DATETIME_FIELD_OVERFLOW);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_DIVISION_BY_ZERO, SQL_STATE_DIVISION_BY_ZERO);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SP_CURSOR_ALREADY_OPEN, SQL_STATE_INVALID_CURSOR_STATE);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SP_CURSOR_NOT_OPEN, SQL_STATE_INVALID_CURSOR_STATE);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_CANT_DO_THIS_DURING_AN_TRANSACTION, SQL_STATE_INVALID_TRANSACTION_STATE);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_READ_ONLY_TRANSACTION, SQL_STATE_INVALID_TRANSACTION_STATE);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_ACCESS_DENIED_ERROR, SQL_STATE_INVALID_AUTH_SPEC);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_ACCESS_DENIED_NO_PASSWORD_ERROR, SQL_STATE_INVALID_AUTH_SPEC);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_ACCESS_DENIED_CHANGE_USER_ERROR, SQL_STATE_INVALID_AUTH_SPEC);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_DA_INVALID_CONDITION_NUMBER, SQL_STATE_INVALID_CONDITION_NUMBER);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_NO_DB_ERROR, SQL_STATE_INVALID_CATALOG_NAME);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_RESIGNAL_WITHOUT_ACTIVE_HANDLER, SQL_STATE_RESIGNAL_WHEN_HANDLER_NOT_ACTIVE);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_GET_STACKED_DA_WITHOUT_ACTIVE_HANDLER, SQL_STATE_STACKED_DIAGNOSTICS_ACCESSED_WITHOUT_ACTIVE_HANDLER);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SP_CASE_NOT_FOUND, SQL_STATE_CASE_NOT_FOUND_FOR_CASE_STATEMENT);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_WRONG_VALUE_COUNT, SQL_STATE_INSERT_VALUE_LIST_NO_MATCH_COL_LIST);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_WRONG_VALUE_COUNT_ON_ROW, SQL_STATE_INSERT_VALUE_LIST_NO_MATCH_COL_LIST);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_INVALID_USE_OF_NULL, SQL_STATE_NULL_VALUE_NOT_ALLOWED);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_INVALID_ARGUMENT_FOR_LOGARITHM, SQL_STATE_INVALID_LOGARITHM_ARGUMENT);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_CANT_CHANGE_TX_ISOLATION, SQL_STATE_ACTIVE_SQL_TRANSACTION);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION, SQL_STATE_READ_ONLY_SQL_TRANSACTION);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SP_NO_RECURSIVE_CREATE, SQL_STATE_SRE_PROHIBITED_SQL_STATEMENT_ATTEMPTED);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SP_NORETURNEND, SQL_STATE_SRE_FUNCTION_EXECUTED_NO_RETURN_STATEMENT);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_TABLE_EXISTS_ERROR, SQL_STATE_ER_TABLE_EXISTS_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_BAD_TABLE_ERROR, SQL_STATE_BASE_TABLE_OR_VIEW_NOT_FOUND);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_UNKNOWN_TABLE, SQL_STATE_BASE_TABLE_OR_VIEW_NOT_FOUND);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_NO_SUCH_TABLE, SQL_STATE_BASE_TABLE_OR_VIEW_NOT_FOUND);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_NO_SUCH_INDEX, SQL_STATE_ER_NO_SUCH_INDEX);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_DUP_FIELDNAME, SQL_STATE_ER_DUP_FIELDNAME);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_BAD_FIELD_ERROR, SQL_STATE_ER_BAD_FIELD_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_ILLEGAL_REFERENCE, SQL_STATE_ER_BAD_FIELD_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_QUERY_INTERRUPTED, SQL_STATE_ER_QUERY_INTERRUPTED);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_OUTOFMEMORY, SQL_STATE_MEMORY_ALLOCATION_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_OUT_OF_SORTMEMORY, SQL_STATE_MEMORY_ALLOCATION_ERROR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_XA_RBROLLBACK, SQL_STATE_XA_RBROLLBACK);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_XA_RBDEADLOCK, SQL_STATE_XA_RBDEADLOCK);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_XA_RBTIMEOUT, SQL_STATE_XA_RBTIMEOUT);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_XA_RMERR, SQL_STATE_XA_RMERR);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_XAER_NOTA, SQL_STATE_XAER_NOTA);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_XAER_INVAL, SQL_STATE_XAER_INVAL);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_XAER_RMFAIL, SQL_STATE_XAER_RMFAIL);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_XAER_DUPID, SQL_STATE_XAER_DUPID);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_XAER_OUTSIDE, SQL_STATE_XAER_OUTSIDE);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_LOCK_WAIT_TIMEOUT, SQL_STATE_ROLLBACK_SERIALIZATION_FAILURE);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_LOCK_DEADLOCK, SQL_STATE_ROLLBACK_SERIALIZATION_FAILURE);
    }

    public static void dumpSqlStatesMappingsAsXml() throws Exception {
        TreeMap<Integer, Integer> allErrorNumbers = new TreeMap<Integer, Integer>();
        Map<Object, String> mysqlErrorNumbersToNames = new HashMap<Object, String>();

        //		Integer errorNumber = null;

        // 
        // First create a list of all 'known' error numbers that are mapped.
        //
        for (Integer errorNumber : mysqlToSql99State.keySet()) {
            allErrorNumbers.put(errorNumber, errorNumber);
        }

        //
        // Now create a list of the actual MySQL error numbers we know about
        //
        java.lang.reflect.Field[] possibleFields = MysqlErrorNumbers.class.getDeclaredFields();

        for (int i = 0; i < possibleFields.length; i++) {
            String fieldName = possibleFields[i].getName();

            if (fieldName.startsWith("ER_")) {
                mysqlErrorNumbersToNames.put(possibleFields[i].get(null), fieldName);
            }
        }

        System.out.println("<ErrorMappings>");

        for (Integer errorNumber : allErrorNumbers.keySet()) {
            String sql92State = mysqlToSql99(errorNumber.intValue());

            System.out.println("   <ErrorMapping mysqlErrorNumber=\"" + errorNumber + "\" mysqlErrorName=\"" + mysqlErrorNumbersToNames.get(errorNumber)
                    + "\" legacySqlState=\"" + "" + "\" sql92SqlState=\"" + ((sql92State == null) ? "" : sql92State) + "\"/>");
        }

        System.out.println("</ErrorMappings>");
    }

    public static String get(String stateCode) {
        return sqlStateMessages.get(stateCode);
    }

    private static String mysqlToSql99(int errno) {
        Integer err = Integer.valueOf(errno);

        if (mysqlToSql99State.containsKey(err)) {
            return mysqlToSql99State.get(err);
        }

        return SQL_STATE_CLI_SPECIFIC_CONDITION;
    }

    /**
     * Map MySQL error codes to SQL-99 error codes
     * 
     * @param errno
     *            the MySQL error code
     * 
     * @return the corresponding SQL-99 error code
     */
    public static String mysqlToSqlState(int errno) {
        return mysqlToSql99(errno);
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

    public static SQLException createSQLException(String message, String sqlState, ExceptionInterceptor interceptor) {
        return createSQLException(message, sqlState, 0, interceptor);
    }

    public static SQLException createSQLException(String message, ExceptionInterceptor interceptor) {
        SQLException sqlEx = new SQLException(message);

        return runThroughExceptionInterceptor(interceptor, sqlEx);
    }

    public static SQLException createSQLException(String message, String sqlState, Throwable cause, ExceptionInterceptor interceptor) {
        SQLException sqlEx = createSQLException(message, sqlState, null);

        if (sqlEx.getCause() == null) {
            if (cause != null) {
                try {
                    sqlEx.initCause(cause);
                } catch (Throwable t) {
                    // we're not going to muck with that here, since it's an error condition anyway!
                }
            }
        }
        // Run through the exception interceptor after setting the init cause.
        return runThroughExceptionInterceptor(interceptor, sqlEx);
    }

    public static SQLException createSQLException(String message, String sqlState, int vendorErrorCode, ExceptionInterceptor interceptor) {
        return createSQLException(message, sqlState, vendorErrorCode, false, interceptor);
    }

    public static SQLException createSQLException(String message, String sqlState, int vendorErrorCode, Throwable cause, ExceptionInterceptor interceptor) {
        return createSQLException(message, sqlState, vendorErrorCode, false, cause, interceptor);
    }

    public static SQLException createSQLException(String message, String sqlState, int vendorErrorCode, boolean isTransient, ExceptionInterceptor interceptor) {
        return createSQLException(message, sqlState, vendorErrorCode, isTransient, null, interceptor);
    }

    public static SQLException createSQLException(String message, String sqlState, int vendorErrorCode, boolean isTransient, Throwable cause,
            ExceptionInterceptor interceptor) {
        try {
            SQLException sqlEx = null;

            if (sqlState != null) {
                if (sqlState.startsWith("08")) {
                    if (isTransient) {
                        sqlEx = new SQLTransientConnectionException(message, sqlState, vendorErrorCode);
                    } else {
                        sqlEx = new SQLNonTransientConnectionException(message, sqlState, vendorErrorCode);
                    }

                } else if (sqlState.startsWith("22")) {
                    sqlEx = new SQLDataException(message, sqlState, vendorErrorCode);

                } else if (sqlState.startsWith("23")) {
                    sqlEx = new SQLIntegrityConstraintViolationException(message, sqlState, vendorErrorCode);

                } else if (sqlState.startsWith("42")) {
                    sqlEx = new SQLSyntaxErrorException(message, sqlState, vendorErrorCode);

                } else if (sqlState.startsWith("40")) {
                    sqlEx = new MySQLTransactionRollbackException(message, sqlState, vendorErrorCode);

                } else if (sqlState.startsWith("70100")) {
                    sqlEx = new MySQLQueryInterruptedException(message, sqlState, vendorErrorCode);

                } else {
                    sqlEx = new SQLException(message, sqlState, vendorErrorCode);
                }
            } else {
                sqlEx = new SQLException(message, sqlState, vendorErrorCode);
            }

            if (cause != null) {
                try {
                    sqlEx.initCause(cause);
                } catch (Throwable t) {
                    // we're not going to muck with that here, since it's an error condition anyway!
                }
            }

            return runThroughExceptionInterceptor(interceptor, sqlEx);

        } catch (Exception sqlEx) {
            SQLException unexpectedEx = new SQLException(
                    "Unable to create correct SQLException class instance, error class/codes may be incorrect. Reason: " + Util.stackTraceToString(sqlEx),
                    SQL_STATE_GENERAL_ERROR);

            return runThroughExceptionInterceptor(interceptor, unexpectedEx);

        }
    }

    public static SQLException createCommunicationsException(JdbcConnection conn, long lastPacketSentTimeMs, long lastPacketReceivedTimeMs,
            Exception underlyingException, ExceptionInterceptor interceptor) {

        SQLException exToReturn = new CommunicationsException(conn, lastPacketSentTimeMs, lastPacketReceivedTimeMs, underlyingException);

        if (underlyingException != null) {
            try {
                exToReturn.initCause(underlyingException);
            } catch (Throwable t) {
                // we're not going to muck with that here, since it's an error condition anyway!
            }
        }

        return runThroughExceptionInterceptor(interceptor, exToReturn);
    }

    public static SQLException createCommunicationsException(String message, Throwable underlyingException, ExceptionInterceptor interceptor) {
        SQLException exToReturn = null;

        exToReturn = new CommunicationsException(message, underlyingException);

        if (underlyingException != null) {
            try {
                exToReturn.initCause(underlyingException);
            } catch (Throwable t) {
                // we're not going to muck with that here, since it's an error condition anyway!
            }
        }

        return runThroughExceptionInterceptor(interceptor, exToReturn);
    }

    public static NotUpdatable notUpdatable() {
        return new NotUpdatable();
    }

    /**
     * Run exception through an ExceptionInterceptor chain.
     * 
     * @param exInterceptor
     * @param sqlEx
     * @param conn
     * @return
     */
    private static SQLException runThroughExceptionInterceptor(ExceptionInterceptor exInterceptor, SQLException sqlEx) {
        if (exInterceptor != null) {
            SQLException interceptedEx = (SQLException) exInterceptor.interceptException(sqlEx);

            if (interceptedEx != null) {
                return interceptedEx;
            }
        }
        return sqlEx;
    }

    /**
     * Create a BatchUpdateException taking in consideration the JDBC version in use. For JDBC version prior to 4.2 the updates count array has int elements
     * while JDBC 4.2 and beyond uses long values.
     * 
     * @param underlyingEx
     * @param updateCounts
     * @param interceptor
     */
    public static SQLException createBatchUpdateException(SQLException underlyingEx, long[] updateCounts, ExceptionInterceptor interceptor)
            throws SQLException {
        SQLException newEx = (SQLException) Util.getInstance("java.sql.BatchUpdateException",
                new Class<?>[] { String.class, String.class, int.class, long[].class, Throwable.class },
                new Object[] { underlyingEx.getMessage(), underlyingEx.getSQLState(), underlyingEx.getErrorCode(), updateCounts, underlyingEx }, interceptor);
        return runThroughExceptionInterceptor(interceptor, newEx);
    }

    /**
     * Create a SQLFeatureNotSupportedException or a NotImplemented exception according to the JDBC version in use.
     */
    public static SQLException createSQLFeatureNotSupportedException() {
        return new SQLFeatureNotSupportedException();
    }

    /**
     * Create a SQLFeatureNotSupportedException or a NotImplemented exception according to the JDBC version in use.
     * 
     * @param message
     * @param sqlState
     * @param interceptor
     */
    public static SQLException createSQLFeatureNotSupportedException(String message, String sqlState, ExceptionInterceptor interceptor) throws SQLException {
        SQLException newEx = new SQLFeatureNotSupportedException(message, sqlState);
        return runThroughExceptionInterceptor(interceptor, newEx);
    }
}
