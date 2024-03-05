/*
 * Copyright (c) 2002, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package com.mysql.cj.exceptions;

import java.util.HashMap;
import java.util.Map;

import com.mysql.cj.Messages;

/**
 * Constants representing MySQL error numbers returned by the server in error messages.
 */
public final class MysqlErrorNumbers {

    public final static int ER_ERROR_MESSAGES = 298;
    public final static int ER_HASHCHK = 1000; //SQLSTATE: HY000 Message: hashchk
    public final static int ER_NISAMCHK = 1001; //SQLSTATE: HY000 Message: isamchk
    public final static int ER_NO = 1002; //SQLSTATE: HY000 Message: NO; Used in the construction of other messages.
    public final static int ER_YES = 1003; //SQLSTATE: HY000 Message: YES
    public final static int ER_CANT_CREATE_FILE = 1004; //SQLSTATE: HY000 Message: Can't create file '%s' (errno: %d - %s)
    public final static int ER_CANT_CREATE_TABLE = 1005; //SQLSTATE: HY000 Message: Can't create table '%s' (errno: %d)
    public final static int ER_CANT_CREATE_DB = 1006; //SQLSTATE: HY000 Message: Can't create database '%s' (errno: %d)
    public final static int ER_DB_CREATE_EXISTS = 1007; //SQLSTATE: HY000 Message: Can't create database '%s'; database exists...
    public final static int ER_DB_DROP_EXISTS = 1008; //SQLSTATE: HY000 Message: Can't drop database '%s'; database doesn't exist
    public final static int ER_DB_DROP_DELETE = 1009; //SQLSTATE: HY000 Message: Error dropping database (can't delete '%s', errno: %d)
    public final static int ER_DB_DROP_RMDIR = 1010; //SQLSTATE: HY000 Message: Error dropping database (can't rmdir '%s', errno: %d)
    public final static int ER_CANT_DELETE_FILE = 1011; //SQLSTATE: HY000 Message: Error on delete of '%s' (errno: %d - %s)
    public final static int ER_CANT_FIND_SYSTEM_REC = 1012; //SQLSTATE: HY000 Message: Can't read record in system table
    public final static int ER_CANT_GET_STAT = 1013; //SQLSTATE: HY000 Message: Can't get status of '%s' (errno: %d - %s)
    public final static int ER_CANT_GET_WD = 1014; //SQLSTATE: HY000 Message: Can't get working directory (errno: %d - %s)
    public final static int ER_CANT_LOCK = 1015; //SQLSTATE: HY000 Message: Can't lock file (errno: %d - %s)
    public final static int ER_CANT_OPEN_FILE = 1016; //SQLSTATE: HY000 Message: Can't open file: '%s' (errno: %d - %s)
    public final static int ER_FILE_NOT_FOUND = 1017; //SQLSTATE: HY000 Message: Can't find file: '%s' (errno: %d - %s)
    public final static int ER_CANT_READ_DIR = 1018; //SQLSTATE: HY000 Message: Can't read dir of '%s' (errno: %d - %s)
    public final static int ER_CANT_SET_WD = 1019; //SQLSTATE: HY000 Message: Can't change dir to '%s' (errno: %d - %s)
    public final static int ER_CHECKREAD = 1020; //SQLSTATE: HY000 Message: Record has changed since last read in table '%s'
    public final static int ER_DISK_FULL = 1021; //SQLSTATE: HY000 Message: Disk full (%s); waiting for someone to free some space... (errno: %d - %s)
    public final static int ER_DUP_KEY = 1022; //SQLSTATE: 23000 Message: Can't write; duplicate key in table '%s'
    public final static int ER_ERROR_ON_CLOSE = 1023; //SQLSTATE: HY000 Message: Error on close of '%s' (errno: %d - %s)
    public final static int ER_ERROR_ON_READ = 1024; //SQLSTATE: HY000 Message: Error reading file '%s' (errno: %d - %s)
    public final static int ER_ERROR_ON_RENAME = 1025; //SQLSTATE: HY000 Message: Error on rename of '%s' to '%s' (errno: %d - %s)
    public final static int ER_ERROR_ON_WRITE = 1026; //SQLSTATE: HY000 Message: Error writing file '%s' (errno: %d - %s)
    public final static int ER_FILE_USED = 1027; //SQLSTATE: HY000 Message: '%s' is locked against change
    public final static int ER_FILSORT_ABORT = 1028; //SQLSTATE: HY000 Message: Sort aborted
    public final static int ER_FORM_NOT_FOUND = 1029; //SQLSTATE: HY000 Message: View '%s' doesn't exist for '%s'
    public final static int ER_GET_ERRNO = 1030; //SQLSTATE: HY000 Message: Got error %d from storage engine...
    public final static int ER_ILLEGAL_HA = 1031; //SQLSTATE: HY000 Message: Table storage engine for '%s' doesn't have this option
    public final static int ER_KEY_NOT_FOUND = 1032; //SQLSTATE: HY000 Message: Can't find record in '%s'
    public final static int ER_NOT_FORM_FILE = 1033; //SQLSTATE: HY000 Message: Incorrect information in file: '%s'
    public final static int ER_NOT_KEYFILE = 1034; //SQLSTATE: HY000 Message: Incorrect key file for table '%s'; try to repair it
    public final static int ER_OLD_KEYFILE = 1035; //SQLSTATE: HY000 Message: Old key file for table '%s'; repair it!
    public final static int ER_OPEN_AS_READONLY = 1036; //SQLSTATE: HY000 Message: Table '%s' is read only
    public final static int ER_OUTOFMEMORY = 1037; //SQLSTATE: HY001 Message: Out of memory; restart server and try again (needed %d bytes)
    public final static int ER_OUT_OF_SORTMEMORY = 1038; //SQLSTATE: HY001 Message: Out of sort memory, consider increasing server sort buffer size
    public final static int ER_UNEXPECTED_EOF = 1039; //SQLSTATE: HY000 Message: Unexpected EOF found when reading file '%s' (errno: %d - %s)
    public final static int ER_CON_COUNT_ERROR = 1040; //SQLSTATE: 08004 Message: Too many connections
    public final static int ER_OUT_OF_RESOURCES = 1041; //SQLSTATE: HY000 Message: Out of memory; check if mysqld or some other process uses all available memory; if not, you may have to use 'ulimit' to allow mysqld to use more memory or you can add more swap space
    public final static int ER_BAD_HOST_ERROR = 1042; //SQLSTATE: 08S01 Message: Can't get hostname for your address
    public final static int ER_HANDSHAKE_ERROR = 1043; //SQLSTATE: 08S01 Message: Bad handshake
    public final static int ER_DBACCESS_DENIED_ERROR = 1044; //SQLSTATE: 42000 Message: Access denied for user '%s'@'%s' to database '%s'
    public final static int ER_ACCESS_DENIED_ERROR = 1045; //SQLSTATE: 28000 Message: Access denied for user '%s'@'%s' (using password: %s)
    public final static int ER_NO_DB_ERROR = 1046; //SQLSTATE: 3D000 Message: No database selected
    public final static int ER_UNKNOWN_COM_ERROR = 1047; //SQLSTATE: 08S01 Message: Unknown command
    public final static int ER_BAD_NULL_ERROR = 1048; //SQLSTATE: 23000 Message: Column '%s' cannot be null
    public final static int ER_BAD_DB_ERROR = 1049; //SQLSTATE: 42000 Message: Unknown database '%s'
    public final static int ER_TABLE_EXISTS_ERROR = 1050; //SQLSTATE: 42S01 Message: Table '%s' already exists
    public final static int ER_BAD_TABLE_ERROR = 1051; //SQLSTATE: 42S02 Message: Unknown table '%s'
    public final static int ER_NON_UNIQ_ERROR = 1052; //SQLSTATE: 23000 Message: Column '%s' in %s is ambiguous
    public final static int ER_SERVER_SHUTDOWN = 1053; //SQLSTATE: 08S01 Message: Server shutdown in progress
    public final static int ER_BAD_FIELD_ERROR = 1054; //SQLSTATE: 42S22 Message: Unknown column '%s' in '%s'
    public final static int ER_WRONG_FIELD_WITH_GROUP = 1055; //SQLSTATE: 42000 Message: '%s' isn't in GROUP BY
    public final static int ER_WRONG_GROUP_FIELD = 1056; //SQLSTATE: 42000 Message: Can't group on '%s'
    public final static int ER_WRONG_SUM_SELECT = 1057; //SQLSTATE: 42000 Message: Statement has sum functions and columns in same statement
    public final static int ER_WRONG_VALUE_COUNT = 1058; //SQLSTATE: 21S01 Message: Column count doesn't match value count
    public final static int ER_TOO_LONG_IDENT = 1059; //SQLSTATE: 42000 Message: Identifier name '%s' is too long
    public final static int ER_DUP_FIELDNAME = 1060; //SQLSTATE: 42S21 Message: Duplicate column name '%s'
    public final static int ER_DUP_KEYNAME = 1061; //SQLSTATE: 42000 Message: Duplicate key name '%s'
    public final static int ER_DUP_ENTRY = 1062; //SQLSTATE: 23000 Message: Duplicate entry '%s' for key %d
    public final static int ER_WRONG_FIELD_SPEC = 1063; //SQLSTATE: 42000 Message: Incorrect column specifier for column '%s'
    public final static int ER_PARSE_ERROR = 1064; //SQLSTATE: 42000 Message: %s near '%s' at line %d
    public final static int ER_EMPTY_QUERY = 1065; //SQLSTATE: 42000 Message: Query was empty
    public final static int ER_NONUNIQ_TABLE = 1066; //SQLSTATE: 42000 Message: Not unique table/alias: '%s'
    public final static int ER_INVALID_DEFAULT = 1067; //SQLSTATE: 42000 Message: Invalid default value for '%s'
    public final static int ER_MULTIPLE_PRI_KEY = 1068; //SQLSTATE: 42000 Message: Multiple primary key defined
    public final static int ER_TOO_MANY_KEYS = 1069; //SQLSTATE: 42000 Message: Too many keys specified; max %d keys allowed
    public final static int ER_TOO_MANY_KEY_PARTS = 1070; //SQLSTATE: 42000 Message: Too many key parts specified; max %d parts allowed
    public final static int ER_TOO_LONG_KEY = 1071; //SQLSTATE: 42000 Message: Specified key was too long; max key length is %d bytes
    public final static int ER_KEY_COLUMN_DOES_NOT_EXITS = 1072; //SQLSTATE: 42000 Message: Key column '%s' doesn't exist in table
    public final static int ER_BLOB_USED_AS_KEY = 1073; //SQLSTATE: 42000 Message: BLOB column '%s' can't be used in key specification with the used table type
    public final static int ER_TOO_BIG_FIELDLENGTH = 1074; //SQLSTATE: 42000 Message: Column length too big for column '%s' (max = %lu); use BLOB or TEXT instead
    public final static int ER_WRONG_AUTO_KEY = 1075; //SQLSTATE: 42000 Message: Incorrect table definition; there can be only one auto column and it must be defined as a key
    public final static int ER_READY = 1076; //SQLSTATE: HY000 Message: %s: ready for connections. Version: '%s' socket: '%s' port: %d
    public final static int ER_NORMAL_SHUTDOWN = 1077; //SQLSTATE: HY000 Message: %s: Normal shutdown
    public final static int ER_GOT_SIGNAL = 1078; //SQLSTATE: HY000 Message: %s: Got signal %d. Aborting!
    public final static int ER_SHUTDOWN_COMPLETE = 1079; //SQLSTATE: HY000 Message: %s: Shutdown complete
    public final static int ER_FORCING_CLOSE = 1080; //SQLSTATE: 08S01 Message: %s: Forcing close of thread %ld user: '%s'
    public final static int ER_IPSOCK_ERROR = 1081; //SQLSTATE: 08S01 Message: Can't create IP socket
    public final static int ER_NO_SUCH_INDEX = 1082; //SQLSTATE: 42S12 Message: Table '%s' has no index like the one used in CREATE INDEX; recreate the table
    public final static int ER_WRONG_FIELD_TERMINATORS = 1083; //SQLSTATE: 42000 Message: Field separator argument is not what is expected; check the manual
    public final static int ER_BLOBS_AND_NO_TERMINATED = 1084; //SQLSTATE: 42000 Message: You can't use fixed rowlength with BLOBs; please use 'fields terminated by'
    public final static int ER_TEXTFILE_NOT_READABLE = 1085; //SQLSTATE: HY000 Message: The file '%s' must be in the database directory or be readable by all
    public final static int ER_FILE_EXISTS_ERROR = 1086; //SQLSTATE: HY000 Message: File '%s' already exists
    public final static int ER_LOAD_INFO = 1087; //SQLSTATE: HY000 Message: Records: %ld Deleted: %ld Skipped: %ld Warnings: %ld
    public final static int ER_ALTER_INFO = 1088; //SQLSTATE: HY000 Message: Records: %ld Duplicates: %ld
    public final static int ER_WRONG_SUB_KEY = 1089; //SQLSTATE: HY000 Message: Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, or the storage engine doesn't support unique prefix keys
    public final static int ER_CANT_REMOVE_ALL_FIELDS = 1090; //SQLSTATE: 42000 Message: You can't delete all columns with ALTER TABLE; use DROP TABLE instead
    public final static int ER_CANT_DROP_FIELD_OR_KEY = 1091; //SQLSTATE: 42000 Message: Can't DROP '%s'; check that column/key exists
    public final static int ER_INSERT_INFO = 1092; //SQLSTATE: HY000 Message: Records: %ld Duplicates: %ld Warnings: %ld
    public final static int ER_UPDATE_TABLE_USED = 1093; //SQLSTATE: HY000 Message: You can't specify target table '%s' for update in FROM clause
    public final static int ER_NO_SUCH_THREAD = 1094; //SQLSTATE: HY000 Message: Unknown thread id: %lu
    public final static int ER_KILL_DENIED_ERROR = 1095; //SQLSTATE: HY000 Message: You are not owner of thread %lu
    public final static int ER_NO_TABLES_USED = 1096; //SQLSTATE: HY000 Message: No tables used
    public final static int ER_TOO_BIG_SET = 1097; //SQLSTATE: HY000 Message: Too many strings for column %s and SET
    public final static int ER_NO_UNIQUE_LOGFILE = 1098; //SQLSTATE: HY000 Message: Can't generate a unique log-filename %s.(1-999)
    public final static int ER_TABLE_NOT_LOCKED_FOR_WRITE = 1099; //SQLSTATE: HY000 Message: Table '%s' was locked with a READ lock and can't be updated
    public final static int ER_TABLE_NOT_LOCKED = 1100; //SQLSTATE: HY000 Message: Table '%s' was not locked with LOCK TABLES
    public final static int ER_BLOB_CANT_HAVE_DEFAULT = 1101; //SQLSTATE: 42000 Message: BLOB/TEXT column '%s' can't have a default value
    public final static int ER_WRONG_DB_NAME = 1102; //SQLSTATE: 42000 Message: Incorrect database name '%s'
    public final static int ER_WRONG_TABLE_NAME = 1103; //SQLSTATE: 42000 Message: Incorrect table name '%s'
    public final static int ER_TOO_BIG_SELECT = 1104; //SQLSTATE: 42000 Message: The SELECT would examine more than MAX_JOIN_SIZE rows; check your WHERE and use SET SQL_BIG_SELECTS=1 or SET MAX_JOIN_SIZE=# if the SELECT is okay
    public final static int ER_UNKNOWN_ERROR = 1105; //SQLSTATE: HY000 Message: Unknown error
    public final static int ER_UNKNOWN_PROCEDURE = 1106; //SQLSTATE: 42000 Message: Unknown procedure '%s'
    public final static int ER_WRONG_PARAMCOUNT_TO_PROCEDURE = 1107; //SQLSTATE: 42000 Message: Incorrect parameter count to procedure '%s'
    public final static int ER_WRONG_PARAMETERS_TO_PROCEDURE = 1108; //SQLSTATE: HY000 Message: Incorrect parameters to procedure '%s'
    public final static int ER_UNKNOWN_TABLE = 1109; //SQLSTATE: 42S02 Message: Unknown table '%s' in %s
    public final static int ER_FIELD_SPECIFIED_TWICE = 1110; //SQLSTATE: 42000 Message: Column '%s' specified twice
    public final static int ER_INVALID_GROUP_FUNC_USE = 1111; //SQLSTATE: HY000 Message: Invalid use of group function
    public final static int ER_UNSUPPORTED_EXTENSION = 1112; //SQLSTATE: 42000 Message: Table '%s' uses an extension that doesn't exist in this MySQL version
    public final static int ER_TABLE_MUST_HAVE_COLUMNS = 1113; //SQLSTATE: 42000 Message: A table must have at least 1 column
    public final static int ER_RECORD_FILE_FULL = 1114; //SQLSTATE: HY000 Message: The table '%s' is full
    public final static int ER_UNKNOWN_CHARACTER_SET = 1115; //SQLSTATE: 42000 Message: Unknown character set: '%s'
    public final static int ER_TOO_MANY_TABLES = 1116; //SQLSTATE: HY000 Message: Too many tables; MySQL can only use %d tables in a join
    public final static int ER_TOO_MANY_FIELDS = 1117; //SQLSTATE: HY000 Message: Too many columns
    public final static int ER_TOO_BIG_ROWSIZE = 1118; //SQLSTATE: 42000 Message: Row size too large. The maximum row size for the used table type, not counting BLOBs, is %ld. This includes storage overhead, check the manual. You have to change some columns to TEXT or BLOBs
    public final static int ER_STACK_OVERRUN = 1119; //SQLSTATE: HY000 Message: Thread stack overrun: Used: %ld of a %ld stack. Use 'mysqld --thread_stack=#' to specify a bigger stack if needed
    public final static int ER_WRONG_OUTER_JOIN = 1120; //SQLSTATE: 42000 Message: Cross dependency found in OUTER JOIN; examine your ON conditions
    public final static int ER_NULL_COLUMN_IN_INDEX = 1121; //SQLSTATE: 42000 Message: Table handler doesn't support NULL in given index. Please change column '%s' to be NOT NULL or use another handler
    public final static int ER_CANT_FIND_UDF = 1122; //SQLSTATE: HY000 Message: Can't load function '%s'
    public final static int ER_CANT_INITIALIZE_UDF = 1123; //SQLSTATE: HY000 Message: Can't initialize function '%s'; %s
    public final static int ER_UDF_NO_PATHS = 1124; //SQLSTATE: HY000 Message: No paths allowed for shared library
    public final static int ER_UDF_EXISTS = 1125; //SQLSTATE: HY000 Message: Function '%s' already exists
    public final static int ER_CANT_OPEN_LIBRARY = 1126; //SQLSTATE: HY000 Message: Can't open shared library '%s' (errno: %d %s)
    public final static int ER_CANT_FIND_DL_ENTRY = 1127; //SQLSTATE: HY000 Message: Can't find symbol '%s' in library
    public final static int ER_FUNCTION_NOT_DEFINED = 1128; //SQLSTATE: HY000 Message: Function '%s' is not defined
    public final static int ER_HOST_IS_BLOCKED = 1129; //SQLSTATE: HY000 Message: Host '%s' is blocked because of many connection errors; unblock with 'mysqladmin flush-hosts'
    public final static int ER_HOST_NOT_PRIVILEGED = 1130; //SQLSTATE: HY000 Message: Host '%s' is not allowed to connect to this MySQL server
    public final static int ER_PASSWORD_ANONYMOUS_USER = 1131; //SQLSTATE: 42000 Message: You are using MySQL as an anonymous user and anonymous users are not allowed to change passwords
    public final static int ER_PASSWORD_NOT_ALLOWED = 1132; //SQLSTATE: 42000 Message: You must have privileges to update tables in the mysql database to be able to change passwords for others
    public final static int ER_PASSWORD_NO_MATCH = 1133; //SQLSTATE: 42000 Message: Can't find any matching row in the user table
    public final static int ER_UPDATE_INFO = 1134; //SQLSTATE: HY000 Message: Rows matched: %ld Changed: %ld Warnings: %ld
    public final static int ER_CANT_CREATE_THREAD = 1135; //SQLSTATE: HY000 Message: Can't create a new thread (errno %d); if you are not out of available memory, you can consult the manual for a possible OS-dependent bug
    public final static int ER_WRONG_VALUE_COUNT_ON_ROW = 1136; //SQLSTATE: 21S01 Message: Column count doesn't match value count at row %ld
    public final static int ER_CANT_REOPEN_TABLE = 1137; //SQLSTATE: HY000 Message: Can't reopen table: '%s'
    public final static int ER_INVALID_USE_OF_NULL = 1138; //SQLSTATE: 22004 Message: Invalid use of NULL value
    public final static int ER_REGEXP_ERROR = 1139; //SQLSTATE: 42000 Message: Got error '%s' from regexp
    public final static int ER_MIX_OF_GROUP_FUNC_AND_FIELDS = 1140; //SQLSTATE: 42000 Message: Mixing of GROUP columns (MIN(),MAX(),COUNT(),...) with no GROUP columns is illegal if there is no GROUP BY clause
    public final static int ER_NONEXISTING_GRANT = 1141; //SQLSTATE: 42000 Message: There is no such grant defined for user '%s' on host '%s'
    public final static int ER_TABLEACCESS_DENIED_ERROR = 1142; //SQLSTATE: 42000 Message: %s command denied to user '%s'@'%s' for table '%s'
    public final static int ER_COLUMNACCESS_DENIED_ERROR = 1143; //SQLSTATE: 42000 Message: %s command denied to user '%s'@'%s' for column '%s' in table '%s'
    public final static int ER_ILLEGAL_GRANT_FOR_TABLE = 1144; //SQLSTATE: 42000 Message: Illegal GRANT/REVOKE command; please consult the manual to see which privileges can be used
    public final static int ER_GRANT_WRONG_HOST_OR_USER = 1145; //SQLSTATE: 42000 Message: The host or user argument to GRANT is too long
    public final static int ER_NO_SUCH_TABLE = 1146; //SQLSTATE: 42S02 Message: Table '%s.%s' doesn't exist
    public final static int ER_NONEXISTING_TABLE_GRANT = 1147; //SQLSTATE: 42000 Message: There is no such grant defined for user '%s' on host '%s' on table '%s'
    public final static int ER_NOT_ALLOWED_COMMAND = 1148; //SQLSTATE: 42000 Message: The used command is not allowed with this MySQL version
    public final static int ER_SYNTAX_ERROR = 1149; //SQLSTATE: 42000 Message: You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use
    public final static int ER_DELAYED_CANT_CHANGE_LOCK = 1150; //SQLSTATE: HY000 Message: Delayed insert thread couldn't get requested lock for table %s
    public final static int ER_TOO_MANY_DELAYED_THREADS = 1151; //SQLSTATE: HY000 Message: Too many delayed threads in use
    public final static int ER_ABORTING_CONNECTION = 1152; //SQLSTATE: 08S01 Message: Aborted connection %ld to db: '%s' user: '%s' (%s)
    public final static int ER_NET_PACKET_TOO_LARGE = 1153; //SQLSTATE: 08S01 Message: Got a packet bigger than 'max_allowed_packet' bytes
    public final static int ER_NET_READ_ERROR_FROM_PIPE = 1154; //SQLSTATE: 08S01 Message: Got a read error from the connection pipe
    public final static int ER_NET_FCNTL_ERROR = 1155; //SQLSTATE: 08S01 Message: Got an error from fcntl()
    public final static int ER_NET_PACKETS_OUT_OF_ORDER = 1156; //SQLSTATE: 08S01 Message: Got packets out of order
    public final static int ER_NET_UNCOMPRESS_ERROR = 1157; //SQLSTATE: 08S01 Message: Couldn't uncompress communication packet
    public final static int ER_NET_READ_ERROR = 1158; //SQLSTATE: 08S01 Message: Got an error reading communication packets
    public final static int ER_NET_READ_INTERRUPTED = 1159; //SQLSTATE: 08S01 Message: Got timeout reading communication packets
    public final static int ER_NET_ERROR_ON_WRITE = 1160; //SQLSTATE: 08S01 Message: Got an error writing communication packets
    public final static int ER_NET_WRITE_INTERRUPTED = 1161; //SQLSTATE: 08S01 Message: Got timeout writing communication packets
    public final static int ER_TOO_LONG_STRING = 1162; //SQLSTATE: 42000 Message: Result string is longer than 'max_allowed_packet' bytes
    public final static int ER_TABLE_CANT_HANDLE_BLOB = 1163; //SQLSTATE: 42000 Message: The used table type doesn't support BLOB/TEXT columns
    public final static int ER_TABLE_CANT_HANDLE_AUTO_INCREMENT = 1164; //SQLSTATE: 42000 Message: The used table type doesn't support AUTO_INCREMENT columns
    public final static int ER_DELAYED_INSERT_TABLE_LOCKED = 1165; //SQLSTATE: HY000 Message: INSERT DELAYED can't be used with table '%s' because it is locked with LOCK TABLES
    public final static int ER_WRONG_COLUMN_NAME = 1166; //SQLSTATE: 42000 Message: Incorrect column name '%s'
    public final static int ER_WRONG_KEY_COLUMN = 1167; //SQLSTATE: 42000 Message: The used storage engine can't index column '%s'
    public final static int ER_WRONG_MRG_TABLE = 1168; //SQLSTATE: HY000 Message: Unable to open underlying table which is differently defined or of non-MyISAM type or doesn't exist
    public final static int ER_DUP_UNIQUE = 1169; //SQLSTATE: 23000 Message: Can't write, because of unique constraint, to table '%s'
    public final static int ER_BLOB_KEY_WITHOUT_LENGTH = 1170; //SQLSTATE: 42000 Message: BLOB/TEXT column '%s' used in key specification without a key length
    public final static int ER_PRIMARY_CANT_HAVE_NULL = 1171; //SQLSTATE: 42000 Message: All parts of a PRIMARY KEY must be NOT NULL; if you need NULL in a key, use UNIQUE instead
    public final static int ER_TOO_MANY_ROWS = 1172; //SQLSTATE: 42000 Message: Result consisted of more than one row
    public final static int ER_REQUIRES_PRIMARY_KEY = 1173; //SQLSTATE: 42000 Message: This table type requires a primary key
    public final static int ER_NO_RAID_COMPILED = 1174; //SQLSTATE: HY000 Message: This version of MySQL is not compiled with RAID support
    public final static int ER_UPDATE_WITHOUT_KEY_IN_SAFE_MODE = 1175; //SQLSTATE: HY000 Message: You are using safe update mode and you tried to update a table without a WHERE that uses a KEY column
    public final static int ER_KEY_DOES_NOT_EXITS = 1176; //SQLSTATE: 42000 Message: Key '%s' doesn't exist in table '%s'
    public final static int ER_CHECK_NO_SUCH_TABLE = 1177; //SQLSTATE: 42000 Message: Can't open table
    public final static int ER_CHECK_NOT_IMPLEMENTED = 1178; //SQLSTATE: 42000 Message: The storage engine for the table doesn't support %s
    public final static int ER_CANT_DO_THIS_DURING_AN_TRANSACTION = 1179; //SQLSTATE: 25000 Message: You are not allowed to execute this command in a transaction
    public final static int ER_ERROR_DURING_COMMIT = 1180; //SQLSTATE: HY000 Message: Got error %d during COMMIT
    public final static int ER_ERROR_DURING_ROLLBACK = 1181; //SQLSTATE: HY000 Message: Got error %d during ROLLBACK
    public final static int ER_ERROR_DURING_FLUSH_LOGS = 1182; //SQLSTATE: HY000 Message: Got error %d during FLUSH_LOGS
    public final static int ER_ERROR_DURING_CHECKPOINT = 1183; //SQLSTATE: HY000 Message: Got error %d during CHECKPOINT
    public final static int ER_NEW_ABORTING_CONNECTION = 1184; //SQLSTATE: 08S01 Message: Aborted connection %ld to db: '%s' user: '%s' host: '%s' (%s)
    public final static int ER_DUMP_NOT_IMPLEMENTED = 1185; //SQLSTATE: HY000 Message: The storage engine for the table does not support binary table dump
    @Deprecated
    public final static int ER_FLUSH_MASTER_BINLOG_CLOSED = 1186;
    public final static int ER_FLUSH_SOURCE_BINLOG_CLOSED = 1186; //SQLSTATE: HY000 Message: Binlog closed, cannot RESET SOURCE
    public final static int ER_INDEX_REBUILD = 1187; //SQLSTATE: HY000 Message: Failed rebuilding the index of dumped table '%s'
    @Deprecated
    public final static int ER_MASTER = 1188;
    public final static int ER_SOURCE = 1188; //SQLSTATE: HY000 Message: Error from source: '%s'
    @Deprecated
    public final static int ER_MASTER_NET_READ = 1189;
    public final static int ER_SOURCE_NET_READ = 1189; //SQLSTATE: 08S01 Message: Net error reading from source
    @Deprecated
    public final static int ER_MASTER_NET_WRITE = 1190;
    public final static int ER_SOURCE_NET_WRITE = 1190; //SQLSTATE: 08S01 Message: Net error writing to source
    public final static int ER_FT_MATCHING_KEY_NOT_FOUND = 1191; //SQLSTATE: HY000 Message: Can't find FULLTEXT index matching the column list
    public final static int ER_LOCK_OR_ACTIVE_TRANSACTION = 1192; //SQLSTATE: HY000 Message: Can't execute the given command because you have active locked tables or an active transaction
    public final static int ER_UNKNOWN_SYSTEM_VARIABLE = 1193; //SQLSTATE: HY000 Message: Unknown system variable '%s'
    public final static int ER_CRASHED_ON_USAGE = 1194; //SQLSTATE: HY000 Message: Table '%s' is marked as crashed and should be repaired
    public final static int ER_CRASHED_ON_REPAIR = 1195; //SQLSTATE: HY000 Message: Table '%s' is marked as crashed and last (automatic?) repair failed
    public final static int ER_WARNING_NOT_COMPLETE_ROLLBACK = 1196; //SQLSTATE: HY000 Message: Some non-transactional changed tables couldn't be rolled back
    public final static int ER_TRANS_CACHE_FULL = 1197; //SQLSTATE: HY000 Message: Multi-statement transaction required more than 'max_binlog_cache_size' bytes of storage; increase this mysqld variable and try again
    @Deprecated
    public final static int ER_SLAVE_MUST_STOP = 1198;
    public final static int ER_REPLICA_MUST_STOP = 1198; //SQLSTATE: HY000 Message: This operation cannot be performed with a running replica; run STOP REPLICA first
    @Deprecated
    public final static int ER_SLAVE_NOT_RUNNING = 1199;
    public final static int ER_REPLICA_NOT_RUNNING = 1199; //SQLSTATE: HY000 Message: This operation requires a running replica; configure replica and do START REPLICA
    @Deprecated
    public final static int ER_BAD_SLAVE = 1200;
    public final static int ER_BAD_REPLICA = 1200; //SQLSTATE: HY000 Message: The server is not configured as replica; fix in config file or with CHANGE SOURCE TO
    @Deprecated
    public final static int ER_MASTER_INFO = 1201;
    public final static int ER_SOURCE_INFO = 1201; //SQLSTATE: HY000 Message: Could not initialize source info structure; more error messages can be found in the MySQL error log
    @Deprecated
    public final static int ER_SLAVE_THREAD = 1202;
    public final static int ER_REPLICA_THREAD = 1202; //SQLSTATE: HY000 Message: Could not create replica thread; check system resources
    public final static int ER_TOO_MANY_USER_CONNECTIONS = 1203; //SQLSTATE: 42000 Message: User %s already has more than 'max_user_connections' active connections
    public final static int ER_SET_CONSTANTS_ONLY = 1204; //SQLSTATE: HY000 Message: You may only use constant expressions with SET
    public final static int ER_LOCK_WAIT_TIMEOUT = 1205; //SQLSTATE: HY000 Message: Lock wait timeout exceeded; try restarting transaction
    public final static int ER_LOCK_TABLE_FULL = 1206; //SQLSTATE: HY000 Message: The total number of locks exceeds the lock table size
    public final static int ER_READ_ONLY_TRANSACTION = 1207; //SQLSTATE: 25000 Message: Update locks cannot be acquired during a READ UNCOMMITTED transaction
    public final static int ER_DROP_DB_WITH_READ_LOCK = 1208; //SQLSTATE: HY000 Message: DROP DATABASE not allowed while thread is holding global read lock
    public final static int ER_CREATE_DB_WITH_READ_LOCK = 1209; //SQLSTATE: HY000 Message: CREATE DATABASE not allowed while thread is holding global read lock
    public final static int ER_WRONG_ARGUMENTS = 1210; //SQLSTATE: HY000 Message: Incorrect arguments to %s
    public final static int ER_NO_PERMISSION_TO_CREATE_USER = 1211; //SQLSTATE: 42000 Message: '%s'@'%s' is not allowed to create new users
    public final static int ER_UNION_TABLES_IN_DIFFERENT_DIR = 1212; //SQLSTATE: HY000 Message: Incorrect table definition; all MERGE tables must be in the same database
    public final static int ER_LOCK_DEADLOCK = 1213; //SQLSTATE: 40001 Message: Deadlock found when trying to get lock; try restarting transaction
    public final static int ER_TABLE_CANT_HANDLE_FT = 1214; //SQLSTATE: HY000 Message: The used table type doesn't support FULLTEXT indexes
    public final static int ER_CANNOT_ADD_FOREIGN = 1215; //SQLSTATE: HY000 Message: Cannot add foreign key constraint
    public final static int ER_NO_REFERENCED_ROW = 1216; //SQLSTATE: 23000 Message: Cannot add or update a child row: a foreign key constraint fails
    public final static int ER_ROW_IS_REFERENCED = 1217; //SQLSTATE: 23000 Message: Cannot delete or update a parent row: a foreign key constraint fails
    @Deprecated
    public final static int ER_CONNECT_TO_MASTER = 1218;
    public final static int ER_CONNECT_TO_SOURCE = 1218; //SQLSTATE: 08S01 Message: Error connecting to source: %s
    @Deprecated
    public final static int ER_QUERY_ON_MASTER = 1219;
    public final static int ER_QUERY_ON_SOURCE = 1219; //SQLSTATE: HY000 Message: Error running query on source: %s
    public final static int ER_ERROR_WHEN_EXECUTING_COMMAND = 1220; //SQLSTATE: HY000 Message: Error when executing command %s: %s
    public final static int ER_WRONG_USAGE = 1221; //SQLSTATE: HY000 Message: Incorrect usage of %s and %s
    public final static int ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT = 1222; //SQLSTATE: 21000 Message: The used SELECT statements have a different number of columns
    public final static int ER_CANT_UPDATE_WITH_READLOCK = 1223; //SQLSTATE: HY000 Message: Can't execute the query because you have a conflicting read lock
    public final static int ER_MIXING_NOT_ALLOWED = 1224; //SQLSTATE: HY000 Message: Mixing of transactional and non-transactional tables is disabled
    public final static int ER_DUP_ARGUMENT = 1225; //SQLSTATE: HY000 Message: Option '%s' used twice in statement
    public final static int ER_USER_LIMIT_REACHED = 1226; //SQLSTATE: 42000 Message: User '%s' has exceeded the '%s' resource (current value: %ld)
    public final static int ER_SPECIFIC_ACCESS_DENIED_ERROR = 1227; //SQLSTATE: 42000 Message: Access denied; you need (at least one of) the %s privilege(s) for this operation
    public final static int ER_LOCAL_VARIABLE = 1228; //SQLSTATE: HY000 Message: Variable '%s' is a SESSION variable and can't be used with SET GLOBAL
    public final static int ER_GLOBAL_VARIABLE = 1229; //SQLSTATE: HY000 Message: Variable '%s' is a GLOBAL variable and should be set with SET GLOBAL
    public final static int ER_NO_DEFAULT = 1230; //SQLSTATE: 42000 Message: Variable '%s' doesn't have a default value
    public final static int ER_WRONG_VALUE_FOR_VAR = 1231; //SQLSTATE: 42000 Message: Variable '%s' can't be set to the value of '%s'
    public final static int ER_WRONG_TYPE_FOR_VAR = 1232; //SQLSTATE: 42000 Message: Incorrect argument type to variable '%s'
    public final static int ER_VAR_CANT_BE_READ = 1233; //SQLSTATE: HY000 Message: Variable '%s' can only be set, not read
    public final static int ER_CANT_USE_OPTION_HERE = 1234; //SQLSTATE: 42000 Message: Incorrect usage/placement of '%s'
    public final static int ER_NOT_SUPPORTED_YET = 1235; //SQLSTATE: 42000 Message: This version of MySQL doesn't yet support '%s'
    @Deprecated
    public final static int ER_MASTER_FATAL_ERROR_READING_BINLOG = 1236;
    public final static int ER_SOURCE_FATAL_ERROR_READING_BINLOG = 1236; //SQLSTATE: HY000 Message: Got fatal error %d from source when reading data from binary log: '%s'
    @Deprecated
    public final static int ER_SLAVE_IGNORED_TABLE = 1237;
    public final static int ER_REPLICA_IGNORED_TABLE = 1237; //SQLSTATE: HY000 Message: Replica SQL thread ignored the query because of replicate-*-table rules
    public final static int ER_INCORRECT_GLOBAL_LOCAL_VAR = 1238; //SQLSTATE: HY000 Message: Variable '%s' is a %s variable
    public final static int ER_WRONG_FK_DEF = 1239; //SQLSTATE: 42000 Message: Incorrect foreign key definition for '%s': %s
    public final static int ER_KEY_REF_DO_NOT_MATCH_TABLE_REF = 1240; //SQLSTATE: HY000 Message: Key reference and table reference don't match
    public final static int ER_OPERAND_COLUMNS = 1241; //SQLSTATE: 21000 Message: Operand should contain %d column(s)
    public final static int ER_SUBQUERY_NO_1_ROW = 1242; //SQLSTATE: 21000 Message: Subquery returns more than 1 row
    public final static int ER_UNKNOWN_STMT_HANDLER = 1243; //SQLSTATE: HY000 Message: Unknown prepared statement handler (%.*s) given to %s
    public final static int ER_CORRUPT_HELP_DB = 1244; //SQLSTATE: HY000 Message: Help database is corrupt or does not exist
    public final static int ER_CYCLIC_REFERENCE = 1245; //SQLSTATE: HY000 Message: Cyclic reference on subqueries
    public final static int ER_AUTO_CONVERT = 1246; //SQLSTATE: HY000 Message: Converting column '%s' from %s to %s
    public final static int ER_ILLEGAL_REFERENCE = 1247; //SQLSTATE: 42S22 Message: Reference '%s' not supported (%s)
    public final static int ER_DERIVED_MUST_HAVE_ALIAS = 1248; //SQLSTATE: 42000 Message: Every derived table must have its own alias
    public final static int ER_SELECT_REDUCED = 1249; //SQLSTATE: 01000 Message: Select %u was reduced during optimization
    public final static int ER_TABLENAME_NOT_ALLOWED_HERE = 1250; //SQLSTATE: 42000 Message: Table '%s' from one of the SELECTs cannot be used in %s
    public final static int ER_NOT_SUPPORTED_AUTH_MODE = 1251; //SQLSTATE: 08004 Message: Client does not support authentication protocol requested by server; consider upgrading MySQL client
    public final static int ER_SPATIAL_CANT_HAVE_NULL = 1252; //SQLSTATE: 42000 Message: All parts of a SPATIAL index must be NOT NULL
    public final static int ER_COLLATION_CHARSET_MISMATCH = 1253; //SQLSTATE: 42000 Message: COLLATION '%s' is not valid for CHARACTER SET '%s'
    @Deprecated
    public final static int ER_SLAVE_WAS_RUNNING = 1254;
    public final static int ER_REPLICA_WAS_RUNNING = 1254; //SQLSTATE: HY000 Message: Replica is already running
    @Deprecated
    public final static int ER_SLAVE_WAS_NOT_RUNNING = 1255;
    public final static int ER_REPLICA_WAS_NOT_RUNNING = 1255; //SQLSTATE: HY000 Message: Replica already has been stopped
    public final static int ER_TOO_BIG_FOR_UNCOMPRESS = 1256; //SQLSTATE: HY000 Message: Uncompressed data size too large; the maximum size is %d (probably, length of uncompressed data was corrupted)
    public final static int ER_ZLIB_Z_MEM_ERROR = 1257; //SQLSTATE: HY000 Message: ZLIB: Not enough memory
    public final static int ER_ZLIB_Z_BUF_ERROR = 1258; //SQLSTATE: HY000 Message: ZLIB: Not enough room in the output buffer (probably, length of uncompressed data was corrupted)
    public final static int ER_ZLIB_Z_DATA_ERROR = 1259; //SQLSTATE: HY000 Message: ZLIB: Input data corrupted
    public final static int ER_CUT_VALUE_GROUP_CONCAT = 1260; //SQLSTATE: HY000 Message: Row %u was cut by GROUP_CONCAT()
    public final static int ER_WARN_TOO_FEW_RECORDS = 1261; //SQLSTATE: 01000 Message: Row %ld doesn't contain data for all columns
    public final static int ER_WARN_TOO_MANY_RECORDS = 1262; //SQLSTATE: 01000 Message: Row %ld was truncated; it contained more data than there were input columns
    public final static int ER_WARN_NULL_TO_NOTNULL = 1263; //SQLSTATE: 22004 Message: Column set to default value; NULL supplied to NOT NULL column '%s' at row %ld
    public final static int ER_WARN_DATA_OUT_OF_RANGE = 1264; //SQLSTATE: 22003 Message: Out of range value for column '%s' at row %ld
    public final static int ER_WARN_DATA_TRUNCATED = 1265; //SQLSTATE: 01000 Message: Data truncated for column '%s' at row %ld
    public final static int ER_WARN_USING_OTHER_HANDLER = 1266; //SQLSTATE: HY000 Message: Using storage engine %s for table '%s'
    public final static int ER_CANT_AGGREGATE_2COLLATIONS = 1267; //SQLSTATE: HY000 Message: Illegal mix of collations (%s,%s) and (%s,%s) for operation '%s'
    public final static int ER_DROP_USER = 1268; //SQLSTATE: HY000 Message: Cannot drop one or more of the requested users
    public final static int ER_REVOKE_GRANTS = 1269; //SQLSTATE: HY000 Message: Can't revoke all privileges for one or more of the requested users
    public final static int ER_CANT_AGGREGATE_3COLLATIONS = 1270; //SQLSTATE: HY000 Message: Illegal mix of collations (%s,%s), (%s,%s), (%s,%s) for operation '%s'
    public final static int ER_CANT_AGGREGATE_NCOLLATIONS = 1271; //SQLSTATE: HY000 Message: Illegal mix of collations for operation '%s'
    public final static int ER_VARIABLE_IS_NOT_STRUCT = 1272; //SQLSTATE: HY000 Message: Variable '%s' is not a variable component (can't be used as XXXX.variable_name)
    public final static int ER_UNKNOWN_COLLATION = 1273; //SQLSTATE: HY000 Message: Unknown collation: '%s'
    @Deprecated
    public final static int ER_SLAVE_IGNORED_SSL_PARAMS = 1274;
    public final static int ER_REPLICA_IGNORED_SSL_PARAMS = 1274; //SQLSTATE: HY000 Message: SSL parameters in CHANGE SOURCE are ignored because this MySQL replica was compiled without SSL support; they can be used later if MySQL replica with SSL is started
    public final static int ER_SERVER_IS_IN_SECURE_AUTH_MODE = 1275; //SQLSTATE: HY000 Message: Server is running in --secure-auth mode, but '%s'@'%s' has a password in the old format; please change the password to the new format
    public final static int ER_WARN_FIELD_RESOLVED = 1276; //SQLSTATE: HY000 Message: Field or reference '%s%s%s%s%s' of SELECT #%d was resolved in SELECT #%d
    @Deprecated
    public final static int ER_BAD_SLAVE_UNTIL_COND = 1277;
    public final static int ER_BAD_REPLICA_UNTIL_COND = 1277; //SQLSTATE: HY000 Message: Incorrect parameter or combination of parameters for START REPLICA UNTIL
    @Deprecated
    public final static int ER_MISSING_SKIP_SLAVE = 1278;
    public final static int ER_MISSING_SKIP_REPLICA = 1278; //SQLSTATE: HY000 Message: It is recommended to use --skip-replica-start when doing step-by-step replication with START REPLICA UNTIL; otherwise, you will get problems if you get an unexpected replica's mysqld restart
    public final static int ER_UNTIL_COND_IGNORED = 1279; //SQLSTATE: HY000 Message: SQL thread is not to be started so UNTIL options are ignored
    public final static int ER_WRONG_NAME_FOR_INDEX = 1280; //SQLSTATE: 42000 Message: Incorrect index name '%s'
    public final static int ER_WRONG_NAME_FOR_CATALOG = 1281; //SQLSTATE: 42000 Message: Incorrect catalog name '%s'
    public final static int ER_WARN_QC_RESIZE = 1282; //SQLSTATE: HY000 Message: Query cache failed to set size %lu; new query cache size is %lu
    public final static int ER_BAD_FT_COLUMN = 1283; //SQLSTATE: HY000 Message: Column '%s' cannot be part of FULLTEXT index
    public final static int ER_UNKNOWN_KEY_CACHE = 1284; //SQLSTATE: HY000 Message: Unknown key cache '%s'
    public final static int ER_WARN_HOSTNAME_WONT_WORK = 1285; //SQLSTATE: HY000 Message: MySQL is started in --skip-name-resolve mode; you must restart it without this switch for this grant to work
    public final static int ER_UNKNOWN_STORAGE_ENGINE = 1286; //SQLSTATE: 42000 Message: Unknown storage engine '%s'
    public final static int ER_WARN_DEPRECATED_SYNTAX = 1287; //SQLSTATE: HY000 Message: '%s' is deprecated and will be removed in a future release. Please use %s instead
    public final static int ER_NON_UPDATABLE_TABLE = 1288; //SQLSTATE: HY000 Message: The target table %s of the %s is not updatable
    public final static int ER_FEATURE_DISABLED = 1289; //SQLSTATE: HY000 Message: The '%s' feature is disabled; you need MySQL built with '%s' to have it working
    public final static int ER_OPTION_PREVENTS_STATEMENT = 1290; //SQLSTATE: HY000 Message: The MySQL server is running with the %s option so it cannot execute this statement
    public final static int ER_DUPLICATED_VALUE_IN_TYPE = 1291; //SQLSTATE: HY000 Message: Column '%s' has duplicated value '%s' in %s
    public final static int ER_TRUNCATED_WRONG_VALUE = 1292; //SQLSTATE: 22007 Message: Truncated incorrect %s value: '%s'
    public final static int ER_TOO_MUCH_AUTO_TIMESTAMP_COLS = 1293; //SQLSTATE: HY000 Message: Incorrect table definition; there can be only one TIMESTAMP column with CURRENT_TIMESTAMP in DEFAULT or ON UPDATE clause
    public final static int ER_INVALID_ON_UPDATE = 1294; //SQLSTATE: HY000 Message: Invalid ON UPDATE clause for '%s' column
    public final static int ER_UNSUPPORTED_PS = 1295; //SQLSTATE: HY000 Message: This command is not supported in the prepared statement protocol yet
    public final static int ER_GET_ERRMSG = 1296; //SQLSTATE: HY000 Message: Got error %d '%s' from %s
    public final static int ER_GET_TEMPORARY_ERRMSG = 1297; //SQLSTATE: HY000 Message: Got temporary error %d '%s' from %s
    public final static int ER_UNKNOWN_TIME_ZONE = 1298; //SQLSTATE: HY000 Message: Unknown or incorrect time zone: '%s'
    public final static int ER_WARN_INVALID_TIMESTAMP = 1299; //SQLSTATE: HY000 Message: Invalid TIMESTAMP value in column '%s' at row %ld
    public final static int ER_INVALID_CHARACTER_STRING = 1300; //SQLSTATE: HY000 Message: Invalid %s character string: '%s'
    public final static int ER_WARN_ALLOWED_PACKET_OVERFLOWED = 1301; //SQLSTATE: HY000 Message: Result of %s() was larger than max_allowed_packet (%ld) - truncated
    public final static int ER_CONFLICTING_DECLARATIONS = 1302; //SQLSTATE: HY000 Message: Conflicting declarations: '%s%s' and '%s%s'
    public final static int ER_SP_NO_RECURSIVE_CREATE = 1303; //SQLSTATE: 2F003 Message: Can't create a %s from within another stored routine
    public final static int ER_SP_ALREADY_EXISTS = 1304; //SQLSTATE: 42000 Message: %s %s already exists
    public final static int ER_SP_DOES_NOT_EXIST = 1305; //SQLSTATE: 42000 Message: %s %s does not exist
    public final static int ER_SP_DROP_FAILED = 1306; //SQLSTATE: HY000 Message: Failed to DROP %s %s
    public final static int ER_SP_STORE_FAILED = 1307; //SQLSTATE: HY000 Message: Failed to CREATE %s %s
    public final static int ER_SP_LILABEL_MISMATCH = 1308; //SQLSTATE: 42000 Message: %s with no matching label: %s
    public final static int ER_SP_LABEL_REDEFINE = 1309; //SQLSTATE: 42000 Message: Redefining label %s
    public final static int ER_SP_LABEL_MISMATCH = 1310; //SQLSTATE: 42000 Message: End-label %s without match
    public final static int ER_SP_UNINIT_VAR = 1311; //SQLSTATE: 01000 Message: Referring to uninitialized variable %s
    public final static int ER_SP_BADSELECT = 1312; //SQLSTATE: 0A000 Message: PROCEDURE %s can't return a result set in the given context
    public final static int ER_SP_BADRETURN = 1313; //SQLSTATE: 42000 Message: RETURN is only allowed in a FUNCTION
    public final static int ER_SP_BADSTATEMENT = 1314; //SQLSTATE: 0A000 Message: %s is not allowed in stored procedures
    public final static int ER_UPDATE_LOG_DEPRECATED_IGNORED = 1315; //SQLSTATE: 42000 Message: The update log is deprecated and replaced by the binary log; SET SQL_LOG_UPDATE has been ignored. This option will be removed in MySQL 5.6.
    public final static int ER_UPDATE_LOG_DEPRECATED_TRANSLATED = 1316; //SQLSTATE: 42000 Message: The update log is deprecated and replaced by the binary log; SET SQL_LOG_UPDATE has been translated to SET SQL_LOG_BIN. This option will be removed in MySQL 5.6.
    public final static int ER_QUERY_INTERRUPTED = 1317; //SQLSTATE: 70100 Message: Query execution was interrupted
    public final static int ER_SP_WRONG_NO_OF_ARGS = 1318; //SQLSTATE: 42000 Message: Incorrect number of arguments for %s %s; expected %u, got %u
    public final static int ER_SP_COND_MISMATCH = 1319; //SQLSTATE: 42000 Message: Undefined CONDITION: %s
    public final static int ER_SP_NORETURN = 1320; //SQLSTATE: 42000 Message: No RETURN found in FUNCTION %s
    public final static int ER_SP_NORETURNEND = 1321; //SQLSTATE: 2F005 Message: FUNCTION %s ended without RETURN
    public final static int ER_SP_BAD_CURSOR_QUERY = 1322; //SQLSTATE: 42000 Message: Cursor statement must be a SELECT
    public final static int ER_SP_BAD_CURSOR_SELECT = 1323; //SQLSTATE: 42000 Message: Cursor SELECT must not have INTO
    public final static int ER_SP_CURSOR_MISMATCH = 1324; //SQLSTATE: 42000 Message: Undefined CURSOR: %s
    public final static int ER_SP_CURSOR_ALREADY_OPEN = 1325; //SQLSTATE: 24000 Message: Cursor is already open
    public final static int ER_SP_CURSOR_NOT_OPEN = 1326; //SQLSTATE: 24000 Message: Cursor is not open
    public final static int ER_SP_UNDECLARED_VAR = 1327; //SQLSTATE: 42000 Message: Undeclared variable: %s
    public final static int ER_SP_WRONG_NO_OF_FETCH_ARGS = 1328; //SQLSTATE: HY000 Message: Incorrect number of FETCH variables
    public final static int ER_SP_FETCH_NO_DATA = 1329; //SQLSTATE: 02000 Message: No data - zero rows fetched, selected, or processed
    public final static int ER_SP_DUP_PARAM = 1330; //SQLSTATE: 42000 Message: Duplicate parameter: %s
    public final static int ER_SP_DUP_VAR = 1331; //SQLSTATE: 42000 Message: Duplicate variable: %s
    public final static int ER_SP_DUP_COND = 1332; //SQLSTATE: 42000 Message: Duplicate condition: %s
    public final static int ER_SP_DUP_CURS = 1333; //SQLSTATE: 42000 Message: Duplicate cursor: %s
    public final static int ER_SP_CANT_ALTER = 1334; //SQLSTATE: HY000 Message: Failed to ALTER %s %s
    public final static int ER_SP_SUBSELECT_NYI = 1335; //SQLSTATE: 0A000 Message: Subquery value not supported
    public final static int ER_STMT_NOT_ALLOWED_IN_SF_OR_TRG = 1336; //SQLSTATE: 0A000 Message: %s is not allowed in stored function or trigger
    public final static int ER_SP_VARCOND_AFTER_CURSHNDLR = 1337; //SQLSTATE: 42000 Message: Variable or condition declaration after cursor or handler declaration
    public final static int ER_SP_CURSOR_AFTER_HANDLER = 1338; //SQLSTATE: 42000 Message: Cursor declaration after handler declaration
    public final static int ER_SP_CASE_NOT_FOUND = 1339; //SQLSTATE: 20000 Message: Case not found for CASE statement
    public final static int ER_FPARSER_TOO_BIG_FILE = 1340; //SQLSTATE: HY000 Message: Configuration file '%s' is too big
    public final static int ER_FPARSER_BAD_HEADER = 1341; //SQLSTATE: HY000 Message: Malformed file type header in file '%s'
    public final static int ER_FPARSER_EOF_IN_COMMENT = 1342; //SQLSTATE: HY000 Message: Unexpected end of file while parsing comment '%s'
    public final static int ER_FPARSER_ERROR_IN_PARAMETER = 1343; //SQLSTATE: HY000 Message: Error while parsing parameter '%s' (line: '%s')
    public final static int ER_FPARSER_EOF_IN_UNKNOWN_PARAMETER = 1344; //SQLSTATE: HY000 Message: Unexpected end of file while skipping unknown parameter '%s'
    public final static int ER_VIEW_NO_EXPLAIN = 1345; //SQLSTATE: HY000 Message: EXPLAIN/SHOW can not be issued; lacking privileges for underlying table
    public final static int ER_FRM_UNKNOWN_TYPE = 1346; //SQLSTATE: HY000 Message: File '%s' has unknown type '%s' in its header
    public final static int ER_WRONG_OBJECT = 1347; //SQLSTATE: HY000 Message: '%s.%s' is not %s
    public final static int ER_NONUPDATEABLE_COLUMN = 1348; //SQLSTATE: HY000 Message: Column '%s' is not updatable
    public final static int ER_VIEW_SELECT_DERIVED = 1349; //SQLSTATE: HY000 Message: View's SELECT contains a subquery in the FROM clause
    public final static int ER_VIEW_SELECT_CLAUSE = 1350; //SQLSTATE: HY000 Message: View's SELECT contains a '%s' clause
    public final static int ER_VIEW_SELECT_VARIABLE = 1351; //SQLSTATE: HY000 Message: View's SELECT contains a variable or parameter
    public final static int ER_VIEW_SELECT_TMPTABLE = 1352; //SQLSTATE: HY000 Message: View's SELECT refers to a temporary table '%s'
    public final static int ER_VIEW_WRONG_LIST = 1353; //SQLSTATE: HY000 Message: View's SELECT and view's field list have different column counts
    public final static int ER_WARN_VIEW_MERGE = 1354; //SQLSTATE: HY000 Message: View merge algorithm can't be used here for now (assumed undefined algorithm)
    public final static int ER_WARN_VIEW_WITHOUT_KEY = 1355; //SQLSTATE: HY000 Message: View being updated does not have complete key of underlying table in it
    public final static int ER_VIEW_INVALID = 1356; //SQLSTATE: HY000 Message: View '%s.%s' references invalid table(s) or column(s) or function(s) or definer/invoker of view lack rights to use them
    public final static int ER_SP_NO_DROP_SP = 1357; //SQLSTATE: HY000 Message: Can't drop or alter a %s from within another stored routine
    public final static int ER_SP_GOTO_IN_HNDLR = 1358; //SQLSTATE: HY000 Message: GOTO is not allowed in a stored procedure handler
    public final static int ER_TRG_ALREADY_EXISTS = 1359; //SQLSTATE: HY000 Message: Trigger already exists
    public final static int ER_TRG_DOES_NOT_EXIST = 1360; //SQLSTATE: HY000 Message: Trigger does not exist
    public final static int ER_TRG_ON_VIEW_OR_TEMP_TABLE = 1361; //SQLSTATE: HY000 Message: Trigger's '%s' is view or temporary table
    public final static int ER_TRG_CANT_CHANGE_ROW = 1362; //SQLSTATE: HY000 Message: Updating of %s row is not allowed in %strigger
    public final static int ER_TRG_NO_SUCH_ROW_IN_TRG = 1363; //SQLSTATE: HY000 Message: There is no %s row in %s trigger
    public final static int ER_NO_DEFAULT_FOR_FIELD = 1364; //SQLSTATE: HY000 Message: Field '%s' doesn't have a default value
    public final static int ER_DIVISION_BY_ZERO = 1365; //SQLSTATE: 22012 Message: Division by 0
    public final static int ER_TRUNCATED_WRONG_VALUE_FOR_FIELD = 1366; //SQLSTATE: HY000 Message: Incorrect %s value: '%s' for column '%s' at row %ld
    public final static int ER_ILLEGAL_VALUE_FOR_TYPE = 1367; //SQLSTATE: 22007 Message: Illegal %s '%s' value found during parsing
    public final static int ER_VIEW_NONUPD_CHECK = 1368; //SQLSTATE: HY000 Message: CHECK OPTION on non-updatable view '%s.%s'
    public final static int ER_VIEW_CHECK_FAILED = 1369; //SQLSTATE: HY000 Message: CHECK OPTION failed '%s.%s'
    public final static int ER_PROCACCESS_DENIED_ERROR = 1370; //SQLSTATE: 42000 Message: %s command denied to user '%s'@'%s' for routine '%s'
    public final static int ER_RELAY_LOG_FAIL = 1371; //SQLSTATE: HY000 Message: Failed purging old relay logs: %s
    public final static int ER_PASSWD_LENGTH = 1372; //SQLSTATE: HY000 Message: Password hash should be a %d-digit hexadecimal number
    public final static int ER_UNKNOWN_TARGET_BINLOG = 1373; //SQLSTATE: HY000 Message: Target log not found in binlog index
    public final static int ER_IO_ERR_LOG_INDEX_READ = 1374; //SQLSTATE: HY000 Message: I/O error reading log index file
    public final static int ER_BINLOG_PURGE_PROHIBITED = 1375; //SQLSTATE: HY000 Message: Server configuration does not permit binlog purge
    public final static int ER_FSEEK_FAIL = 1376; //SQLSTATE: HY000 Message: Failed on fseek()
    public final static int ER_BINLOG_PURGE_FATAL_ERR = 1377; //SQLSTATE: HY000 Message: Fatal error during log purge
    public final static int ER_LOG_IN_USE = 1378; //SQLSTATE: HY000 Message: A purgeable log is in use, will not purge
    public final static int ER_LOG_PURGE_UNKNOWN_ERR = 1379; //SQLSTATE: HY000 Message: Unknown error during log purge
    public final static int ER_RELAY_LOG_INIT = 1380; //SQLSTATE: HY000 Message: Failed initializing relay log position: %s
    public final static int ER_NO_BINARY_LOGGING = 1381; //SQLSTATE: HY000 Message: You are not using binary logging
    public final static int ER_RESERVED_SYNTAX = 1382; //SQLSTATE: HY000 Message: The '%s' syntax is reserved for purposes internal to the MySQL server
    public final static int ER_WSAS_FAILED = 1383; //SQLSTATE: HY000 Message: WSAStartup Failed
    public final static int ER_DIFF_GROUPS_PROC = 1384; //SQLSTATE: HY000 Message: Can't handle procedures with different groups yet
    public final static int ER_NO_GROUP_FOR_PROC = 1385; //SQLSTATE: HY000 Message: Select must have a group with this procedure
    public final static int ER_ORDER_WITH_PROC = 1386; //SQLSTATE: HY000 Message: Can't use ORDER clause with this procedure
    public final static int ER_LOGGING_PROHIBIT_CHANGING_OF = 1387; //SQLSTATE: HY000 Message: Binary logging and replication forbid changing the global server %s
    public final static int ER_NO_FILE_MAPPING = 1388; //SQLSTATE: HY000 Message: Can't map file: %s, errno: %d
    public final static int ER_WRONG_MAGIC = 1389; //SQLSTATE: HY000 Message: Wrong magic in %s
    public final static int ER_PS_MANY_PARAM = 1390; //SQLSTATE: HY000 Message: Prepared statement contains too many placeholders
    public final static int ER_KEY_PART_0 = 1391; //SQLSTATE: HY000 Message: Key part '%s' length cannot be 0
    public final static int ER_VIEW_CHECKSUM = 1392; //SQLSTATE: HY000 Message: View text checksum failed
    public final static int ER_VIEW_MULTIUPDATE = 1393; //SQLSTATE: HY000 Message: Can not modify more than one base table through a join view '%s.%s'
    public final static int ER_VIEW_NO_INSERT_FIELD_LIST = 1394; //SQLSTATE: HY000 Message: Can not insert into join view '%s.%s' without fields list
    public final static int ER_VIEW_DELETE_MERGE_VIEW = 1395; //SQLSTATE: HY000 Message: Can not delete from join view '%s.%s'
    public final static int ER_CANNOT_USER = 1396; //SQLSTATE: HY000 Message: Operation %s failed for %s
    public final static int ER_XAER_NOTA = 1397; //SQLSTATE: XAE04 Message: XAER_NOTA: Unknown XID
    public final static int ER_XAER_INVAL = 1398; //SQLSTATE: XAE05 Message: XAER_INVAL: Invalid arguments (or unsupported command)
    public final static int ER_XAER_RMFAIL = 1399; //SQLSTATE: XAE07 Message: XAER_RMFAIL: The command cannot be executed when global transaction is in the %s state
    public final static int ER_XAER_OUTSIDE = 1400; //SQLSTATE: XAE09 Message: XAER_OUTSIDE: Some work is done outside global transaction
    public final static int ER_XA_RMERR = 1401;
    public final static int ER_XA_RBROLLBACK = 1402; //SQLSTATE: XA100 Message: XA_RBROLLBACK: Transaction branch was rolled back
    public final static int ER_NONEXISTING_PROC_GRANT = 1403; //SQLSTATE: 42000 Message: There is no such grant defined for user '%s' on host '%s' on routine '%s'
    public final static int ER_PROC_AUTO_GRANT_FAIL = 1404; //SQLSTATE: HY000 Message: Failed to grant EXECUTE and ALTER ROUTINE privileges
    public final static int ER_PROC_AUTO_REVOKE_FAIL = 1405; //SQLSTATE: HY000 Message: Failed to revoke all privileges to dropped routine
    public final static int ER_DATA_TOO_LONG = 1406; //SQLSTATE: 22001 Message: Data too long for column '%s' at row %ld
    public final static int ER_SP_BAD_SQLSTATE = 1407; //SQLSTATE: 42000 Message: Bad; //SQLSTATE: '%s'
    public final static int ER_STARTUP = 1408; //SQLSTATE: HY000 Message: %s: ready for connections. Version: '%s' socket: '%s' port: %d %s
    public final static int ER_LOAD_FROM_FIXED_SIZE_ROWS_TO_VAR = 1409; //SQLSTATE: HY000 Message: Can't load value from file with fixed size rows to variable
    public final static int ER_CANT_CREATE_USER_WITH_GRANT = 1410; //SQLSTATE: 42000 Message: You are not allowed to create a user with GRANT
    public final static int ER_WRONG_VALUE_FOR_TYPE = 1411; //SQLSTATE: HY000 Message: Incorrect %s value: '%s' for function %s
    public final static int ER_TABLE_DEF_CHANGED = 1412; //SQLSTATE: HY000 Message: Table definition has changed, please retry transaction
    public final static int ER_SP_DUP_HANDLER = 1413; //SQLSTATE: 42000 Message: Duplicate handler declared in the same block
    public final static int ER_SP_NOT_VAR_ARG = 1414; //SQLSTATE: 42000 Message: OUT or INOUT argument %d for routine %s is not a variable or NEW pseudo-variable in BEFORE trigger
    public final static int ER_SP_NO_RETSET = 1415; //SQLSTATE: 0A000 Message: Not allowed to return a result set from a %s
    public final static int ER_CANT_CREATE_GEOMETRY_OBJECT = 1416; //SQLSTATE: 22003 Message: Cannot get geometry object from data you send to the GEOMETRY field
    public final static int ER_FAILED_ROUTINE_BREAK_BINLOG = 1417; //SQLSTATE: HY000 Message: A routine failed and has neither NO SQL nor READS SQL DATA in its declaration and binary logging is enabled; if non-transactional tables were updated, the binary log will miss their changes
    public final static int ER_BINLOG_UNSAFE_ROUTINE = 1418; //SQLSTATE: HY000 Message: This function has none of DETERMINISTIC, NO SQL, or READS SQL DATA in its declaration and binary logging is enabled (you *might* want to use the less safe log_bin_trust_function_creators variable)
    public final static int ER_BINLOG_CREATE_ROUTINE_NEED_SUPER = 1419; //SQLSTATE: HY000 Message: You do not have the SUPER privilege and binary logging is enabled (you *might* want to use the less safe log_bin_trust_function_creators variable)
    public final static int ER_EXEC_STMT_WITH_OPEN_CURSOR = 1420; //SQLSTATE: HY000 Message: You can't execute a prepared statement which has an open cursor associated with it. Reset the statement to re-execute it.
    public final static int ER_STMT_HAS_NO_OPEN_CURSOR = 1421; //SQLSTATE: HY000 Message: The statement (%lu) has no open cursor.
    public final static int ER_COMMIT_NOT_ALLOWED_IN_SF_OR_TRG = 1422; //SQLSTATE: HY000 Message: Explicit or implicit commit is not allowed in stored function or trigger.
    public final static int ER_NO_DEFAULT_FOR_VIEW_FIELD = 1423; //SQLSTATE: HY000 Message: Field of view '%s.%s' underlying table doesn't have a default value
    public final static int ER_SP_NO_RECURSION = 1424; //SQLSTATE: HY000 Message: Recursive stored functions and triggers are not allowed.
    public final static int ER_TOO_BIG_SCALE = 1425; //SQLSTATE: 42000 Message: Too big scale %d specified for column '%s'. Maximum is %lu.
    public final static int ER_TOO_BIG_PRECISION = 1426; //SQLSTATE: 42000 Message: Too big precision %d specified for column '%s'. Maximum is %lu.
    public final static int ER_M_BIGGER_THAN_D = 1427; //SQLSTATE: 42000 Message: For float(M,D), double(M,D) or decimal(M,D), M must be >= D (column '%s').
    public final static int ER_WRONG_LOCK_OF_SYSTEM_TABLE = 1428; //SQLSTATE: HY000 Message: You can't combine write-locking of system tables with other tables or lock types
    public final static int ER_CONNECT_TO_FOREIGN_DATA_SOURCE = 1429; //SQLSTATE: HY000 Message: Unable to connect to foreign data source: %s
    public final static int ER_QUERY_ON_FOREIGN_DATA_SOURCE = 1430; //SQLSTATE: HY000 Message: There was a problem processing the query on the foreign data source. Data source error: %s
    public final static int ER_FOREIGN_DATA_SOURCE_DOESNT_EXIST = 1431; //SQLSTATE: HY000 Message: The foreign data source you are trying to reference does not exist. Data source error: %s
    public final static int ER_FOREIGN_DATA_STRING_INVALID_CANT_CREATE = 1432; //SQLSTATE: HY000 Message: Can't create federated table. The data source connection string '%s' is not in the correct format
    public final static int ER_FOREIGN_DATA_STRING_INVALID = 1433; //SQLSTATE: HY000 Message: The data source connection string '%s' is not in the correct format
    public final static int ER_CANT_CREATE_FEDERATED_TABLE = 1434; //SQLSTATE: HY000 Message: Can't create federated table. Foreign data src error: %s
    public final static int ER_TRG_IN_WRONG_SCHEMA = 1435; //SQLSTATE: HY000 Message: Trigger in wrong schema
    public final static int ER_STACK_OVERRUN_NEED_MORE = 1436; //SQLSTATE: HY000 Message: Thread stack overrun: %ld bytes used of a %ld byte stack, and %ld bytes needed. Use 'mysqld --thread_stack=#' to specify a bigger stack.
    public final static int ER_TOO_LONG_BODY = 1437; //SQLSTATE: 42000 Message: Routine body for '%s' is too long
    public final static int ER_WARN_CANT_DROP_DEFAULT_KEYCACHE = 1438; //SQLSTATE: HY000 Message: Cannot drop default keycache
    public final static int ER_TOO_BIG_DISPLAYWIDTH = 1439; //SQLSTATE: 42000 Message: Display width out of range for column '%s' (max = %lu)
    public final static int ER_XAER_DUPID = 1440; //SQLSTATE: XAE08 Message: XAER_DUPID: The XID already exists
    public final static int ER_DATETIME_FUNCTION_OVERFLOW = 1441; //SQLSTATE: 22008 Message: Datetime function: %s field overflow
    public final static int ER_CANT_UPDATE_USED_TABLE_IN_SF_OR_TRG = 1442; //SQLSTATE: HY000 Message: Can't update table '%s' in stored function/trigger because it is already used by statement which invoked this stored function/trigger.
    public final static int ER_VIEW_PREVENT_UPDATE = 1443; //SQLSTATE: HY000 Message: The definition of table '%s' prevents operation %s on table '%s'.
    public final static int ER_PS_NO_RECURSION = 1444; //SQLSTATE: HY000 Message: The prepared statement contains a stored routine call that refers to that same statement. It's not allowed to execute a prepared statement in such a recursive manner
    public final static int ER_SP_CANT_SET_AUTOCOMMIT = 1445; //SQLSTATE: HY000 Message: Not allowed to set autocommit from a stored function or trigger
    public final static int ER_MALFORMED_DEFINER = 1446; //SQLSTATE: HY000 Message: Definer is not fully qualified
    public final static int ER_VIEW_FRM_NO_USER = 1447; //SQLSTATE: HY000 Message: View '%s'.'%s' has no definer information (old table format). Current user is used as definer. Please recreate the view!
    public final static int ER_VIEW_OTHER_USER = 1448; //SQLSTATE: HY000 Message: You need the SUPER privilege for creation view with '%s'@'%s' definer
    public final static int ER_NO_SUCH_USER = 1449; //SQLSTATE: HY000 Message: The user specified as a definer ('%s'@'%s') does not exist
    public final static int ER_FORBID_SCHEMA_CHANGE = 1450; //SQLSTATE: HY000 Message: Changing schema from '%s' to '%s' is not allowed.
    public final static int ER_ROW_IS_REFERENCED_2 = 1451; //SQLSTATE: 23000 Message: Cannot delete or update a parent row: a foreign key constraint fails (%s)
    public final static int ER_NO_REFERENCED_ROW_2 = 1452; //SQLSTATE: 23000 Message: Cannot add or update a child row: a foreign key constraint fails (%s)
    public final static int ER_SP_BAD_VAR_SHADOW = 1453; //SQLSTATE: 42000 Message: Variable '%s' must be quoted with `...`, or renamed
    public final static int ER_TRG_NO_DEFINER = 1454; //SQLSTATE: HY000 Message: No definer attribute for trigger '%s'.'%s'. The trigger will be activated under the authorization of the invoker, which may have insufficient privileges. Please recreate the trigger.
    public final static int ER_OLD_FILE_FORMAT = 1455; //SQLSTATE: HY000 Message: '%s' has an old format, you should re-create the '%s' object(s)
    public final static int ER_SP_RECURSION_LIMIT = 1456; //SQLSTATE: HY000 Message: Recursive limit %d (as set by the max_sp_recursion_depth variable) was exceeded for routine %s
    public final static int ER_SP_PROC_TABLE_CORRUPT = 1457; //SQLSTATE: HY000 Message: Failed to load routine %s. The table mysql.proc is missing, corrupt, or contains bad data (internal code %d)
    public final static int ER_SP_WRONG_NAME = 1458; //SQLSTATE: 42000 Message: Incorrect routine name '%s'
    public final static int ER_TABLE_NEEDS_UPGRADE = 1459; //SQLSTATE: HY000 Message: Table upgrade required. Please do "REPAIR TABLE `%s`" or dump/reload to fix it!
    public final static int ER_SP_NO_AGGREGATE = 1460; //SQLSTATE: 42000 Message: AGGREGATE is not supported for stored functions
    public final static int ER_MAX_PREPARED_STMT_COUNT_REACHED = 1461; //SQLSTATE: 42000 Message: Can't create more than max_prepared_stmt_count statements (current value: %lu)
    public final static int ER_VIEW_RECURSIVE = 1462; //SQLSTATE: HY000 Message: `%s`.`%s` contains view recursion
    public final static int ER_NON_GROUPING_FIELD_USED = 1463; //SQLSTATE: 42000 Message: non-grouping field '%s' is used in %s clause
    public final static int ER_TABLE_CANT_HANDLE_SPKEYS = 1464; //SQLSTATE: HY000 Message: The used table type doesn't support SPATIAL indexes
    public final static int ER_NO_TRIGGERS_ON_SYSTEM_SCHEMA = 1465; //SQLSTATE: HY000 Message: Triggers can not be created on system tables
    public final static int ER_REMOVED_SPACES = 1466; //SQLSTATE: HY000 Message: Leading spaces are removed from name '%s'
    public final static int ER_AUTOINC_READ_FAILED = 1467; //SQLSTATE: HY000 Message: Failed to read auto-increment value from storage engine
    public final static int ER_USERNAME = 1468; //SQLSTATE: HY000 Message: user name
    public final static int ER_HOSTNAME = 1469; //SQLSTATE: HY000 Message: host name
    public final static int ER_WRONG_STRING_LENGTH = 1470; //SQLSTATE: HY000 Message: String '%s' is too long for %s (should be no longer than %d)
    public final static int ER_NON_INSERTABLE_TABLE = 1471; //SQLSTATE: HY000 Message: The target table %s of the %s is not insertable-into
    public final static int ER_ADMIN_WRONG_MRG_TABLE = 1472; //SQLSTATE: HY000 Message: Table '%s' is differently defined or of non-MyISAM type or doesn't exist
    public final static int ER_TOO_HIGH_LEVEL_OF_NESTING_FOR_SELECT = 1473; //SQLSTATE: HY000 Message: Too high level of nesting for select
    public final static int ER_NAME_BECOMES_EMPTY = 1474; //SQLSTATE: HY000 Message: Name '%s' has become ''
    public final static int ER_AMBIGUOUS_FIELD_TERM = 1475; //SQLSTATE: HY000 Message: First character of the FIELDS TERMINATED string is ambiguous; please use non-optional and non-empty FIELDS ENCLOSED BY
    public final static int ER_FOREIGN_SERVER_EXISTS = 1476; //SQLSTATE: HY000 Message: The foreign server, %s, you are trying to create already exists.
    public final static int ER_FOREIGN_SERVER_DOESNT_EXIST = 1477; //SQLSTATE: HY000 Message: The foreign server name you are trying to reference does not exist. Data source error: %s
    public final static int ER_ILLEGAL_HA_CREATE_OPTION = 1478; //SQLSTATE: HY000 Message: Table storage engine '%s' does not support the create option '%s'
    public final static int ER_PARTITION_REQUIRES_VALUES_ERROR = 1479; //SQLSTATE: HY000 Message: Syntax error: %s PARTITIONING requires definition of VALUES %s for each partition
    public final static int ER_PARTITION_WRONG_VALUES_ERROR = 1480; //SQLSTATE: HY000 Message: Only %s PARTITIONING can use VALUES %s in partition definition
    public final static int ER_PARTITION_MAXVALUE_ERROR = 1481; //SQLSTATE: HY000 Message: MAXVALUE can only be used in last partition definition
    public final static int ER_PARTITION_SUBPARTITION_ERROR = 1482; //SQLSTATE: HY000 Message: Subpartitions can only be hash partitions and by key
    public final static int ER_PARTITION_SUBPART_MIX_ERROR = 1483; //SQLSTATE: HY000 Message: Must define subpartitions on all partitions if on one partition
    public final static int ER_PARTITION_WRONG_NO_PART_ERROR = 1484; //SQLSTATE: HY000 Message: Wrong number of partitions defined, mismatch with previous setting
    public final static int ER_PARTITION_WRONG_NO_SUBPART_ERROR = 1485; //SQLSTATE: HY000 Message: Wrong number of subpartitions defined, mismatch with previous setting
    public final static int ER_WRONG_EXPR_IN_PARTITION_FUNC_ERROR = 1486; //SQLSTATE: HY000 Message: Constant, random or timezone-dependent expressions in (sub)partitioning function are not allowed
    public final static int ER_NO_CONST_EXPR_IN_RANGE_OR_LIST_ERROR = 1487; //SQLSTATE: HY000 Message: Expression in RANGE/LIST VALUES must be constant
    public final static int ER_FIELD_NOT_FOUND_PART_ERROR = 1488; //SQLSTATE: HY000 Message: Field in list of fields for partition function not found in table
    public final static int ER_LIST_OF_FIELDS_ONLY_IN_HASH_ERROR = 1489; //SQLSTATE: HY000 Message: List of fields is only allowed in KEY partitions
    public final static int ER_INCONSISTENT_PARTITION_INFO_ERROR = 1490; //SQLSTATE: HY000 Message: The partition info in the frm file is not consistent with what can be written into the frm file
    public final static int ER_PARTITION_FUNC_NOT_ALLOWED_ERROR = 1491; //SQLSTATE: HY000 Message: The %s function returns the wrong type
    public final static int ER_PARTITIONS_MUST_BE_DEFINED_ERROR = 1492; //SQLSTATE: HY000 Message: For %s partitions each partition must be defined
    public final static int ER_RANGE_NOT_INCREASING_ERROR = 1493; //SQLSTATE: HY000 Message: VALUES LESS THAN value must be strictly increasing for each partition
    public final static int ER_INCONSISTENT_TYPE_OF_FUNCTIONS_ERROR = 1494; //SQLSTATE: HY000 Message: VALUES value must be of same type as partition function
    public final static int ER_MULTIPLE_DEF_CONST_IN_LIST_PART_ERROR = 1495; //SQLSTATE: HY000 Message: Multiple definition of same constant in list partitioning
    public final static int ER_PARTITION_ENTRY_ERROR = 1496; //SQLSTATE: HY000 Message: Partitioning can not be used stand-alone in query
    public final static int ER_MIX_HANDLER_ERROR = 1497; //SQLSTATE: HY000 Message: The mix of handlers in the partitions is not allowed in this version of MySQL
    public final static int ER_PARTITION_NOT_DEFINED_ERROR = 1498; //SQLSTATE: HY000 Message: For the partitioned engine it is necessary to define all %s
    public final static int ER_TOO_MANY_PARTITIONS_ERROR = 1499; //SQLSTATE: HY000 Message: Too many partitions (including subpartitions) were defined
    public final static int ER_SUBPARTITION_ERROR = 1500; //SQLSTATE: HY000 Message: It is only possible to mix RANGE/LIST partitioning with HASH/KEY partitioning for subpartitioning
    public final static int ER_CANT_CREATE_HANDLER_FILE = 1501; //SQLSTATE: HY000 Message: Failed to create specific handler file
    public final static int ER_BLOB_FIELD_IN_PART_FUNC_ERROR = 1502; //SQLSTATE: HY000 Message: A BLOB field is not allowed in partition function
    public final static int ER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF = 1503; //SQLSTATE: HY000 Message: A %s must include all columns in the table's partitioning function
    public final static int ER_NO_PARTS_ERROR = 1504; //SQLSTATE: HY000 Message: Number of %s = 0 is not an allowed value
    public final static int ER_PARTITION_MGMT_ON_NONPARTITIONED = 1505; //SQLSTATE: HY000 Message: Partition management on a not partitioned table is not possible
    public final static int ER_FOREIGN_KEY_ON_PARTITIONED = 1506; //SQLSTATE: HY000 Message: Foreign key clause is not yet supported in conjunction with partitioning
    public final static int ER_DROP_PARTITION_NON_EXISTENT = 1507; //SQLSTATE: HY000 Message: Error in list of partitions to %s
    public final static int ER_DROP_LAST_PARTITION = 1508; //SQLSTATE: HY000 Message: Cannot remove all partitions, use DROP TABLE instead
    public final static int ER_COALESCE_ONLY_ON_HASH_PARTITION = 1509; //SQLSTATE: HY000 Message: COALESCE PARTITION can only be used on HASH/KEY partitions
    public final static int ER_REORG_HASH_ONLY_ON_SAME_NO = 1510; //SQLSTATE: HY000 Message: REORGANIZE PARTITION can only be used to reorganize partitions not to change their numbers
    public final static int ER_REORG_NO_PARAM_ERROR = 1511; //SQLSTATE: HY000 Message: REORGANIZE PARTITION without parameters can only be used on auto-partitioned tables using HASH PARTITIONs
    public final static int ER_ONLY_ON_RANGE_LIST_PARTITION = 1512; //SQLSTATE: HY000 Message: %s PARTITION can only be used on RANGE/LIST partitions
    public final static int ER_ADD_PARTITION_SUBPART_ERROR = 1513; //SQLSTATE: HY000 Message: Trying to Add partition(s) with wrong number of subpartitions
    public final static int ER_ADD_PARTITION_NO_NEW_PARTITION = 1514; //SQLSTATE: HY000 Message: At least one partition must be added
    public final static int ER_COALESCE_PARTITION_NO_PARTITION = 1515; //SQLSTATE: HY000 Message: At least one partition must be coalesced
    public final static int ER_REORG_PARTITION_NOT_EXIST = 1516; //SQLSTATE: HY000 Message: More partitions to reorganize than there are partitions
    public final static int ER_SAME_NAME_PARTITION = 1517; //SQLSTATE: HY000 Message: Duplicate partition name %s
    public final static int ER_NO_BINLOG_ERROR = 1518; //SQLSTATE: HY000 Message: It is not allowed to shut off binlog on this command
    public final static int ER_CONSECUTIVE_REORG_PARTITIONS = 1519; //SQLSTATE: HY000 Message: When reorganizing a set of partitions they must be in consecutive order
    public final static int ER_REORG_OUTSIDE_RANGE = 1520; //SQLSTATE: HY000 Message: Reorganize of range partitions cannot change total ranges except for last partition where it can extend the range
    public final static int ER_PARTITION_FUNCTION_FAILURE = 1521; //SQLSTATE: HY000 Message: Partition function not supported in this version for this handler
    public final static int ER_PART_STATE_ERROR = 1522; //SQLSTATE: HY000 Message: Partition state cannot be defined from CREATE/ALTER TABLE
    public final static int ER_LIMITED_PART_RANGE = 1523; //SQLSTATE: HY000 Message: The %s handler only supports 32 bit integers in VALUES
    public final static int ER_PLUGIN_IS_NOT_LOADED = 1524; //SQLSTATE: HY000 Message: Plugin '%s' is not loaded
    public final static int ER_WRONG_VALUE = 1525; //SQLSTATE: HY000 Message: Incorrect %s value: '%s'
    public final static int ER_NO_PARTITION_FOR_GIVEN_VALUE = 1526; //SQLSTATE: HY000 Message: Table has no partition for value %s
    public final static int ER_FILEGROUP_OPTION_ONLY_ONCE = 1527; //SQLSTATE: HY000 Message: It is not allowed to specify %s more than once
    public final static int ER_CREATE_FILEGROUP_FAILED = 1528; //SQLSTATE: HY000 Message: Failed to create %s
    public final static int ER_DROP_FILEGROUP_FAILED = 1529; //SQLSTATE: HY000 Message: Failed to drop %s
    public final static int ER_TABLESPACE_AUTO_EXTEND_ERROR = 1530; //SQLSTATE: HY000 Message: The handler doesn't support autoextend of tablespaces
    public final static int ER_WRONG_SIZE_NUMBER = 1531; //SQLSTATE: HY000 Message: A size parameter was incorrectly specified, either number or on the form 10M
    public final static int ER_SIZE_OVERFLOW_ERROR = 1532; //SQLSTATE: HY000 Message: The size number was correct but we don't allow the digit part to be more than 2 billion
    public final static int ER_ALTER_FILEGROUP_FAILED = 1533; //SQLSTATE: HY000 Message: Failed to alter: %s
    public final static int ER_BINLOG_ROW_LOGGING_FAILED = 1534; //SQLSTATE: HY000 Message: Writing one row to the row-based binary log failed
    public final static int ER_BINLOG_ROW_WRONG_TABLE_DEF = 1535; //SQLSTATE: HY000 Message: Table definition on source and replica does not match: %s
    public final static int ER_BINLOG_ROW_RBR_TO_SBR = 1536; //SQLSTATE: HY000 Message: Replica running with --log-replica-updates must use row-based binary logging to be able to replicate row-based binary log events
    public final static int ER_EVENT_ALREADY_EXISTS = 1537; //SQLSTATE: HY000 Message: Event '%s' already exists
    public final static int ER_EVENT_STORE_FAILED = 1538; //SQLSTATE: HY000 Message: Failed to store event %s. Error code %d from storage engine.
    public final static int ER_EVENT_DOES_NOT_EXIST = 1539; //SQLSTATE: HY000 Message: Unknown event '%s'
    public final static int ER_EVENT_CANT_ALTER = 1540; //SQLSTATE: HY000 Message: Failed to alter event '%s'
    public final static int ER_EVENT_DROP_FAILED = 1541; //SQLSTATE: HY000 Message: Failed to drop %s
    public final static int ER_EVENT_INTERVAL_NOT_POSITIVE_OR_TOO_BIG = 1542; //SQLSTATE: HY000 Message: INTERVAL is either not positive or too big
    public final static int ER_EVENT_ENDS_BEFORE_STARTS = 1543; //SQLSTATE: HY000 Message: ENDS is either invalid or before STARTS
    public final static int ER_EVENT_EXEC_TIME_IN_THE_PAST = 1544; //SQLSTATE: HY000 Message: Event execution time is in the past. Event has been disabled
    public final static int ER_EVENT_OPEN_TABLE_FAILED = 1545; //SQLSTATE: HY000 Message: Failed to open mysql.event
    public final static int ER_EVENT_NEITHER_M_EXPR_NOR_M_AT = 1546; //SQLSTATE: HY000 Message: No datetime expression provided
    public final static int ER_COL_COUNT_DOESNT_MATCH_CORRUPTED = 1547; //SQLSTATE: HY000 Message: Column count of mysql.%s is wrong. Expected %d, found %d. The table is probably corrupted
    public final static int ER_CANNOT_LOAD_FROM_TABLE = 1548; //SQLSTATE: HY000 Message: Cannot load from mysql.%s. The table is probably corrupted
    public final static int ER_EVENT_CANNOT_DELETE = 1549; //SQLSTATE: HY000 Message: Failed to delete the event from mysql.event
    public final static int ER_EVENT_COMPILE_ERROR = 1550; //SQLSTATE: HY000 Message: Error during compilation of event's body
    public final static int ER_EVENT_SAME_NAME = 1551; //SQLSTATE: HY000 Message: Same old and new event name
    public final static int ER_EVENT_DATA_TOO_LONG = 1552; //SQLSTATE: HY000 Message: Data for column '%s' too long
    public final static int ER_DROP_INDEX_FK = 1553; //SQLSTATE: HY000 Message: Cannot drop index '%s': needed in a foreign key constraint
    public final static int ER_WARN_DEPRECATED_SYNTAX_WITH_VER = 1554; //SQLSTATE: HY000 Message: The syntax '%s' is deprecated and will be removed in MySQL %s. Please use %s instead
    public final static int ER_CANT_WRITE_LOCK_LOG_TABLE = 1555; //SQLSTATE: HY000 Message: You can't write-lock a log table. Only read access is possible
    public final static int ER_CANT_LOCK_LOG_TABLE = 1556; //SQLSTATE: HY000 Message: You can't use locks with log tables.
    public final static int ER_FOREIGN_DUPLICATE_KEY = 1557; //SQLSTATE: 23000 Message: Upholding foreign key constraints for table '%s', entry '%s', key %d would lead to a duplicate entry
    public final static int ER_COL_COUNT_DOESNT_MATCH_PLEASE_UPDATE = 1558; //SQLSTATE: HY000 Message: Column count of mysql.%s is wrong. Expected %d, found %d. Created with MySQL %d, now running %d. Please use mysql_upgrade to fix this error.
    public final static int ER_TEMP_TABLE_PREVENTS_SWITCH_OUT_OF_RBR = 1559; //SQLSTATE: HY000 Message: Cannot switch out of the row-based binary log format when the session has open temporary tables
    public final static int ER_STORED_FUNCTION_PREVENTS_SWITCH_BINLOG_FORMAT = 1560; //SQLSTATE: HY000 Message: Cannot change the binary logging format inside a stored function or trigger
    public final static int ER_NDB_CANT_SWITCH_BINLOG_FORMAT = 1561; //SQLSTATE: HY000 Message: The NDB cluster engine does not support changing the binlog format on the fly yet
    public final static int ER_PARTITION_NO_TEMPORARY = 1562; //SQLSTATE: HY000 Message: Cannot create temporary table with partitions
    public final static int ER_PARTITION_CONST_DOMAIN_ERROR = 1563; //SQLSTATE: HY000 Message: Partition constant is out of partition function domain
    public final static int ER_PARTITION_FUNCTION_IS_NOT_ALLOWED = 1564; //SQLSTATE: HY000 Message: This partition function is not allowed
    public final static int ER_DDL_LOG_ERROR = 1565; //SQLSTATE: HY000 Message: Error in DDL log
    public final static int ER_NULL_IN_VALUES_LESS_THAN = 1566; //SQLSTATE: HY000 Message: Not allowed to use NULL value in VALUES LESS THAN
    public final static int ER_WRONG_PARTITION_NAME = 1567; //SQLSTATE: HY000 Message: Incorrect partition name
    public final static int ER_CANT_CHANGE_TX_ISOLATION = 1568; //SQLSTATE: 25001 Message: Transaction isolation level can't be changed while a transaction is in progress
    public final static int ER_DUP_ENTRY_AUTOINCREMENT_CASE = 1569; //SQLSTATE: HY000 Message: ALTER TABLE causes auto_increment resequencing, resulting in duplicate entry '%s' for key '%s'
    public final static int ER_EVENT_MODIFY_QUEUE_ERROR = 1570; //SQLSTATE: HY000 Message: Internal scheduler error %d
    public final static int ER_EVENT_SET_VAR_ERROR = 1571; //SQLSTATE: HY000 Message: Error during starting/stopping of the scheduler. Error code %u
    public final static int ER_PARTITION_MERGE_ERROR = 1572; //SQLSTATE: HY000 Message: Engine cannot be used in partitioned tables
    public final static int ER_CANT_ACTIVATE_LOG = 1573; //SQLSTATE: HY000 Message: Cannot activate '%s' log
    public final static int ER_RBR_NOT_AVAILABLE = 1574; //SQLSTATE: HY000 Message: The server was not built with row-based replication
    public final static int ER_BASE64_DECODE_ERROR = 1575; //SQLSTATE: HY000 Message: Decoding of base64 string failed
    public final static int ER_EVENT_RECURSION_FORBIDDEN = 1576; //SQLSTATE: HY000 Message: Recursion of EVENT DDL statements is forbidden when body is present
    public final static int ER_EVENTS_DB_ERROR = 1577; //SQLSTATE: HY000 Message: Cannot proceed because system tables used by Event Scheduler were found damaged at server start
    public final static int ER_ONLY_INTEGERS_ALLOWED = 1578; //SQLSTATE: HY000 Message: Only integers allowed as number here
    public final static int ER_UNSUPORTED_LOG_ENGINE = 1579; //SQLSTATE: HY000 Message: This storage engine cannot be used for log tables"
    public final static int ER_BAD_LOG_STATEMENT = 1580; //SQLSTATE: HY000 Message: You cannot '%s' a log table if logging is enabled
    public final static int ER_CANT_RENAME_LOG_TABLE = 1581; //SQLSTATE: HY000 Message: Cannot rename '%s'. When logging enabled, rename to/from log table must rename two tables: the log table to an archive table and another table back to '%s'
    public final static int ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT = 1582; //SQLSTATE: 42000 Message: Incorrect parameter count in the call to native function '%s'
    public final static int ER_WRONG_PARAMETERS_TO_NATIVE_FCT = 1583; //SQLSTATE: 42000 Message: Incorrect parameters in the call to native function '%s'
    public final static int ER_WRONG_PARAMETERS_TO_STORED_FCT = 1584; //SQLSTATE: 42000 Message: Incorrect parameters in the call to stored function '%s'
    public final static int ER_NATIVE_FCT_NAME_COLLISION = 1585; //SQLSTATE: HY000 Message: This function '%s' has the same name as a native function
    public final static int ER_DUP_ENTRY_WITH_KEY_NAME = 1586; //SQLSTATE: 23000 Message: Duplicate entry '%s' for key '%s'
    public final static int ER_BINLOG_PURGE_EMFILE = 1587; //SQLSTATE: HY000 Message: Too many files opened, please execute the command again
    public final static int ER_EVENT_CANNOT_CREATE_IN_THE_PAST = 1588; //SQLSTATE: HY000 Message: Event execution time is in the past and ON COMPLETION NOT PRESERVE is set. The event was dropped immediately after creation.
    public final static int ER_EVENT_CANNOT_ALTER_IN_THE_PAST = 1589; //SQLSTATE: HY000 Message: Event execution time is in the past and ON COMPLETION NOT PRESERVE is set. The event was dropped immediately after creation.
    @Deprecated
    public final static int ER_SLAVE_INCIDENT = 1590;
    public final static int ER_REPLICA_INCIDENT = 1590; //SQLSTATE: HY000 Message: The incident %s occured on the source. Message: %s
    public final static int ER_NO_PARTITION_FOR_GIVEN_VALUE_SILENT = 1591; //SQLSTATE: HY000 Message: Table has no partition for some existing values
    public final static int ER_BINLOG_UNSAFE_STATEMENT = 1592; //SQLSTATE: HY000 Message: Unsafe statement written to the binary log using statement format since BINLOG_FORMAT = STATEMENT. %s
    @Deprecated
    public final static int ER_SLAVE_FATAL_ERROR = 1593;
    public final static int ER_REPLICA_FATAL_ERROR = 1593; //SQLSTATE: HY000 Message: Fatal error: %s
    @Deprecated
    public final static int ER_SLAVE_RELAY_LOG_READ_FAILURE = 1594;
    public final static int ER_REPLICA_RELAY_LOG_READ_FAILURE = 1594; //SQLSTATE: HY000 Message: Relay log read failure: %s
    @Deprecated
    public final static int ER_SLAVE_RELAY_LOG_WRITE_FAILURE = 1595;
    public final static int ER_REPLICA_RELAY_LOG_WRITE_FAILURE = 1595; //SQLSTATE: HY000 Message: Relay log write failure: %s
    @Deprecated
    public final static int ER_SLAVE_CREATE_EVENT_FAILURE = 1596;
    public final static int ER_REPLICA_CREATE_EVENT_FAILURE = 1596; //SQLSTATE: HY000 Message: Failed to create %s
    @Deprecated
    public final static int ER_SLAVE_MASTER_COM_FAILURE = 1597;
    public final static int ER_REPLICA_SOURCE_COM_FAILURE = 1597; //SQLSTATE: HY000 Message: Source command %s failed: %s
    public final static int ER_BINLOG_LOGGING_IMPOSSIBLE = 1598; //SQLSTATE: HY000 Message: Binary logging not possible. Message: %s
    public final static int ER_VIEW_NO_CREATION_CTX = 1599; //SQLSTATE: HY000 Message: View `%s`.`%s` has no creation context
    public final static int ER_VIEW_INVALID_CREATION_CTX = 1600; //SQLSTATE: HY000 Message: Creation context of view `%s`.`%s' is invalid
    public final static int ER_SR_INVALID_CREATION_CTX = 1601; //SQLSTATE: HY000 Message: Creation context of stored routine `%s`.`%s` is invalid
    public final static int ER_TRG_CORRUPTED_FILE = 1602; //SQLSTATE: HY000 Message: Corrupted TRG file for table `%s`.`%s`
    public final static int ER_TRG_NO_CREATION_CTX = 1603; //SQLSTATE: HY000 Message: Triggers for table `%s`.`%s` have no creation context
    public final static int ER_TRG_INVALID_CREATION_CTX = 1604; //SQLSTATE: HY000 Message: Trigger creation context of table `%s`.`%s` is invalid
    public final static int ER_EVENT_INVALID_CREATION_CTX = 1605; //SQLSTATE: HY000 Message: Creation context of event `%s`.`%s` is invalid
    public final static int ER_TRG_CANT_OPEN_TABLE = 1606; //SQLSTATE: HY000 Message: Cannot open table for trigger `%s`.`%s`
    public final static int ER_CANT_CREATE_SROUTINE = 1607; //SQLSTATE: HY000 Message: Cannot create stored routine `%s`. Check warnings
    public final static int ER_NEVER_USED = 1608; //SQLSTATE: HY000 Message: Ambiguous replica modes combination. %s
    public final static int ER_NO_FORMAT_DESCRIPTION_EVENT_BEFORE_BINLOG_STATEMENT = 1609; //SQLSTATE: HY000 Message: The BINLOG statement of type `%s` was not preceded by a format description BINLOG statement.
    @Deprecated
    public final static int ER_SLAVE_CORRUPT_EVENT = 1610;
    public final static int ER_REPLICA_CORRUPT_EVENT = 1610; //SQLSTATE: HY000 Message: Corrupted replication event was detected
    public final static int ER_LOAD_DATA_INVALID_COLUMN = 1611; //SQLSTATE: HY000 Message: Invalid column reference (%s) in LOAD DATA
    public final static int ER_LOG_PURGE_NO_FILE = 1612; //SQLSTATE: HY000 Message: Being purged log %s was not found
    public final static int ER_XA_RBTIMEOUT = 1613; //SQLSTATE: XA106 Message: XA_RBTIMEOUT: Transaction branch was rolled back: took too long
    public final static int ER_XA_RBDEADLOCK = 1614; //SQLSTATE: XA102 Message: XA_RBDEADLOCK: Transaction branch was rolled back: deadlock was detected
    public final static int ER_NEED_REPREPARE = 1615; //SQLSTATE: HY000 Message: Prepared statement needs to be re-prepared
    public final static int ER_DELAYED_NOT_SUPPORTED = 1616; //SQLSTATE: HY000 Message: DELAYED option not supported for table '%s'
    @Deprecated
    public final static int WARN_NO_MASTER_INFO = 1617;
    public final static int WARN_NO_SOURCE_INFO = 1617; //SQLSTATE: HY000 Message: The source info structure does not exist
    public final static int WARN_OPTION_IGNORED = 1618; //SQLSTATE: HY000 Message: <%s> option ignored
    public final static int WARN_PLUGIN_DELETE_BUILTIN = 1619; //SQLSTATE: HY000 Message: Built-in plugins cannot be deleted
    public final static int WARN_PLUGIN_BUSY = 1620; //SQLSTATE: HY000 Message: Plugin is busy and will be uninstalled on shutdown
    public final static int ER_VARIABLE_IS_READONLY = 1621; //SQLSTATE: HY000 Message: %s variable '%s' is read-only. Use SET %s to assign the value
    public final static int ER_WARN_ENGINE_TRANSACTION_ROLLBACK = 1622; //SQLSTATE: HY000 Message: Storage engine %s does not support rollback for this statement. Transaction rolled back and must be restarted
    @Deprecated
    public final static int ER_SLAVE_HEARTBEAT_FAILURE = 1623;
    public final static int ER_REPLICA_HEARTBEAT_FAILURE = 1623; //SQLSTATE: HY000 Message: Unexpected source's heartbeat data: %s
    @Deprecated
    public final static int ER_SLAVE_HEARTBEAT_VALUE_OUT_OF_RANGE = 1624;
    public final static int ER_REPLICA_HEARTBEAT_VALUE_OUT_OF_RANGE = 1624; //SQLSTATE: HY000 Message: The requested value for the heartbeat period is either negative or exceeds the maximum allowed (%s seconds).
    public final static int ER_NDB_REPLICATION_SCHEMA_ERROR = 1625; //SQLSTATE: HY000 Message: Bad schema for mysql.ndb_replication table. Message: %s
    public final static int ER_CONFLICT_FN_PARSE_ERROR = 1626; //SQLSTATE: HY000 Message: Error in parsing conflict function. Message: %s
    public final static int ER_EXCEPTIONS_WRITE_ERROR = 1627; //SQLSTATE: HY000 Message: Write to exceptions table failed. Message: %s"
    public final static int ER_TOO_LONG_TABLE_COMMENT = 1628; //SQLSTATE: HY000 Message: Comment for table '%s' is too long (max = %lu)
    public final static int ER_TOO_LONG_FIELD_COMMENT = 1629; //SQLSTATE: HY000 Message: Comment for field '%s' is too long (max = %lu)
    public final static int ER_FUNC_INEXISTENT_NAME_COLLISION = 1630; //SQLSTATE: 42000 Message: FUNCTION %s does not exist. Check the 'Function Name Parsing and Resolution' section in the Reference Manual
    public final static int ER_DATABASE_NAME = 1631; //SQLSTATE: HY000 Message: Database
    public final static int ER_TABLE_NAME = 1632; //SQLSTATE: HY000 Message: Table
    public final static int ER_PARTITION_NAME = 1633; //SQLSTATE: HY000 Message: Partition
    public final static int ER_SUBPARTITION_NAME = 1634; //SQLSTATE: HY000 Message: Subpartition
    public final static int ER_TEMPORARY_NAME = 1635; //SQLSTATE: HY000 Message: Temporary
    public final static int ER_RENAMED_NAME = 1636; //SQLSTATE: HY000 Message: Renamed
    public final static int ER_TOO_MANY_CONCURRENT_TRXS = 1637; //SQLSTATE: HY000 Message: Too many active concurrent transactions
    public final static int WARN_NON_ASCII_SEPARATOR_NOT_IMPLEMENTED = 1638; //SQLSTATE: HY000 Message: Non-ASCII separator arguments are not fully supported
    public final static int ER_DEBUG_SYNC_TIMEOUT = 1639; //SQLSTATE: HY000 Message: debug sync point wait timed out
    public final static int ER_DEBUG_SYNC_HIT_LIMIT = 1640; //SQLSTATE: HY000 Message: debug sync point hit limit reached
    public final static int ER_DUP_SIGNAL_SET = 1641; //SQLSTATE: 42000 Message: Duplicate condition information item '%s'
    public final static int ER_SIGNAL_WARN = 1642; //SQLSTATE: 01000 Message: Unhandled user-defined warning condition
    public final static int ER_SIGNAL_NOT_FOUND = 1643; //SQLSTATE: 02000 Message: Unhandled user-defined not found condition
    public final static int ER_SIGNAL_EXCEPTION = 1644; //SQLSTATE: HY000 Message: Unhandled user-defined exception condition
    public final static int ER_RESIGNAL_WITHOUT_ACTIVE_HANDLER = 1645; //SQLSTATE: 0K000 Message: RESIGNAL when handler not active
    public final static int ER_SIGNAL_BAD_CONDITION_TYPE = 1646; //SQLSTATE: HY000 Message: SIGNAL/RESIGNAL can only use a CONDITION defined with; //SQLSTATE
    public final static int WARN_COND_ITEM_TRUNCATED = 1647; //SQLSTATE: HY000 Message: Data truncated for condition item '%s'
    public final static int ER_COND_ITEM_TOO_LONG = 1648; //SQLSTATE: HY000 Message: Data too long for condition item '%s'
    public final static int ER_UNKNOWN_LOCALE = 1649; //SQLSTATE: HY000 Message: Unknown locale: '%s'
    @Deprecated
    public final static int ER_SLAVE_IGNORE_SERVER_IDS = 1650;
    public final static int ER_REPLICA_IGNORE_SERVER_IDS = 1650; //SQLSTATE: HY000 Message: The requested server id %d clashes with the replica startup option --replicate-same-server-id
    public final static int ER_QUERY_CACHE_DISABLED = 1651; //SQLSTATE: HY000 Message: Query cache is disabled; restart the server with query_cache_type=1 to enable it
    public final static int ER_SAME_NAME_PARTITION_FIELD = 1652; //SQLSTATE: HY000 Message: Duplicate partition field name '%s'
    public final static int ER_PARTITION_COLUMN_LIST_ERROR = 1653; //SQLSTATE: HY000 Message: Inconsistency in usage of column lists for partitioning
    public final static int ER_WRONG_TYPE_COLUMN_VALUE_ERROR = 1654; //SQLSTATE: HY000 Message: Partition column values of incorrect type
    public final static int ER_TOO_MANY_PARTITION_FUNC_FIELDS_ERROR = 1655; //SQLSTATE: HY000 Message: Too many fields in '%s'
    public final static int ER_MAXVALUE_IN_VALUES_IN = 1656; //SQLSTATE: HY000 Message: Cannot use MAXVALUE as value in VALUES IN
    public final static int ER_TOO_MANY_VALUES_ERROR = 1657; //SQLSTATE: HY000 Message: Cannot have more than one value for this type of %s partitioning
    public final static int ER_ROW_SINGLE_PARTITION_FIELD_ERROR = 1658; //SQLSTATE: HY000 Message: Row expressions in VALUES IN only allowed for multi-field column partitioning
    public final static int ER_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD = 1659; //SQLSTATE: HY000 Message: Field '%s' is of a not allowed type for this type of partitioning
    public final static int ER_PARTITION_FIELDS_TOO_LONG = 1660; //SQLSTATE: HY000 Message: The total length of the partitioning fields is too large
    public final static int ER_BINLOG_ROW_ENGINE_AND_STMT_ENGINE = 1661; //SQLSTATE: HY000 Message: Cannot execute statement: impossible to write to binary log since both row-incapable engines and statement-incapable engines are involved.
    public final static int ER_BINLOG_ROW_MODE_AND_STMT_ENGINE = 1662; //SQLSTATE: HY000 Message: Cannot execute statement: impossible to write to binary log since BINLOG_FORMAT = ROW and at least one table uses a storage engine limited to statement-based logging.
    public final static int ER_BINLOG_UNSAFE_AND_STMT_ENGINE = 1663; //SQLSTATE: HY000 Message: Cannot execute statement: impossible to write to binary log since statement is unsafe, storage engine is limited to statement-based logging, and BINLOG_FORMAT = MIXED. %s
    public final static int ER_BINLOG_ROW_INJECTION_AND_STMT_ENGINE = 1664; //SQLSTATE: HY000 Message: Cannot execute statement: impossible to write to binary log since statement is in row format and at least one table uses a storage engine limited to statement-based logging.
    public final static int ER_BINLOG_STMT_MODE_AND_ROW_ENGINE = 1665; //SQLSTATE: HY000 Message: Cannot execute statement: impossible to write to binary log since BINLOG_FORMAT = STATEMENT and at least one table uses a storage engine limited to row-based logging.%s
    public final static int ER_BINLOG_ROW_INJECTION_AND_STMT_MODE = 1666; //SQLSTATE: HY000 Message: Cannot execute statement: impossible to write to binary log since statement is in row format and BINLOG_FORMAT = STATEMENT.
    public final static int ER_BINLOG_MULTIPLE_ENGINES_AND_SELF_LOGGING_ENGINE = 1667; //SQLSTATE: HY000 Message: Cannot execute statement: impossible to write to binary log since more than one engine is involved and at least one engine is self-logging.
    public final static int ER_BINLOG_UNSAFE_LIMIT = 1668; //SQLSTATE: HY000 Message: The statement is unsafe because it uses a LIMIT clause. This is unsafe because the set of rows included cannot be predicted.
    public final static int ER_BINLOG_UNSAFE_INSERT_DELAYED = 1669; //SQLSTATE: HY000 Message: The statement is unsafe because it uses INSERT DELAYED. This is unsafe because the times when rows are inserted cannot be predicted.
    public final static int ER_BINLOG_UNSAFE_SYSTEM_TABLE = 1670; //SQLSTATE: HY000 Message: The statement is unsafe because it uses the general log, slow query log, or performance_schema table(s). This is unsafe because system tables may differ on replicas.
    public final static int ER_BINLOG_UNSAFE_AUTOINC_COLUMNS = 1671; //SQLSTATE: HY000 Message: Statement is unsafe because it invokes a trigger or a stored function that inserts into an AUTO_INCREMENT column. Inserted values cannot be logged correctly.
    public final static int ER_BINLOG_UNSAFE_UDF = 1672; //SQLSTATE: HY000 Message: Statement is unsafe because it uses a UDF which may not return the same value on the replica.
    public final static int ER_BINLOG_UNSAFE_SYSTEM_VARIABLE = 1673; //SQLSTATE: HY000 Message: Statement is unsafe because it uses a system variable that may have a different value on the replica.
    public final static int ER_BINLOG_UNSAFE_SYSTEM_FUNCTION = 1674; //SQLSTATE: HY000 Message: Statement is unsafe because it uses a system function that may return a different value on the replica.
    public final static int ER_BINLOG_UNSAFE_NONTRANS_AFTER_TRANS = 1675; //SQLSTATE: HY000 Message: Statement is unsafe because it accesses a non-transactional table after accessing a transactional table within the same transaction.
    public final static int ER_MESSAGE_AND_STATEMENT = 1676; //SQLSTATE: HY000 Message: %s Statement: %s
    @Deprecated
    public final static int ER_SLAVE_CONVERSION_FAILED = 1677;
    public final static int ER_REPLICA_CONVERSION_FAILED = 1677; //SQLSTATE: HY000 Message: Column %d of table '%s.%s' cannot be converted from type '%s' to type '%s'
    @Deprecated
    public final static int ER_SLAVE_CANT_CREATE_CONVERSION = 1678;
    public final static int ER_REPLICA_CANT_CREATE_CONVERSION = 1678; //SQLSTATE: HY000 Message: Can't create conversion table for table '%s.%s'
    public final static int ER_INSIDE_TRANSACTION_PREVENTS_SWITCH_BINLOG_FORMAT = 1679; //SQLSTATE: HY000 Message: Cannot modify @@session.binlog_format inside a transaction
    public final static int ER_PATH_LENGTH = 1680; //SQLSTATE: HY000 Message: The path specified for %s is too long.
    public final static int ER_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT = 1681; //SQLSTATE: HY000 Message: The syntax '%s' is deprecated and will be removed in MySQL %s.
    public final static int ER_WRONG_NATIVE_TABLE_STRUCTURE = 1682; //SQLSTATE: HY000 Message: Native table '%s'.'%s' has the wrong structure
    public final static int ER_WRONG_PERFSCHEMA_USAGE = 1683; //SQLSTATE: HY000 Message: Invalid performance_schema usage.
    public final static int ER_WARN_I_S_SKIPPED_TABLE = 1684; //SQLSTATE: HY000 Message: Table '%s'.'%s' was skipped since its definition is being modified by concurrent DDL statement
    public final static int ER_INSIDE_TRANSACTION_PREVENTS_SWITCH_BINLOG_DIRECT = 1685; //SQLSTATE: HY000 Message: Cannot modify @@session.binlog_direct_non_transactional_updates inside a transaction
    public final static int ER_STORED_FUNCTION_PREVENTS_SWITCH_BINLOG_DIRECT = 1686; //SQLSTATE: HY000 Message: Cannot change the binlog direct flag inside a stored function or trigger
    public final static int ER_SPATIAL_MUST_HAVE_GEOM_COL = 1687; //SQLSTATE: 42000 Message: A SPATIAL index may only contain a geometrical type column
    public final static int ER_TOO_LONG_INDEX_COMMENT = 1688; //SQLSTATE: HY000 Message: Comment for index '%s' is too long (max = %lu)
    public final static int ER_LOCK_ABORTED = 1689; //SQLSTATE: HY000 Message: Wait on a lock was aborted due to a pending exclusive lock
    public final static int ER_DATA_OUT_OF_RANGE = 1690; //SQLSTATE: 22003 Message: %s value is out of range in '%s'
    public final static int ER_WRONG_SPVAR_TYPE_IN_LIMIT = 1691; //SQLSTATE: HY000 Message: A variable of a non-integer type in LIMIT clause
    public final static int ER_BINLOG_UNSAFE_MULTIPLE_ENGINES_AND_SELF_LOGGING_ENGINE = 1692; //SQLSTATE: HY000 Message: Mixing self-logging and non-self-logging engines in a statement is unsafe.
    public final static int ER_BINLOG_UNSAFE_MIXED_STATEMENT = 1693; //SQLSTATE: HY000 Message: Statement accesses nontransactional table as well as transactional or temporary table, and writes to any of them.
    public final static int ER_INSIDE_TRANSACTION_PREVENTS_SWITCH_SQL_LOG_BIN = 1694; //SQLSTATE: HY000 Message: Cannot modify @@session.sql_log_bin inside a transaction
    public final static int ER_STORED_FUNCTION_PREVENTS_SWITCH_SQL_LOG_BIN = 1695; //SQLSTATE: HY000 Message: Cannot change the sql_log_bin inside a stored function or trigger
    public final static int ER_FAILED_READ_FROM_PAR_FILE = 1696; //SQLSTATE: HY000 Message: Failed to read from the .par file
    public final static int ER_VALUES_IS_NOT_INT_TYPE_ERROR = 1697; //SQLSTATE: HY000 Message: VALUES value for partition '%s' must have type INT
    public final static int ER_ACCESS_DENIED_NO_PASSWORD_ERROR = 1698; //SQLSTATE: 28000 Message: Access denied for user '%s'@'%s'
    public final static int ER_SET_PASSWORD_AUTH_PLUGIN = 1699; //SQLSTATE: HY000 Message: SET PASSWORD has no significance for users authenticating via plugins
    public final static int ER_GRANT_PLUGIN_USER_EXISTS = 1700; //SQLSTATE: HY000 Message: GRANT with IDENTIFIED WITH is illegal because the user %-.*s already exists
    public final static int ER_TRUNCATE_ILLEGAL_FK = 1701; //SQLSTATE: 42000 Message: Cannot truncate a table referenced in a foreign key constraint (%s)
    public final static int ER_PLUGIN_IS_PERMANENT = 1702; //SQLSTATE: HY000 Message: Plugin '%s' is force_plus_permanent and can not be unloaded
    @Deprecated
    public final static int ER_SLAVE_HEARTBEAT_VALUE_OUT_OF_RANGE_MIN = 1703;
    public final static int ER_REPLICA_HEARTBEAT_VALUE_OUT_OF_RANGE_MIN = 1703; //SQLSTATE: HY000 Message: The requested value for the heartbeat period is less than 1 millisecond. The value is reset to 0, meaning that heartbeating will effectively be disabled.
    @Deprecated
    public final static int ER_SLAVE_HEARTBEAT_VALUE_OUT_OF_RANGE_MAX = 1704;
    public final static int ER_REPLICA_HEARTBEAT_VALUE_OUT_OF_RANGE_MAX = 1704; //SQLSTATE: HY000 Message: The requested value for the heartbeat period exceeds the value of `replica_net_timeout' seconds. A sensible value for the period should be less than the timeout.
    public final static int ER_STMT_CACHE_FULL = 1705; //SQLSTATE: HY000 Message: Multi-row statements required more than 'max_binlog_stmt_cache_size' bytes of storage; increase this mysqld variable and try again

    public final static int ER_MULTI_UPDATE_KEY_CONFLICT = 1706; //SQLSTATE: HY000 Message: Primary key/partition key update is not allowed since the table is updated both as '%s' and '%s'.
    public final static int ER_TABLE_NEEDS_REBUILD = 1707; //SQLSTATE: HY000 Message: Table rebuild required. Please do "ALTER TABLE `%s` FORCE" or dump/reload to fix it!
    public final static int WARN_OPTION_BELOW_LIMIT = 1708; //SQLSTATE: HY000 Message: The value of '%s' should be no less than the value of '%s'
    public final static int ER_INDEX_COLUMN_TOO_LONG = 1709; //SQLSTATE: HY000 Message: Index column size too large. The maximum column size is %lu bytes.
    public final static int ER_ERROR_IN_TRIGGER_BODY = 1710; //SQLSTATE: HY000 Message: Trigger '%s' has an error in its body: '%s'
    public final static int ER_ERROR_IN_UNKNOWN_TRIGGER_BODY = 1711; //SQLSTATE: HY000 Message: Unknown trigger has an error in its body: '%s'
    public final static int ER_INDEX_CORRUPT = 1712; //SQLSTATE: HY000 Message: Index %s is corrupted
    public final static int ER_UNDO_RECORD_TOO_BIG = 1713; //SQLSTATE: HY000 Message: Undo log record is too big.
    public final static int ER_BINLOG_UNSAFE_INSERT_IGNORE_SELECT = 1714; //SQLSTATE: HY000 Message: INSERT IGNORE... SELECT is unsafe because the order in which rows are retrieved by the SELECT determines which (if any) rows are ignored. This order cannot be predicted and may differ on source and the replica.
    public final static int ER_BINLOG_UNSAFE_INSERT_SELECT_UPDATE = 1715; //SQLSTATE: HY000 Message: INSERT... SELECT... ON DUPLICATE KEY UPDATE is unsafe because the order in which rows are retrieved by the SELECT determines which (if any) rows are updated. This order cannot be predicted and may differ on source and the replica.
    public final static int ER_BINLOG_UNSAFE_REPLACE_SELECT = 1716; //SQLSTATE: HY000 Message: REPLACE... SELECT is unsafe because the order in which rows are retrieved by the SELECT determines which (if any) rows are replaced. This order cannot be predicted and may differ on source and the replica.
    public final static int ER_BINLOG_UNSAFE_CREATE_IGNORE_SELECT = 1717; //SQLSTATE: HY000 Message: CREATE... IGNORE SELECT is unsafe because the order in which rows are retrieved by the SELECT determines which (if any) rows are ignored. This order cannot be predicted and may differ on source and the replica.
    public final static int ER_BINLOG_UNSAFE_CREATE_REPLACE_SELECT = 1718; //SQLSTATE: HY000 Message: CREATE... REPLACE SELECT is unsafe because the order in which rows are retrieved by the SELECT determines which (if any) rows are replaced. This order cannot be predicted and may differ on source and the replica.
    public final static int ER_BINLOG_UNSAFE_UPDATE_IGNORE = 1719; //SQLSTATE: HY000 Message: UPDATE IGNORE is unsafe because the order in which rows are updated determines which (if any) rows are ignored. This order cannot be predicted and may differ on source and the replica.
    public final static int ER_PLUGIN_NO_UNINSTALL = 1720; //SQLSTATE: HY000 Message: Plugin '%s' is marked as not dynamically uninstallable. You have to stop the server to uninstall it.
    public final static int ER_PLUGIN_NO_INSTALL = 1721; //SQLSTATE: HY000 Message: Plugin '%s' is marked as not dynamically installable. You have to stop the server to install it.
    public final static int ER_BINLOG_UNSAFE_WRITE_AUTOINC_SELECT = 1722; //SQLSTATE: HY000 Message: Statements writing to a table with an auto-increment column after selecting from another table are unsafe because the order in which rows are retrieved determines what (if any) rows will be written. This order cannot be predicted and may differ on source and the replica.
    public final static int ER_BINLOG_UNSAFE_CREATE_SELECT_AUTOINC = 1723; //SQLSTATE: HY000 Message: CREATE TABLE... SELECT... on a table with an auto-increment column is unsafe because the order in which rows are retrieved by the SELECT determines which (if any) rows are inserted. This order cannot be predicted and may differ on source and the replica.
    public final static int ER_BINLOG_UNSAFE_INSERT_TWO_KEYS = 1724; //SQLSTATE: HY000 Message: INSERT... ON DUPLICATE KEY UPDATE on a table with more than one UNIQUE KEY is unsafe
    public final static int ER_TABLE_IN_FK_CHECK = 1725; //SQLSTATE: HY000 Message: Table is being used in foreign key check.
    public final static int ER_UNSUPPORTED_ENGINE = 1726; //SQLSTATE: HY000 Message: Storage engine '%s' does not support system tables. [%s.%s]
    public final static int ER_BINLOG_UNSAFE_AUTOINC_NOT_FIRST = 1727; //SQLSTATE: HY000 Message: INSERT into autoincrement field which is not the first part in the composed primary key is unsafe.
    public final static int ER_CANNOT_LOAD_FROM_TABLE_V2 = 1728; //SQLSTATE: HY000 Message: Cannot load from %s.%s. The table is probably corrupted
    @Deprecated
    public final static int ER_MASTER_DELAY_VALUE_OUT_OF_RANGE = 1729;
    public final static int ER_SOURCE_DELAY_VALUE_OUT_OF_RANGE = 1729; //SQLSTATE: HY000 Message: The requested value %u for the source delay exceeds the maximum %u
    public final static int ER_ONLY_FD_AND_RBR_EVENTS_ALLOWED_IN_BINLOG_STATEMENT = 1730; //SQLSTATE: HY000 Message: Only Format_description_log_event and row events are allowed in BINLOG statements (but %s was provided)
    public final static int ER_PARTITION_EXCHANGE_DIFFERENT_OPTION = 1731; //SQLSTATE: HY000 Message: Non matching attribute '%s' between partition and table
    public final static int ER_PARTITION_EXCHANGE_PART_TABLE = 1732; //SQLSTATE: HY000 Message: Table to exchange with partition is partitioned: '%s'
    public final static int ER_PARTITION_EXCHANGE_TEMP_TABLE = 1733; //SQLSTATE: HY000 Message: Table to exchange with partition is temporary: '%s'
    public final static int ER_PARTITION_INSTEAD_OF_SUBPARTITION = 1734; //SQLSTATE: HY000 Message: Subpartitioned table, use subpartition instead of partition
    public final static int ER_UNKNOWN_PARTITION = 1735; //SQLSTATE: HY000 Message: Unknown partition '%s' in table '%s'
    public final static int ER_TABLES_DIFFERENT_METADATA = 1736; //SQLSTATE: HY000 Message: Tables have different definitions
    public final static int ER_ROW_DOES_NOT_MATCH_PARTITION = 1737; //SQLSTATE: HY000 Message: Found a row that does not match the partition
    public final static int ER_BINLOG_CACHE_SIZE_GREATER_THAN_MAX = 1738; //SQLSTATE: HY000 Message: Option binlog_cache_size (%lu) is greater than max_binlog_cache_size (%lu); setting binlog_cache_size equal to max_binlog_cache_size.
    public final static int ER_WARN_INDEX_NOT_APPLICABLE = 1739; //SQLSTATE: HY000 Message: Cannot use %s access on index '%s' due to type or collation conversion on field '%s'
    public final static int ER_PARTITION_EXCHANGE_FOREIGN_KEY = 1740; //SQLSTATE: HY000 Message: Table to exchange with partition has foreign key references: '%s'
    public final static int ER_NO_SUCH_KEY_VALUE = 1741; //SQLSTATE: HY000 Message: Key value '%s' was not found in table '%s.%s'
    public final static int ER_RPL_INFO_DATA_TOO_LONG = 1742; //SQLSTATE: HY000 Message: Data for column '%s' too long
    public final static int ER_NETWORK_READ_EVENT_CHECKSUM_FAILURE = 1743; //SQLSTATE: HY000 Message: Replication event checksum verification failed while reading from network.
    public final static int ER_BINLOG_READ_EVENT_CHECKSUM_FAILURE = 1744; //SQLSTATE: HY000 Message: Replication event checksum verification failed while reading from a log file.
    public final static int ER_BINLOG_STMT_CACHE_SIZE_GREATER_THAN_MAX = 1745; //SQLSTATE: HY000 Message: Option binlog_stmt_cache_size (%lu) is greater than max_binlog_stmt_cache_size (%lu); setting binlog_stmt_cache_size equal to max_binlog_stmt_cache_size.
    public final static int ER_CANT_UPDATE_TABLE_IN_CREATE_TABLE_SELECT = 1746; //SQLSTATE: HY000 Message: Can't update table '%s' while '%s' is being created.
    public final static int ER_PARTITION_CLAUSE_ON_NONPARTITIONED = 1747; //SQLSTATE: HY000 Message: PARTITION () clause on non partitioned table
    public final static int ER_ROW_DOES_NOT_MATCH_GIVEN_PARTITION_SET = 1748; //SQLSTATE: HY000 Message: Found a row not matching the given partition set
    public final static int ER_NO_SUCH_PARTITION__UNUSED = 1749; //SQLSTATE: HY000 Message: partition '%s' doesn't exist
    public final static int ER_CHANGE_RPL_INFO_REPOSITORY_FAILURE = 1750; //SQLSTATE: HY000 Message: Failure while changing the type of replication repository: %s.
    public final static int ER_WARNING_NOT_COMPLETE_ROLLBACK_WITH_CREATED_TEMP_TABLE = 1751; //SQLSTATE: HY000 Message: The creation of some temporary tables could not be rolled back.
    public final static int ER_WARNING_NOT_COMPLETE_ROLLBACK_WITH_DROPPED_TEMP_TABLE = 1752; //SQLSTATE: HY000 Message: Some temporary tables were dropped, but these operations could not be rolled back.
    public final static int ER_MTS_FEATURE_IS_NOT_SUPPORTED = 1753; //SQLSTATE: HY000 Message: %s is not supported in multi-threaded replica mode. %s
    public final static int ER_MTS_UPDATED_DBS_GREATER_MAX = 1754; //SQLSTATE: HY000 Message: The number of modified databases exceeds the maximum %d; the database names will not be included in the replication event metadata.
    public final static int ER_MTS_CANT_PARALLEL = 1755; //SQLSTATE: HY000 Message: Cannot execute the current event group in the parallel mode. Encountered event %s, relay-log name %s, position %s which prevents execution of this event group in parallel mode. Reason: %s.
    public final static int ER_MTS_INCONSISTENT_DATA = 1756; //SQLSTATE: HY000 Message: %s
    public final static int ER_FULLTEXT_NOT_SUPPORTED_WITH_PARTITIONING = 1757; //SQLSTATE: HY000 Message: FULLTEXT index is not supported for partitioned tables.
    public final static int ER_DA_INVALID_CONDITION_NUMBER = 1758; //SQLSTATE: 35000 Message: Invalid condition number
    public final static int ER_INSECURE_PLAIN_TEXT = 1759; //SQLSTATE: HY000 Message: Sending passwords in plain text without SSL/TLS is extremely insecure.
    @Deprecated
    public final static int ER_INSECURE_CHANGE_MASTER = 1760;
    public final static int ER_INSECURE_CHANGE_SOURCE = 1760; //SQLSTATE: HY000 Message: Storing MySQL user name or password information in the source.info repository is not secure and is therefore not recommended. Please see the MySQL Manual for more about this issue and possible alternatives.
    public final static int ER_FOREIGN_DUPLICATE_KEY_WITH_CHILD_INFO = 1761; //SQLSTATE: 23000 Message: Foreign key constraint for table '%s', record '%s' would lead to a duplicate entry in table '%s', key '%s'
    public final static int ER_FOREIGN_DUPLICATE_KEY_WITHOUT_CHILD_INFO = 1762; //SQLSTATE: 23000 Message: Foreign key constraint for table '%s', record '%s' would lead to a duplicate entry in a child table
    @Deprecated
    public final static int ER_SQLTHREAD_WITH_SECURE_SLAVE = 1763;
    public final static int ER_SQLTHREAD_WITH_SECURE_REPLICA = 1763; //SQLSTATE: HY000 Message: Setting authentication options is not possible when only the Replica SQL Thread is being started.
    public final static int ER_TABLE_HAS_NO_FT = 1764; //SQLSTATE: HY000 Message: The table does not have FULLTEXT index to support this query
    public final static int ER_VARIABLE_NOT_SETTABLE_IN_SF_OR_TRIGGER = 1765; //SQLSTATE: HY000 Message: The system variable %s cannot be set in stored functions or triggers.
    public final static int ER_VARIABLE_NOT_SETTABLE_IN_TRANSACTION = 1766; //SQLSTATE: HY000 Message: The system variable %s cannot be set when there is an ongoing transaction.
    public final static int ER_GTID_NEXT_IS_NOT_IN_GTID_NEXT_LIST = 1767; //SQLSTATE: HY000 Message: The system variable @@SESSION.GTID_NEXT has the value %s, which is not listed in @@SESSION.GTID_NEXT_LIST.
    public final static int ER_CANT_CHANGE_GTID_NEXT_IN_TRANSACTION_WHEN_GTID_NEXT_LIST_IS_NULL = 1768; //SQLSTATE: HY000 Message: When @@SESSION.GTID_NEXT_LIST == NULL, the system variable @@SESSION.GTID_NEXT cannot change inside a transaction.
    public final static int ER_SET_STATEMENT_CANNOT_INVOKE_FUNCTION = 1769; //SQLSTATE: HY000 Message: The statement 'SET %s' cannot invoke a stored function.
    public final static int ER_GTID_NEXT_CANT_BE_AUTOMATIC_IF_GTID_NEXT_LIST_IS_NON_NULL = 1770; //SQLSTATE: HY000 Message: The system variable @@SESSION.GTID_NEXT cannot be 'AUTOMATIC' when @@SESSION.GTID_NEXT_LIST is non-NULL.
    public final static int ER_SKIPPING_LOGGED_TRANSACTION = 1771; //SQLSTATE: HY000 Message: Skipping transaction %s because it has already been executed and logged.
    public final static int ER_MALFORMED_GTID_SET_SPECIFICATION = 1772; //SQLSTATE: HY000 Message: Malformed GTID set specification '%s'.
    public final static int ER_MALFORMED_GTID_SET_ENCODING = 1773; //SQLSTATE: HY000 Message: Malformed GTID set encoding.
    public final static int ER_MALFORMED_GTID_SPECIFICATION = 1774; //SQLSTATE: HY000 Message: Malformed GTID specification '%s'.
    public final static int ER_GNO_EXHAUSTED = 1775; //SQLSTATE: HY000 Message: Impossible to generate Global Transaction Identifier: the integer component reached the maximal value. Restart the server with a new server_uuid.
    @Deprecated
    public final static int ER_BAD_SLAVE_AUTO_POSITION = 1776;
    public final static int ER_BAD_REPLICA_AUTO_POSITION = 1776; //SQLSTATE: HY000 Message: Parameters SOURCE_LOG_FILE, SOURCE_LOG_POS, RELAY_LOG_FILE and RELAY_LOG_POS cannot be set when SOURCE_AUTO_POSITION is active.
    public final static int ER_AUTO_POSITION_REQUIRES_GTID_MODE_ON = 1777; //SQLSTATE: HY000 Message: CHANGE SOURCE TO SOURCE_AUTO_POSITION = 1 can only be executed when GTID_MODE = ON.
    public final static int ER_CANT_DO_IMPLICIT_COMMIT_IN_TRX_WHEN_GTID_NEXT_IS_SET = 1778; //SQLSTATE: HY000 Message: Cannot execute statements with implicit commit inside a transaction when GTID_NEXT != AUTOMATIC or GTID_NEXT_LIST != NULL.
    public final static int ER_GTID_MODE_2_OR_3_REQUIRES_ENFORCE_GTID_CONSISTENCY_ON = 1779; //SQLSTATE: HY000 Message: GTID_MODE = ON or GTID_MODE = UPGRADE_STEP_2 requires ENFORCE_GTID_CONSISTENCY = 1.
    public final static int ER_GTID_MODE_REQUIRES_BINLOG = 1780; //SQLSTATE: HY000 Message: GTID_MODE = ON or UPGRADE_STEP_1 or UPGRADE_STEP_2 requires --log-bin and --log-replica-updates.
    public final static int ER_CANT_SET_GTID_NEXT_TO_GTID_WHEN_GTID_MODE_IS_OFF = 1781; //SQLSTATE: HY000 Message: GTID_NEXT cannot be set to UUID:NUMBER when GTID_MODE = OFF.
    public final static int ER_CANT_SET_GTID_NEXT_TO_ANONYMOUS_WHEN_GTID_MODE_IS_ON = 1782; //SQLSTATE: HY000 Message: GTID_NEXT cannot be set to ANONYMOUS when GTID_MODE = ON.
    public final static int ER_CANT_SET_GTID_NEXT_LIST_TO_NON_NULL_WHEN_GTID_MODE_IS_OFF = 1783; //SQLSTATE: HY000 Message: GTID_NEXT_LIST cannot be set to a non-NULL value when GTID_MODE = OFF.
    public final static int ER_FOUND_GTID_EVENT_WHEN_GTID_MODE_IS_OFF = 1784; //SQLSTATE: HY000 Message: Found a Gtid_log_event or Previous_gtids_log_event when GTID_MODE = OFF.
    public final static int ER_GTID_UNSAFE_NON_TRANSACTIONAL_TABLE = 1785; //SQLSTATE: HY000 Message: When ENFORCE_GTID_CONSISTENCY = 1, updates to non-transactional tables can only be done in either autocommitted statements or single-statement transactions, and never in the same statement as updates to transactional tables.
    public final static int ER_GTID_UNSAFE_CREATE_SELECT = 1786; //SQLSTATE: HY000 Message: CREATE TABLE ... SELECT is forbidden when ENFORCE_GTID_CONSISTENCY = 1.
    public final static int ER_GTID_UNSAFE_CREATE_DROP_TEMPORARY_TABLE_IN_TRANSACTION = 1787; //SQLSTATE: HY000 Message: When ENFORCE_GTID_CONSISTENCY = 1, the statements CREATE TEMPORARY TABLE and DROP TEMPORARY TABLE can be executed in a non-transactional context only, and require that AUTOCOMMIT = 1.
    public final static int ER_GTID_MODE_CAN_ONLY_CHANGE_ONE_STEP_AT_A_TIME = 1788; //SQLSTATE: HY000 Message: The value of GTID_MODE can only change one step at a time: OFF <-> UPGRADE_STEP_1 <-> UPGRADE_STEP_2 <-> ON. Also note that this value must be stepped up or down simultaneously on all servers; see the Manual for instructions.
    @Deprecated
    public final static int ER_MASTER_HAS_PURGED_REQUIRED_GTIDS = 1789;
    public final static int ER_SOURCE_HAS_PURGED_REQUIRED_GTIDS = 1789; //SQLSTATE: HY000 Message: The replica is connecting using CHANGE SOURCE TO SOURCE_AUTO_POSITION = 1, but the source has purged binary logs containing GTIDs that the replica requires.
    public final static int ER_CANT_SET_GTID_NEXT_WHEN_OWNING_GTID = 1790; //SQLSTATE: HY000 Message: GTID_NEXT cannot be changed by a client that owns a GTID. The client owns %s. Ownership is released on COMMIT or ROLLBACK.
    public final static int ER_UNKNOWN_EXPLAIN_FORMAT = 1791; //SQLSTATE: HY000 Message: Unknown EXPLAIN format name: '%s'
    public final static int ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION = 1792; //SQLSTATE: 25006 Message: Cannot execute statement in a READ ONLY transaction.
    public final static int ER_TOO_LONG_TABLE_PARTITION_COMMENT = 1793; //SQLSTATE: HY000 Message: Comment for table partition '%s' is too long (max = %lu)
    @Deprecated
    public final static int ER_SLAVE_CONFIGURATION = 1794;
    public final static int ER_REPLICA_CONFIGURATION = 1794; //SQLSTATE: HY000 Message: Replica is not configured or failed to initialize properly. You must at least set --server-id to enable either a source or a replica. Additional error messages can be found in the MySQL error log.
    public final static int ER_INNODB_FT_LIMIT = 1795; //SQLSTATE: HY000 Message: InnoDB presently supports one FULLTEXT index creation at a time
    public final static int ER_INNODB_NO_FT_TEMP_TABLE = 1796; //SQLSTATE: HY000 Message: Cannot create FULLTEXT index on temporary InnoDB table
    public final static int ER_INNODB_FT_WRONG_DOCID_COLUMN = 1797; //SQLSTATE: HY000 Message: Column '%s' is of wrong type for an InnoDB FULLTEXT index
    public final static int ER_INNODB_FT_WRONG_DOCID_INDEX = 1798; //SQLSTATE: HY000 Message: Index '%s' is of wrong type for an InnoDB FULLTEXT index
    public final static int ER_INNODB_ONLINE_LOG_TOO_BIG = 1799; //SQLSTATE: HY000 Message: Creating index '%s' required more than 'innodb_online_alter_log_max_size' bytes of modification log. Please try again.
    public final static int ER_UNKNOWN_ALTER_ALGORITHM = 1800; //SQLSTATE: HY000 Message: Unknown ALGORITHM '%s'
    public final static int ER_UNKNOWN_ALTER_LOCK = 1801; //SQLSTATE: HY000 Message: Unknown LOCK type '%s'
    @Deprecated
    public final static int ER_MTS_CHANGE_MASTER_CANT_RUN_WITH_GAPS = 1802;
    public final static int ER_MTS_CHANGE_SOURCE_CANT_RUN_WITH_GAPS = 1802; //SQLSTATE: HY000 Message: CHANGE SOURCE cannot be executed when the replica was stopped with an error or killed in MTS mode. Consider using RESET REPLICA or START REPLICA UNTIL.
    public final static int ER_MTS_RECOVERY_FAILURE = 1803; //SQLSTATE: HY000 Message: Cannot recover after REPLICA errored out in parallel execution mode. Additional error messages can be found in the MySQL error log.
    public final static int ER_MTS_RESET_WORKERS = 1804; //SQLSTATE: HY000 Message: Cannot clean up worker info tables. Additional error messages can be found in the MySQL error log.
    public final static int ER_COL_COUNT_DOESNT_MATCH_CORRUPTED_V2 = 1805; //SQLSTATE: HY000 Message: Column count of %s.%s is wrong. Expected %d, found %d. The table is probably corrupted
    @Deprecated
    public final static int ER_SLAVE_SILENT_RETRY_TRANSACTION = 1806;
    public final static int ER_REPLICA_SILENT_RETRY_TRANSACTION = 1806; //SQLSTATE: HY000 Message: Replica must silently retry current transaction
    public final static int ER_DISCARD_FK_CHECKS_RUNNING = 1807; //SQLSTATE: HY000 Message: There is a foreign key check running on table '%s'. Cannot discard the table.
    public final static int ER_TABLE_SCHEMA_MISMATCH = 1808; //SQLSTATE: HY000 Message: Schema mismatch (%s)
    public final static int ER_TABLE_IN_SYSTEM_TABLESPACE = 1809; //SQLSTATE: HY000 Message: Table '%s' in system tablespace
    public final static int ER_IO_READ_ERROR = 1810; //SQLSTATE: HY000 Message: IO Read error: (%lu, %s) %s
    public final static int ER_IO_WRITE_ERROR = 1811; //SQLSTATE: HY000 Message: IO Write error: (%lu, %s) %s
    public final static int ER_TABLESPACE_MISSING = 1812; //SQLSTATE: HY000 Message: Tablespace is missing for table '%s'
    public final static int ER_TABLESPACE_EXISTS = 1813; //SQLSTATE: HY000 Message: Tablespace for table '%s' exists. Please DISCARD the tablespace before IMPORT.
    public final static int ER_TABLESPACE_DISCARDED = 1814; //SQLSTATE: HY000 Message: Tablespace has been discarded for table '%s'
    public final static int ER_INTERNAL_ERROR = 1815; //SQLSTATE: HY000 Message: Internal error: %s
    public final static int ER_INNODB_IMPORT_ERROR = 1816; //SQLSTATE: HY000 Message: ALTER TABLE '%s' IMPORT TABLESPACE failed with error %lu : '%s'
    public final static int ER_INNODB_INDEX_CORRUPT = 1817; //SQLSTATE: HY000 Message: Index corrupt: %s
    public final static int ER_INVALID_YEAR_COLUMN_LENGTH = 1818; //SQLSTATE: HY000 Message: YEAR(%lu) column type is deprecated. Creating YEAR(4) column instead.
    public final static int ER_NOT_VALID_PASSWORD = 1819; //SQLSTATE: HY000 Message: Your password does not satisfy the current policy requirements
    public final static int ER_MUST_CHANGE_PASSWORD = 1820; //SQLSTATE: HY000 Message: You must SET PASSWORD before executing this statement
    public final static int ER_FK_NO_INDEX_CHILD = 1821; //SQLSTATE: HY000 Message: Failed to add the foreign key constaint. Missing index for constraint '%s' in the foreign table '%s'
    public final static int ER_FK_NO_INDEX_PARENT = 1822; //SQLSTATE: HY000 Message: Failed to add the foreign key constaint. Missing index for constraint '%s' in the referenced table '%s'
    public final static int ER_FK_FAIL_ADD_SYSTEM = 1823; //SQLSTATE: HY000 Message: Failed to add the foreign key constraint '%s' to system tables
    public final static int ER_FK_CANNOT_OPEN_PARENT = 1824; //SQLSTATE: HY000 Message: Failed to open the referenced table '%s'
    public final static int ER_FK_INCORRECT_OPTION = 1825; //SQLSTATE: HY000 Message: Failed to add the foreign key constraint on table '%s'. Incorrect options in FOREIGN KEY constraint '%s'
    public final static int ER_FK_DUP_NAME = 1826; //SQLSTATE: HY000 Message: Duplicate foreign key constraint name '%s'
    public final static int ER_PASSWORD_FORMAT = 1827; //SQLSTATE: HY000 Message: The password hash doesn't have the expected format. Check if the correct password algorithm is being used with the PASSWORD() function.
    public final static int ER_FK_COLUMN_CANNOT_DROP = 1828; //SQLSTATE: HY000 Message: Cannot drop column '%s': needed in a foreign key constraint '%s'
    public final static int ER_FK_COLUMN_CANNOT_DROP_CHILD = 1829; //SQLSTATE: HY000 Message: Cannot drop column '%s': needed in a foreign key constraint '%s' of table '%s'
    public final static int ER_FK_COLUMN_NOT_NULL = 1830; //SQLSTATE: HY000 Message: Column '%s' cannot be NOT NULL: needed in a foreign key constraint '%s' SET NULL
    public final static int ER_DUP_INDEX = 1831; //SQLSTATE: HY000 Message: Duplicate index '%s' defined on the table '%s.%s'. This is deprecated and will be disallowed in a future release.
    public final static int ER_FK_COLUMN_CANNOT_CHANGE = 1832; //SQLSTATE: HY000 Message: Cannot change column '%s': used in a foreign key constraint '%s'
    public final static int ER_FK_COLUMN_CANNOT_CHANGE_CHILD = 1833; //SQLSTATE: HY000 Message: Cannot change column '%s': used in a foreign key constraint '%s' of table '%s'
    public final static int ER_FK_CANNOT_DELETE_PARENT = 1834; //SQLSTATE: HY000 Message: Cannot delete rows from table which is parent in a foreign key constraint '%s' of table '%s'
    public final static int ER_MALFORMED_PACKET = 1835; //SQLSTATE: HY000 Message: Malformed communication packet.
    public final static int ER_READ_ONLY_MODE = 1836; //SQLSTATE: HY000 Message: Running in read-only mode
    public final static int ER_GTID_NEXT_TYPE_UNDEFINED_GROUP = 1837; //SQLSTATE: HY000 Message: When GTID_NEXT is set to a GTID, you must explicitly set it again after a COMMIT or ROLLBACK. If you see this error message in the replica SQL thread, it means that a table in the current transaction is transactional on the source and non-transactional on the replica. In a client connection, it means that you executed SET GTID_NEXT before a transaction and forgot to set GTID_NEXT to a different identifier or to 'AUTOMATIC' after COMMIT or ROLLBACK. Current GTID_NEXT is '%s'.
    public final static int ER_VARIABLE_NOT_SETTABLE_IN_SP = 1838; //SQLSTATE: HY000 Message: The system variable %s cannot be set in stored procedures.
    public final static int ER_CANT_SET_GTID_PURGED_WHEN_GTID_MODE_IS_OFF = 1839; //SQLSTATE: HY000 Message: GTID_PURGED can only be set when GTID_MODE = ON.
    public final static int ER_CANT_SET_GTID_PURGED_WHEN_GTID_EXECUTED_IS_NOT_EMPTY = 1840; //SQLSTATE: HY000 Message: GTID_PURGED can only be set when GTID_EXECUTED is empty.
    public final static int ER_CANT_SET_GTID_PURGED_WHEN_OWNED_GTIDS_IS_NOT_EMPTY = 1841; //SQLSTATE: HY000 Message: GTID_PURGED can only be set when there are no ongoing transactions (not even in other clients).
    public final static int ER_GTID_PURGED_WAS_CHANGED = 1842; //SQLSTATE: HY000 Message: GTID_PURGED was changed from '%s' to '%s'.
    public final static int ER_GTID_EXECUTED_WAS_CHANGED = 1843; //SQLSTATE: HY000 Message: GTID_EXECUTED was changed from '%s' to '%s'.
    public final static int ER_BINLOG_STMT_MODE_AND_NO_REPL_TABLES = 1844; //SQLSTATE: HY000 Message: Cannot execute statement: impossible to write to binary log since BINLOG_FORMAT = STATEMENT, and both replicated and non replicated tables are written to.
    public final static int ER_ALTER_OPERATION_NOT_SUPPORTED = 1845; //SQLSTATE: 0A000 Message: %s is not supported for this operation. Try %s.
    public final static int ER_ALTER_OPERATION_NOT_SUPPORTED_REASON = 1846; //SQLSTATE: 0A000 Message: %s is not supported. Reason: %s. Try %s.
    public final static int ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_COPY = 1847; //SQLSTATE: HY000 Message: COPY algorithm requires a lock
    public final static int ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_PARTITION = 1848; //SQLSTATE: HY000 Message: Partition specific operations do not yet support LOCK/ALGORITHM
    public final static int ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_FK_RENAME = 1849; //SQLSTATE: HY000 Message: Columns participating in a foreign key are renamed
    public final static int ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_COLUMN_TYPE = 1850; //SQLSTATE: HY000 Message: Cannot change column type INPLACE
    public final static int ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_FK_CHECK = 1851; //SQLSTATE: HY000 Message: Adding foreign keys needs foreign_key_checks=OFF
    public final static int ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_IGNORE = 1852; //SQLSTATE: HY000 Message: Creating unique indexes with IGNORE requires COPY algorithm to remove duplicate rows
    public final static int ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_NOPK = 1853; //SQLSTATE: HY000 Message: Dropping a primary key is not allowed without also adding a new primary key
    public final static int ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_AUTOINC = 1854; //SQLSTATE: HY000 Message: Adding an auto-increment column requires a lock
    public final static int ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_HIDDEN_FTS = 1855; //SQLSTATE: HY000 Message: Cannot replace hidden FTS_DOC_ID with a user-visible one
    public final static int ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_CHANGE_FTS = 1856; //SQLSTATE: HY000 Message: Cannot drop or rename FTS_DOC_ID
    public final static int ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_FTS = 1857; //SQLSTATE: HY000 Message: Fulltext index creation requires a lock
    @Deprecated
    public final static int ER_SQL_SLAVE_SKIP_COUNTER_NOT_SETTABLE_IN_GTID_MODE = 1858;
    public final static int ER_SQL_REPLICA_SKIP_COUNTER_NOT_SETTABLE_IN_GTID_MODE = 1858; //SQLSTATE: HY000 Message: sql_replica_skip_counter can not be set when the server is running with GTID_MODE = ON. Instead, for each transaction that you want to skip, generate an empty transaction with the same GTID as the transaction
    public final static int ER_DUP_UNKNOWN_IN_INDEX = 1859; //SQLSTATE: 23000 Message: Duplicate entry for key '%s'
    public final static int ER_IDENT_CAUSES_TOO_LONG_PATH = 1860; //SQLSTATE: HY000 Message: Long database name and identifier for object resulted in path length exceeding %d characters. Path: '%s'.
    public final static int ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_NOT_NULL = 1861; //SQLSTATE: HY000 Message: cannot silently convert NULL values, as required in this SQL_MODE; was introduced in 5.7.1.
    public final static int ER_MUST_CHANGE_PASSWORD_LOGIN = 1862; //SQLSTATE: HY000 Message: Your password has expired. To log in you must change it using a client that supports expired passwords. was introduced in 5.7.1.
    public final static int ER_ROW_IN_WRONG_PARTITION = 1863; //SQLSTATE: HY000 Message: Found a row in wrong partition %s; was introduced in 5.7.1.
    public final static int ER_MTS_EVENT_BIGGER_PENDING_JOBS_SIZE_MAX = 1864; //SQLSTATE: HY000 Message: Cannot schedule event %s, relay-log name %s, position %s to Worker thread because its size %lu exceeds %lu of replica_pending_jobs_size_max.; was introduced in 5.7.2.

    public final static int ER_INNODB_NO_FT_USES_PARSER = 1865; //SQLSTATE: HY000 Message: Cannot CREATE FULLTEXT INDEX WITH PARSER on InnoDB table; was introduced in 5.7.2.
    public final static int ER_BINLOG_LOGICAL_CORRUPTION = 1866; //SQLSTATE: HY000 Message: The binary log file '%s' is logically corrupted: %s; was introduced in 5.7.2.
    public final static int ER_WARN_PURGE_LOG_IN_USE = 1867; //SQLSTATE: HY000 Message: file %s was not purged because it was being read by %d thread(s), purged only %d out of %d files. was introduced in 5.7.2.
    public final static int ER_WARN_PURGE_LOG_IS_ACTIVE = 1868; //SQLSTATE: HY000 Message: file %s was not purged because it is the active log file.; was introduced in 5.7.2.
    public final static int ER_AUTO_INCREMENT_CONFLICT = 1869; //SQLSTATE: HY000 Message: Auto-increment value in UPDATE conflicts with internally generated values; was introduced in 5.7.2.
    public final static int WARN_ON_BLOCKHOLE_IN_RBR = 1870; //SQLSTATE: HY000 Message: Row events are not logged for %s statements that modify BLACKHOLE tables in row format. Table(s): '%s'; was introduced in 5.7.2.
    @Deprecated
    public final static int ER_SLAVE_MI_INIT_REPOSITORY = 1871;
    public final static int ER_REPLICA_MI_INIT_REPOSITORY = 1871; //SQLSTATE: HY000 Message: Replica failed to initialize source info structure from the repository; was introduced in 5.7.2.
    @Deprecated
    public final static int ER_SLAVE_RLI_INIT_REPOSITORY = 1872;
    public final static int ER_REPLICA_RLI_INIT_REPOSITORY = 1872; //SQLSTATE: HY000 Message: Replica failed to initialize relay log info structure from the repository; was introduced in 5.7.2.
    public final static int ER_ACCESS_DENIED_CHANGE_USER_ERROR = 1873; //SQLSTATE: 28000 Message: Access denied trying to change to user '%s'@'%s' (using password: %s). Disconnecting. was introduced in 5.7.2.
    public final static int ER_INNODB_READ_ONLY = 1874; //SQLSTATE: HY000 Message: InnoDB is in read only mode.; was introduced in 5.7.2.
    @Deprecated
    public final static int ER_STOP_SLAVE_SQL_THREAD_TIMEOUT = 1875;
    public final static int ER_STOP_REPLICA_SQL_THREAD_TIMEOUT = 1875; //SQLSTATE: HY000 Message: STOP REPLICA command execution is incomplete: Replica SQL thread got the stop signal, thread is busy, SQL thread will stop once the current task is complete.; was introduced in 5.7.2.
    @Deprecated
    public final static int ER_STOP_SLAVE_IO_THREAD_TIMEOUT = 1876;
    public final static int ER_STOP_REPLICA_IO_THREAD_TIMEOUT = 1876; //SQLSTATE: HY000 Message: STOP REPLICA command execution is incomplete: Replica IO thread got the stop signal, thread is busy, IO thread will stop once the current task is complete.; was introduced in 5.7.2.
    public final static int ER_TABLE_CORRUPT = 1877; //SQLSTATE: HY000 Message: Operation cannot be performed. The table '%s.%s' is missing, corrupt or contains bad data.; was introduced in 5.7.2.
    public final static int ER_TEMP_FILE_WRITE_FAILURE = 1878; //SQLSTATE: HY000 Message: Temporary file write failure.; was introduced in 5.7.3.
    public final static int ER_INNODB_FT_AUX_NOT_HEX_ID = 1879; //SQLSTATE: HY000 Message: Upgrade index name failed, please use create index(alter table) algorithm copy to rebuild index.; was introduced in 5.7.4.
    public final static int ER_OLD_TEMPORALS_UPGRADED = 1880; //SQLSTATE: HY000 Message: TIME/TIMESTAMP/DATETIME columns of old format have been upgraded to the new format.; was introduced in 5.7.4.
    public final static int ER_INNODB_FORCED_RECOVERY = 1881; //SQLSTATE: HY000 Message: Operation not allowed when innodb_forced_recovery > 0.; was introduced in 5.7.4.
    public final static int ER_AES_INVALID_IV = 1882; //SQLSTATE: HY000 Message: The initialization vector supplied to %s is too short. Must be at least %d bytes long; was introduced in 5.7.4.
    public final static int ER_FILE_CORRUPT = 1883; //SQLSTATE: HY000 Message: File %s is corrupted
    @Deprecated
    public final static int ER_ERROR_ON_MASTER = 1884;
    public final static int ER_ERROR_ON_SOURCE = 1884; //SQLSTATE: HY000 Message: Query partially completed on the source (error on source: %d) and was aborted. There is a chance that your source is inconsistent at this point. If you are sure that your source is ok, run this query manually on the replica and then restart the replica with SET GLOBAL SQL_REPLICA_SKIP_COUNTER=1; START REPLICA;. Query:'%s'
    public final static int ER_INCONSISTENT_ERROR = 1885; //SQLSTATE: HY000 Message: Query caused different errors on source and replica. Error on source: message (format)='%s' error code=%d; Error on replica:actual message='%s', error code=%d. Default database:'%s'. Query:'%s'
    public final static int ER_STORAGE_ENGINE_NOT_LOADED = 1886; //SQLSTATE: HY000 Message: Storage engine for table '%s'.'%s' is not loaded.
    public final static int ER_GET_STACKED_DA_WITHOUT_ACTIVE_HANDLER = 1887; //SQLSTATE: 0Z002 Message: GET STACKED DIAGNOSTICS when handler not active
    public final static int ER_WARN_LEGACY_SYNTAX_CONVERTED = 1888; //SQLSTATE: HY000 Message: %s is no longer supported. The statement was converted to %s.
    public final static int ER_BINLOG_UNSAFE_FULLTEXT_PLUGIN = 1889; //SQLSTATE: HY000 Message: Statement is unsafe because it uses a fulltext parser plugin which may not return the same value on the replica.; was introduced in 5.7.1.
    public final static int ER_CANNOT_DISCARD_TEMPORARY_TABLE = 1890; //SQLSTATE: HY000 Message: Cannot DISCARD/IMPORT tablespace associated with temporary table; was introduced in 5.7.1.
    public final static int ER_FK_DEPTH_EXCEEDED = 1891; //SQLSTATE: HY000 Message: Foreign key cascade delete/update exceeds max depth of %d.; was introduced in 5.7.2.
    public final static int ER_COL_COUNT_DOESNT_MATCH_PLEASE_UPDATE_V2 = 1892; //SQLSTATE: HY000 Message: Column count of %s.%s is wrong. Expected %d, found %d. Created with MySQL %d, now running %d. Please use mysql_upgrade to fix this error.; was introduced in 5.7.2.
    public final static int ER_WARN_TRIGGER_DOESNT_HAVE_CREATED = 1893; //SQLSTATE: HY000 Message: Trigger %s.%s.%s does not have CREATED attribute.; was introduced in 5.7.2.
    public final static int ER_REFERENCED_TRG_DOES_NOT_EXIST = 1894; //SQLSTATE: HY000 Message: Referenced trigger '%s' for the given action time and event type does not exist.; was introduced in 5.7.2.
    public final static int ER_EXPLAIN_NOT_SUPPORTED = 1895; //SQLSTATE: HY000 Message: EXPLAIN FOR CONNECTION command is supported only for SELECT/UPDATE/INSERT/DELETE/REPLACE; was introduced in 5.7.2.
    public final static int ER_INVALID_FIELD_SIZE = 1896; //SQLSTATE: HY000 Message: Invalid size for column '%s'.; was introduced in 5.7.2.
    public final static int ER_MISSING_HA_CREATE_OPTION = 1897; //SQLSTATE: HY000 Message: Table storage engine '%s' found required create option missing; was introduced in 5.7.2.
    public final static int ER_ENGINE_OUT_OF_MEMORY = 1898; //SQLSTATE: HY000 Message: Out of memory in storage engine '%s'.; was introduced in 5.7.3.
    public final static int ER_PASSWORD_EXPIRE_ANONYMOUS_USER = 1899; //SQLSTATE: HY000 Message: The password for anonymous user cannot be expired.; was introduced in 5.7.3.
    @Deprecated
    public final static int ER_SLAVE_SQL_THREAD_MUST_STOP = 1900;
    public final static int ER_REPLICA_SQL_THREAD_MUST_STOP = 1900; //SQLSTATE: HY000 Message: This operation cannot be performed with a running replica sql thread; run STOP REPLICA SQL_THREAD first; was introduced in 5.7.3.
    public final static int ER_NO_FT_MATERIALIZED_SUBQUERY = 1901; //SQLSTATE: HY000 Message: Cannot create FULLTEXT index on materialized subquery; was introduced in 5.7.4.
    public final static int ER_INNODB_UNDO_LOG_FULL = 1902; //SQLSTATE: HY000 Message: Undo Log error: %s; was introduced in 5.7.4.
    public final static int ER_INVALID_ARGUMENT_FOR_LOGARITHM = 1903; //SQLSTATE: 2201E Message: Invalid argument for logarithm; was introduced in 5.7.4.
    @Deprecated
    public final static int ER_SLAVE_IO_THREAD_MUST_STOP = 1904;
    public final static int ER_REPLICA_IO_THREAD_MUST_STOP = 1904; //SQLSTATE: HY000 Message: This operation cannot be performed with a running replica io thread; run STOP REPLICA IO_THREAD first.; was introduced in 5.7.4.
    public final static int ER_WARN_OPEN_TEMP_TABLES_MUST_BE_ZERO = 1905; //SQLSTATE: HY000 Message: This operation may not be safe when the replica has temporary tables. The tables will be kept open until the server restarts or until the tables are deleted by any replicated DROP statement. Suggest to wait until replica_open_temp_tables = 0.; was introduced in 5.7.4.
    @Deprecated
    public final static int ER_WARN_ONLY_MASTER_LOG_FILE_NO_POS = 1906;
    public final static int ER_WARN_ONLY_SOURCE_LOG_FILE_NO_POS = 1906; //SQLSTATE: HY000 Message: CHANGE SOURCE TO with a SOURCE_LOG_FILE clause but no SOURCE_LOG_POS clause may not be safe. The old position value may not be valid for the new binary log file.; was introduced in 5.7.4.
    public final static int ER_QUERY_TIMEOUT = 1907; //SQLSTATE: HY000 Message: Query execution was interrupted, max_statement_time exceeded; was introduced in 5.7.4.
    public final static int ER_NON_RO_SELECT_DISABLE_TIMER = 1908; //SQLSTATE: HY000 Message: Select is not a read only statement, disabling timer; was introduced in 5.7.4.
    public final static int ER_DUP_LIST_ENTRY = 1909; //SQLSTATE: HY000 Message: Duplicate entry '%s'.; was introduced in 5.7.4.
    public final static int ER_SQL_MODE_NO_EFFECT = 1910; //SQLSTATE: HY000 Message: '%s' mode no longer has any effect. Use STRICT_ALL_TABLES or STRICT_TRANS_TABLES instead.; was introduced in 5.7.4.

    public static final int ER_SESSION_WAS_KILLED = 3169;

    public final static int ER_CLIENT_INTERACTION_TIMEOUT = 4031; //SQLSTATE: HY000 Message: The client was disconnected by the server because of inactivity. See wait_timeout and interactive_timeout for configuring this behavior. Was added in 8.0.24.

    public static final int ER_X_BAD_MESSAGE = 5000;
    public static final int ER_X_CAPABILITIES_PREPARE_FAILED = 5001;
    public static final int ER_X_CAPABILITY_NOT_FOUND = 5002;
    public static final int ER_X_INVALID_PROTOCOL_DATA = 5003;
    public static final int ER_X_BAD_CONNECTION_SESSION_ATTRIBUTE_VALUE_LENGTH = 5004;
    public static final int ER_X_BAD_CONNECTION_SESSION_ATTRIBUTE_KEY_LENGTH = 5005;
    public static final int ER_X_BAD_CONNECTION_SESSION_ATTRIBUTE_EMPTY_KEY = 5006;
    public static final int ER_X_BAD_CONNECTION_SESSION_ATTRIBUTE_LENGTH = 5007;
    public static final int ER_X_BAD_CONNECTION_SESSION_ATTRIBUTE_TYPE = 5008;
    public static final int ER_X_CAPABILITY_SET_NOT_ALLOWED = 5009;
    public static final int ER_X_SERVICE_ERROR = 5010;
    public static final int ER_X_SESSION = 5011;
    public static final int ER_X_INVALID_ARGUMENT = 5012;
    public static final int ER_X_MISSING_ARGUMENT = 5013;
    public static final int ER_X_BAD_INSERT_DATA = 5014;
    public static final int ER_X_CMD_NUM_ARGUMENTS = 5015;
    public static final int ER_X_CMD_ARGUMENT_TYPE = 5016;
    public static final int ER_X_CMD_ARGUMENT_VALUE = 5017;
    public static final int ER_X_BAD_UPSERT_DATA = 5018;
    public static final int ER_X_DUPLICATED_CAPABILITIES = 5019;
    public static final int ER_X_CMD_ARGUMENT_OBJECT_EMPTY = 5020;
    public static final int ER_X_CMD_INVALID_ARGUMENT = 5021;
    public static final int ER_X_BAD_UPDATE_DATA = 5050;
    public static final int ER_X_BAD_TYPE_OF_UPDATE = 5051;
    public static final int ER_X_BAD_COLUMN_TO_UPDATE = 5052;
    public static final int ER_X_BAD_MEMBER_TO_UPDATE = 5053;
    public static final int ER_X_BAD_STATEMENT_ID = 5110;
    public static final int ER_X_BAD_CURSOR_ID = 5111;
    public static final int ER_X_BAD_SCHEMA = 5112;
    public static final int ER_X_BAD_TABLE = 5113;
    public static final int ER_X_BAD_PROJECTION = 5114;
    public static final int ER_X_DOC_ID_MISSING = 5115;
    public static final int ER_X_DUPLICATE_ENTRY = 5116;
    public static final int ER_X_DOC_REQUIRED_FIELD_MISSING = 5117;
    public static final int ER_X_PROJ_BAD_KEY_NAME = 5120;
    public static final int ER_X_BAD_DOC_PATH = 5121;
    public static final int ER_X_CURSOR_EXISTS = 5122;
    public static final int ER_X_PREPARED_STATMENT_CAN_HAVE_ONE_CURSOR = 5131;
    public static final int ER_X_PREPARED_EXECUTE_ARGUMENT_NOT_SUPPORTED = 5133;
    public static final int ER_X_PREPARED_EXECUTE_ARGUMENT_CONSISTENCY = 5134;
    public static final int ER_X_CURSOR_REACHED_EOF = 5123;
    public static final int ER_X_EXPR_BAD_OPERATOR = 5150;
    public static final int ER_X_EXPR_BAD_NUM_ARGS = 5151;
    public static final int ER_X_EXPR_MISSING_ARG = 5152;
    public static final int ER_X_EXPR_BAD_TYPE_VALUE = 5153;
    public static final int ER_X_EXPR_BAD_VALUE = 5154;
    public static final int ER_X_EXPR_BAD_REGEX = 5155;
    public static final int ER_X_INVALID_COLLECTION = 5156;
    public static final int ER_X_INVALID_ADMIN_COMMAND = 5157;
    public static final int ER_X_EXPECT_NOT_OPEN = 5158;
    public static final int ER_X_EXPECT_NO_ERROR_FAILED = 5159;
    public static final int ER_X_EXPECT_BAD_CONDITION = 5160;
    public static final int ER_X_EXPECT_BAD_CONDITION_VALUE = 5161;
    public static final int ER_X_INVALID_NAMESPACE = 5162;
    public static final int ER_X_BAD_NOTICE = 5163;
    public static final int ER_X_CANNOT_DISABLE_NOTICE = 5164;
    public static final int ER_X_BAD_CONFIGURATION = 5165;
    public static final int ER_X_MYSQLX_ACCOUNT_MISSING_PERMISSIONS = 5167;
    public static final int ER_X_EXPECT_FIELD_EXISTS_FAILED = 5168;
    public static final int ER_X_BAD_LOCKING = 5169;
    public static final int ER_X_FRAME_COMPRESSION_DISABLED = 5170;
    public static final int ER_X_DECOMPRESSION_FAILED = 5171;
    public static final int ER_X_BAD_COMPRESSED_FRAME = 5174;
    public static final int ER_X_CAPABILITY_COMPRESSION_INVALID_ALGORITHM = 5175;
    public static final int ER_X_CAPABILITY_COMPRESSION_INVALID_SERVER_STYLE = 5176;
    public static final int ER_X_CAPABILITY_COMPRESSION_INVALID_CLIENT_STYLE = 5177;
    public static final int ER_X_CAPABILITY_COMPRESSION_INVALID_OPTION = 5178;
    public static final int ER_X_CAPABILITY_COMPRESSION_MISSING_REQUIRED_FIELDS = 5179;
    public static final int ER_X_DOCUMENT_DOESNT_MATCH_EXPECTED_SCHEMA = 5180;
    public static final int ER_X_COLLECTION_OPTION_DOESNT_EXISTS = 5181;
    public static final int ER_X_INVALID_VALIDATION_SCHEMA = 5182;

    // Connector/J-specific errors outside the space of server errors.
    public static final int ERROR_CODE_NULL_LOAD_BALANCED_CONNECTION = 1000001;
    public static final int ERROR_CODE_REPLICATION_CONNECTION_WITH_NO_HOSTS = 1000002;

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

    public static final String SQL_STATE_BAD_SSL_PARAMS = "08000";

    private static Map<String, String> sqlStateMessages;

    public static Map<Integer, String> mysqlToSql99State;

    static {

        sqlStateMessages = new HashMap<>();
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

        mysqlToSql99State = new HashMap<>();

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
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SOURCE_NET_READ, SQL_STATE_COMMUNICATION_LINK_FAILURE);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_SOURCE_NET_WRITE, SQL_STATE_COMMUNICATION_LINK_FAILURE);
        mysqlToSql99State.put(MysqlErrorNumbers.ER_CONNECT_TO_SOURCE, SQL_STATE_COMMUNICATION_LINK_FAILURE);
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

    public static String get(String stateCode) {
        return sqlStateMessages.get(stateCode);
    }

    public static String mysqlToSql99(int errno) {
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

    private MysqlErrorNumbers() {
        // prevent instantiation
    }

}
