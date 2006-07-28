/*
 Copyright (C) 2002-2004 MySQL AB

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

/**
 * Constants representing MySQL error numbers returned by the server in error
 * messages.
 * 
 * @author Mark Matthews
 * 
 * @version $Id: MysqlErrorNumbers.java,v 1.1.2.1 2005/05/13 18:58:38 mmatthews
 *          Exp $
 */
public final class MysqlErrorNumbers {

	public final static int ER_ABORTING_CONNECTION = 1152;

	public final static int ER_ACCESS_DENIED_ERROR = 1045;

	public final static int ER_ALTER_INFO = 1088;

	public final static int ER_AUTO_CONVERT = 1246;

	public final static int ER_BAD_DB_ERROR = 1049;

	public final static int ER_BAD_FIELD_ERROR = 1054;

	public final static int ER_BAD_FT_COLUMN = 1283;

	public final static int ER_BAD_HOST_ERROR = 1042;

	public final static int ER_BAD_NULL_ERROR = 1048;

	public final static int ER_BAD_SLAVE = 1200;

	public final static int ER_BAD_SLAVE_UNTIL_COND = 1277;

	public final static int ER_BAD_TABLE_ERROR = 1051;

	public final static int ER_BLOB_CANT_HAVE_DEFAULT = 1101;

	public final static int ER_BLOB_KEY_WITHOUT_LENGTH = 1170;

	public final static int ER_BLOB_USED_AS_KEY = 1073;

	public final static int ER_BLOBS_AND_NO_TERMINATED = 1084;

	public final static int ER_CANNOT_ADD_FOREIGN = 1215;

	public final static int ER_CANT_AGGREGATE_2COLLATIONS = 1267;

	public final static int ER_CANT_AGGREGATE_3COLLATIONS = 1270;

	public final static int ER_CANT_AGGREGATE_NCOLLATIONS = 1271;

	public final static int ER_CANT_CREATE_DB = 1006;

	public final static int ER_CANT_CREATE_FILE = 1004;

	public final static int ER_CANT_CREATE_TABLE = 1005;

	public final static int ER_CANT_CREATE_THREAD = 1135;

	public final static int ER_CANT_DELETE_FILE = 1011;

	public final static int ER_CANT_DO_THIS_DURING_AN_TRANSACTION = 1179;

	public final static int ER_CANT_DROP_FIELD_OR_KEY = 1091;

	public final static int ER_CANT_FIND_DL_ENTRY = 1127;

	public final static int ER_CANT_FIND_SYSTEM_REC = 1012;

	public final static int ER_CANT_FIND_UDF = 1122;

	public final static int ER_CANT_GET_STAT = 1013;

	public final static int ER_CANT_GET_WD = 1014;

	public final static int ER_CANT_INITIALIZE_UDF = 1123;

	public final static int ER_CANT_LOCK = 1015;

	public final static int ER_CANT_OPEN_FILE = 1016;

	public final static int ER_CANT_OPEN_LIBRARY = 1126;

	public final static int ER_CANT_READ_DIR = 1018;

	public final static int ER_CANT_REMOVE_ALL_FIELDS = 1090;

	public final static int ER_CANT_REOPEN_TABLE = 1137;

	public final static int ER_CANT_SET_WD = 1019;

	public final static int ER_CANT_UPDATE_WITH_READLOCK = 1223;

	public final static int ER_CANT_USE_OPTION_HERE = 1234;

	public final static int ER_CHECK_NO_SUCH_TABLE = 1177;

	public final static int ER_CHECK_NOT_IMPLEMENTED = 1178;

	public final static int ER_CHECKREAD = 1020;

	public final static int ER_COLLATION_CHARSET_MISMATCH = 1253;

	public final static int ER_COLUMNACCESS_DENIED_ERROR = 1143;

	public final static int ER_CON_COUNT_ERROR = 1040;

	public final static int ER_CONNECT_TO_MASTER = 1218;

	public final static int ER_CORRUPT_HELP_DB = 1244;

	public final static int ER_CRASHED_ON_REPAIR = 1195;

	public final static int ER_CRASHED_ON_USAGE = 1194;

	public final static int ER_CREATE_DB_WITH_READ_LOCK = 1209;

	public final static int ER_CUT_VALUE_GROUP_CONCAT = 1260;

	public final static int ER_CYCLIC_REFERENCE = 1245;

	public final static int ER_DB_CREATE_EXISTS = 1007;

	public final static int ER_DB_DROP_DELETE = 1009;

	public final static int ER_DB_DROP_EXISTS = 1008;

	public final static int ER_DB_DROP_RMDIR = 1010;

	public final static int ER_DBACCESS_DENIED_ERROR = 1044;

	public final static int ER_DELAYED_CANT_CHANGE_LOCK = 1150;

	public final static int ER_DELAYED_INSERT_TABLE_LOCKED = 1165;

	public final static int ER_DERIVED_MUST_HAVE_ALIAS = 1248;

	public final static int ER_DISK_FULL = 1021;

	public final static int ER_DROP_DB_WITH_READ_LOCK = 1208;

	public final static int ER_DROP_USER = 1268;

	public final static int ER_DUMP_NOT_IMPLEMENTED = 1185;

	public final static int ER_DUP_ARGUMENT = 1225;

	public final static int ER_DUP_ENTRY = 1062;

	public final static int ER_DUP_FIELDNAME = 1060;

	public final static int ER_DUP_KEY = 1022;

	public final static int ER_DUP_KEYNAME = 1061;

	public final static int ER_DUP_UNIQUE = 1169;

	public final static int ER_DUPLICATED_VALUE_IN_TYPE = 1291;

	public final static int ER_EMPTY_QUERY = 1065;

	public final static int ER_ERROR_DURING_CHECKPOINT = 1183;

	public final static int ER_ERROR_DURING_COMMIT = 1180;

	public final static int ER_ERROR_DURING_FLUSH_LOGS = 1182;

	public final static int ER_ERROR_DURING_ROLLBACK = 1181;

	public final static int ER_ERROR_MESSAGES = 298;

	public final static int ER_ERROR_ON_CLOSE = 1023;

	public final static int ER_ERROR_ON_READ = 1024;

	public final static int ER_ERROR_ON_RENAME = 1025;

	public final static int ER_ERROR_ON_WRITE = 1026;

	public final static int ER_ERROR_WHEN_EXECUTING_COMMAND = 1220;

	public final static int ER_FEATURE_DISABLED = 1289;

	public final static int ER_FIELD_SPECIFIED_TWICE = 1110;

	public final static int ER_FILE_EXISTS_ERROR = 1086;

	public final static int ER_FILE_NOT_FOUND = 1017;

	public final static int ER_FILE_USED = 1027;

	public final static int ER_FILSORT_ABORT = 1028;

	public final static int ER_FLUSH_MASTER_BINLOG_CLOSED = 1186;

	public final static int ER_FORCING_CLOSE = 1080;

	public final static int ER_FORM_NOT_FOUND = 1029;

	public final static int ER_FT_MATCHING_KEY_NOT_FOUND = 1191;

	public final static int ER_FUNCTION_NOT_DEFINED = 1128;

	public final static int ER_GET_ERRMSG = 1296;

	public final static int ER_GET_ERRNO = 1030;

	public final static int ER_GET_TEMPORARY_ERRMSG = 1297;

	public final static int ER_GLOBAL_VARIABLE = 1229;

	public final static int ER_GOT_SIGNAL = 1078;

	public final static int ER_GRANT_WRONG_HOST_OR_USER = 1145;

	public final static int ER_HANDSHAKE_ERROR = 1043;

	public final static int ER_HASHCHK = 1000;

	public final static int ER_HOST_IS_BLOCKED = 1129;

	public final static int ER_HOST_NOT_PRIVILEGED = 1130;

	public final static int ER_ILLEGAL_GRANT_FOR_TABLE = 1144;

	public final static int ER_ILLEGAL_HA = 1031;

	public final static int ER_ILLEGAL_REFERENCE = 1247;

	public final static int ER_INCORRECT_GLOBAL_LOCAL_VAR = 1238;

	public final static int ER_INDEX_REBUILD = 1187;

	public final static int ER_INSERT_INFO = 1092;

	public final static int ER_INVALID_DEFAULT = 1067;

	public final static int ER_INVALID_GROUP_FUNC_USE = 1111;

	public final static int ER_INVALID_ON_UPDATE = 1294;

	public final static int ER_INVALID_USE_OF_NULL = 1138;

	public final static int ER_IPSOCK_ERROR = 1081;

	public final static int ER_KEY_COLUMN_DOES_NOT_EXITS = 1072;

	public final static int ER_KEY_DOES_NOT_EXITS = 1176;

	public final static int ER_KEY_NOT_FOUND = 1032;

	public final static int ER_KEY_REF_DO_NOT_MATCH_TABLE_REF = 1240;

	public final static int ER_KILL_DENIED_ERROR = 1095;

	public final static int ER_LOAD_INFO = 1087;

	public final static int ER_LOCAL_VARIABLE = 1228;

	public final static int ER_LOCK_DEADLOCK = 1213;

	public final static int ER_LOCK_OR_ACTIVE_TRANSACTION = 1192;

	public final static int ER_LOCK_TABLE_FULL = 1206;

	public final static int ER_LOCK_WAIT_TIMEOUT = 1205;

	public final static int ER_MASTER = 1188;

	public final static int ER_MASTER_FATAL_ERROR_READING_BINLOG = 1236;

	public final static int ER_MASTER_INFO = 1201;

	public final static int ER_MASTER_NET_READ = 1189;

	public final static int ER_MASTER_NET_WRITE = 1190;

	public final static int ER_MISSING_SKIP_SLAVE = 1278;

	public final static int ER_MIX_OF_GROUP_FUNC_AND_FIELDS = 1140;

	public final static int ER_MIXING_NOT_ALLOWED = 1224;

	public final static int ER_MULTIPLE_PRI_KEY = 1068;

	public final static int ER_NET_ERROR_ON_WRITE = 1160;

	public final static int ER_NET_FCNTL_ERROR = 1155;

	public final static int ER_NET_PACKET_TOO_LARGE = 1153;

	public final static int ER_NET_PACKETS_OUT_OF_ORDER = 1156;

	public final static int ER_NET_READ_ERROR = 1158;

	public final static int ER_NET_READ_ERROR_FROM_PIPE = 1154;

	public final static int ER_NET_READ_INTERRUPTED = 1159;

	public final static int ER_NET_UNCOMPRESS_ERROR = 1157;

	public final static int ER_NET_WRITE_INTERRUPTED = 1161;

	public final static int ER_NEW_ABORTING_CONNECTION = 1184;

	public final static int ER_NISAMCHK = 1001;

	public final static int ER_NO = 1002;

	public final static int ER_NO_DB_ERROR = 1046;

	public final static int ER_NO_DEFAULT = 1230;

	public final static int ER_NO_PERMISSION_TO_CREATE_USER = 1211;

	public final static int ER_NO_RAID_COMPILED = 1174;

	public final static int ER_NO_REFERENCED_ROW = 1216;

	public final static int ER_NO_SUCH_INDEX = 1082;

	public final static int ER_NO_SUCH_TABLE = 1146;

	public final static int ER_NO_SUCH_THREAD = 1094;

	public final static int ER_NO_TABLES_USED = 1096;

	public final static int ER_NO_UNIQUE_LOGFILE = 1098;

	public final static int ER_NON_UNIQ_ERROR = 1052;

	public final static int ER_NON_UPDATABLE_TABLE = 1288;

	public final static int ER_NONEXISTING_GRANT = 1141;

	public final static int ER_NONEXISTING_TABLE_GRANT = 1147;

	public final static int ER_NONUNIQ_TABLE = 1066;

	public final static int ER_NORMAL_SHUTDOWN = 1077;

	public final static int ER_NOT_ALLOWED_COMMAND = 1148;

	public final static int ER_NOT_FORM_FILE = 1033;

	public final static int ER_NOT_KEYFILE = 1034;

	public final static int ER_NOT_SUPPORTED_AUTH_MODE = 1251;

	public final static int ER_NOT_SUPPORTED_YET = 1235;

	public final static int ER_NULL_COLUMN_IN_INDEX = 1121;

	public final static int ER_OLD_KEYFILE = 1035;

	public final static int ER_OPEN_AS_READONLY = 1036;

	public final static int ER_OPERAND_COLUMNS = 1241;

	public final static int ER_OPTION_PREVENTS_STATEMENT = 1290;

	public final static int ER_OUT_OF_RESOURCES = 1041;

	public final static int ER_OUT_OF_SORTMEMORY = 1038;

	public final static int ER_OUTOFMEMORY = 1037;

	public final static int ER_PARSE_ERROR = 1064;

	public final static int ER_PASSWORD_ANONYMOUS_USER = 1131;

	public final static int ER_PASSWORD_NO_MATCH = 1133;

	public final static int ER_PASSWORD_NOT_ALLOWED = 1132;

	public final static int ER_PRIMARY_CANT_HAVE_NULL = 1171;

	public final static int ER_QUERY_ON_MASTER = 1219;

	public final static int ER_READ_ONLY_TRANSACTION = 1207;

	public final static int ER_READY = 1076;

	public final static int ER_RECORD_FILE_FULL = 1114;

	public final static int ER_REGEXP_ERROR = 1139;

	public final static int ER_REQUIRES_PRIMARY_KEY = 1173;

	public final static int ER_REVOKE_GRANTS = 1269;

	public final static int ER_ROW_IS_REFERENCED = 1217;

	public final static int ER_SELECT_REDUCED = 1249;

	public final static int ER_SERVER_IS_IN_SECURE_AUTH_MODE = 1275;

	public final static int ER_SERVER_SHUTDOWN = 1053;

	public final static int ER_SET_CONSTANTS_ONLY = 1204;

	public final static int ER_SHUTDOWN_COMPLETE = 1079;

	public final static int ER_SLAVE_IGNORED_SSL_PARAMS = 1274;

	public final static int ER_SLAVE_IGNORED_TABLE = 1237;

	public final static int ER_SLAVE_MUST_STOP = 1198;

	public final static int ER_SLAVE_NOT_RUNNING = 1199;

	public final static int ER_SLAVE_THREAD = 1202;

	public final static int ER_SLAVE_WAS_NOT_RUNNING = 1255;

	public final static int ER_SLAVE_WAS_RUNNING = 1254;

	public final static int ER_SPATIAL_CANT_HAVE_NULL = 1252;

	public final static int ER_SPECIFIC_ACCESS_DENIED_ERROR = 1227;

	public final static int ER_STACK_OVERRUN = 1119;

	public final static int ER_SUBQUERY_NO_1_ROW = 1242;

	public final static int ER_SYNTAX_ERROR = 1149;

	public final static int ER_TABLE_CANT_HANDLE_AUTO_INCREMENT = 1164;

	public final static int ER_TABLE_CANT_HANDLE_BLOB = 1163;

	public final static int ER_TABLE_CANT_HANDLE_FT = 1214;

	public final static int ER_TABLE_EXISTS_ERROR = 1050;

	public final static int ER_TABLE_MUST_HAVE_COLUMNS = 1113;

	public final static int ER_TABLE_NOT_LOCKED = 1100;

	public final static int ER_TABLE_NOT_LOCKED_FOR_WRITE = 1099;

	public final static int ER_TABLEACCESS_DENIED_ERROR = 1142;

	public final static int ER_TABLENAME_NOT_ALLOWED_HERE = 1250;

	public final static int ER_TEXTFILE_NOT_READABLE = 1085;

	public final static int ER_TOO_BIG_FIELDLENGTH = 1074;

	public final static int ER_TOO_BIG_FOR_UNCOMPRESS = 1256;

	public final static int ER_TOO_BIG_ROWSIZE = 1118;

	public final static int ER_TOO_BIG_SELECT = 1104;

	public final static int ER_TOO_BIG_SET = 1097;

	public final static int ER_TOO_LONG_IDENT = 1059;

	public final static int ER_TOO_LONG_KEY = 1071;

	public final static int ER_TOO_LONG_STRING = 1162;

	public final static int ER_TOO_MANY_DELAYED_THREADS = 1151;

	public final static int ER_TOO_MANY_FIELDS = 1117;

	public final static int ER_TOO_MANY_KEY_PARTS = 1070;

	public final static int ER_TOO_MANY_KEYS = 1069;

	public final static int ER_TOO_MANY_ROWS = 1172;

	public final static int ER_TOO_MANY_TABLES = 1116;

	public final static int ER_TOO_MANY_USER_CONNECTIONS = 1203;

	public final static int ER_TOO_MUCH_AUTO_TIMESTAMP_COLS = 1293;

	public final static int ER_TRANS_CACHE_FULL = 1197;

	public final static int ER_TRUNCATED_WRONG_VALUE = 1292;

	public final static int ER_UDF_EXISTS = 1125;

	public final static int ER_UDF_NO_PATHS = 1124;

	public final static int ER_UNEXPECTED_EOF = 1039;

	public final static int ER_UNION_TABLES_IN_DIFFERENT_DIR = 1212;

	public final static int ER_UNKNOWN_CHARACTER_SET = 1115;

	public final static int ER_UNKNOWN_COLLATION = 1273;

	public final static int ER_UNKNOWN_COM_ERROR = 1047;

	public final static int ER_UNKNOWN_ERROR = 1105;

	public final static int ER_UNKNOWN_KEY_CACHE = 1284;

	public final static int ER_UNKNOWN_PROCEDURE = 1106;

	public final static int ER_UNKNOWN_STMT_HANDLER = 1243;

	public final static int ER_UNKNOWN_STORAGE_ENGINE = 1286;

	public final static int ER_UNKNOWN_SYSTEM_VARIABLE = 1193;

	public final static int ER_UNKNOWN_TABLE = 1109;

	public final static int ER_UNSUPPORTED_EXTENSION = 1112;

	public final static int ER_UNSUPPORTED_PS = 1295;

	public final static int ER_UNTIL_COND_IGNORED = 1279;

	public final static int ER_UPDATE_INFO = 1134;

	public final static int ER_UPDATE_TABLE_USED = 1093;

	public final static int ER_UPDATE_WITHOUT_KEY_IN_SAFE_MODE = 1175;

	public final static int ER_USER_LIMIT_REACHED = 1226;

	public final static int ER_VAR_CANT_BE_READ = 1233;

	public final static int ER_VARIABLE_IS_NOT_STRUCT = 1272;

	public final static int ER_WARN_DATA_OUT_OF_RANGE = 1264;

	public final static int ER_WARN_DATA_TRUNCATED = 1265;

	public final static int ER_WARN_DEPRECATED_SYNTAX = 1287;

	public final static int ER_WARN_FIELD_RESOLVED = 1276;

	public final static int ER_WARN_HOSTNAME_WONT_WORK = 1285;

	public final static int ER_WARN_NULL_TO_NOTNULL = 1263;

	public final static int ER_WARN_QC_RESIZE = 1282;

	public final static int ER_WARN_TOO_FEW_RECORDS = 1261;

	public final static int ER_WARN_TOO_MANY_RECORDS = 1262;

	public final static int ER_WARN_USING_OTHER_HANDLER = 1266;

	public final static int ER_WARNING_NOT_COMPLETE_ROLLBACK = 1196;

	public final static int ER_WRONG_ARGUMENTS = 1210;

	public final static int ER_WRONG_AUTO_KEY = 1075;

	public final static int ER_WRONG_COLUMN_NAME = 1166;

	public final static int ER_WRONG_DB_NAME = 1102;

	public final static int ER_WRONG_FIELD_SPEC = 1063;

	public final static int ER_WRONG_FIELD_TERMINATORS = 1083;

	public final static int ER_WRONG_FIELD_WITH_GROUP = 1055;

	public final static int ER_WRONG_FK_DEF = 1239;

	public final static int ER_WRONG_GROUP_FIELD = 1056;

	public final static int ER_WRONG_KEY_COLUMN = 1167;

	public final static int ER_WRONG_MRG_TABLE = 1168;

	public final static int ER_WRONG_NAME_FOR_CATALOG = 1281;

	public final static int ER_WRONG_NAME_FOR_INDEX = 1280;

	public final static int ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT = 1222;

	public final static int ER_WRONG_OUTER_JOIN = 1120;

	public final static int ER_WRONG_PARAMCOUNT_TO_PROCEDURE = 1107;

	public final static int ER_WRONG_PARAMETERS_TO_PROCEDURE = 1108;

	public final static int ER_WRONG_SUB_KEY = 1089;

	public final static int ER_WRONG_SUM_SELECT = 1057;

	public final static int ER_WRONG_TABLE_NAME = 1103;

	public final static int ER_WRONG_TYPE_FOR_VAR = 1232;

	public final static int ER_WRONG_USAGE = 1221;

	public final static int ER_WRONG_VALUE_COUNT = 1058;

	public final static int ER_WRONG_VALUE_COUNT_ON_ROW = 1136;

	public final static int ER_WRONG_VALUE_FOR_VAR = 1231;
	
	public final static int ER_XA_RMERR = 1401;

	public final static int ER_YES = 1003;

	public final static int ER_ZLIB_Z_BUF_ERROR = 1258;

	public final static int ER_ZLIB_Z_DATA_ERROR = 1259;

	public final static int ER_ZLIB_Z_MEM_ERROR = 1257;

	private MysqlErrorNumbers() {
		// prevent instantiation
	}
}
