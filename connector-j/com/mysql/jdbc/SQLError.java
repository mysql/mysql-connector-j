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
package com.mysql.jdbc;

import java.util.Hashtable;


/**
 * SQLError is a utility class that maps MySQL error codes to X/Open
 * error codes as is required by the JDBC spec.
 *
 * @author Mark Matthews <mmatthew@worldserver.com>
 * @version $Id$
 */
class SQLError
{

    //~ Instance/static variables .............................................

    static Hashtable Map;
    static Hashtable Msg;

    //~ Initializers ..........................................................

    static {
        Msg = new Hashtable();
        Msg.put("01002", "Disconnect error");
        Msg.put("01004", "Data truncated");
        Msg.put("01006", "Privilege not revoked");
        Msg.put("01S00", "Invalid connection string attribute");
        Msg.put("01S01", "Error in row");
        Msg.put("01S03", "No rows updated or deleted");
        Msg.put("01S04", "More than one row updated or deleted");
        Msg.put("07001", "Wrong number of parameters");
        Msg.put("08001", "Unable to connect to data source");
        Msg.put("08002", "Connection in use");
        Msg.put("08003", "Connection not open");
        Msg.put("08004", "Data source rejected establishment of connection");
        Msg.put("08007", "Connection failure during transaction");
        Msg.put("08S01", "Communication link failure");
        Msg.put("21S01", "Insert value list does not match column list");
        Msg.put("22003", "Numeric value out of range");
        Msg.put("22005", "Numeric value out of range");
        Msg.put("22008", "Datetime field overflow");
        Msg.put("22012", "Division by zero");
        Msg.put("28000", "Invalid authorization specification");
        Msg.put("42000", "Syntax error or access violation");
        Msg.put("S0001", "Base table or view already exists");
        Msg.put("S0002", "Base table not found");
        Msg.put("S0011", "Index already exists");
        Msg.put("S0012", "Index not found");
        Msg.put("S0021", "Column already exists");
        Msg.put("S0022", "Column not found");
        Msg.put("S0023", "No default for column");
        Msg.put("S1000", "General error");
        Msg.put("S1001", "Memory allocation failure");
        Msg.put("S1002", "Invalid column number");
        Msg.put("S1009", "Invalid argument value");
        Msg.put("S1C00", "Driver not capable");
        Msg.put("S1T00", "Timeout expired");

        //
        // Map MySQL error codes to X/Open error codes
        //
        Map = new Hashtable();

        //
        // Communications Errors
        //
        // ER_BAD_HOST_ERROR 1042
        // ER_HANDSHAKE_ERROR 1043
        // ER_UNKNOWN_COM_ERROR 1047
        // ER_IPSOCK_ERROR 1081
        //
        Map.put(new Integer(1042), "08S01");
        Map.put(new Integer(1043), "08S01");
        Map.put(new Integer(1047), "08S01");
        Map.put(new Integer(1081), "08S01");

        //
        // Authentication Errors
        //
        // ER_ACCESS_DENIED_ERROR 1045
        //
        Map.put(new Integer(1045), "28000");

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
        Map.put(new Integer(1037), "S1001");
        Map.put(new Integer(1038), "S1001");

        //
        // Syntax Errors
        //
        // ER_PARSE_ERROR 1064
        // ER_EMPTY_QUERY 1065
        //
        Map.put(new Integer(1064), "42000");
        Map.put(new Integer(1065), "42000");

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
        Map.put(new Integer(1055), "S1009");
        Map.put(new Integer(1056), "S1009");
        Map.put(new Integer(1057), "S1009");
        Map.put(new Integer(1059), "S1009");
        Map.put(new Integer(1060), "S1009");
        Map.put(new Integer(1061), "S1009");
        Map.put(new Integer(1062), "S1009");
        Map.put(new Integer(1063), "S1009");
        Map.put(new Integer(1066), "S1009");
        Map.put(new Integer(1067), "S1009");
        Map.put(new Integer(1068), "S1009");
        Map.put(new Integer(1069), "S1009");
        Map.put(new Integer(1070), "S1009");
        Map.put(new Integer(1071), "S1009");
        Map.put(new Integer(1072), "S1009");
        Map.put(new Integer(1073), "S1009");
        Map.put(new Integer(1074), "S1009");
        Map.put(new Integer(1075), "S1009");
        Map.put(new Integer(1082), "S1009");
        Map.put(new Integer(1083), "S1009");
        Map.put(new Integer(1084), "S1009");

        //
        // ER_WRONG_VALUE_COUNT 1058
        //
        Map.put(new Integer(1058), "21S01");

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
        // ER_NON_UNIQ_ERROR 1052
        // ER_BAD_FIELD_ERROR 1054
        Map.put(new Integer(1054), "S0022");

        // ER_TEXTFILE_NOT_READABLE 1085
        // ER_FILE_EXISTS_ERROR 1086
        // ER_LOAD_INFO 1087
        // ER_ALTER_INFO 1088
        // ER_WRONG_SUB_KEY 1089
        // ER_CANT_REMOVE_ALL_FIELDS 1090
        // ER_CANT_DROP_FIELD_OR_KEY 1091
        // ER_INSERT_INFO 1092
        // ER_INSERT_TABLE_USED 1093
    }

    //~ Methods ...............................................................

    static String get(String StateCode)
    {

        return (String)Msg.get(StateCode);
    }

    /**
   * Map MySQL error codes to X/Open error codes
   *
   * @param errno the MySQL error code
   * @return the corresponding X/Open error code
   */
    static String mysqlToXOpen(int errno)
    {

        Integer Err = new Integer(errno);

        if (Map.containsKey(Err)) {

            return (String)Map.get(Err);
        }
         else {

            return "S1000";
        }
    }
}