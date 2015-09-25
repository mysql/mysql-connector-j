/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.core;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import com.mysql.cj.core.util.StringUtils;

public enum MysqlType {

    /**
     * DECIMAL[(M[,D])] [UNSIGNED] [ZEROFILL]
     * A packed “exact” fixed-point number. M is the total number of digits (the precision) and D is the number of digits
     * after the decimal point (the scale). The decimal point and (for negative numbers) the “-” sign are not counted in M.
     * If D is 0, values have no decimal point or fractional part. The maximum number of digits (M) for DECIMAL is 65.
     * The maximum number of supported decimals (D) is 30. If D is omitted, the default is 0. If M is omitted, the default is 10.
     * 
     * Protocol: FIELD_TYPE_DECIMAL = 0
     * Protocol: FIELD_TYPE_NEWDECIMAL = 246
     *
     * These types are synonyms for DECIMAL:
     * DEC[(M[,D])] [UNSIGNED] [ZEROFILL],
     * NUMERIC[(M[,D])] [UNSIGNED] [ZEROFILL],
     * FIXED[(M[,D])] [UNSIGNED] [ZEROFILL]
     */
    DECIMAL("DECIMAL", Types.DECIMAL, BigDecimal.class, MysqlType.FIELD_FLAG_ZEROFILL, 0),
    //
    DECIMAL_UNSIGNED("DECIMAL", Types.DECIMAL, BigDecimal.class, MysqlType.FIELD_FLAG_UNSIGNED | MysqlType.FIELD_FLAG_ZEROFILL, 0),
    /**
     * TINYINT[(M)] [UNSIGNED] [ZEROFILL]
     * A very small integer. The signed range is -128 to 127. The unsigned range is 0 to 255.
     * 
     * Protocol: FIELD_TYPE_TINY = 1
     */
    TINYINT("TINYINT", Types.TINYINT, Integer.class, MysqlType.FIELD_FLAG_ZEROFILL, 0), // TODO is length=1 ?
    //
    TINYINT_UNSIGNED("TINYINT", Types.TINYINT, Integer.class, MysqlType.FIELD_FLAG_UNSIGNED | MysqlType.FIELD_FLAG_ZEROFILL, 0), // TODO is length=1 ?
    /**
     * BOOL, BOOLEAN
     * These types are synonyms for TINYINT(1). A value of zero is considered false. Nonzero values are considered true
     * 
     * Protocol: FIELD_TYPE_TINY = 1
     */
    BOOLEAN("BOOLEAN", Types.BOOLEAN, Boolean.class, 0, 0), // TODO is length=1 ?
    /**
     * SMALLINT[(M)] [UNSIGNED] [ZEROFILL]
     * A small integer. The signed range is -32768 to 32767. The unsigned range is 0 to 65535.
     * 
     * Protocol: FIELD_TYPE_SHORT = 2
     */
    SMALLINT("SMALLINT", Types.SMALLINT, Integer.class, MysqlType.FIELD_FLAG_ZEROFILL, 0),
    //
    SMALLINT_UNSIGNED("SMALLINT", Types.SMALLINT, Integer.class, MysqlType.FIELD_FLAG_UNSIGNED | MysqlType.FIELD_FLAG_ZEROFILL, 0),
    /**
     * INT[(M)] [UNSIGNED] [ZEROFILL]
     * A normal-size integer. The signed range is -2147483648 to 2147483647. The unsigned range is 0 to 4294967295.
     * 
     * Protocol: FIELD_TYPE_LONG = 3
     * 
     * INTEGER[(M)] [UNSIGNED] [ZEROFILL] is a synonym for INT.
     */
    INT("INT", Types.INTEGER, Integer.class, MysqlType.FIELD_FLAG_ZEROFILL, 0),
    //
    INT_UNSIGNED("INT", Types.INTEGER, Long.class, MysqlType.FIELD_FLAG_UNSIGNED | MysqlType.FIELD_FLAG_ZEROFILL, 0),
    /**
     * FLOAT[(M,D)] [UNSIGNED] [ZEROFILL]
     * A small (single-precision) floating-point number. Permissible values are -3.402823466E+38 to -1.175494351E-38, 0,
     * and 1.175494351E-38 to 3.402823466E+38. These are the theoretical limits, based on the IEEE standard. The actual
     * range might be slightly smaller depending on your hardware or operating system.
     * 
     * M is the total number of digits and D is the number of digits following the decimal point. If M and D are omitted,
     * values are stored to the limits permitted by the hardware. A single-precision floating-point number is accurate to
     * approximately 7 decimal places.
     * 
     * Protocol: FIELD_TYPE_FLOAT = 4
     * 
     * Additionally:
     * FLOAT(p) [UNSIGNED] [ZEROFILL]
     * A floating-point number. p represents the precision in bits, but MySQL uses this value only to determine whether
     * to use FLOAT or DOUBLE for the resulting data type. If p is from 0 to 24, the data type becomes FLOAT with no M or D values.
     * If p is from 25 to 53, the data type becomes DOUBLE with no M or D values. The range of the resulting column is the same as
     * for the single-precision FLOAT or double-precision DOUBLE data types.
     */
    FLOAT("FLOAT", Types.REAL, Float.class, MysqlType.FIELD_FLAG_ZEROFILL, 0),
    //
    FLOAT_UNSIGNED("FLOAT", Types.REAL, Float.class, MysqlType.FIELD_FLAG_UNSIGNED | MysqlType.FIELD_FLAG_ZEROFILL, 0),
    /**
     * DOUBLE[(M,D)] [UNSIGNED] [ZEROFILL]
     * A normal-size (double-precision) floating-point number. Permissible values are -1.7976931348623157E+308 to
     * -2.2250738585072014E-308, 0, and 2.2250738585072014E-308 to 1.7976931348623157E+308. These are the theoretical limits,
     * based on the IEEE standard. The actual range might be slightly smaller depending on your hardware or operating system.
     * 
     * M is the total number of digits and D is the number of digits following the decimal point. If M and D are omitted,
     * values are stored to the limits permitted by the hardware. A double-precision floating-point number is accurate to
     * approximately 15 decimal places.
     * 
     * Protocol: FIELD_TYPE_DOUBLE = 5
     * 
     * These types are synonyms for DOUBLE:
     * DOUBLE PRECISION[(M,D)] [UNSIGNED] [ZEROFILL],
     * REAL[(M,D)] [UNSIGNED] [ZEROFILL]. Exception: If the REAL_AS_FLOAT SQL mode is enabled, REAL is a synonym for FLOAT rather than DOUBLE.
     */
    DOUBLE("DOUBLE", Types.DOUBLE, Double.class, MysqlType.FIELD_FLAG_ZEROFILL, 0),
    //
    DOUBLE_UNSIGNED("DOUBLE", Types.DOUBLE, Double.class, MysqlType.FIELD_FLAG_UNSIGNED | MysqlType.FIELD_FLAG_ZEROFILL, 0),
    // FIELD_TYPE_NULL = 6
    NULL("NULL", Types.NULL, Object.class, 0, 0),
    /**
     * TIMESTAMP[(fsp)]
     * A timestamp. The range is '1970-01-01 00:00:01.000000' UTC to '2038-01-19 03:14:07.999999' UTC.
     * TIMESTAMP values are stored as the number of seconds since the epoch ('1970-01-01 00:00:00' UTC).
     * A TIMESTAMP cannot represent the value '1970-01-01 00:00:00' because that is equivalent to 0 seconds
     * from the epoch and the value 0 is reserved for representing '0000-00-00 00:00:00', the “zero” TIMESTAMP value.
     * An optional fsp value in the range from 0 to 6 may be given to specify fractional seconds precision. A value
     * of 0 signifies that there is no fractional part. If omitted, the default precision is 0.
     * 
     * Protocol: FIELD_TYPE_TIMESTAMP = 7
     * 
     */
    // TODO If MySQL server run with the MAXDB SQL mode enabled, TIMESTAMP is identical with DATETIME. If this mode is enabled at the time that a table is created, TIMESTAMP columns are created as DATETIME columns.
    // As a result, such columns use DATETIME display format, have the same range of values, and there is no automatic initialization or updating to the current date and time
    TIMESTAMP("TIMESTAMP", Types.TIMESTAMP, Timestamp.class, 0, 0),
    /**
     * BIGINT[(M)] [UNSIGNED] [ZEROFILL]
     * A large integer. The signed range is -9223372036854775808 to 9223372036854775807. The unsigned range is 0 to 18446744073709551615.
     * 
     * Protocol: FIELD_TYPE_LONGLONG = 8
     * 
     * SERIAL is an alias for BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE.
     */
    BIGINT("BIGINT", Types.BIGINT, Long.class, MysqlType.FIELD_FLAG_ZEROFILL, 0),
    //
    BIGINT_UNSIGNED("BIGINT", Types.BIGINT, BigInteger.class, MysqlType.FIELD_FLAG_UNSIGNED | MysqlType.FIELD_FLAG_ZEROFILL, 0),
    /**
     * MEDIUMINT[(M)] [UNSIGNED] [ZEROFILL]
     * A medium-sized integer. The signed range is -8388608 to 8388607. The unsigned range is 0 to 16777215.
     * 
     * Protocol: FIELD_TYPE_INT24 = 9
     */
    MEDIUMINT("MEDIUMINT", Types.INTEGER, Integer.class, MysqlType.FIELD_FLAG_ZEROFILL, 0),
    //
    MEDIUMINT_UNSIGNED("MEDIUMINT", Types.INTEGER, Integer.class, MysqlType.FIELD_FLAG_UNSIGNED | MysqlType.FIELD_FLAG_ZEROFILL, 0),
    /**
     * DATE
     * A date. The supported range is '1000-01-01' to '9999-12-31'. MySQL displays DATE values in 'YYYY-MM-DD' format,
     * but permits assignment of values to DATE columns using either strings or numbers.
     * 
     * Protocol: FIELD_TYPE_DATE = 10
     */
    DATE("DATE", Types.DATE, Date.class, 0, 0),
    /**
     * TIME[(fsp)]
     * A time. The range is '-838:59:59.000000' to '838:59:59.000000'. MySQL displays TIME values in
     * 'HH:MM:SS[.fraction]' format, but permits assignment of values to TIME columns using either strings or numbers.
     * An optional fsp value in the range from 0 to 6 may be given to specify fractional seconds precision. A value
     * of 0 signifies that there is no fractional part. If omitted, the default precision is 0.
     * 
     * Protocol: FIELD_TYPE_TIME = 11
     */
    TIME("TIME", Types.TIME, Time.class, 0, 0),
    /**
     * DATETIME[(fsp)]
     * A date and time combination. The supported range is '1000-01-01 00:00:00.000000' to '9999-12-31 23:59:59.999999'.
     * MySQL displays DATETIME values in 'YYYY-MM-DD HH:MM:SS[.fraction]' format, but permits assignment of values to
     * DATETIME columns using either strings or numbers.
     * An optional fsp value in the range from 0 to 6 may be given to specify fractional seconds precision. A value
     * of 0 signifies that there is no fractional part. If omitted, the default precision is 0.
     * 
     * Protocol: FIELD_TYPE_DATETIME = 12
     */
    DATETIME("DATETIME", Types.TIMESTAMP, Timestamp.class, 0, 0),
    /**
     * YEAR[(4)]
     * A year in four-digit format. MySQL displays YEAR values in YYYY format, but permits assignment of
     * values to YEAR columns using either strings or numbers. Values display as 1901 to 2155, and 0000.
     * Protocol: FIELD_TYPE_YEAR = 13
     */
    YEAR("YEAR", Types.DATE, Date.class, 0, 0),
    /**
     * [NATIONAL] VARCHAR(M) [CHARACTER SET charset_name] [COLLATE collation_name]
     * A variable-length string. M represents the maximum column length in characters. The range of M is 0 to 65,535.
     * The effective maximum length of a VARCHAR is subject to the maximum row size (65,535 bytes, which is shared among
     * all columns) and the character set used. For example, utf8 characters can require up to three bytes per character,
     * so a VARCHAR column that uses the utf8 character set can be declared to be a maximum of 21,844 characters.
     * 
     * MySQL stores VARCHAR values as a 1-byte or 2-byte length prefix plus data. The length prefix indicates the number
     * of bytes in the value. A VARCHAR column uses one length byte if values require no more than 255 bytes, two length
     * bytes if values may require more than 255 bytes.
     * 
     * Note
     * MySQL 5.7 follows the standard SQL specification, and does not remove trailing spaces from VARCHAR values.
     * 
     * VARCHAR is shorthand for CHARACTER VARYING. NATIONAL VARCHAR is the standard SQL way to define that a VARCHAR
     * column should use some predefined character set. MySQL 4.1 and up uses utf8 as this predefined character set.
     * NVARCHAR is shorthand for NATIONAL VARCHAR.
     * 
     * Protocol: FIELD_TYPE_VARCHAR = 15
     * Protocol: FIELD_TYPE_VAR_STRING = 253
     */
    VARCHAR("VARCHAR", Types.VARCHAR, String.class, 0, 0), // TODO is length=65535 ?
    /**
     * VARBINARY(M)
     * The VARBINARY type is similar to the VARCHAR type, but stores binary byte strings rather than nonbinary
     * character strings. M represents the maximum column length in bytes.
     * 
     * Protocol: FIELD_TYPE_VARCHAR = 15
     * Protocol: FIELD_TYPE_VAR_STRING = 253
     */
    VARBINARY("VARBINARY", Types.VARBINARY, null, 0, 0),
    /**
     * BIT[(M)]
     * A bit-field type. M indicates the number of bits per value, from 1 to 64. The default is 1 if M is omitted.
     * Protocol: FIELD_TYPE_BIT = 16
     */
    BIT("BIT", Types.BIT, Boolean.class, 0, 0), // TODO is length=8 ?
    // FIELD_TYPE_JSON = 245
    JSON("JSON", Types.VARCHAR, String.class, 0, 0),
    /**
     * ENUM('value1','value2',...) [CHARACTER SET charset_name] [COLLATE collation_name]
     * An enumeration. A string object that can have only one value, chosen from the list of values 'value1',
     * 'value2', ..., NULL or the special '' error value. ENUM values are represented internally as integers.
     * An ENUM column can have a maximum of 65,535 distinct elements. (The practical limit is less than 3000.)
     * A table can have no more than 255 unique element list definitions among its ENUM and SET columns considered as a group
     * 
     * Protocol: FIELD_TYPE_ENUM = 247
     */
    ENUM("ENUM", Types.CHAR, String.class, 0, 0),
    /**
     * SET('value1','value2',...) [CHARACTER SET charset_name] [COLLATE collation_name]
     * A set. A string object that can have zero or more values, each of which must be chosen from the list
     * of values 'value1', 'value2', ... SET values are represented internally as integers.
     * A SET column can have a maximum of 64 distinct members. A table can have no more than 255 unique
     * element list definitions among its ENUM and SET columns considered as a group
     * 
     * Protocol: FIELD_TYPE_SET = 248
     */
    SET("SET", Types.CHAR, String.class, 0, 0),
    /**
     * TINYBLOB
     * A BLOB column with a maximum length of 255 (28 − 1) bytes. Each TINYBLOB value is stored using a
     * 1-byte length prefix that indicates the number of bytes in the value.
     * 
     * Protocol:FIELD_TYPE_TINY_BLOB = 249
     */
    TINYBLOB("TINYBLOB", Types.VARBINARY, null, 0, 255), // TODO Types.VARBINARY was used in mysqlToJavaType but Types.BINARY in mysqlToJdbcTypesMap, which is correct?
    /**
     * TINYTEXT [CHARACTER SET charset_name] [COLLATE collation_name]
     * A TEXT column with a maximum length of 255 (28 − 1) characters. The effective maximum length
     * is less if the value contains multibyte characters. Each TINYTEXT value is stored using
     * a 1-byte length prefix that indicates the number of bytes in the value.
     * 
     * Protocol:FIELD_TYPE_TINY_BLOB = 249
     */
    TINYTEXT("TINYTEXT", Types.VARCHAR, String.class, 0, 255),
    /**
     * MEDIUMBLOB
     * A BLOB column with a maximum length of 16,777,215 (224 − 1) bytes. Each MEDIUMBLOB value is stored
     * using a 3-byte length prefix that indicates the number of bytes in the value.
     * 
     * Protocol: FIELD_TYPE_MEDIUM_BLOB = 250
     */
    MEDIUMBLOB("MEDIUMBLOB", Types.LONGVARBINARY, null, 0, 16777215),
    /**
     * MEDIUMTEXT [CHARACTER SET charset_name] [COLLATE collation_name]
     * A TEXT column with a maximum length of 16,777,215 (224 − 1) characters. The effective maximum length
     * is less if the value contains multibyte characters. Each MEDIUMTEXT value is stored using a 3-byte
     * length prefix that indicates the number of bytes in the value.
     * 
     * Protocol: FIELD_TYPE_MEDIUM_BLOB = 250
     */
    MEDIUMTEXT("MEDIUMTEXT", Types.LONGVARCHAR, String.class, 0, 16777215),
    /**
     * LONGBLOB
     * A BLOB column with a maximum length of 4,294,967,295 or 4GB (232 − 1) bytes. The effective maximum length
     * of LONGBLOB columns depends on the configured maximum packet size in the client/server protocol and available
     * memory. Each LONGBLOB value is stored using a 4-byte length prefix that indicates the number of bytes in the value.
     * 
     * Protocol: FIELD_TYPE_LONG_BLOB = 251
     */
    LONGBLOB("LONGBLOB", Types.LONGVARBINARY, null, 0, 4294967295L),
    /**
     * LONGTEXT [CHARACTER SET charset_name] [COLLATE collation_name]
     * A TEXT column with a maximum length of 4,294,967,295 or 4GB (232 − 1) characters. The effective
     * maximum length is less if the value contains multibyte characters. The effective maximum length
     * of LONGTEXT columns also depends on the configured maximum packet size in the client/server protocol
     * and available memory. Each LONGTEXT value is stored using a 4-byte length prefix that indicates
     * the number of bytes in the value.
     * 
     * Protocol: FIELD_TYPE_LONG_BLOB = 251
     */
    LONGTEXT("LONGTEXT", Types.LONGVARCHAR, String.class, 0, 4294967295L),
    /**
     * BLOB[(M)]
     * A BLOB column with a maximum length of 65,535 (216 − 1) bytes. Each BLOB value is stored using
     * a 2-byte length prefix that indicates the number of bytes in the value.
     * An optional length M can be given for this type. If this is done, MySQL creates the column as
     * the smallest BLOB type large enough to hold values M bytes long.
     * 
     * Protocol: FIELD_TYPE_BLOB = 252
     */
    BLOB("BLOB", Types.LONGVARBINARY, null, 0, 65535),
    /**
     * TEXT[(M)] [CHARACTER SET charset_name] [COLLATE collation_name]
     * A TEXT column with a maximum length of 65,535 (216 − 1) characters. The effective maximum length
     * is less if the value contains multibyte characters. Each TEXT value is stored using a 2-byte length
     * prefix that indicates the number of bytes in the value.
     * An optional length M can be given for this type. If this is done, MySQL creates the column as
     * the smallest TEXT type large enough to hold values M characters long.
     * 
     * Protocol: FIELD_TYPE_BLOB = 252
     */
    TEXT("TEXT", Types.LONGVARCHAR, String.class, 0, 65535),
    /**
     * [NATIONAL] CHAR[(M)] [CHARACTER SET charset_name] [COLLATE collation_name]
     * A fixed-length string that is always right-padded with spaces to the specified length when stored.
     * M represents the column length in characters. The range of M is 0 to 255. If M is omitted, the length is 1.
     * Note
     * Trailing spaces are removed when CHAR values are retrieved unless the PAD_CHAR_TO_FULL_LENGTH SQL mode is enabled.
     * CHAR is shorthand for CHARACTER. NATIONAL CHAR (or its equivalent short form, NCHAR) is the standard SQL way
     * to define that a CHAR column should use some predefined character set. MySQL 4.1 and up uses utf8
     * as this predefined character set.
     * 
     * MySQL permits you to create a column of type CHAR(0). This is useful primarily when you have to be compliant
     * with old applications that depend on the existence of a column but that do not actually use its value.
     * CHAR(0) is also quite nice when you need a column that can take only two values: A column that is defined
     * as CHAR(0) NULL occupies only one bit and can take only the values NULL and '' (the empty string).
     * 
     * Protocol: FIELD_TYPE_STRING = 254
     */
    CHAR("CHAR", Types.CHAR, String.class, 0, 0), // TODO is length=255 ?
    /**
     * BINARY(M)
     * The BINARY type is similar to the CHAR type, but stores binary byte strings rather than nonbinary character strings.
     * M represents the column length in bytes.
     * 
     * The CHAR BYTE data type is an alias for the BINARY data type.
     * 
     * Protocol: no concrete type on the wire TODO: really?
     */
    BINARY("BINARY", Types.BINARY, null, 0, 0),
    // FIELD_TYPE_GEOMETRY = 255
    GEOMETRY("GEOMETRY", Types.BINARY, null, 0, 0),
    // 
    UNKNOWN("UNKNOWN", Types.VARCHAR, String.class, 0, 0); // TODO Types.OTHER was used in mysqlToJavaType but Types.VARCHAR in mysqlToJdbcTypesMap, which is correct?

    /**
     * Get MysqlType matching the full MySQL type name, for example "DECIMAL(5,3) UNSIGNED ZEROFILL".
     * Distinct *_UNSIGNED type will be returned if "UNSIGNED" is present in fullMysqlTypeName.
     * 
     * @param fullMysqlTypeName
     * @return MysqlType
     */
    public static MysqlType getByName(String fullMysqlTypeName) {

        // the order of checks is important because some short names could match parts of longer names
        if (StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "DECIMAL") != -1 || StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "DEC") != -1
                || StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "NUMERIC") != -1 || StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "FIXED") != -1) {
            return StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "UNSIGNED") != -1 ? DECIMAL_UNSIGNED : DECIMAL;

        } else if (StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "TINYBLOB") != -1) {
            // IMPORTANT: "TINYBLOB" must be checked before "TINY"
            return TINYBLOB;

        } else if (StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "TINYTEXT") != -1) {
            // IMPORTANT: "TINYTEXT" must be checked before "TINY"
            return TINYTEXT;

        } else if (StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "TINYINT") != -1 || StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "TINY") != -1
                || StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "INT1") != -1) {

            // TODO BOOLEAN is a synonym for TINYINT(1)
            return StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "UNSIGNED") != -1 ? TINYINT_UNSIGNED : TINYINT;

        } else if (StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "MEDIUMINT") != -1
                // IMPORTANT: "INT24" must be checked before "INT2"
                || StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "INT24") != -1 || StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "INT3") != -1
                || StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "MIDDLEINT") != -1) {
            return StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "UNSIGNED") != -1 ? MEDIUMINT_UNSIGNED : MEDIUMINT;

        } else if (StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "SMALLINT") != -1 || StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "INT2") != -1) {
            return StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "UNSIGNED") != -1 ? SMALLINT_UNSIGNED : SMALLINT;

        } else if (StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "BIGINT") != -1 || StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "SERIAL") != -1
                || StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "INT8") != -1) {
            // SERIAL is an alias for BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE.
            return StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "UNSIGNED") != -1 ? BIGINT_UNSIGNED : BIGINT;

        } else if (StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "INT") != -1 || StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "INTEGER") != -1
                || StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "INT4") != -1) {
            // IMPORTANT: "INT" must be checked after all "*INT*" types
            return StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "UNSIGNED") != -1 ? INT_UNSIGNED : INT;

        } else if (StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "DOUBLE") != -1 || StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "REAL") != -1
        /* || StringUtils.indexOfIgnoreCase(name, "DOUBLE PRECISION") != -1 is caught by "DOUBLE" check */
        // IMPORTANT: "FLOAT8" must be checked before "FLOAT"
                || StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "FLOAT8") != -1) {
            // TODO Exception: If the REAL_AS_FLOAT SQL mode is enabled, REAL is a synonym for FLOAT rather than DOUBLE.
            return StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "UNSIGNED") != -1 ? DOUBLE_UNSIGNED : DOUBLE;

        } else if (StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "FLOAT") != -1 /*
                                                                                    * || StringUtils.indexOfIgnoreCase(name, "FLOAT4") != -1 is caught by
                                                                                    * "FLOAT" check
                                                                                    */) {
            // TODO FLOAT(p) [UNSIGNED] [ZEROFILL]. If p is from 0 to 24, the data type becomes FLOAT with no M or D values. If p is from 25 to 53, the data type becomes DOUBLE with no M or D values.
            return StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "UNSIGNED") != -1 ? FLOAT_UNSIGNED : FLOAT;

        } else if (StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "NULL") != -1) {
            return NULL;

        } else if (StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "TIMESTAMP") != -1) {
            // IMPORTANT: "TIMESTAMP" must be checked before "TIME"
            return TIMESTAMP;

        } else if (StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "DATETIME") != -1) {
            // IMPORTANT: "DATETIME" must be checked before "DATE" and "TIME"
            return DATETIME;

        } else if (StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "DATE") != -1) {
            return DATE;

        } else if (StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "TIME") != -1) {
            return TIME;

        } else if (StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "YEAR") != -1) {
            return YEAR;

        } else if (StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "LONGBLOB") != -1) {
            // IMPORTANT: "LONGBLOB" must be checked before "LONG" and "BLOB"
            return LONGBLOB;

        } else if (StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "LONGTEXT") != -1) {
            // IMPORTANT: "LONGTEXT" must be checked before "LONG" and "TEXT"
            return LONGTEXT;

        } else if (StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "MEDIUMBLOB") != -1
                || StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "LONG VARBINARY") != -1) {
            // IMPORTANT: "MEDIUMBLOB" must be checked before "BLOB"
            // IMPORTANT: "LONG VARBINARY" must be checked before "LONG" and "VARBINARY"
            return MEDIUMBLOB;

        } else if (StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "MEDIUMTEXT") != -1
                || StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "LONG VARCHAR") != -1 || StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "LONG") != -1) {
            // IMPORTANT: "MEDIUMTEXT" must be checked before "TEXT"
            // IMPORTANT: "LONG VARCHAR" must be checked before "VARCHAR"
            return MEDIUMTEXT;

        } else if (StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "VARCHAR") != -1 || StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "NVARCHAR") != -1
                || StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "NATIONAL VARCHAR") != -1
                || StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "CHARACTER VARYING") != -1) {
            // IMPORTANT: "CHARACTER VARYING" must be checked before "CHARACTER" and "CHAR"
            return VARCHAR;

        } else if (StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "VARBINARY") != -1) {
            return VARBINARY;

        } else if (StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "BINARY") != -1 || StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "CHAR BYTE") != -1) {
            // IMPORTANT: "BINARY" must be checked after all "*BINARY" types
            // IMPORTANT: "CHAR BYTE" must be checked before "CHAR"
            return BINARY;

        } else if (StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "STRING") != -1
                // IMPORTANT: "CHAR" must be checked after all "*CHAR*" types
                || StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "CHAR") != -1 || StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "NCHAR") != -1
                || StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "NATIONAL CHAR") != -1
                || StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "CHARACTER") != -1) {
            return CHAR;

        } else if (StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "BOOLEAN") != -1 || StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "BOOL") != -1) {
            return BOOLEAN;

        } else if (StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "BIT") != -1) {
            return BIT;

        } else if (StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "JSON") != -1) {
            return JSON;

        } else if (StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "ENUM") != -1) {
            return ENUM;

        } else if (StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "SET") != -1) {
            return SET;

        } else if (StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "BLOB") != -1) {
            return BLOB;

        } else if (StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "TEXT") != -1) {
            return TEXT;

        } else if (StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "GEOMETRY") != -1) {
            return GEOMETRY;

        }

        return UNKNOWN;
    }

    public static MysqlType getByJdbcType(int jdbcType) {
        switch (jdbcType) {
            case Types.BIGINT:
                return BIGINT;
            case Types.BINARY:
                return BINARY;
            case Types.BIT:
                return BIT;
            case Types.BOOLEAN:
                return BOOLEAN;
            case Types.CHAR:
                return CHAR;
            case Types.DATE:
                return DATE;
            case Types.DECIMAL:
            case Types.NUMERIC:
                return DECIMAL;
            case Types.DOUBLE:
            case Types.FLOAT:
                return DOUBLE;
            case Types.INTEGER:
                return INT;
            case Types.LONGVARBINARY:
                return BLOB;
            case Types.LONGVARCHAR:
                return TEXT;
            case Types.NULL:
                return NULL;
            case Types.REAL:
                return FLOAT;
            case Types.SMALLINT:
                return SMALLINT;
            case Types.TIME:
                return TIME;
            case Types.TIMESTAMP:
                return TIMESTAMP;
            case Types.TINYINT:
                return TINYINT;
            case Types.VARBINARY:
                return VARBINARY;
            case Types.VARCHAR:
                return VARCHAR;

                // TODO check next types
            case Types.ARRAY:
            case Types.BLOB:
            case Types.CLOB:
            case Types.DATALINK:
            case Types.DISTINCT:
            case Types.JAVA_OBJECT:
            case Types.LONGNVARCHAR:
            case Types.NCHAR:
            case Types.NCLOB:
            case Types.NVARCHAR:
            case Types.OTHER:
            case Types.REF:
            case Types.REF_CURSOR:
            case Types.ROWID:
            case Types.SQLXML:
            case Types.STRUCT:
            case Types.TIME_WITH_TIMEZONE:
            case Types.TIMESTAMP_WITH_TIMEZONE:

            default:
                return UNKNOWN;
        }
    }

    private final String name;
    protected int jdbcType;
    protected final Class<?> javaClass;
    private final int flagsMask;
    private final long maxLength;

    private MysqlType(String mysqlTypeName, int jdbcType, Class<?> javaClass, int allowedFlags, long maxLen) {
        this.name = mysqlTypeName;
        this.jdbcType = jdbcType;
        this.javaClass = javaClass;
        this.flagsMask = allowedFlags;
        this.maxLength = maxLen;
    }

    public String getName() {
        return this.name;
    }

    public int getJdbcType() {
        return this.jdbcType;
    }

    public boolean isAllowed(int flag) {
        return ((this.flagsMask & flag) > 0);
    }

    /**
     * 0 length means unlimited or not important
     * 
     * @return max data length in bytes
     */
    public long getMaxLength() {
        return this.maxLength;
    }

    public String getClassName() {
        if (this.javaClass == null) {
            return "[B";
        }
        return this.javaClass.getName();
    }

    public static final int FIELD_FLAG_NOT_NULL = 1;
    public static final int FIELD_FLAG_PRIMARY_KEY = 2;
    public static final int FIELD_FLAG_UNIQUE_KEY = 4;
    public static final int FIELD_FLAG_MULTIPLE_KEY = 8;
    public static final int FIELD_FLAG_BLOB = 16;
    public static final int FIELD_FLAG_UNSIGNED = 32;
    public static final int FIELD_FLAG_ZEROFILL = 64;
    public static final int FIELD_FLAG_BINARY = 128;
    public static final int FIELD_FLAG_AUTO_INCREMENT = 512;

}