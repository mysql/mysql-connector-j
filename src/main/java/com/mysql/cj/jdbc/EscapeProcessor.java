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

package com.mysql.cj.jdbc;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TimeZone;

import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.util.EscapeTokenizer;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.jdbc.util.TimeUtil;

/**
 * EscapeProcessor performs all escape code processing as outlined in the JDBC spec by JavaSoft.
 */
class EscapeProcessor {
    private static Map<String, String> JDBC_CONVERT_TO_MYSQL_TYPE_MAP;

    static {
        Map<String, String> tempMap = new HashMap<String, String>();

        tempMap.put("BIGINT", "0 + ?");
        tempMap.put("BINARY", "BINARY");
        tempMap.put("BIT", "0 + ?");
        tempMap.put("CHAR", "CHAR");
        tempMap.put("DATE", "DATE");
        tempMap.put("DECIMAL", "0.0 + ?");
        tempMap.put("DOUBLE", "0.0 + ?");
        tempMap.put("FLOAT", "0.0 + ?");
        tempMap.put("INTEGER", "0 + ?");
        tempMap.put("LONGVARBINARY", "BINARY");
        tempMap.put("LONGVARCHAR", "CONCAT(?)");
        tempMap.put("REAL", "0.0 + ?");
        tempMap.put("SMALLINT", "CONCAT(?)");
        tempMap.put("TIME", "TIME");
        tempMap.put("TIMESTAMP", "DATETIME");
        tempMap.put("TINYINT", "CONCAT(?)");
        tempMap.put("VARBINARY", "BINARY");
        tempMap.put("VARCHAR", "CONCAT(?)");

        JDBC_CONVERT_TO_MYSQL_TYPE_MAP = Collections.unmodifiableMap(tempMap);

    }

    /**
     * Escape process one string
     * 
     * @param sql
     *            the SQL to escape process.
     * 
     * @return the SQL after it has been escape processed.
     * 
     * @throws java.sql.SQLException
     * @throws SQLException
     */
    public static final Object escapeSQL(String sql, TimeZone defaultTimeZone, boolean serverSupportsFractionalSecond,
            ExceptionInterceptor exceptionInterceptor) throws java.sql.SQLException {
        boolean replaceEscapeSequence = false;
        String escapeSequence = null;

        if (sql == null) {
            return null;
        }

        /*
         * Short circuit this code if we don't have a matching pair of "{}". - Suggested by Ryan Gustafason
         */
        int beginBrace = sql.indexOf('{');
        int nextEndBrace = (beginBrace == -1) ? (-1) : sql.indexOf('}', beginBrace);

        if (nextEndBrace == -1) {
            return sql;
        }

        StringBuilder newSql = new StringBuilder();

        EscapeTokenizer escapeTokenizer = new EscapeTokenizer(sql);

        byte usesVariables = StatementImpl.USES_VARIABLES_FALSE;
        boolean callingStoredFunction = false;

        while (escapeTokenizer.hasMoreTokens()) {
            String token = escapeTokenizer.nextToken();

            if (token.length() != 0) {
                if (token.charAt(0) == '{') { // It's an escape code

                    if (!token.endsWith("}")) {
                        throw SQLError.createSQLException(Messages.getString("EscapeProcessor.0", new Object[] { token }), exceptionInterceptor);
                    }

                    if (token.length() > 2) {
                        int nestedBrace = token.indexOf('{', 2);

                        if (nestedBrace != -1) {
                            StringBuilder buf = new StringBuilder(token.substring(0, 1));

                            Object remainingResults = escapeSQL(token.substring(1, token.length() - 1), defaultTimeZone, serverSupportsFractionalSecond,
                                    exceptionInterceptor);

                            String remaining = null;

                            if (remainingResults instanceof String) {
                                remaining = (String) remainingResults;
                            } else {
                                remaining = ((EscapeProcessorResult) remainingResults).escapedSql;

                                if (usesVariables != StatementImpl.USES_VARIABLES_TRUE) {
                                    usesVariables = ((EscapeProcessorResult) remainingResults).usesVariables;
                                }
                            }

                            buf.append(remaining);

                            buf.append('}');

                            token = buf.toString();
                        }
                    }

                    // nested escape code
                    // Compare to tokens with _no_ whitespace
                    String collapsedToken = removeWhitespace(token);

                    /*
                     * Process the escape code
                     */
                    if (StringUtils.startsWithIgnoreCase(collapsedToken, "{escape")) {
                        try {
                            StringTokenizer st = new StringTokenizer(token, " '");
                            st.nextToken(); // eat the "escape" token
                            escapeSequence = st.nextToken();

                            if (escapeSequence.length() < 3) {
                                newSql.append(token); // it's just part of the query, push possible syntax errors onto server's shoulders
                            } else {

                                escapeSequence = escapeSequence.substring(1, escapeSequence.length() - 1);
                                replaceEscapeSequence = true;
                            }
                        } catch (java.util.NoSuchElementException e) {
                            newSql.append(token); // it's just part of the query, push possible syntax errors onto server's shoulders
                        }
                    } else if (StringUtils.startsWithIgnoreCase(collapsedToken, "{fn")) {
                        int startPos = token.toLowerCase().indexOf("fn ") + 3;
                        int endPos = token.length() - 1; // no }

                        String fnToken = token.substring(startPos, endPos);

                        // We need to handle 'convert' by ourselves

                        if (StringUtils.startsWithIgnoreCaseAndWs(fnToken, "convert")) {
                            newSql.append(processConvertToken(fnToken, exceptionInterceptor));
                        } else {
                            // just pass functions right to the DB
                            newSql.append(fnToken);
                        }
                    } else if (StringUtils.startsWithIgnoreCase(collapsedToken, "{d")) {
                        int startPos = token.indexOf('\'') + 1;
                        int endPos = token.lastIndexOf('\''); // no }

                        if ((startPos == -1) || (endPos == -1)) {
                            newSql.append(token); // it's just part of the query, push possible syntax errors onto server's shoulders
                        } else {

                            String argument = token.substring(startPos, endPos);

                            try {
                                StringTokenizer st = new StringTokenizer(argument, " -");
                                String year4 = st.nextToken();
                                String month2 = st.nextToken();
                                String day2 = st.nextToken();
                                String dateString = "'" + year4 + "-" + month2 + "-" + day2 + "'";
                                newSql.append(dateString);
                            } catch (java.util.NoSuchElementException e) {
                                throw SQLError.createSQLException(Messages.getString("EscapeProcessor.1", new Object[] { argument }),
                                        SQLError.SQL_STATE_SYNTAX_ERROR, exceptionInterceptor);
                            }
                        }
                    } else if (StringUtils.startsWithIgnoreCase(collapsedToken, "{ts")) {
                        processTimestampToken(defaultTimeZone, newSql, token, serverSupportsFractionalSecond, exceptionInterceptor);
                    } else if (StringUtils.startsWithIgnoreCase(collapsedToken, "{t")) {
                        processTimeToken(newSql, token, serverSupportsFractionalSecond, exceptionInterceptor);
                    } else if (StringUtils.startsWithIgnoreCase(collapsedToken, "{call") || StringUtils.startsWithIgnoreCase(collapsedToken, "{?=call")) {

                        int startPos = StringUtils.indexOfIgnoreCase(token, "CALL") + 5;
                        int endPos = token.length() - 1;

                        if (StringUtils.startsWithIgnoreCase(collapsedToken, "{?=call")) {
                            callingStoredFunction = true;
                            newSql.append("SELECT ");
                            newSql.append(token.substring(startPos, endPos));
                        } else {
                            callingStoredFunction = false;
                            newSql.append("CALL ");
                            newSql.append(token.substring(startPos, endPos));
                        }

                        for (int i = endPos - 1; i >= startPos; i--) {
                            char c = token.charAt(i);

                            if (Character.isWhitespace(c)) {
                                continue;
                            }

                            if (c != ')') {
                                newSql.append("()"); // handle no-parenthesis no-arg call not supported by MySQL parser
                            }

                            break;
                        }
                    } else if (StringUtils.startsWithIgnoreCase(collapsedToken, "{oj")) {
                        // MySQL already handles this escape sequence because of ODBC. Cool.
                        newSql.append(token);
                    } else {
                        // not an escape code, just part of the query
                        newSql.append(token);
                    }
                } else {
                    newSql.append(token); // it's just part of the query
                }
            }
        }

        String escapedSql = newSql.toString();

        //
        // FIXME: Let MySQL do this, however requires lightweight parsing of statement
        //
        if (replaceEscapeSequence) {
            String currentSql = escapedSql;

            while (currentSql.indexOf(escapeSequence) != -1) {
                int escapePos = currentSql.indexOf(escapeSequence);
                String lhs = currentSql.substring(0, escapePos);
                String rhs = currentSql.substring(escapePos + 1, currentSql.length());
                currentSql = lhs + "\\" + rhs;
            }

            escapedSql = currentSql;
        }

        EscapeProcessorResult epr = new EscapeProcessorResult();
        epr.escapedSql = escapedSql;
        epr.callingStoredFunction = callingStoredFunction;

        if (usesVariables != StatementImpl.USES_VARIABLES_TRUE) {
            if (escapeTokenizer.sawVariableUse()) {
                epr.usesVariables = StatementImpl.USES_VARIABLES_TRUE;
            } else {
                epr.usesVariables = StatementImpl.USES_VARIABLES_FALSE;
            }
        }

        return epr;
    }

    private static void processTimeToken(StringBuilder newSql, String token, boolean serverSupportsFractionalSecond, ExceptionInterceptor exceptionInterceptor)
            throws SQLException {
        int startPos = token.indexOf('\'') + 1;
        int endPos = token.lastIndexOf('\''); // no }

        if ((startPos == -1) || (endPos == -1)) {
            newSql.append(token); // it's just part of the query, push possible syntax errors onto server's shoulders
        } else {

            String argument = token.substring(startPos, endPos);

            try {
                StringTokenizer st = new StringTokenizer(argument, " :.");
                String hour = st.nextToken();
                String minute = st.nextToken();
                String second = st.nextToken();

                String fractionalSecond = "";

                if (serverSupportsFractionalSecond && st.hasMoreTokens()) {
                    fractionalSecond = "." + st.nextToken();
                }

                newSql.append("'");
                newSql.append(hour);
                newSql.append(":");
                newSql.append(minute);
                newSql.append(":");
                newSql.append(second);
                if (serverSupportsFractionalSecond) {
                    newSql.append(fractionalSecond);
                }
                newSql.append("'");
            } catch (java.util.NoSuchElementException e) {
                throw SQLError.createSQLException(Messages.getString("EscapeProcessor.3", new Object[] { argument }), SQLError.SQL_STATE_SYNTAX_ERROR,
                        exceptionInterceptor);
            }
        }
    }

    private static void processTimestampToken(TimeZone tz, StringBuilder newSql, String token, boolean serverSupportsFractionalSecond,
            ExceptionInterceptor exceptionInterceptor) throws SQLException {
        int startPos = token.indexOf('\'') + 1;
        int endPos = token.lastIndexOf('\''); // no }

        if ((startPos == -1) || (endPos == -1)) {
            newSql.append(token); // it's just part of the query, push possible syntax errors onto server's shoulders
        } else {

            String argument = token.substring(startPos, endPos);

            try {
                Timestamp ts = Timestamp.valueOf(argument);
                SimpleDateFormat tsdf = new SimpleDateFormat("''yyyy-MM-dd HH:mm:ss", Locale.US);

                tsdf.setTimeZone(tz);

                newSql.append(tsdf.format(ts));

                if (serverSupportsFractionalSecond && ts.getNanos() > 0) {
                    newSql.append('.');
                    newSql.append(TimeUtil.formatNanos(ts.getNanos(), true));
                }

                newSql.append('\'');
            } catch (IllegalArgumentException illegalArgumentException) {
                SQLException sqlEx = SQLError.createSQLException(Messages.getString("EscapeProcessor.2", new Object[] { argument }),
                        SQLError.SQL_STATE_SYNTAX_ERROR, exceptionInterceptor);
                sqlEx.initCause(illegalArgumentException);

                throw sqlEx;
            }
        }
    }

    /**
     * Re-writes {fn convert (expr, type)} as cast(expr AS type)
     * 
     * @param functionToken
     * @throws SQLException
     */
    private static String processConvertToken(String functionToken, ExceptionInterceptor exceptionInterceptor) throws SQLException {
        // The JDBC spec requires these types:
        //
        // BIGINT
        // BINARY
        // BIT
        // CHAR
        // DATE
        // DECIMAL
        // DOUBLE
        // FLOAT
        // INTEGER
        // LONGVARBINARY
        // LONGVARCHAR
        // REAL
        // SMALLINT
        // TIME
        // TIMESTAMP
        // TINYINT
        // VARBINARY
        // VARCHAR

        // MySQL supports these types:
        //
        // BINARY
        // CHAR
        // DATE
        // DATETIME
        // SIGNED (integer)
        // UNSIGNED (integer)
        // TIME

        int firstIndexOfParen = functionToken.indexOf("(");

        if (firstIndexOfParen == -1) {
            throw SQLError.createSQLException(Messages.getString("EscapeProcessor.4", new Object[] { functionToken }), SQLError.SQL_STATE_SYNTAX_ERROR,
                    exceptionInterceptor);
        }

        int indexOfComma = functionToken.lastIndexOf(",");

        if (indexOfComma == -1) {
            throw SQLError.createSQLException(Messages.getString("EscapeProcessor.5", new Object[] { functionToken }), SQLError.SQL_STATE_SYNTAX_ERROR,
                    exceptionInterceptor);
        }

        int indexOfCloseParen = functionToken.indexOf(')', indexOfComma);

        if (indexOfCloseParen == -1) {
            throw SQLError.createSQLException(Messages.getString("EscapeProcessor.6", new Object[] { functionToken }), SQLError.SQL_STATE_SYNTAX_ERROR,
                    exceptionInterceptor);

        }

        String expression = functionToken.substring(firstIndexOfParen + 1, indexOfComma);
        String type = functionToken.substring(indexOfComma + 1, indexOfCloseParen);

        String newType = null;

        String trimmedType = type.trim();

        if (StringUtils.startsWithIgnoreCase(trimmedType, "SQL_")) {
            trimmedType = trimmedType.substring(4, trimmedType.length());
        }

        newType = JDBC_CONVERT_TO_MYSQL_TYPE_MAP.get(trimmedType.toUpperCase(Locale.ENGLISH));

        if (newType == null) {
            throw SQLError.createSQLException(Messages.getString("EscapeProcessor.7", new Object[] { type.trim() }), SQLError.SQL_STATE_GENERAL_ERROR,
                    exceptionInterceptor);
        }

        int replaceIndex = newType.indexOf("?");

        if (replaceIndex != -1) {
            StringBuilder convertRewrite = new StringBuilder(newType.substring(0, replaceIndex));
            convertRewrite.append(expression);
            convertRewrite.append(newType.substring(replaceIndex + 1, newType.length()));

            return convertRewrite.toString();
        }

        StringBuilder castRewrite = new StringBuilder("CAST(");
        castRewrite.append(expression);
        castRewrite.append(" AS ");
        castRewrite.append(newType);
        castRewrite.append(")");

        return castRewrite.toString();

    }

    /**
     * Removes all whitespace from the given String. We use this to make escape
     * token comparison white-space ignorant.
     * 
     * @param toCollapse
     *            the string to remove the whitespace from
     * 
     * @return a string with _no_ whitespace.
     */
    private static String removeWhitespace(String toCollapse) {
        if (toCollapse == null) {
            return null;
        }

        int length = toCollapse.length();

        StringBuilder collapsed = new StringBuilder(length);

        for (int i = 0; i < length; i++) {
            char c = toCollapse.charAt(i);

            if (!Character.isWhitespace(c)) {
                collapsed.append(c);
            }
        }

        return collapsed.toString();
    }
}
