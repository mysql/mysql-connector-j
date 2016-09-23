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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.sql.Array;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Wrapper;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import com.mysql.cj.api.ProfilerEvent;
import com.mysql.cj.api.conf.ReadableProperty;
import com.mysql.cj.api.jdbc.JdbcConnection;
import com.mysql.cj.api.jdbc.ParameterBindings;
import com.mysql.cj.api.jdbc.result.ResultSetInternalMethods;
import com.mysql.cj.api.mysqla.io.NativeProtocol.IntegerDataType;
import com.mysql.cj.api.mysqla.io.NativeProtocol.StringLengthDataType;
import com.mysql.cj.api.mysqla.io.PacketPayload;
import com.mysql.cj.api.mysqla.result.ColumnDefinition;
import com.mysql.cj.api.result.Row;
import com.mysql.cj.core.CharsetMapping;
import com.mysql.cj.core.Constants;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.MysqlType;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exceptions.CJException;
import com.mysql.cj.core.exceptions.FeatureNotAvailableException;
import com.mysql.cj.core.exceptions.StatementIsClosedException;
import com.mysql.cj.core.profiler.ProfilerEventImpl;
import com.mysql.cj.core.result.Field;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.core.util.Util;
import com.mysql.cj.jdbc.exceptions.MySQLStatementCancelledException;
import com.mysql.cj.jdbc.exceptions.MySQLTimeoutException;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.jdbc.exceptions.SQLExceptionsMapping;
import com.mysql.cj.jdbc.result.CachedResultSetMetaData;
import com.mysql.cj.jdbc.result.ResultSetImpl;
import com.mysql.cj.jdbc.result.ResultSetMetaData;
import com.mysql.cj.jdbc.util.TimeUtil;
import com.mysql.cj.mysqla.MysqlaConstants;
import com.mysql.cj.mysqla.result.ByteArrayRow;
import com.mysql.cj.mysqla.result.MysqlaColumnDefinition;
import com.mysql.cj.mysqla.result.ResultsetRowsStatic;

/**
 * A SQL Statement is pre-compiled and stored in a PreparedStatement object. This object can then be used to efficiently execute this statement multiple times.
 * 
 * <p>
 * <B>Note:</B> The setXXX methods for setting IN parameter values must specify types that are compatible with the defined SQL type of the input parameter. For
 * instance, if the IN parameter has SQL type Integer, then setInt should be used.
 * </p>
 * 
 * <p>
 * If arbitrary parameter type conversions are required, then the setObject method should be used with a target SQL type.
 * </p>
 */
public class PreparedStatement extends com.mysql.cj.jdbc.StatementImpl implements java.sql.PreparedStatement {

    public class BatchParams {
        public boolean[] isNull = null;

        public boolean[] isStream = null;

        public InputStream[] parameterStreams = null;

        public byte[][] parameterStrings = null;

        public int[] streamLengths = null;

        BatchParams(byte[][] strings, InputStream[] streams, boolean[] isStreamFlags, int[] lengths, boolean[] isNullFlags) {
            //
            // Make copies
            //
            this.parameterStrings = new byte[strings.length][];
            this.parameterStreams = new InputStream[streams.length];
            this.isStream = new boolean[isStreamFlags.length];
            this.streamLengths = new int[lengths.length];
            this.isNull = new boolean[isNullFlags.length];
            System.arraycopy(strings, 0, this.parameterStrings, 0, strings.length);
            System.arraycopy(streams, 0, this.parameterStreams, 0, streams.length);
            System.arraycopy(isStreamFlags, 0, this.isStream, 0, isStreamFlags.length);
            System.arraycopy(lengths, 0, this.streamLengths, 0, lengths.length);
            System.arraycopy(isNullFlags, 0, this.isNull, 0, isNullFlags.length);
        }
    }

    class EndPoint {
        int begin;

        int end;

        EndPoint(int b, int e) {
            this.begin = b;
            this.end = e;
        }
    }

    public static final class ParseInfo {
        char firstStmtChar = 0;

        boolean foundLoadData = false;

        long lastUsed = 0;

        int statementLength = 0;

        int statementStartPos = 0;

        boolean canRewriteAsMultiValueInsert = false;

        byte[][] staticSql = null;

        boolean isOnDuplicateKeyUpdate = false;

        int locationOfOnDuplicateKeyUpdate = -1;

        String valuesClause;

        boolean parametersInDuplicateKeyClause = false;

        String charEncoding;

        /**
         * Represents the "parsed" state of a client-side prepared statement, with the statement broken up into it's static and dynamic (where parameters are
         * bound) parts.
         */
        ParseInfo(String sql, JdbcConnection conn, java.sql.DatabaseMetaData dbmd, String encoding) throws SQLException {
            this(sql, conn, dbmd, encoding, true);
        }

        public ParseInfo(String sql, JdbcConnection conn, java.sql.DatabaseMetaData dbmd, String encoding, boolean buildRewriteInfo) throws SQLException {
            try {
                if (sql == null) {
                    throw SQLError.createSQLException(Messages.getString("PreparedStatement.61"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                            conn.getExceptionInterceptor());
                }

                this.charEncoding = encoding;
                this.lastUsed = System.currentTimeMillis();

                String quotedIdentifierString = dbmd.getIdentifierQuoteString();

                char quotedIdentifierChar = 0;

                if ((quotedIdentifierString != null) && !quotedIdentifierString.equals(" ") && (quotedIdentifierString.length() > 0)) {
                    quotedIdentifierChar = quotedIdentifierString.charAt(0);
                }

                this.statementLength = sql.length();

                ArrayList<int[]> endpointList = new ArrayList<int[]>();
                boolean inQuotes = false;
                char quoteChar = 0;
                boolean inQuotedId = false;
                int lastParmEnd = 0;
                int i;

                boolean noBackslashEscapes = conn.isNoBackslashEscapesSet();

                // we're not trying to be real pedantic here, but we'd like to  skip comments at the beginning of statements, as frameworks such as Hibernate
                // use them to aid in debugging

                this.statementStartPos = findStartOfStatement(sql);

                for (i = this.statementStartPos; i < this.statementLength; ++i) {
                    char c = sql.charAt(i);

                    if ((this.firstStmtChar == 0) && Character.isLetter(c)) {
                        // Determine what kind of statement we're doing (_S_elect, _I_nsert, etc.)
                        this.firstStmtChar = Character.toUpperCase(c);

                        // no need to search for "ON DUPLICATE KEY UPDATE" if not an INSERT statement
                        if (this.firstStmtChar == 'I') {
                            this.locationOfOnDuplicateKeyUpdate = getOnDuplicateKeyLocation(sql,
                                    conn.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_dontCheckOnDuplicateKeyUpdateInSQL).getValue(),
                                    conn.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_rewriteBatchedStatements).getValue(),
                                    conn.isNoBackslashEscapesSet());
                            this.isOnDuplicateKeyUpdate = this.locationOfOnDuplicateKeyUpdate != -1;
                        }
                    }

                    if (!noBackslashEscapes && c == '\\' && i < (this.statementLength - 1)) {
                        i++;
                        continue; // next character is escaped
                    }

                    // are we in a quoted identifier? (only valid when the id is not inside a 'string')
                    if (!inQuotes && (quotedIdentifierChar != 0) && (c == quotedIdentifierChar)) {
                        inQuotedId = !inQuotedId;
                    } else if (!inQuotedId) {
                        //	only respect quotes when not in a quoted identifier

                        if (inQuotes) {
                            if (((c == '\'') || (c == '"')) && c == quoteChar) {
                                if (i < (this.statementLength - 1) && sql.charAt(i + 1) == quoteChar) {
                                    i++;
                                    continue; // inline quote escape
                                }

                                inQuotes = !inQuotes;
                                quoteChar = 0;
                            } else if (((c == '\'') || (c == '"')) && c == quoteChar) {
                                inQuotes = !inQuotes;
                                quoteChar = 0;
                            }
                        } else {
                            if (c == '#' || (c == '-' && (i + 1) < this.statementLength && sql.charAt(i + 1) == '-')) {
                                // run out to end of statement, or newline, whichever comes first
                                int endOfStmt = this.statementLength - 1;

                                for (; i < endOfStmt; i++) {
                                    c = sql.charAt(i);

                                    if (c == '\r' || c == '\n') {
                                        break;
                                    }
                                }

                                continue;
                            } else if (c == '/' && (i + 1) < this.statementLength) {
                                // Comment?
                                char cNext = sql.charAt(i + 1);

                                if (cNext == '*') {
                                    i += 2;

                                    for (int j = i; j < this.statementLength; j++) {
                                        i++;
                                        cNext = sql.charAt(j);

                                        if (cNext == '*' && (j + 1) < this.statementLength) {
                                            if (sql.charAt(j + 1) == '/') {
                                                i++;

                                                if (i < this.statementLength) {
                                                    c = sql.charAt(i);
                                                }

                                                break; // comment done
                                            }
                                        }
                                    }
                                }
                            } else if ((c == '\'') || (c == '"')) {
                                inQuotes = true;
                                quoteChar = c;
                            }
                        }
                    }

                    if ((c == '?') && !inQuotes && !inQuotedId) {
                        endpointList.add(new int[] { lastParmEnd, i });
                        lastParmEnd = i + 1;

                        if (this.isOnDuplicateKeyUpdate && i > this.locationOfOnDuplicateKeyUpdate) {
                            this.parametersInDuplicateKeyClause = true;
                        }
                    }
                }

                if (this.firstStmtChar == 'L') {
                    if (StringUtils.startsWithIgnoreCaseAndWs(sql, "LOAD DATA")) {
                        this.foundLoadData = true;
                    } else {
                        this.foundLoadData = false;
                    }
                } else {
                    this.foundLoadData = false;
                }

                endpointList.add(new int[] { lastParmEnd, this.statementLength });
                this.staticSql = new byte[endpointList.size()][];

                for (i = 0; i < this.staticSql.length; i++) {
                    int[] ep = endpointList.get(i);
                    int end = ep[1];
                    int begin = ep[0];
                    int len = end - begin;

                    if (this.foundLoadData) {
                        this.staticSql[i] = StringUtils.getBytes(sql, begin, len);
                    } else if (encoding == null) {
                        byte[] buf = new byte[len];

                        for (int j = 0; j < len; j++) {
                            buf[j] = (byte) sql.charAt(begin + j);
                        }

                        this.staticSql[i] = buf;
                    } else {
                        this.staticSql[i] = StringUtils.getBytes(sql, begin, len, encoding);
                    }
                }
            } catch (StringIndexOutOfBoundsException oobEx) {
                throw SQLError.createSQLException(Messages.getString("PreparedStatement.62", new Object[] { sql }), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, oobEx,
                        conn.getExceptionInterceptor());
            } catch (CJException e) {
                throw SQLExceptionsMapping.translateException(e, conn.getExceptionInterceptor());
            }

            if (buildRewriteInfo) {
                this.canRewriteAsMultiValueInsert = PreparedStatement.canRewrite(sql, this.isOnDuplicateKeyUpdate, this.locationOfOnDuplicateKeyUpdate,
                        this.statementStartPos) && !this.parametersInDuplicateKeyClause;

                if (this.canRewriteAsMultiValueInsert
                        && conn.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_rewriteBatchedStatements).getValue()) {
                    buildRewriteBatchedParams(sql, conn, dbmd, encoding);
                }
            }
        }

        private ParseInfo batchHead;

        private ParseInfo batchValues;

        private ParseInfo batchODKUClause;

        private void buildRewriteBatchedParams(String sql, JdbcConnection conn, DatabaseMetaData metadata, String encoding) throws SQLException {
            this.valuesClause = extractValuesClause(sql, conn.getMetaData().getIdentifierQuoteString());
            String odkuClause = this.isOnDuplicateKeyUpdate ? sql.substring(this.locationOfOnDuplicateKeyUpdate) : null;

            String headSql = null;

            if (this.isOnDuplicateKeyUpdate) {
                headSql = sql.substring(0, this.locationOfOnDuplicateKeyUpdate);
            } else {
                headSql = sql;
            }

            this.batchHead = new ParseInfo(headSql, conn, metadata, encoding, false);
            this.batchValues = new ParseInfo("," + this.valuesClause, conn, metadata, encoding, false);
            this.batchODKUClause = null;

            if (odkuClause != null && odkuClause.length() > 0) {
                this.batchODKUClause = new ParseInfo("," + this.valuesClause + " " + odkuClause, conn, metadata, encoding, false);
            }
        }

        private String extractValuesClause(String sql, String quoteCharStr) throws SQLException {
            int indexOfValues = -1;
            int valuesSearchStart = this.statementStartPos;

            while (indexOfValues == -1) {
                if (quoteCharStr.length() > 0) {
                    indexOfValues = StringUtils.indexOfIgnoreCase(valuesSearchStart, sql, "VALUES", quoteCharStr, quoteCharStr,
                            StringUtils.SEARCH_MODE__MRK_COM_WS);
                } else {
                    indexOfValues = StringUtils.indexOfIgnoreCase(valuesSearchStart, sql, "VALUES");
                }

                if (indexOfValues > 0) {
                    /* check if the char immediately preceding VALUES may be part of the table name */
                    char c = sql.charAt(indexOfValues - 1);
                    if (!(Character.isWhitespace(c) || c == ')' || c == '`')) {
                        valuesSearchStart = indexOfValues + 6;
                        indexOfValues = -1;
                    } else {
                        /* check if the char immediately following VALUES may be whitespace or open parenthesis */
                        c = sql.charAt(indexOfValues + 6);
                        if (!(Character.isWhitespace(c) || c == '(')) {
                            valuesSearchStart = indexOfValues + 6;
                            indexOfValues = -1;
                        }
                    }
                } else {
                    break;
                }
            }

            if (indexOfValues == -1) {
                return null;
            }

            int indexOfFirstParen = sql.indexOf('(', indexOfValues + 6);

            if (indexOfFirstParen == -1) {
                return null;
            }

            int endOfValuesClause = sql.lastIndexOf(')');

            if (endOfValuesClause == -1) {
                return null;
            }

            if (this.isOnDuplicateKeyUpdate) {
                endOfValuesClause = this.locationOfOnDuplicateKeyUpdate - 1;
            }

            return sql.substring(indexOfFirstParen, endOfValuesClause + 1);
        }

        /**
         * Returns a ParseInfo for a multi-value INSERT for a batch of size numBatch (without parsing!).
         */
        synchronized ParseInfo getParseInfoForBatch(int numBatch) {
            AppendingBatchVisitor apv = new AppendingBatchVisitor();
            buildInfoForBatch(numBatch, apv);

            ParseInfo batchParseInfo = new ParseInfo(apv.getStaticSqlStrings(), this.firstStmtChar, this.foundLoadData, this.isOnDuplicateKeyUpdate,
                    this.locationOfOnDuplicateKeyUpdate, this.statementLength, this.statementStartPos);

            return batchParseInfo;
        }

        /**
         * Returns a preparable SQL string for the number of batched parameters, used by server-side prepared statements
         * when re-writing batch INSERTs.
         */

        String getSqlForBatch(int numBatch) throws UnsupportedEncodingException {
            ParseInfo batchInfo = getParseInfoForBatch(numBatch);

            return getSqlForBatch(batchInfo);
        }

        /**
         * Used for filling in the SQL for getPreparedSql() - for debugging
         */
        String getSqlForBatch(ParseInfo batchInfo) throws UnsupportedEncodingException {
            int size = 0;
            final byte[][] sqlStrings = batchInfo.staticSql;
            final int sqlStringsLength = sqlStrings.length;

            for (int i = 0; i < sqlStringsLength; i++) {
                size += sqlStrings[i].length;
                size++; // for the '?'
            }

            StringBuilder buf = new StringBuilder(size);

            for (int i = 0; i < sqlStringsLength - 1; i++) {
                buf.append(StringUtils.toString(sqlStrings[i], this.charEncoding));
                buf.append("?");
            }

            buf.append(StringUtils.toString(sqlStrings[sqlStringsLength - 1]));

            return buf.toString();
        }

        /**
         * Builds a ParseInfo for the given batch size, without parsing. We use
         * a visitor pattern here, because the if {}s make computing a size for the
         * resultant byte[][] make this too complex, and we don't necessarily want to
         * use a List for this, because the size can be dynamic, and thus we'll not be
         * able to guess a good initial size for an array-based list, and it's not
         * efficient to convert a LinkedList to an array.
         */
        private void buildInfoForBatch(int numBatch, BatchVisitor visitor) {
            final byte[][] headStaticSql = this.batchHead.staticSql;
            final int headStaticSqlLength = headStaticSql.length;

            if (headStaticSqlLength > 1) {
                for (int i = 0; i < headStaticSqlLength - 1; i++) {
                    visitor.append(headStaticSql[i]).increment();
                }
            }

            // merge end of head, with beginning of a value clause
            byte[] endOfHead = headStaticSql[headStaticSqlLength - 1];
            final byte[][] valuesStaticSql = this.batchValues.staticSql;
            byte[] beginOfValues = valuesStaticSql[0];

            visitor.merge(endOfHead, beginOfValues).increment();

            int numValueRepeats = numBatch - 1; // first one is in the "head"

            if (this.batchODKUClause != null) {
                numValueRepeats--; // Last one is in the ODKU clause
            }

            final int valuesStaticSqlLength = valuesStaticSql.length;
            byte[] endOfValues = valuesStaticSql[valuesStaticSqlLength - 1];

            for (int i = 0; i < numValueRepeats; i++) {
                for (int j = 1; j < valuesStaticSqlLength - 1; j++) {
                    visitor.append(valuesStaticSql[j]).increment();
                }
                visitor.merge(endOfValues, beginOfValues).increment();
            }

            if (this.batchODKUClause != null) {
                final byte[][] batchOdkuStaticSql = this.batchODKUClause.staticSql;
                byte[] beginOfOdku = batchOdkuStaticSql[0];
                visitor.decrement().merge(endOfValues, beginOfOdku).increment();

                final int batchOdkuStaticSqlLength = batchOdkuStaticSql.length;

                if (numBatch > 1) {
                    for (int i = 1; i < batchOdkuStaticSqlLength; i++) {
                        visitor.append(batchOdkuStaticSql[i]).increment();
                    }
                } else {
                    visitor.decrement().append(batchOdkuStaticSql[(batchOdkuStaticSqlLength - 1)]);
                }
            } else {
                // Everything after the values clause, but not ODKU, which today is nothing but a syntax error, but we should still not mangle the SQL!
                visitor.decrement().append(this.staticSql[this.staticSql.length - 1]);
            }
        }

        private ParseInfo(byte[][] staticSql, char firstStmtChar, boolean foundLoadData, boolean isOnDuplicateKeyUpdate, int locationOfOnDuplicateKeyUpdate,
                int statementLength, int statementStartPos) {
            this.firstStmtChar = firstStmtChar;
            this.foundLoadData = foundLoadData;
            this.isOnDuplicateKeyUpdate = isOnDuplicateKeyUpdate;
            this.locationOfOnDuplicateKeyUpdate = locationOfOnDuplicateKeyUpdate;
            this.statementLength = statementLength;
            this.statementStartPos = statementStartPos;
            this.staticSql = staticSql;
        }
    }

    interface BatchVisitor {
        abstract BatchVisitor increment();

        abstract BatchVisitor decrement();

        abstract BatchVisitor append(byte[] values);

        abstract BatchVisitor merge(byte[] begin, byte[] end);
    }

    static class AppendingBatchVisitor implements BatchVisitor {
        LinkedList<byte[]> statementComponents = new LinkedList<byte[]>();

        public BatchVisitor append(byte[] values) {
            this.statementComponents.addLast(values);

            return this;
        }

        public BatchVisitor increment() {
            // no-op
            return this;
        }

        public BatchVisitor decrement() {
            this.statementComponents.removeLast();

            return this;
        }

        public BatchVisitor merge(byte[] front, byte[] back) {
            int mergedLength = front.length + back.length;
            byte[] merged = new byte[mergedLength];
            System.arraycopy(front, 0, merged, 0, front.length);
            System.arraycopy(back, 0, merged, front.length, back.length);
            this.statementComponents.addLast(merged);
            return this;
        }

        public byte[][] getStaticSqlStrings() {
            byte[][] asBytes = new byte[this.statementComponents.size()][];
            this.statementComponents.toArray(asBytes);

            return asBytes;
        }

        @Override
        public String toString() {
            StringBuilder buf = new StringBuilder();
            Iterator<byte[]> iter = this.statementComponents.iterator();
            while (iter.hasNext()) {
                buf.append(StringUtils.toString(iter.next()));
            }

            return buf.toString();
        }

    }

    private final static byte[] HEX_DIGITS = new byte[] { (byte) '0', (byte) '1', (byte) '2', (byte) '3', (byte) '4', (byte) '5', (byte) '6', (byte) '7',
            (byte) '8', (byte) '9', (byte) 'A', (byte) 'B', (byte) 'C', (byte) 'D', (byte) 'E', (byte) 'F' };

    /**
     * Reads length bytes from reader into buf. Blocks until enough input is
     * available
     * 
     * @param reader
     * @param buf
     * @param length
     * 
     * @throws IOException
     */
    protected static int readFully(Reader reader, char[] buf, int length) throws IOException {
        int numCharsRead = 0;

        while (numCharsRead < length) {
            int count = reader.read(buf, numCharsRead, length - numCharsRead);

            if (count < 0) {
                break;
            }

            numCharsRead += count;
        }

        return numCharsRead;
    }

    /**
     * Does the batch (if any) contain "plain" statements added by
     * Statement.addBatch(String)?
     * 
     * If so, we can't re-write it to use multi-value or multi-queries.
     */
    protected boolean batchHasPlainStatements = false;

    private java.sql.DatabaseMetaData dbmd = null;

    /**
     * What is the first character of the prepared statement (used to check for
     * SELECT vs. INSERT/UPDATE/DELETE)
     */
    protected char firstCharOfStmt = 0;

    /** Is this query a LOAD DATA query? */
    protected boolean isLoadDataQuery = false;

    protected boolean[] isNull = null;

    private boolean[] isStream = null;

    protected int numberOfExecutions = 0;

    /** The SQL that was passed in to 'prepare' */
    protected String originalSql = null;

    /** The number of parameters in this PreparedStatement */
    protected int parameterCount;

    protected MysqlParameterMetadata parameterMetaData;

    private InputStream[] parameterStreams = null;

    private byte[][] parameterValues = null;

    /**
     * Only used by statement interceptors at the moment to
     * provide introspection of bound values
     */
    protected MysqlType[] parameterTypes = null;

    protected ParseInfo parseInfo;

    private java.sql.ResultSetMetaData pstmtResultMetaData;

    private byte[][] staticSqlStrings = null;

    private byte[] streamConvertBuf = null;

    private int[] streamLengths = null;

    private SimpleDateFormat tsdf = null;

    private SimpleDateFormat ddf;

    private SimpleDateFormat tdf;

    protected boolean usingAnsiMode;

    protected String batchedValuesClause;

    private boolean doPingInstead;

    private boolean compensateForOnDuplicateKeyUpdate = false;

    /** Charset encoder used to escape if needed, such as Yen sign in SJIS */
    private CharsetEncoder charsetEncoder;

    /** Command index of currently executing batch command. */
    protected int batchCommandIndex = -1;

    protected ReadableProperty<Boolean> useStreamLengthsInPrepStmts;
    protected ReadableProperty<Boolean> autoClosePStmtStreams;
    protected ReadableProperty<Boolean> treatUtilDateAsTimestamp;

    /**
     * Creates a prepared statement instance
     */

    protected static PreparedStatement getInstance(JdbcConnection conn, String catalog) throws SQLException {
        return new PreparedStatement(conn, catalog);
    }

    /**
     * Creates a prepared statement instance
     */

    protected static PreparedStatement getInstance(JdbcConnection conn, String sql, String catalog) throws SQLException {
        return new PreparedStatement(conn, sql, catalog);
    }

    /**
     * Creates a prepared statement instance
     */

    protected static PreparedStatement getInstance(JdbcConnection conn, String sql, String catalog, ParseInfo cachedParseInfo) throws SQLException {
        return new PreparedStatement(conn, sql, catalog, cachedParseInfo);
    }

    /**
     * Constructor used by server-side prepared statements
     * 
     * @param conn
     *            the connection that created us
     * @param catalog
     *            the catalog in use when we were created
     * 
     * @throws SQLException
     *             if an error occurs
     */
    public PreparedStatement(JdbcConnection conn, String catalog) throws SQLException {
        super(conn, catalog);

        this.compensateForOnDuplicateKeyUpdate = this.session.getPropertySet()
                .getBooleanReadableProperty(PropertyDefinitions.PNAME_compensateOnDuplicateKeyUpdateCounts).getValue();
        this.useStreamLengthsInPrepStmts = this.session.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useStreamLengthsInPrepStmts);
        this.autoClosePStmtStreams = this.session.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_autoClosePStmtStreams);
        this.treatUtilDateAsTimestamp = this.session.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_treatUtilDateAsTimestamp);

    }

    /**
     * Constructor for the PreparedStatement class.
     * 
     * @param conn
     *            the connection creating this statement
     * @param sql
     *            the SQL for this statement
     * @param catalog
     *            the catalog/database this statement should be issued against
     * 
     * @throws SQLException
     *             if a database error occurs.
     */
    public PreparedStatement(JdbcConnection conn, String sql, String catalog) throws SQLException {
        this(conn, catalog);

        if (sql == null) {
            throw SQLError.createSQLException(Messages.getString("PreparedStatement.0"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }

        this.originalSql = sql;

        if (this.originalSql.startsWith(PING_MARKER)) {
            this.doPingInstead = true;
        } else {
            this.doPingInstead = false;
        }

        this.dbmd = this.connection.getMetaData();

        this.parseInfo = new ParseInfo(sql, this.connection, this.dbmd, this.charEncoding);

        initializeFromParseInfo();

        if (conn.getRequiresEscapingEncoder()) {
            this.charsetEncoder = Charset.forName(conn.getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_characterEncoding).getValue())
                    .newEncoder();
        }
    }

    /**
     * Creates a new PreparedStatement object.
     * 
     * @param conn
     *            the connection creating this statement
     * @param sql
     *            the SQL for this statement
     * @param catalog
     *            the catalog/database this statement should be issued against
     * @param cachedParseInfo
     *            already created parseInfo.
     * 
     * @throws SQLException
     */
    public PreparedStatement(JdbcConnection conn, String sql, String catalog, ParseInfo cachedParseInfo) throws SQLException {
        this(conn, catalog);

        if (sql == null) {
            throw SQLError.createSQLException(Messages.getString("PreparedStatement.1"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }

        this.originalSql = sql;

        this.dbmd = this.connection.getMetaData();

        this.parseInfo = cachedParseInfo;

        this.usingAnsiMode = !this.connection.useAnsiQuotedIdentifiers();

        initializeFromParseInfo();

        if (conn.getRequiresEscapingEncoder()) {
            this.charsetEncoder = Charset.forName(conn.getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_characterEncoding).getValue())
                    .newEncoder();
        }
    }

    /**
     * JDBC 2.0 Add a set of parameters to the batch.
     * 
     * @exception SQLException
     *                if a database-access error occurs.
     * 
     * @see StatementImpl#addBatch
     */
    public void addBatch() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (this.batchedArgs == null) {
                this.batchedArgs = new ArrayList<Object>();
            }

            for (int i = 0; i < this.parameterValues.length; i++) {
                checkAllParametersSet(this.parameterValues[i], this.parameterStreams[i], i);
            }

            this.batchedArgs.add(new BatchParams(this.parameterValues, this.parameterStreams, this.isStream, this.streamLengths, this.isNull));
        }
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            this.batchHasPlainStatements = true;

            super.addBatch(sql);
        }
    }

    public String asSql() throws SQLException {
        return asSql(false);
    }

    public String asSql(boolean quoteStreamsAndUnknowns) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            StringBuilder buf = new StringBuilder();

            int realParameterCount = this.parameterCount + getParameterIndexOffset();
            Object batchArg = null;
            if (this.batchCommandIndex != -1) {
                batchArg = this.batchedArgs.get(this.batchCommandIndex);
            }

            for (int i = 0; i < realParameterCount; ++i) {
                if (this.charEncoding != null) {
                    buf.append(StringUtils.toString(this.staticSqlStrings[i], this.charEncoding));
                } else {
                    buf.append(StringUtils.toString(this.staticSqlStrings[i]));
                }

                byte val[] = null;
                if (batchArg != null && batchArg instanceof String) {
                    buf.append((String) batchArg);
                    continue;
                }
                if (this.batchCommandIndex == -1) {
                    val = this.parameterValues[i];
                } else {
                    val = ((BatchParams) batchArg).parameterStrings[i];
                }

                boolean isStreamParam = false;
                if (this.batchCommandIndex == -1) {
                    isStreamParam = this.isStream[i];
                } else {
                    isStreamParam = ((BatchParams) batchArg).isStream[i];
                }

                if ((val == null) && !isStreamParam) {
                    if (quoteStreamsAndUnknowns) {
                        buf.append("'");
                    }

                    buf.append("** NOT SPECIFIED **");

                    if (quoteStreamsAndUnknowns) {
                        buf.append("'");
                    }
                } else if (isStreamParam) {
                    if (quoteStreamsAndUnknowns) {
                        buf.append("'");
                    }

                    buf.append("** STREAM DATA **");

                    if (quoteStreamsAndUnknowns) {
                        buf.append("'");
                    }
                } else {
                    buf.append(StringUtils.toString(val, this.charEncoding));
                }
            }

            if (this.charEncoding != null) {
                buf.append(StringUtils.toString(this.staticSqlStrings[this.parameterCount + getParameterIndexOffset()], this.charEncoding));
            } else {
                buf.append(StringUtils.toAsciiString(this.staticSqlStrings[this.parameterCount + getParameterIndexOffset()]));
            }

            return buf.toString();
        }
    }

    @Override
    public void clearBatch() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            this.batchHasPlainStatements = false;

            super.clearBatch();
        }
    }

    /**
     * In general, parameter values remain in force for repeated used of a
     * Statement. Setting a parameter value automatically clears its previous
     * value. However, in some cases, it is useful to immediately release the
     * resources used by the current parameter values; this can be done by
     * calling clearParameters
     * 
     * @exception SQLException
     *                if a database access error occurs
     */
    public void clearParameters() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            for (int i = 0; i < this.parameterValues.length; i++) {
                this.parameterValues[i] = null;
                this.parameterStreams[i] = null;
                this.isStream[i] = false;
                this.isNull[i] = false;
                this.parameterTypes[i] = MysqlType.NULL;
            }
        }
    }

    private final void escapeblockFast(byte[] buf, PacketPayload packet, int size) throws SQLException {
        int lastwritten = 0;

        for (int i = 0; i < size; i++) {
            byte b = buf[i];

            if (b == '\0') {
                // write stuff not yet written
                if (i > lastwritten) {
                    packet.writeBytes(StringLengthDataType.STRING_FIXED, buf, lastwritten, i - lastwritten);
                }

                // write escape
                packet.writeInteger(IntegerDataType.INT1, (byte) '\\');
                packet.writeInteger(IntegerDataType.INT1, (byte) '0');
                lastwritten = i + 1;
            } else {
                if ((b == '\\') || (b == '\'') || (!this.usingAnsiMode && b == '"')) {
                    // write stuff not yet written
                    if (i > lastwritten) {
                        packet.writeBytes(StringLengthDataType.STRING_FIXED, buf, lastwritten, i - lastwritten);
                    }

                    // write escape
                    packet.writeInteger(IntegerDataType.INT1, (byte) '\\');
                    lastwritten = i; // not i+1 as b wasn't written.
                }
            }
        }

        // write out remaining stuff from buffer
        if (lastwritten < size) {
            packet.writeBytes(StringLengthDataType.STRING_FIXED, buf, lastwritten, size - lastwritten);
        }
    }

    private final void escapeblockFast(byte[] buf, ByteArrayOutputStream bytesOut, int size) {
        int lastwritten = 0;

        for (int i = 0; i < size; i++) {
            byte b = buf[i];

            if (b == '\0') {
                // write stuff not yet written
                if (i > lastwritten) {
                    bytesOut.write(buf, lastwritten, i - lastwritten);
                }

                // write escape
                bytesOut.write('\\');
                bytesOut.write('0');
                lastwritten = i + 1;
            } else {
                if ((b == '\\') || (b == '\'') || (!this.usingAnsiMode && b == '"')) {
                    // write stuff not yet written
                    if (i > lastwritten) {
                        bytesOut.write(buf, lastwritten, i - lastwritten);
                    }

                    // write escape
                    bytesOut.write('\\');
                    lastwritten = i; // not i+1 as b wasn't written.
                }
            }
        }

        // write out remaining stuff from buffer
        if (lastwritten < size) {
            bytesOut.write(buf, lastwritten, size - lastwritten);
        }
    }

    /**
     * Check to see if the statement is safe for read-only slaves after failover.
     * 
     * @return true if safe for read-only.
     * @throws SQLException
     */
    protected boolean checkReadOnlySafeStatement() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            return this.firstCharOfStmt == 'S' || !this.connection.isReadOnly();
        }
    }

    /**
     * Some prepared statements return multiple results; the execute method
     * handles these complex statements as well as the simpler form of
     * statements handled by executeQuery and executeUpdate
     * 
     * @return true if the next result is a ResultSet; false if it is an update
     *         count or there are no more results
     * 
     * @exception SQLException
     *                if a database access error occurs
     */
    public boolean execute() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            JdbcConnection locallyScopedConn = this.connection;

            if (!checkReadOnlySafeStatement()) {
                throw SQLError.createSQLException(Messages.getString("PreparedStatement.20") + Messages.getString("PreparedStatement.21"),
                        SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }

            ResultSetInternalMethods rs = null;

            CachedResultSetMetaData cachedMetadata = null;

            this.lastQueryIsOnDupKeyUpdate = false;

            if (this.retrieveGeneratedKeys) {
                this.lastQueryIsOnDupKeyUpdate = containsOnDuplicateKeyUpdateInSQL();
            }

            clearWarnings();

            setupStreamingTimeout(locallyScopedConn);

            this.batchedGeneratedKeys = null;

            PacketPayload sendPacket = fillSendPacket();

            String oldCatalog = null;

            if (!locallyScopedConn.getCatalog().equals(this.getCurrentCatalog())) {
                oldCatalog = locallyScopedConn.getCatalog();
                locallyScopedConn.setCatalog(this.getCurrentCatalog());
            }

            //
            // Check if we have cached metadata for this query...
            //
            boolean cacheResultSetMetadata = locallyScopedConn.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_cacheResultSetMetadata)
                    .getValue();
            if (cacheResultSetMetadata) {
                cachedMetadata = locallyScopedConn.getCachedMetaData(this.originalSql);
            }

            boolean oldInfoMsgState = false;

            if (this.retrieveGeneratedKeys) {
                oldInfoMsgState = locallyScopedConn.isReadInfoMsgEnabled();
                locallyScopedConn.setReadInfoMsgEnabled(true);
            }

            //
            // Only apply max_rows to selects
            //
            locallyScopedConn.setSessionMaxRows(this.firstCharOfStmt == 'S' ? this.maxRows : -1);

            rs = executeInternal(this.maxRows, sendPacket, createStreamingResultSet(), (this.firstCharOfStmt == 'S'), cachedMetadata, false);

            if (cachedMetadata != null) {
                locallyScopedConn.initializeResultsMetadataFromCache(this.originalSql, cachedMetadata, rs);
            } else {
                if (rs.hasRows() && cacheResultSetMetadata) {
                    locallyScopedConn.initializeResultsMetadataFromCache(this.originalSql, null /* will be created */, rs);
                }
            }

            if (this.retrieveGeneratedKeys) {
                locallyScopedConn.setReadInfoMsgEnabled(oldInfoMsgState);
                rs.setFirstCharOfQuery(this.firstCharOfStmt);
            }

            if (oldCatalog != null) {
                locallyScopedConn.setCatalog(oldCatalog);
            }

            if (rs != null) {
                this.lastInsertId = rs.getUpdateID();

                this.results = rs;
            }

            return ((rs != null) && rs.hasRows());
        }
    }

    @Override
    protected long[] executeBatchInternal() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            if (this.connection.isReadOnly()) {
                throw new SQLException(Messages.getString("PreparedStatement.25") + Messages.getString("PreparedStatement.26"),
                        SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
            }

            if (this.batchedArgs == null || this.batchedArgs.size() == 0) {
                return new long[0];
            }

            // we timeout the entire batch, not individual statements
            int batchTimeout = this.timeoutInMillis;
            this.timeoutInMillis = 0;

            resetCancelledState();

            try {
                statementBegins();

                clearWarnings();

                if (!this.batchHasPlainStatements && this.rewriteBatchedStatements.getValue()) {

                    if (canRewriteAsMultiValueInsertAtSqlLevel()) {
                        return executeBatchedInserts(batchTimeout);
                    }

                    if (!this.batchHasPlainStatements && this.batchedArgs != null && this.batchedArgs.size() > 3 /* cost of option setting rt-wise */) {
                        return executePreparedBatchAsMultiStatement(batchTimeout);
                    }
                }

                return executeBatchSerially(batchTimeout);
            } finally {
                this.statementExecuting.set(false);

                clearBatch();
            }
        }
    }

    public boolean canRewriteAsMultiValueInsertAtSqlLevel() throws SQLException {
        return this.parseInfo.canRewriteAsMultiValueInsert;
    }

    protected int getLocationOfOnDuplicateKeyUpdate() throws SQLException {
        return this.parseInfo.locationOfOnDuplicateKeyUpdate;
    }

    /**
     * Rewrites the already prepared statement into a multi-statement
     * query of 'statementsPerBatch' values and executes the entire batch
     * using this new statement.
     * 
     * @return update counts in the same fashion as executeBatch()
     * 
     * @throws SQLException
     */

    protected long[] executePreparedBatchAsMultiStatement(int batchTimeout) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            // This is kind of an abuse, but it gets the job done
            if (this.batchedValuesClause == null) {
                this.batchedValuesClause = this.originalSql + ";";
            }

            JdbcConnection locallyScopedConn = this.connection;

            boolean multiQueriesEnabled = locallyScopedConn.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_allowMultiQueries).getValue();
            CancelTask timeoutTask = null;

            try {
                clearWarnings();

                int numBatchedArgs = this.batchedArgs.size();

                if (this.retrieveGeneratedKeys) {
                    this.batchedGeneratedKeys = new ArrayList<Row>(numBatchedArgs);
                }

                int numValuesPerBatch = computeBatchSize(numBatchedArgs);

                if (numBatchedArgs < numValuesPerBatch) {
                    numValuesPerBatch = numBatchedArgs;
                }

                java.sql.PreparedStatement batchedStatement = null;

                int batchedParamIndex = 1;
                int numberToExecuteAsMultiValue = 0;
                int batchCounter = 0;
                int updateCountCounter = 0;
                long[] updateCounts = new long[numBatchedArgs];
                SQLException sqlEx = null;

                try {
                    if (!multiQueriesEnabled) {
                        locallyScopedConn.getSession().enableMultiQueries();
                    }

                    if (this.retrieveGeneratedKeys) {
                        batchedStatement = ((Wrapper) locallyScopedConn.prepareStatement(generateMultiStatementForBatch(numValuesPerBatch),
                                RETURN_GENERATED_KEYS)).unwrap(java.sql.PreparedStatement.class);
                    } else {
                        batchedStatement = ((Wrapper) locallyScopedConn.prepareStatement(generateMultiStatementForBatch(numValuesPerBatch)))
                                .unwrap(java.sql.PreparedStatement.class);
                    }

                    if (locallyScopedConn.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_enableQueryTimeouts).getValue()
                            && batchTimeout != 0) {
                        timeoutTask = new CancelTask((StatementImpl) batchedStatement);
                        locallyScopedConn.getCancelTimer().schedule(timeoutTask, batchTimeout);
                    }

                    if (numBatchedArgs < numValuesPerBatch) {
                        numberToExecuteAsMultiValue = numBatchedArgs;
                    } else {
                        numberToExecuteAsMultiValue = numBatchedArgs / numValuesPerBatch;
                    }

                    int numberArgsToExecute = numberToExecuteAsMultiValue * numValuesPerBatch;

                    for (int i = 0; i < numberArgsToExecute; i++) {
                        if (i != 0 && i % numValuesPerBatch == 0) {
                            try {
                                batchedStatement.execute();
                            } catch (SQLException ex) {
                                sqlEx = handleExceptionForBatch(batchCounter, numValuesPerBatch, updateCounts, ex);
                            }

                            updateCountCounter = processMultiCountsAndKeys((StatementImpl) batchedStatement, updateCountCounter, updateCounts);

                            batchedStatement.clearParameters();
                            batchedParamIndex = 1;
                        }

                        batchedParamIndex = setOneBatchedParameterSet(batchedStatement, batchedParamIndex, this.batchedArgs.get(batchCounter++));
                    }

                    try {
                        batchedStatement.execute();
                    } catch (SQLException ex) {
                        sqlEx = handleExceptionForBatch(batchCounter - 1, numValuesPerBatch, updateCounts, ex);
                    }

                    updateCountCounter = processMultiCountsAndKeys((StatementImpl) batchedStatement, updateCountCounter, updateCounts);

                    batchedStatement.clearParameters();

                    numValuesPerBatch = numBatchedArgs - batchCounter;
                } finally {
                    if (batchedStatement != null) {
                        batchedStatement.close();
                        batchedStatement = null;
                    }
                }

                try {
                    if (numValuesPerBatch > 0) {

                        if (this.retrieveGeneratedKeys) {
                            batchedStatement = locallyScopedConn.prepareStatement(generateMultiStatementForBatch(numValuesPerBatch), RETURN_GENERATED_KEYS);
                        } else {
                            batchedStatement = locallyScopedConn.prepareStatement(generateMultiStatementForBatch(numValuesPerBatch));
                        }

                        if (timeoutTask != null) {
                            timeoutTask.toCancel = (StatementImpl) batchedStatement;
                        }

                        batchedParamIndex = 1;

                        while (batchCounter < numBatchedArgs) {
                            batchedParamIndex = setOneBatchedParameterSet(batchedStatement, batchedParamIndex, this.batchedArgs.get(batchCounter++));
                        }

                        try {
                            batchedStatement.execute();
                        } catch (SQLException ex) {
                            sqlEx = handleExceptionForBatch(batchCounter - 1, numValuesPerBatch, updateCounts, ex);
                        }

                        updateCountCounter = processMultiCountsAndKeys((StatementImpl) batchedStatement, updateCountCounter, updateCounts);

                        batchedStatement.clearParameters();
                    }

                    if (timeoutTask != null) {
                        if (timeoutTask.caughtWhileCancelling != null) {
                            throw timeoutTask.caughtWhileCancelling;
                        }

                        timeoutTask.cancel();

                        locallyScopedConn.getCancelTimer().purge();

                        timeoutTask = null;
                    }

                    if (sqlEx != null) {
                        throw SQLError.createBatchUpdateException(sqlEx, updateCounts, getExceptionInterceptor());
                    }

                    return updateCounts;
                } finally {
                    if (batchedStatement != null) {
                        batchedStatement.close();
                    }
                }
            } finally {
                if (timeoutTask != null) {
                    timeoutTask.cancel();
                    locallyScopedConn.getCancelTimer().purge();
                }

                resetCancelledState();

                if (!multiQueriesEnabled) {
                    locallyScopedConn.getSession().disableMultiQueries();
                }

                clearBatch();
            }
        }
    }

    private String generateMultiStatementForBatch(int numBatches) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            StringBuilder newStatementSql = new StringBuilder((this.originalSql.length() + 1) * numBatches);

            newStatementSql.append(this.originalSql);

            for (int i = 0; i < numBatches - 1; i++) {
                newStatementSql.append(';');
                newStatementSql.append(this.originalSql);
            }

            return newStatementSql.toString();
        }
    }

    /**
     * Rewrites the already prepared statement into a multi-value insert
     * statement of 'statementsPerBatch' values and executes the entire batch
     * using this new statement.
     * 
     * @return update counts in the same fashion as executeBatch()
     * 
     * @throws SQLException
     */
    protected long[] executeBatchedInserts(int batchTimeout) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            String valuesClause = getValuesClause();

            JdbcConnection locallyScopedConn = this.connection;

            if (valuesClause == null) {
                return executeBatchSerially(batchTimeout);
            }

            int numBatchedArgs = this.batchedArgs.size();

            if (this.retrieveGeneratedKeys) {
                this.batchedGeneratedKeys = new ArrayList<Row>(numBatchedArgs);
            }

            int numValuesPerBatch = computeBatchSize(numBatchedArgs);

            if (numBatchedArgs < numValuesPerBatch) {
                numValuesPerBatch = numBatchedArgs;
            }

            PreparedStatement batchedStatement = null;

            int batchedParamIndex = 1;
            long updateCountRunningTotal = 0;
            int numberToExecuteAsMultiValue = 0;
            int batchCounter = 0;
            CancelTask timeoutTask = null;
            SQLException sqlEx = null;

            long[] updateCounts = new long[numBatchedArgs];

            try {
                try {
                    batchedStatement = /* FIXME -if we ever care about folks proxying our JdbcConnection */
                    prepareBatchedInsertSQL(locallyScopedConn, numValuesPerBatch);

                    if (locallyScopedConn.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_enableQueryTimeouts).getValue()
                            && batchTimeout != 0) {
                        timeoutTask = new CancelTask(batchedStatement);
                        locallyScopedConn.getCancelTimer().schedule(timeoutTask, batchTimeout);
                    }

                    if (numBatchedArgs < numValuesPerBatch) {
                        numberToExecuteAsMultiValue = numBatchedArgs;
                    } else {
                        numberToExecuteAsMultiValue = numBatchedArgs / numValuesPerBatch;
                    }

                    int numberArgsToExecute = numberToExecuteAsMultiValue * numValuesPerBatch;

                    for (int i = 0; i < numberArgsToExecute; i++) {
                        if (i != 0 && i % numValuesPerBatch == 0) {
                            try {
                                updateCountRunningTotal += batchedStatement.executeLargeUpdate();
                            } catch (SQLException ex) {
                                sqlEx = handleExceptionForBatch(batchCounter - 1, numValuesPerBatch, updateCounts, ex);
                            }

                            getBatchedGeneratedKeys(batchedStatement);
                            batchedStatement.clearParameters();
                            batchedParamIndex = 1;

                        }

                        batchedParamIndex = setOneBatchedParameterSet(batchedStatement, batchedParamIndex, this.batchedArgs.get(batchCounter++));
                    }

                    try {
                        updateCountRunningTotal += batchedStatement.executeLargeUpdate();
                    } catch (SQLException ex) {
                        sqlEx = handleExceptionForBatch(batchCounter - 1, numValuesPerBatch, updateCounts, ex);
                    }

                    getBatchedGeneratedKeys(batchedStatement);

                    numValuesPerBatch = numBatchedArgs - batchCounter;
                } finally {
                    if (batchedStatement != null) {
                        batchedStatement.close();
                        batchedStatement = null;
                    }
                }

                try {
                    if (numValuesPerBatch > 0) {
                        batchedStatement = prepareBatchedInsertSQL(locallyScopedConn, numValuesPerBatch);

                        if (timeoutTask != null) {
                            timeoutTask.toCancel = batchedStatement;
                        }

                        batchedParamIndex = 1;

                        while (batchCounter < numBatchedArgs) {
                            batchedParamIndex = setOneBatchedParameterSet(batchedStatement, batchedParamIndex, this.batchedArgs.get(batchCounter++));
                        }

                        try {
                            updateCountRunningTotal += batchedStatement.executeLargeUpdate();
                        } catch (SQLException ex) {
                            sqlEx = handleExceptionForBatch(batchCounter - 1, numValuesPerBatch, updateCounts, ex);
                        }

                        getBatchedGeneratedKeys(batchedStatement);
                    }

                    if (sqlEx != null) {
                        throw SQLError.createBatchUpdateException(sqlEx, updateCounts, getExceptionInterceptor());
                    }

                    if (numBatchedArgs > 1) {
                        long updCount = updateCountRunningTotal > 0 ? java.sql.Statement.SUCCESS_NO_INFO : 0;
                        for (int j = 0; j < numBatchedArgs; j++) {
                            updateCounts[j] = updCount;
                        }
                    } else {
                        updateCounts[0] = updateCountRunningTotal;
                    }
                    return updateCounts;
                } finally {
                    if (batchedStatement != null) {
                        batchedStatement.close();
                    }
                }
            } finally {
                if (timeoutTask != null) {
                    timeoutTask.cancel();
                    locallyScopedConn.getCancelTimer().purge();
                }

                resetCancelledState();
            }
        }
    }

    protected String getValuesClause() throws SQLException {
        return this.parseInfo.valuesClause;
    }

    /**
     * Computes the optimum number of batched parameter lists to send
     * without overflowing max_allowed_packet.
     * 
     * @param numBatchedArgs
     * @throws SQLException
     */
    protected int computeBatchSize(int numBatchedArgs) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            long[] combinedValues = computeMaxParameterSetSizeAndBatchSize(numBatchedArgs);

            long maxSizeOfParameterSet = combinedValues[0];
            long sizeOfEntireBatch = combinedValues[1];

            if (sizeOfEntireBatch < this.maxAllowedPacket.getValue() - this.originalSql.length()) {
                return numBatchedArgs;
            }

            return (int) Math.max(1, (this.maxAllowedPacket.getValue() - this.originalSql.length()) / maxSizeOfParameterSet);
        }
    }

    /**
     * Computes the maximum parameter set size, and entire batch size given
     * the number of arguments in the batch.
     * 
     * @throws SQLException
     */
    protected long[] computeMaxParameterSetSizeAndBatchSize(int numBatchedArgs) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            long sizeOfEntireBatch = 0;
            long maxSizeOfParameterSet = 0;

            for (int i = 0; i < numBatchedArgs; i++) {
                BatchParams paramArg = (BatchParams) this.batchedArgs.get(i);

                boolean[] isNullBatch = paramArg.isNull;
                boolean[] isStreamBatch = paramArg.isStream;

                long sizeOfParameterSet = 0;

                for (int j = 0; j < isNullBatch.length; j++) {
                    if (!isNullBatch[j]) {

                        if (isStreamBatch[j]) {
                            int streamLength = paramArg.streamLengths[j];

                            if (streamLength != -1) {
                                sizeOfParameterSet += streamLength * 2; // for safety in escaping
                            } else {
                                int paramLength = paramArg.parameterStrings[j].length;
                                sizeOfParameterSet += paramLength;
                            }
                        } else {
                            sizeOfParameterSet += paramArg.parameterStrings[j].length;
                        }
                    } else {
                        sizeOfParameterSet += 4; // for NULL literal in SQL 
                    }
                }

                //
                // Account for static part of values clause
                // This is a little naive, because the ?s will be replaced but it gives us some padding, and is less housekeeping to ignore them. We're looking
                // for a "fuzzy" value here anyway
                //

                if (getValuesClause() != null) {
                    sizeOfParameterSet += getValuesClause().length() + 1;
                } else {
                    sizeOfParameterSet += this.originalSql.length() + 1;
                }

                sizeOfEntireBatch += sizeOfParameterSet;

                if (sizeOfParameterSet > maxSizeOfParameterSet) {
                    maxSizeOfParameterSet = sizeOfParameterSet;
                }
            }

            return new long[] { maxSizeOfParameterSet, sizeOfEntireBatch };
        }
    }

    /**
     * Executes the current batch of statements by executing them one-by-one.
     * 
     * @return a list of update counts
     * @throws SQLException
     *             if an error occurs
     */
    protected long[] executeBatchSerially(int batchTimeout) throws SQLException {

        synchronized (checkClosed().getConnectionMutex()) {
            JdbcConnection locallyScopedConn = this.connection;

            if (locallyScopedConn == null) {
                checkClosed();
            }

            long[] updateCounts = null;

            if (this.batchedArgs != null) {
                int nbrCommands = this.batchedArgs.size();
                updateCounts = new long[nbrCommands];

                for (int i = 0; i < nbrCommands; i++) {
                    updateCounts[i] = -3;
                }

                SQLException sqlEx = null;

                CancelTask timeoutTask = null;

                try {
                    if (locallyScopedConn.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_enableQueryTimeouts).getValue()
                            && batchTimeout != 0) {
                        timeoutTask = new CancelTask(this);
                        locallyScopedConn.getCancelTimer().schedule(timeoutTask, batchTimeout);
                    }

                    if (this.retrieveGeneratedKeys) {
                        this.batchedGeneratedKeys = new ArrayList<Row>(nbrCommands);
                    }

                    for (this.batchCommandIndex = 0; this.batchCommandIndex < nbrCommands; this.batchCommandIndex++) {
                        Object arg = this.batchedArgs.get(this.batchCommandIndex);

                        try {
                            if (arg instanceof String) {
                                updateCounts[this.batchCommandIndex] = executeUpdateInternal((String) arg, true, this.retrieveGeneratedKeys);

                                // limit one generated key per OnDuplicateKey statement
                                getBatchedGeneratedKeys(this.results.getFirstCharOfQuery() == 'I' && containsOnDuplicateKeyInString((String) arg) ? 1 : 0);
                            } else {
                                BatchParams paramArg = (BatchParams) arg;
                                updateCounts[this.batchCommandIndex] = executeUpdateInternal(paramArg.parameterStrings, paramArg.parameterStreams,
                                        paramArg.isStream, paramArg.streamLengths, paramArg.isNull, true);

                                // limit one generated key per OnDuplicateKey statement
                                getBatchedGeneratedKeys(containsOnDuplicateKeyUpdateInSQL() ? 1 : 0);
                            }
                        } catch (SQLException ex) {
                            updateCounts[this.batchCommandIndex] = EXECUTE_FAILED;

                            if (this.continueBatchOnError && !(ex instanceof MySQLTimeoutException) && !(ex instanceof MySQLStatementCancelledException)
                                    && !hasDeadlockOrTimeoutRolledBackTx(ex)) {
                                sqlEx = ex;
                            } else {
                                long[] newUpdateCounts = new long[this.batchCommandIndex];
                                System.arraycopy(updateCounts, 0, newUpdateCounts, 0, this.batchCommandIndex);

                                throw SQLError.createBatchUpdateException(ex, newUpdateCounts, getExceptionInterceptor());
                            }
                        }
                    }

                    if (sqlEx != null) {
                        throw SQLError.createBatchUpdateException(sqlEx, updateCounts, getExceptionInterceptor());
                    }
                } catch (NullPointerException npe) {
                    try {
                        checkClosed();
                    } catch (StatementIsClosedException connectionClosedEx) {
                        updateCounts[this.batchCommandIndex] = EXECUTE_FAILED;

                        long[] newUpdateCounts = new long[this.batchCommandIndex];

                        System.arraycopy(updateCounts, 0, newUpdateCounts, 0, this.batchCommandIndex);

                        throw SQLError.createBatchUpdateException(SQLExceptionsMapping.translateException(connectionClosedEx), newUpdateCounts,
                                getExceptionInterceptor());
                    }

                    throw npe; // we don't know why this happened, punt
                } finally {
                    this.batchCommandIndex = -1;

                    if (timeoutTask != null) {
                        timeoutTask.cancel();
                        locallyScopedConn.getCancelTimer().purge();
                    }

                    resetCancelledState();
                }
            }

            return (updateCounts != null) ? updateCounts : new long[0];
        }

    }

    public String getDateTime(String pattern) {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        return sdf.format(new java.util.Date());
    }

    /**
     * Actually execute the prepared statement. This is here so server-side
     * PreparedStatements can re-use most of the code from this class.
     * 
     * @param maxRowsToRetrieve
     *            the max number of rows to return
     * @param sendPacket
     *            the packet to send
     * @param createStreamingResultSet
     *            should a 'streaming' result set be created?
     * @param queryIsSelectOnly
     *            is this query doing a SELECT?
     * @param metadata
     *            use this metadata instead of the one provided on wire
     * @param isBatch
     * 
     * @return the results as a ResultSet
     * 
     * @throws SQLException
     *             if an error occurs.
     */
    protected ResultSetInternalMethods executeInternal(int maxRowsToRetrieve, PacketPayload sendPacket, boolean createStreamingResultSet,
            boolean queryIsSelectOnly, ColumnDefinition metadata, boolean isBatch) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            try {

                resetCancelledState();

                JdbcConnection locallyScopedConnection = this.connection;

                this.numberOfExecutions++;

                if (this.doPingInstead) {
                    doPingInstead();

                    return this.results;
                }

                ResultSetInternalMethods rs;

                CancelTask timeoutTask = null;

                try {
                    if (locallyScopedConnection.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_enableQueryTimeouts).getValue()
                            && this.timeoutInMillis != 0) {
                        timeoutTask = new CancelTask(this);
                        locallyScopedConnection.getCancelTimer().schedule(timeoutTask, this.timeoutInMillis);
                    }

                    if (!isBatch) {
                        statementBegins();
                    }

                    rs = locallyScopedConnection.execSQL(this, null, maxRowsToRetrieve, sendPacket, createStreamingResultSet, this.getCurrentCatalog(),
                            metadata, isBatch);

                    if (timeoutTask != null) {
                        timeoutTask.cancel();

                        locallyScopedConnection.getCancelTimer().purge();

                        if (timeoutTask.caughtWhileCancelling != null) {
                            throw timeoutTask.caughtWhileCancelling;
                        }

                        timeoutTask = null;
                    }

                    synchronized (this.cancelTimeoutMutex) {
                        if (this.wasCancelled) {
                            SQLException cause = null;

                            if (this.wasCancelledByTimeout) {
                                cause = new MySQLTimeoutException();
                            } else {
                                cause = new MySQLStatementCancelledException();
                            }

                            resetCancelledState();

                            throw cause;
                        }
                    }
                } finally {
                    if (!isBatch) {
                        this.statementExecuting.set(false);
                    }

                    if (timeoutTask != null) {
                        timeoutTask.cancel();
                        locallyScopedConnection.getCancelTimer().purge();
                    }
                }

                return rs;
            } catch (NullPointerException npe) {
                checkClosed(); // we can't synchronize ourselves against async connection-close due to deadlock issues, so this is the next best thing for
                              // this particular corner case.

                throw npe;
            }
        }
    }

    /**
     * A Prepared SQL query is executed and its ResultSet is returned
     * 
     * @return a ResultSet that contains the data produced by the query - never
     *         null
     * 
     * @exception SQLException
     *                if a database access error occurs
     */
    public java.sql.ResultSet executeQuery() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            JdbcConnection locallyScopedConn = this.connection;

            checkForDml(this.originalSql, this.firstCharOfStmt);

            CachedResultSetMetaData cachedMetadata = null;

            clearWarnings();

            this.batchedGeneratedKeys = null;

            setupStreamingTimeout(locallyScopedConn);

            PacketPayload sendPacket = fillSendPacket();

            implicitlyCloseAllOpenResults();

            String oldCatalog = null;

            if (!locallyScopedConn.getCatalog().equals(this.getCurrentCatalog())) {
                oldCatalog = locallyScopedConn.getCatalog();
                locallyScopedConn.setCatalog(this.getCurrentCatalog());
            }

            //
            // Check if we have cached metadata for this query...
            //
            boolean cacheResultSetMetadata = locallyScopedConn.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_cacheResultSetMetadata)
                    .getValue();
            if (cacheResultSetMetadata) {
                cachedMetadata = locallyScopedConn.getCachedMetaData(this.originalSql);
            }

            locallyScopedConn.setSessionMaxRows(this.maxRows);

            this.results = executeInternal(this.maxRows, sendPacket, createStreamingResultSet(), true, cachedMetadata, false);

            if (oldCatalog != null) {
                locallyScopedConn.setCatalog(oldCatalog);
            }

            if (cachedMetadata != null) {
                locallyScopedConn.initializeResultsMetadataFromCache(this.originalSql, cachedMetadata, this.results);
            } else {
                if (cacheResultSetMetadata) {
                    locallyScopedConn.initializeResultsMetadataFromCache(this.originalSql, null /* will be created */, this.results);
                }
            }

            this.lastInsertId = this.results.getUpdateID();

            return this.results;
        }
    }

    /**
     * Execute a SQL INSERT, UPDATE or DELETE statement. In addition, SQL
     * statements that return nothing such as SQL DDL statements can be
     * executed.
     * 
     * @return either the row count for INSERT, UPDATE or DELETE; or 0 for SQL
     *         statements that return nothing.
     * 
     * @exception SQLException
     *                if a database access error occurs
     */
    public int executeUpdate() throws SQLException {
        return Util.truncateAndConvertToInt(executeLargeUpdate());
    }

    /*
     * We need this variant, because ServerPreparedStatement calls this for
     * batched updates, which will end up clobbering the warnings and generated
     * keys we need to gather for the batch.
     */
    protected long executeUpdateInternal(boolean clearBatchedGeneratedKeysAndWarnings, boolean isBatch) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (clearBatchedGeneratedKeysAndWarnings) {
                clearWarnings();
                this.batchedGeneratedKeys = null;
            }

            return executeUpdateInternal(this.parameterValues, this.parameterStreams, this.isStream, this.streamLengths, this.isNull, isBatch);
        }
    }

    /**
     * Added to allow batch-updates
     * 
     * @param batchedParameterStrings
     *            string values used in single statement
     * @param batchedParameterStreams
     *            stream values used in single statement
     * @param batchedIsStream
     *            flags for streams used in single statement
     * @param batchedStreamLengths
     *            lengths of streams to be read.
     * @param batchedIsNull
     *            flags for parameters that are null
     * 
     * @return the update count
     * 
     * @throws SQLException
     *             if a database error occurs
     */
    protected long executeUpdateInternal(byte[][] batchedParameterStrings, InputStream[] batchedParameterStreams, boolean[] batchedIsStream,
            int[] batchedStreamLengths, boolean[] batchedIsNull, boolean isReallyBatch) throws SQLException {

        synchronized (checkClosed().getConnectionMutex()) {

            JdbcConnection locallyScopedConn = this.connection;

            if (locallyScopedConn.isReadOnly(false)) {
                throw SQLError.createSQLException(Messages.getString("PreparedStatement.34") + Messages.getString("PreparedStatement.35"),
                        SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }

            if ((this.firstCharOfStmt == 'S') && isSelectQuery()) {
                throw SQLError.createSQLException(Messages.getString("PreparedStatement.37"), "01S03", getExceptionInterceptor());
            }

            implicitlyCloseAllOpenResults();

            ResultSetInternalMethods rs = null;

            PacketPayload sendPacket = fillSendPacket(batchedParameterStrings, batchedParameterStreams, batchedIsStream, batchedStreamLengths);

            String oldCatalog = null;

            if (!locallyScopedConn.getCatalog().equals(this.getCurrentCatalog())) {
                oldCatalog = locallyScopedConn.getCatalog();
                locallyScopedConn.setCatalog(this.getCurrentCatalog());
            }

            //
            // Only apply max_rows to selects
            //
            locallyScopedConn.setSessionMaxRows(-1);

            boolean oldInfoMsgState = false;

            if (this.retrieveGeneratedKeys) {
                oldInfoMsgState = locallyScopedConn.isReadInfoMsgEnabled();
                locallyScopedConn.setReadInfoMsgEnabled(true);
            }

            rs = executeInternal(-1, sendPacket, false, false, null, isReallyBatch);

            if (this.retrieveGeneratedKeys) {
                locallyScopedConn.setReadInfoMsgEnabled(oldInfoMsgState);
                rs.setFirstCharOfQuery(this.firstCharOfStmt);
            }

            if (oldCatalog != null) {
                locallyScopedConn.setCatalog(oldCatalog);
            }

            this.results = rs;

            this.updateCount = rs.getUpdateCount();

            if (containsOnDuplicateKeyUpdateInSQL() && this.compensateForOnDuplicateKeyUpdate) {
                if (this.updateCount == 2 || this.updateCount == 0) {
                    this.updateCount = 1;
                }
            }

            this.lastInsertId = rs.getUpdateID();

            return this.updateCount;
        }
    }

    protected boolean containsOnDuplicateKeyUpdateInSQL() {
        return this.parseInfo.isOnDuplicateKeyUpdate;
    }

    /**
     * Creates the packet that contains the query to be sent to the server.
     * 
     * @return A Buffer filled with the query representing the
     *         PreparedStatement.
     * 
     * @throws SQLException
     *             if an error occurs.
     */
    protected PacketPayload fillSendPacket() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            return fillSendPacket(this.parameterValues, this.parameterStreams, this.isStream, this.streamLengths);
        }
    }

    /**
     * Creates the packet that contains the query to be sent to the server.
     * 
     * @param batchedParameterStrings
     *            the parameters as strings
     * @param batchedParameterStreams
     *            the parameters as streams
     * @param batchedIsStream
     *            is the given parameter a stream?
     * @param batchedStreamLengths
     *            the lengths of the streams (if appropriate)
     * 
     * @return a Buffer filled with the query that represents this statement
     * 
     * @throws SQLException
     *             if an error occurs.
     */
    protected PacketPayload fillSendPacket(byte[][] batchedParameterStrings, InputStream[] batchedParameterStreams, boolean[] batchedIsStream,
            int[] batchedStreamLengths) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            PacketPayload sendPacket = this.session.getSharedSendPacket();

            sendPacket.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_QUERY);

            boolean useStreamLengths = this.useStreamLengthsInPrepStmts.getValue();

            //
            // Try and get this allocation as close as possible for BLOBs
            //
            int ensurePacketSize = 0;

            String statementComment = this.connection.getStatementComment();

            byte[] commentAsBytes = null;

            if (statementComment != null) {
                commentAsBytes = StringUtils.getBytes(statementComment, this.charEncoding);

                ensurePacketSize += commentAsBytes.length;
                ensurePacketSize += 6; // for /*[space] [space]*/
            }

            for (int i = 0; i < batchedParameterStrings.length; i++) {
                if (batchedIsStream[i] && useStreamLengths) {
                    ensurePacketSize += batchedStreamLengths[i];
                }
            }

            if (ensurePacketSize != 0) {
                sendPacket.ensureCapacity(ensurePacketSize);
            }

            if (commentAsBytes != null) {
                sendPacket.writeBytes(StringLengthDataType.STRING_FIXED, Constants.SLASH_STAR_SPACE_AS_BYTES);
                sendPacket.writeBytes(StringLengthDataType.STRING_FIXED, commentAsBytes);
                sendPacket.writeBytes(StringLengthDataType.STRING_FIXED, Constants.SPACE_STAR_SLASH_SPACE_AS_BYTES);
            }

            for (int i = 0; i < batchedParameterStrings.length; i++) {
                checkAllParametersSet(batchedParameterStrings[i], batchedParameterStreams[i], i);

                sendPacket.writeBytes(StringLengthDataType.STRING_FIXED, this.staticSqlStrings[i]);

                if (batchedIsStream[i]) {
                    streamToBytes(sendPacket, batchedParameterStreams[i], true, batchedStreamLengths[i], useStreamLengths);
                } else {
                    sendPacket.writeBytes(StringLengthDataType.STRING_FIXED, batchedParameterStrings[i]);
                }
            }

            sendPacket.writeBytes(StringLengthDataType.STRING_FIXED, this.staticSqlStrings[batchedParameterStrings.length]);

            return sendPacket;
        }
    }

    private void checkAllParametersSet(byte[] parameterString, InputStream parameterStream, int columnIndex) throws SQLException {
        if ((parameterString == null) && parameterStream == null) {

            throw SQLError.createSQLException(Messages.getString("PreparedStatement.40") + (columnIndex + 1), SQLError.SQL_STATE_WRONG_NO_OF_PARAMETERS,
                    getExceptionInterceptor());
        }
    }

    /**
     * Returns a prepared statement for the number of batched parameters, used when re-writing batch INSERTs.
     */
    protected PreparedStatement prepareBatchedInsertSQL(JdbcConnection localConn, int numBatches) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            PreparedStatement pstmt = new PreparedStatement(localConn, "Rewritten batch of: " + this.originalSql, this.getCurrentCatalog(),
                    this.parseInfo.getParseInfoForBatch(numBatches));
            pstmt.setRetrieveGeneratedKeys(this.retrieveGeneratedKeys);
            pstmt.rewrittenBatchSize = numBatches;

            return pstmt;
        }
    }

    protected void setRetrieveGeneratedKeys(boolean flag) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            this.retrieveGeneratedKeys = flag;
        }
    }

    protected int rewrittenBatchSize = 0;

    public int getRewrittenBatchSize() {
        return this.rewrittenBatchSize;
    }

    public String getNonRewrittenSql() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            int indexOfBatch = this.originalSql.indexOf(" of: ");

            if (indexOfBatch != -1) {
                return this.originalSql.substring(indexOfBatch + 5);
            }

            return this.originalSql;
        }
    }

    /**
     * @param parameterIndex
     * 
     * @throws SQLException
     */
    public byte[] getBytesRepresentation(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (this.isStream[parameterIndex]) {
                return streamToBytes(this.parameterStreams[parameterIndex], false, this.streamLengths[parameterIndex],
                        this.useStreamLengthsInPrepStmts.getValue());
            }

            byte[] parameterVal = this.parameterValues[parameterIndex];

            if (parameterVal == null) {
                return null;
            }

            if ((parameterVal[0] == '\'') && (parameterVal[parameterVal.length - 1] == '\'')) {
                byte[] valNoQuotes = new byte[parameterVal.length - 2];
                System.arraycopy(parameterVal, 1, valNoQuotes, 0, parameterVal.length - 2);

                return valNoQuotes;
            }

            return parameterVal;
        }
    }

    /**
     * Get bytes representation for a parameter in a statement batch.
     * 
     * @param parameterIndex
     * @param commandIndex
     * @throws SQLException
     */
    protected byte[] getBytesRepresentationForBatch(int parameterIndex, int commandIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            Object batchedArg = this.batchedArgs.get(commandIndex);
            if (batchedArg instanceof String) {
                return StringUtils.getBytes((String) batchedArg, this.charEncoding);
            }

            BatchParams params = (BatchParams) batchedArg;
            if (params.isStream[parameterIndex]) {
                return streamToBytes(params.parameterStreams[parameterIndex], false, params.streamLengths[parameterIndex],
                        this.useStreamLengthsInPrepStmts.getValue());
            }
            byte parameterVal[] = params.parameterStrings[parameterIndex];
            if (parameterVal == null) {
                return null;
            }

            if ((parameterVal[0] == '\'') && (parameterVal[parameterVal.length - 1] == '\'')) {
                byte[] valNoQuotes = new byte[parameterVal.length - 2];
                System.arraycopy(parameterVal, 1, valNoQuotes, 0, parameterVal.length - 2);

                return valNoQuotes;
            }

            return parameterVal;
        }
    }

    // --------------------------JDBC 2.0-----------------------------

    private final String getDateTimePattern(String dt, boolean toTime) throws IOException {
        //
        // Special case
        //
        int dtLength = (dt != null) ? dt.length() : 0;

        if ((dtLength >= 8) && (dtLength <= 10)) {
            int dashCount = 0;
            boolean isDateOnly = true;

            for (int i = 0; i < dtLength; i++) {
                char c = dt.charAt(i);

                if (!Character.isDigit(c) && (c != '-')) {
                    isDateOnly = false;

                    break;
                }

                if (c == '-') {
                    dashCount++;
                }
            }

            if (isDateOnly && (dashCount == 2)) {
                return "yyyy-MM-dd";
            }
        }

        //
        // Special case - time-only
        //
        boolean colonsOnly = true;

        for (int i = 0; i < dtLength; i++) {
            char c = dt.charAt(i);

            if (!Character.isDigit(c) && (c != ':')) {
                colonsOnly = false;

                break;
            }
        }

        if (colonsOnly) {
            return "HH:mm:ss";
        }

        int n;
        int z;
        int count;
        int maxvecs;
        char c;
        char separator;
        StringReader reader = new StringReader(dt + " ");
        ArrayList<Object[]> vec = new ArrayList<Object[]>();
        ArrayList<Object[]> vecRemovelist = new ArrayList<Object[]>();
        Object[] nv = new Object[3];
        Object[] v;
        nv[0] = Character.valueOf('y');
        nv[1] = new StringBuilder();
        nv[2] = Integer.valueOf(0);
        vec.add(nv);

        if (toTime) {
            nv = new Object[3];
            nv[0] = Character.valueOf('h');
            nv[1] = new StringBuilder();
            nv[2] = Integer.valueOf(0);
            vec.add(nv);
        }

        while ((z = reader.read()) != -1) {
            separator = (char) z;
            maxvecs = vec.size();

            for (count = 0; count < maxvecs; count++) {
                v = vec.get(count);
                n = ((Integer) v[2]).intValue();
                c = getSuccessor(((Character) v[0]).charValue(), n);

                if (!Character.isLetterOrDigit(separator)) {
                    if ((c == ((Character) v[0]).charValue()) && (c != 'S')) {
                        vecRemovelist.add(v);
                    } else {
                        ((StringBuilder) v[1]).append(separator);

                        if ((c == 'X') || (c == 'Y')) {
                            v[2] = Integer.valueOf(4);
                        }
                    }
                } else {
                    if (c == 'X') {
                        c = 'y';
                        nv = new Object[3];
                        nv[1] = (new StringBuilder(((StringBuilder) v[1]).toString())).append('M');
                        nv[0] = Character.valueOf('M');
                        nv[2] = Integer.valueOf(1);
                        vec.add(nv);
                    } else if (c == 'Y') {
                        c = 'M';
                        nv = new Object[3];
                        nv[1] = (new StringBuilder(((StringBuilder) v[1]).toString())).append('d');
                        nv[0] = Character.valueOf('d');
                        nv[2] = Integer.valueOf(1);
                        vec.add(nv);
                    }

                    ((StringBuilder) v[1]).append(c);

                    if (c == ((Character) v[0]).charValue()) {
                        v[2] = Integer.valueOf(n + 1);
                    } else {
                        v[0] = Character.valueOf(c);
                        v[2] = Integer.valueOf(1);
                    }
                }
            }

            int size = vecRemovelist.size();

            for (int i = 0; i < size; i++) {
                v = vecRemovelist.get(i);
                vec.remove(v);
            }

            vecRemovelist.clear();
        }

        int size = vec.size();

        for (int i = 0; i < size; i++) {
            v = vec.get(i);
            c = ((Character) v[0]).charValue();
            n = ((Integer) v[2]).intValue();

            boolean bk = getSuccessor(c, n) != c;
            boolean atEnd = (((c == 's') || (c == 'm') || ((c == 'h') && toTime)) && bk);
            boolean finishesAtDate = (bk && (c == 'd') && !toTime);
            boolean containsEnd = (((StringBuilder) v[1]).toString().indexOf('W') != -1);

            if ((!atEnd && !finishesAtDate) || (containsEnd)) {
                vecRemovelist.add(v);
            }
        }

        size = vecRemovelist.size();

        for (int i = 0; i < size; i++) {
            vec.remove(vecRemovelist.get(i));
        }

        vecRemovelist.clear();
        v = vec.get(0); // might throw exception

        StringBuilder format = (StringBuilder) v[1];
        format.setLength(format.length() - 1);

        return format.toString();
    }

    /**
     * The number, types and properties of a ResultSet's columns are provided by
     * the getMetaData method.
     * 
     * @return the description of a ResultSet's columns
     * 
     * @exception SQLException
     *                if a database-access error occurs.
     */
    public java.sql.ResultSetMetaData getMetaData() throws SQLException {

        synchronized (checkClosed().getConnectionMutex()) {
            //
            // We could just tack on a LIMIT 0 here no matter what the  statement, and check if a result set was returned or not, but I'm not comfortable with
            // that, myself, so we take the "safer" road, and only allow metadata for _actual_ SELECTS (but not SHOWs).
            // 
            // CALL's are trapped further up and you end up with a  CallableStatement anyway.
            //

            if (!isSelectQuery()) {
                return null;
            }

            PreparedStatement mdStmt = null;
            java.sql.ResultSet mdRs = null;

            if (this.pstmtResultMetaData == null) {
                try {
                    mdStmt = new PreparedStatement(this.connection, this.originalSql, this.getCurrentCatalog(), this.parseInfo);

                    mdStmt.setMaxRows(1);

                    int paramCount = this.parameterValues.length;

                    for (int i = 1; i <= paramCount; i++) {
                        mdStmt.setString(i, "");
                    }

                    boolean hadResults = mdStmt.execute();

                    if (hadResults) {
                        mdRs = mdStmt.getResultSet();

                        this.pstmtResultMetaData = mdRs.getMetaData();
                    } else {
                        this.pstmtResultMetaData = new ResultSetMetaData(this.session, new Field[0],
                                this.session.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useOldAliasMetadataBehavior).getValue(),
                                this.session.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_yearIsDateType).getValue(),
                                getExceptionInterceptor());
                    }
                } finally {
                    SQLException sqlExRethrow = null;

                    if (mdRs != null) {
                        try {
                            mdRs.close();
                        } catch (SQLException sqlEx) {
                            sqlExRethrow = sqlEx;
                        }

                        mdRs = null;
                    }

                    if (mdStmt != null) {
                        try {
                            mdStmt.close();
                        } catch (SQLException sqlEx) {
                            sqlExRethrow = sqlEx;
                        }

                        mdStmt = null;
                    }

                    if (sqlExRethrow != null) {
                        throw sqlExRethrow;
                    }
                }
            }

            return this.pstmtResultMetaData;
        }
    }

    protected boolean isSelectQuery() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            return StringUtils.startsWithIgnoreCaseAndWs(StringUtils.stripComments(this.originalSql, "'\"", "'\"", true, false, true, true), "SELECT");
        }
    }

    /**
     * @see PreparedStatement#getParameterMetaData()
     */
    public ParameterMetaData getParameterMetaData() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (this.parameterMetaData == null) {
                if (this.session.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_generateSimpleParameterMetadata).getValue()) {
                    this.parameterMetaData = new MysqlParameterMetadata(this.parameterCount);
                } else {
                    this.parameterMetaData = new MysqlParameterMetadata(this.session, null, this.parameterCount, getExceptionInterceptor());
                }
            }

            return this.parameterMetaData;
        }
    }

    ParseInfo getParseInfo() {
        return this.parseInfo;
    }

    private final char getSuccessor(char c, int n) {
        return ((c == 'y') && (n == 2)) ? 'X'
                : (((c == 'y') && (n < 4)) ? 'y' : ((c == 'y') ? 'M' : (((c == 'M') && (n == 2)) ? 'Y'
                        : (((c == 'M') && (n < 3)) ? 'M' : ((c == 'M') ? 'd' : (((c == 'd') && (n < 2)) ? 'd' : ((c == 'd') ? 'H' : (((c == 'H') && (n < 2))
                                ? 'H'
                                : ((c == 'H') ? 'm' : (((c == 'm') && (n < 2)) ? 'm' : ((c == 'm') ? 's' : (((c == 's') && (n < 2)) ? 's' : 'W'))))))))))));
    }

    /**
     * Used to escape binary data with hex for mb charsets
     * 
     * @param buf
     * @param packet
     * @param size
     * @throws SQLException
     */
    private final void hexEscapeBlock(byte[] buf, PacketPayload packet, int size) throws SQLException {
        for (int i = 0; i < size; i++) {
            byte b = buf[i];
            int lowBits = (b & 0xff) / 16;
            int highBits = (b & 0xff) % 16;

            packet.writeInteger(IntegerDataType.INT1, HEX_DIGITS[lowBits]);
            packet.writeInteger(IntegerDataType.INT1, HEX_DIGITS[highBits]);
        }
    }

    private void initializeFromParseInfo() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            this.staticSqlStrings = this.parseInfo.staticSql;
            this.isLoadDataQuery = this.parseInfo.foundLoadData;
            this.firstCharOfStmt = this.parseInfo.firstStmtChar;

            this.parameterCount = this.staticSqlStrings.length - 1;

            this.parameterValues = new byte[this.parameterCount][];
            this.parameterStreams = new InputStream[this.parameterCount];
            this.isStream = new boolean[this.parameterCount];
            this.streamLengths = new int[this.parameterCount];
            this.isNull = new boolean[this.parameterCount];
            this.parameterTypes = new MysqlType[this.parameterCount];

            clearParameters();

            for (int j = 0; j < this.parameterCount; j++) {
                this.isStream[j] = false;
            }
        }
    }

    public boolean isNull(int paramIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            return this.isNull[paramIndex];
        }
    }

    private final int readblock(InputStream i, byte[] b) throws SQLException {
        try {
            return i.read(b);
        } catch (Throwable ex) {
            throw SQLError.createSQLException(Messages.getString("PreparedStatement.56") + ex.getClass().getName(), SQLError.SQL_STATE_GENERAL_ERROR, ex,
                    getExceptionInterceptor());
        }
    }

    private final int readblock(InputStream i, byte[] b, int length) throws SQLException {
        try {
            int lengthToRead = length;

            if (lengthToRead > b.length) {
                lengthToRead = b.length;
            }

            return i.read(b, 0, lengthToRead);
        } catch (Throwable ex) {
            throw SQLError.createSQLException(Messages.getString("PreparedStatement.56") + ex.getClass().getName(), SQLError.SQL_STATE_GENERAL_ERROR, ex,
                    getExceptionInterceptor());
        }
    }

    /**
     * Closes this statement, releasing all resources
     * 
     * @param calledExplicitly
     *            was this called by close()?
     * 
     * @throws SQLException
     *             if an error occurs
     */
    @Override
    public void realClose(boolean calledExplicitly, boolean closeOpenResults) throws SQLException {
        JdbcConnection locallyScopedConn = this.connection;

        if (locallyScopedConn == null) {
            return; // already closed
        }

        synchronized (locallyScopedConn.getConnectionMutex()) {

            // additional check in case Statement was closed
            // while current thread was waiting for lock
            if (this.isClosed) {
                return;
            }

            if (this.useUsageAdvisor) {
                if (this.numberOfExecutions <= 1) {
                    String message = Messages.getString("PreparedStatement.43");

                    this.eventSink.consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_WARN, "", this.getCurrentCatalog(), this.connectionId, this.getId(),
                            -1, System.currentTimeMillis(), 0, Constants.MILLIS_I18N, null, this.pointOfOrigin, message));
                }
            }

            super.realClose(calledExplicitly, closeOpenResults);

            this.dbmd = null;
            this.originalSql = null;
            this.staticSqlStrings = null;
            this.parameterValues = null;
            this.parameterStreams = null;
            this.isStream = null;
            this.streamLengths = null;
            this.isNull = null;
            this.streamConvertBuf = null;
            this.parameterTypes = null;
        }
    }

    /**
     * JDBC 2.0 Set an Array parameter.
     * 
     * @param i
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            an object representing an SQL array
     * 
     * @throws SQLException
     *             because this method is not implemented.
     * @throws SQLFeatureNotSupportedException
     */
    public void setArray(int i, Array x) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    /**
     * When a very large ASCII value is input to a LONGVARCHAR parameter, it may
     * be more practical to send it via a java.io.InputStream. JDBC will read
     * the data from the stream as needed, until it reaches end-of-file. The
     * JDBC driver will do any necessary conversion from ASCII to the database
     * char format.
     * 
     * <P>
     * <B>Note:</B> This stream object can either be a standard Java stream object or your own subclass that implements the standard interface.
     * </p>
     * 
     * @param parameterIndex
     *            the first parameter is 1...
     * @param x
     *            the parameter value
     * @param length
     *            the number of bytes in the stream
     * 
     * @exception SQLException
     *                if a database access error occurs
     */
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, MysqlType.VARCHAR);
        } else {
            setBinaryStream(parameterIndex, x, length);
        }
    }

    /**
     * Set a parameter to a java.math.BigDecimal value. The driver converts this
     * to a SQL NUMERIC value when it sends it to the database.
     * 
     * @param parameterIndex
     *            the first parameter is 1...
     * @param x
     *            the parameter value
     * 
     * @exception SQLException
     *                if a database access error occurs
     */
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, MysqlType.DECIMAL);
        } else {
            setInternal(parameterIndex, StringUtils.fixDecimalExponent(x.toPlainString()));

            this.parameterTypes[parameterIndex - 1 + getParameterIndexOffset()] = MysqlType.DECIMAL;
        }
    }

    /**
     * When a very large binary value is input to a LONGVARBINARY parameter, it
     * may be more practical to send it via a java.io.InputStream. JDBC will
     * read the data from the stream as needed, until it reaches end-of-file.
     * 
     * <P>
     * <B>Note:</B> This stream object can either be a standard Java stream object or your own subclass that implements the standard interface.
     * </p>
     * 
     * @param parameterIndex
     *            the first parameter is 1...
     * @param x
     *            the parameter value
     * @param length
     *            the number of bytes to read from the stream (ignored)
     * 
     * @throws SQLException
     *             if a database access error occurs
     */
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (x == null) {
                setNull(parameterIndex, MysqlType.BINARY);
            } else {
                int parameterIndexOffset = getParameterIndexOffset();

                if ((parameterIndex < 1) || (parameterIndex > this.staticSqlStrings.length)) {
                    throw SQLError.createSQLException(
                            Messages.getString("PreparedStatement.2") + parameterIndex + Messages.getString("PreparedStatement.3")
                                    + this.staticSqlStrings.length + Messages.getString("PreparedStatement.4"),
                            SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
                } else if (parameterIndexOffset == -1 && parameterIndex == 1) {
                    throw SQLError.createSQLException(Messages.getString("PreparedStatement.63"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                            getExceptionInterceptor());
                }

                this.parameterStreams[parameterIndex - 1 + parameterIndexOffset] = x;
                this.isStream[parameterIndex - 1 + parameterIndexOffset] = true;
                this.streamLengths[parameterIndex - 1 + parameterIndexOffset] = length;
                this.isNull[parameterIndex - 1 + parameterIndexOffset] = false;
                this.parameterTypes[parameterIndex - 1 + getParameterIndexOffset()] = MysqlType.BLOB; // TODO use length to find the right BLOB type
            }
        }
    }

    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        setBinaryStream(parameterIndex, inputStream, (int) length);
    }

    /**
     * JDBC 2.0 Set a BLOB parameter.
     * 
     * @param i
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            an object representing a BLOB
     * 
     * @throws SQLException
     *             if a database error occurs
     */
    public void setBlob(int i, java.sql.Blob x) throws SQLException {
        if (x == null) {
            setNull(i, MysqlType.BLOB);
        } else {
            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();

            bytesOut.write('\'');
            escapeblockFast(x.getBytes(1, (int) x.length()), bytesOut, (int) x.length());
            bytesOut.write('\'');

            setInternal(i, bytesOut.toByteArray());

            this.parameterTypes[i - 1 + getParameterIndexOffset()] = MysqlType.BLOB;
        }
    }

    /**
     * Set a parameter to a Java boolean value. The driver converts this to a
     * SQL BIT value when it sends it to the database.
     * 
     * @param parameterIndex
     *            the first parameter is 1...
     * @param x
     *            the parameter value
     * 
     * @throws SQLException
     *             if a database access error occurs
     */
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setInternal(parameterIndex, x ? "1" : "0");
    }

    /**
     * Set a parameter to a Java byte value. The driver converts this to a SQL
     * TINYINT value when it sends it to the database.
     * 
     * @param parameterIndex
     *            the first parameter is 1...
     * @param x
     *            the parameter value
     * 
     * @exception SQLException
     *                if a database access error occurs
     */
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setInternal(parameterIndex, String.valueOf(x));

        this.parameterTypes[parameterIndex - 1 + getParameterIndexOffset()] = MysqlType.TINYINT;
    }

    /**
     * Set a parameter to a Java array of bytes. The driver converts this to a
     * SQL VARBINARY or LONGVARBINARY (depending on the argument's size relative
     * to the driver's limits on VARBINARYs) when it sends it to the database.
     * 
     * @param parameterIndex
     *            the first parameter is 1...
     * @param x
     *            the parameter value
     * 
     * @exception SQLException
     *                if a database access error occurs
     */
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        setBytes(parameterIndex, x, true, true);

        if (x != null) {
            this.parameterTypes[parameterIndex - 1 + getParameterIndexOffset()] = MysqlType.BINARY; // TODO VARBINARY ?
        }
    }

    public void setBytes(int parameterIndex, byte[] x, boolean checkForIntroducer, boolean escapeForMBChars) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (x == null) {
                setNull(parameterIndex, MysqlType.BINARY);
            } else {
                String connectionEncoding = this.connection.getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_characterEncoding).getValue();

                try {
                    if (this.connection.isNoBackslashEscapesSet()
                            || (escapeForMBChars && connectionEncoding != null && CharsetMapping.isMultibyteCharset(connectionEncoding))) {

                        // Send as hex

                        ByteArrayOutputStream bOut = new ByteArrayOutputStream((x.length * 2) + 3);
                        bOut.write('x');
                        bOut.write('\'');

                        for (int i = 0; i < x.length; i++) {
                            int lowBits = (x[i] & 0xff) / 16;
                            int highBits = (x[i] & 0xff) % 16;

                            bOut.write(HEX_DIGITS[lowBits]);
                            bOut.write(HEX_DIGITS[highBits]);
                        }

                        bOut.write('\'');

                        setInternal(parameterIndex, bOut.toByteArray());

                        return;
                    }
                } catch (SQLException ex) {
                    throw ex;
                } catch (RuntimeException ex) {
                    throw SQLError.createSQLException(ex.toString(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, ex, null);
                }

                // escape them
                int numBytes = x.length;

                int pad = 2;

                if (checkForIntroducer) {
                    pad += 7;
                }

                ByteArrayOutputStream bOut = new ByteArrayOutputStream(numBytes + pad);

                if (checkForIntroducer) {
                    bOut.write('_');
                    bOut.write('b');
                    bOut.write('i');
                    bOut.write('n');
                    bOut.write('a');
                    bOut.write('r');
                    bOut.write('y');
                }
                bOut.write('\'');

                for (int i = 0; i < numBytes; ++i) {
                    byte b = x[i];

                    switch (b) {
                        case 0: /* Must be escaped for 'mysql' */
                            bOut.write('\\');
                            bOut.write('0');

                            break;

                        case '\n': /* Must be escaped for logs */
                            bOut.write('\\');
                            bOut.write('n');

                            break;

                        case '\r':
                            bOut.write('\\');
                            bOut.write('r');

                            break;

                        case '\\':
                            bOut.write('\\');
                            bOut.write('\\');

                            break;

                        case '\'':
                            bOut.write('\\');
                            bOut.write('\'');

                            break;

                        case '"': /* Better safe than sorry */
                            bOut.write('\\');
                            bOut.write('"');

                            break;

                        case '\032': /* This gives problems on Win32 */
                            bOut.write('\\');
                            bOut.write('Z');

                            break;

                        default:
                            bOut.write(b);
                    }
                }

                bOut.write('\'');

                setInternal(parameterIndex, bOut.toByteArray());
            }
        }
    }

    /**
     * Used by updatable result sets for refreshRow() because the parameter has
     * already been escaped for updater or inserter prepared statements.
     * 
     * @param parameterIndex
     *            the parameter to set.
     * @param parameterAsBytes
     *            the parameter as a string.
     * 
     * @throws SQLException
     *             if an error occurs
     */
    public void setBytesNoEscape(int parameterIndex, byte[] parameterAsBytes) throws SQLException {
        byte[] parameterWithQuotes = new byte[parameterAsBytes.length + 2];
        parameterWithQuotes[0] = '\'';
        System.arraycopy(parameterAsBytes, 0, parameterWithQuotes, 1, parameterAsBytes.length);
        parameterWithQuotes[parameterAsBytes.length + 1] = '\'';

        setInternal(parameterIndex, parameterWithQuotes);
    }

    public void setBytesNoEscapeNoQuotes(int parameterIndex, byte[] parameterAsBytes) throws SQLException {
        setInternal(parameterIndex, parameterAsBytes);
    }

    /**
     * JDBC 2.0 When a very large UNICODE value is input to a LONGVARCHAR
     * parameter, it may be more practical to send it via a java.io.Reader. JDBC
     * will read the data from the stream as needed, until it reaches
     * end-of-file. The JDBC driver will do any necessary conversion from
     * UNICODE to the database char format.
     * 
     * <P>
     * <B>Note:</B> This stream object can either be a standard Java stream object or your own subclass that implements the standard interface.
     * </p>
     * 
     * @param parameterIndex
     *            the first parameter is 1, the second is 2, ...
     * @param reader
     *            the java reader which contains the UNICODE data
     * @param length
     *            the number of characters in the stream
     * 
     * @exception SQLException
     *                if a database-access error occurs.
     */
    public void setCharacterStream(int parameterIndex, java.io.Reader reader, int length) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            try {
                if (reader == null) {
                    setNull(parameterIndex, MysqlType.TEXT);
                } else {
                    char[] c = null;
                    int len = 0;

                    boolean useLength = this.useStreamLengthsInPrepStmts.getValue();

                    String forcedEncoding = this.session.getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_clobCharacterEncoding)
                            .getStringValue();

                    if (useLength && (length != -1)) {
                        c = new char[length];

                        int numCharsRead = readFully(reader, c, length); // blocks until all read

                        if (forcedEncoding == null) {
                            setString(parameterIndex, new String(c, 0, numCharsRead));
                        } else {
                            setBytes(parameterIndex, StringUtils.getBytes(new String(c, 0, numCharsRead), forcedEncoding));
                        }
                    } else {
                        c = new char[4096];

                        StringBuilder buf = new StringBuilder();

                        while ((len = reader.read(c)) != -1) {
                            buf.append(c, 0, len);
                        }

                        if (forcedEncoding == null) {
                            setString(parameterIndex, buf.toString());
                        } else {
                            setBytes(parameterIndex, StringUtils.getBytes(buf.toString(), forcedEncoding));
                        }
                    }

                    this.parameterTypes[parameterIndex - 1 + getParameterIndexOffset()] = MysqlType.TEXT; // TODO was Types.CLOB
                }
            } catch (UnsupportedEncodingException uec) {
                throw SQLError.createSQLException(uec.toString(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, uec, getExceptionInterceptor());
            } catch (IOException ioEx) {
                throw SQLError.createSQLException(ioEx.toString(), SQLError.SQL_STATE_GENERAL_ERROR, ioEx, getExceptionInterceptor());
            }
        }
    }

    /**
     * JDBC 2.0 Set a CLOB parameter.
     * 
     * @param i
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            an object representing a CLOB
     * 
     * @throws SQLException
     *             if a database error occurs
     */
    public void setClob(int i, Clob x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (x == null) {
                setNull(i, MysqlType.TEXT);
            } else {

                String forcedEncoding = this.session.getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_clobCharacterEncoding)
                        .getStringValue();

                if (forcedEncoding == null) {
                    setString(i, x.getSubString(1L, (int) x.length()));
                } else {
                    setBytes(i, StringUtils.getBytes(x.getSubString(1L, (int) x.length()), forcedEncoding));
                }

                this.parameterTypes[i - 1 + getParameterIndexOffset()] = MysqlType.TEXT; // TODO was Types.CLOB
            }
        }
    }

    /**
     * Set a parameter to a java.sql.Date value. The driver converts this to a
     * SQL DATE value when it sends it to the database.
     * 
     * @param parameterIndex
     *            the first parameter is 1...
     * @param x
     *            the parameter value
     * 
     * @exception java.sql.SQLException
     *                if a database access error occurs
     */
    public void setDate(int parameterIndex, java.sql.Date x) throws java.sql.SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            setDateInternal(parameterIndex, x, this.session.getDefaultTimeZone());
        }
    }

    /**
     * Set a parameter to a java.sql.Date value. The driver converts this to a
     * SQL DATE value when it sends it to the database.
     * 
     * @param parameterIndex
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the parameter value
     * @param cal
     *            the calendar to interpret the date with
     * 
     * @exception SQLException
     *                if a database-access error occurs.
     */
    public void setDate(int parameterIndex, java.sql.Date x, Calendar cal) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            setDateInternal(parameterIndex, x, cal.getTimeZone());
        }
    }

    private void setDateInternal(int parameterIndex, Date x, TimeZone tz) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, MysqlType.DATE);
        } else {
            if (this.ddf == null) {
                this.ddf = new SimpleDateFormat("''yyyy-MM-dd''", Locale.US);
            }

            this.ddf.setTimeZone(tz);

            setInternal(parameterIndex, this.ddf.format(x));
        }
    }

    /**
     * Set a parameter to a Java double value. The driver converts this to a SQL
     * DOUBLE value when it sends it to the database
     * 
     * @param parameterIndex
     *            the first parameter is 1...
     * @param x
     *            the parameter value
     * 
     * @exception SQLException
     *                if a database access error occurs
     */
    public void setDouble(int parameterIndex, double x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (!this.session.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_allowNanAndInf).getValue()
                    && (x == Double.POSITIVE_INFINITY || x == Double.NEGATIVE_INFINITY || Double.isNaN(x))) {
                throw SQLError.createSQLException(Messages.getString("PreparedStatement.64", new Object[] { x }), SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                        getExceptionInterceptor());

            }

            setInternal(parameterIndex, StringUtils.fixDecimalExponent(String.valueOf(x)));

            this.parameterTypes[parameterIndex - 1 + getParameterIndexOffset()] = MysqlType.DOUBLE;
        }
    }

    /**
     * Set a parameter to a Java float value. The driver converts this to a SQL
     * FLOAT value when it sends it to the database.
     * 
     * @param parameterIndex
     *            the first parameter is 1...
     * @param x
     *            the parameter value
     * 
     * @exception SQLException
     *                if a database access error occurs
     */
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setInternal(parameterIndex, StringUtils.fixDecimalExponent(String.valueOf(x)));

        this.parameterTypes[parameterIndex - 1 + getParameterIndexOffset()] = MysqlType.FLOAT; // TODO check; was Types.FLOAT but should be Types.REAL to map to SQL FLOAT
    }

    /**
     * Set a parameter to a Java int value. The driver converts this to a SQL
     * INTEGER value when it sends it to the database.
     * 
     * @param parameterIndex
     *            the first parameter is 1...
     * @param x
     *            the parameter value
     * 
     * @exception SQLException
     *                if a database access error occurs
     */
    public void setInt(int parameterIndex, int x) throws SQLException {
        setInternal(parameterIndex, String.valueOf(x));

        this.parameterTypes[parameterIndex - 1 + getParameterIndexOffset()] = MysqlType.INT;
    }

    protected final void setInternal(int paramIndex, byte[] val) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            int parameterIndexOffset = getParameterIndexOffset();

            checkBounds(paramIndex, parameterIndexOffset);

            this.isStream[paramIndex - 1 + parameterIndexOffset] = false;
            this.isNull[paramIndex - 1 + parameterIndexOffset] = false;
            this.parameterStreams[paramIndex - 1 + parameterIndexOffset] = null;
            this.parameterValues[paramIndex - 1 + parameterIndexOffset] = val;
        }
    }

    protected void checkBounds(int paramIndex, int parameterIndexOffset) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if ((paramIndex < 1)) {
                throw SQLError.createSQLException(Messages.getString("PreparedStatement.49") + paramIndex + Messages.getString("PreparedStatement.50"),
                        SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            } else if (paramIndex > this.parameterCount) {
                throw SQLError
                        .createSQLException(
                                Messages.getString("PreparedStatement.51") + paramIndex + Messages.getString("PreparedStatement.52")
                                        + (this.parameterValues.length) + Messages.getString("PreparedStatement.53"),
                                SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            } else if (parameterIndexOffset == -1 && paramIndex == 1) {
                throw SQLError.createSQLException(Messages.getString("PreparedStatement.63"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }
        }
    }

    protected final void setInternal(int paramIndex, String val) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            byte[] parameterAsBytes = null;

            parameterAsBytes = StringUtils.getBytes(val, this.charEncoding);

            setInternal(paramIndex, parameterAsBytes);
        }
    }

    /**
     * Set a parameter to a Java long value. The driver converts this to a SQL
     * BIGINT value when it sends it to the database.
     * 
     * @param parameterIndex
     *            the first parameter is 1...
     * @param x
     *            the parameter value
     * 
     * @exception SQLException
     *                if a database access error occurs
     */
    public void setLong(int parameterIndex, long x) throws SQLException {
        setInternal(parameterIndex, String.valueOf(x));

        this.parameterTypes[parameterIndex - 1 + getParameterIndexOffset()] = MysqlType.BIGINT;
    }

    public void setBigInteger(int parameterIndex, BigInteger x) throws SQLException {
        setInternal(parameterIndex, x.toString());

        this.parameterTypes[parameterIndex - 1 + getParameterIndexOffset()] = MysqlType.BIGINT_UNSIGNED;
    }

    /**
     * Set a parameter to SQL NULL
     * 
     * <p>
     * <B>Note:</B> You must specify the parameters SQL type (although MySQL ignores it)
     * </p>
     * 
     * @param parameterIndex
     *            the first parameter is 1, etc...
     * @param sqlType
     *            the SQL type code defined in java.sql.Types
     * 
     * @exception SQLException
     *                if a database access error occurs
     */
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            setInternal(parameterIndex, "null");
            this.isNull[parameterIndex - 1 + getParameterIndexOffset()] = true;

            this.parameterTypes[parameterIndex - 1 + getParameterIndexOffset()] = MysqlType.NULL;
        }
    }

    public void setNull(int parameterIndex, MysqlType mysqlType) throws SQLException {
        setNull(parameterIndex, mysqlType.getJdbcType());
    }

    /**
     * Set a parameter to SQL NULL.
     * 
     * <P>
     * <B>Note:</B> You must specify the parameter's SQL type.
     * </p>
     * 
     * @param parameterIndex
     *            the first parameter is 1, the second is 2, ...
     * @param sqlType
     *            SQL type code defined by java.sql.Types
     * @param arg
     *            argument parameters for null
     * 
     * @exception SQLException
     *                if a database-access error occurs.
     */
    public void setNull(int parameterIndex, int sqlType, String arg) throws SQLException {
        setNull(parameterIndex, sqlType);

        this.parameterTypes[parameterIndex - 1 + getParameterIndexOffset()] = MysqlType.NULL;
    }

    private void setNumericObject(int parameterIndex, Object parameterObj, MysqlType targetMysqlType, int scale) throws SQLException {
        Number parameterAsNum;

        if (parameterObj instanceof Boolean) {
            parameterAsNum = ((Boolean) parameterObj).booleanValue() ? Integer.valueOf(1) : Integer.valueOf(0);
        } else if (parameterObj instanceof String) {
            switch (targetMysqlType) {
                case BIT:
                    if ("1".equals(parameterObj) || "0".equals(parameterObj)) {
                        parameterAsNum = Integer.valueOf((String) parameterObj);
                    } else {
                        boolean parameterAsBoolean = "true".equalsIgnoreCase((String) parameterObj);

                        parameterAsNum = parameterAsBoolean ? Integer.valueOf(1) : Integer.valueOf(0);
                    }
                    break;

                case TINYINT:
                case TINYINT_UNSIGNED:
                case SMALLINT:
                case SMALLINT_UNSIGNED:
                case INT:
                case INT_UNSIGNED:
                    parameterAsNum = Integer.valueOf((String) parameterObj);
                    break;

                case BIGINT:
                    parameterAsNum = Long.valueOf((String) parameterObj);
                    break;

                case BIGINT_UNSIGNED:
                    parameterAsNum = new BigInteger((String) parameterObj);
                    break;

                case FLOAT:
                case FLOAT_UNSIGNED:
                    parameterAsNum = Float.valueOf((String) parameterObj);

                    break;

                case DOUBLE:
                case DOUBLE_UNSIGNED:
                    parameterAsNum = Double.valueOf((String) parameterObj);
                    break;

                case DECIMAL:
                case DECIMAL_UNSIGNED:
                default:
                    parameterAsNum = new java.math.BigDecimal((String) parameterObj);
            }
        } else {
            parameterAsNum = (Number) parameterObj;
        }

        switch (targetMysqlType) {
            case BIT:
            case TINYINT:
            case TINYINT_UNSIGNED:
            case SMALLINT:
            case SMALLINT_UNSIGNED:
            case INT:
            case INT_UNSIGNED:
                setInt(parameterIndex, parameterAsNum.intValue());
                break;

            case BIGINT:
            case BIGINT_UNSIGNED:
                setLong(parameterIndex, parameterAsNum.longValue());
                break;

            case FLOAT:
            case FLOAT_UNSIGNED:
                setFloat(parameterIndex, parameterAsNum.floatValue());
                break;

            case DOUBLE:
            case DOUBLE_UNSIGNED:
                setDouble(parameterIndex, parameterAsNum.doubleValue());
                break;

            case DECIMAL:
            case DECIMAL_UNSIGNED:
                if (parameterAsNum instanceof java.math.BigDecimal) {
                    BigDecimal scaledBigDecimal = null;

                    try {
                        scaledBigDecimal = ((java.math.BigDecimal) parameterAsNum).setScale(scale);
                    } catch (ArithmeticException ex) {
                        try {
                            scaledBigDecimal = ((java.math.BigDecimal) parameterAsNum).setScale(scale, BigDecimal.ROUND_HALF_UP);
                        } catch (ArithmeticException arEx) {
                            throw SQLError.createSQLException(Messages.getString("PreparedStatement.65", new Object[] { scale, parameterAsNum }),
                                    SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
                        }
                    }

                    setBigDecimal(parameterIndex, scaledBigDecimal);
                } else if (parameterAsNum instanceof java.math.BigInteger) {
                    setBigDecimal(parameterIndex, new java.math.BigDecimal((java.math.BigInteger) parameterAsNum, scale));
                } else {
                    setBigDecimal(parameterIndex, new java.math.BigDecimal(parameterAsNum.doubleValue()));
                }

                break;
            default:
                break;
        }
    }

    public void setObject(int parameterIndex, Object parameterObj) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (parameterObj == null) {
                setNull(parameterIndex, MysqlType.UNKNOWN);
            } else {
                if (parameterObj instanceof Byte) {
                    setInt(parameterIndex, ((Byte) parameterObj).intValue());

                } else if (parameterObj instanceof String) {
                    setString(parameterIndex, (String) parameterObj);

                } else if (parameterObj instanceof BigDecimal) {
                    setBigDecimal(parameterIndex, (BigDecimal) parameterObj);

                } else if (parameterObj instanceof Short) {
                    setShort(parameterIndex, ((Short) parameterObj).shortValue());

                } else if (parameterObj instanceof Integer) {
                    setInt(parameterIndex, ((Integer) parameterObj).intValue());

                } else if (parameterObj instanceof Long) {
                    setLong(parameterIndex, ((Long) parameterObj).longValue());

                } else if (parameterObj instanceof Float) {
                    setFloat(parameterIndex, ((Float) parameterObj).floatValue());

                } else if (parameterObj instanceof Double) {
                    setDouble(parameterIndex, ((Double) parameterObj).doubleValue());

                } else if (parameterObj instanceof byte[]) {
                    setBytes(parameterIndex, (byte[]) parameterObj);

                } else if (parameterObj instanceof java.sql.Date) {
                    setDate(parameterIndex, (java.sql.Date) parameterObj);

                } else if (parameterObj instanceof Time) {
                    setTime(parameterIndex, (Time) parameterObj);

                } else if (parameterObj instanceof Timestamp) {
                    setTimestamp(parameterIndex, (Timestamp) parameterObj);

                } else if (parameterObj instanceof Boolean) {
                    setBoolean(parameterIndex, ((Boolean) parameterObj).booleanValue());

                } else if (parameterObj instanceof InputStream) {
                    setBinaryStream(parameterIndex, (InputStream) parameterObj, -1);

                } else if (parameterObj instanceof java.sql.Blob) {
                    setBlob(parameterIndex, (java.sql.Blob) parameterObj);

                } else if (parameterObj instanceof java.sql.Clob) {
                    setClob(parameterIndex, (java.sql.Clob) parameterObj);

                } else if (this.treatUtilDateAsTimestamp.getValue() && parameterObj instanceof java.util.Date) {
                    setTimestamp(parameterIndex, new Timestamp(((java.util.Date) parameterObj).getTime()));

                } else if (parameterObj instanceof BigInteger) {
                    setString(parameterIndex, parameterObj.toString());

                } else if (parameterObj instanceof LocalDate) {
                    setDate(parameterIndex, Date.valueOf((LocalDate) parameterObj));

                } else if (parameterObj instanceof LocalDateTime) {
                    setTimestamp(parameterIndex, Timestamp.valueOf((LocalDateTime) parameterObj));

                } else if (parameterObj instanceof LocalTime) {
                    setTime(parameterIndex, Time.valueOf((LocalTime) parameterObj));

                } else {
                    setSerializableObject(parameterIndex, parameterObj);
                }
            }
        }
    }

    /**
     * @param parameterIndex
     * @param parameterObj
     * @param targetSqlType
     * 
     * @throws SQLException
     */
    public void setObject(int parameterIndex, Object parameterObj, int targetSqlType) throws SQLException {
        if (!(parameterObj instanceof BigDecimal)) {
            setObject(parameterIndex, parameterObj, targetSqlType, 0);
        } else {
            setObject(parameterIndex, parameterObj, targetSqlType, ((BigDecimal) parameterObj).scale());
        }
    }

    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            setObject(parameterIndex, x, targetSqlType.getVendorTypeNumber());
        }
    }

    /**
     * Set the value of a parameter using an object; use the java.lang
     * equivalent objects for integral values.
     * 
     * <P>
     * The given Java object will be converted to the targetSqlType before being sent to the database.
     * </p>
     * 
     * <P>
     * note that this method may be used to pass database-specific abstract data types. This is done by using a Driver-specific Java type and using a
     * targetSqlType of Types.OTHER
     * </p>
     * 
     * @param parameterIndex
     *            the first parameter is 1...
     * @param parameterObj
     *            the object containing the input parameter value
     * @param targetSqlType
     *            The SQL type to be send to the database
     * @param scale
     *            For Types.DECIMAL or Types.NUMERIC types
     *            this is the number of digits after the decimal. For all other
     *            types this value will be ignored.
     * 
     * @throws SQLException
     *             if a database access error occurs
     */
    public void setObject(int parameterIndex, Object parameterObj, int targetSqlType, int scale) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (parameterObj == null) {
                setNull(parameterIndex, MysqlType.UNKNOWN);
            } else {

                //JDBC42Helper.convertJavaTimeToJavaSql(x)
                if (parameterObj instanceof LocalDate) {
                    parameterObj = Date.valueOf((LocalDate) parameterObj);
                } else if (parameterObj instanceof LocalDateTime) {
                    parameterObj = Timestamp.valueOf((LocalDateTime) parameterObj);
                } else if (parameterObj instanceof LocalTime) {
                    parameterObj = Time.valueOf((LocalTime) parameterObj);
                }

                try {
                    MysqlType targetMysqlType = MysqlType.getByJdbcType(targetSqlType);

                    /*
                     * From Table-B5 in the JDBC Spec
                     */
                    switch (targetMysqlType) {
                        case BOOLEAN:
                            if (parameterObj instanceof Boolean) {
                                setBoolean(parameterIndex, ((Boolean) parameterObj).booleanValue());
                                break;

                            } else if (parameterObj instanceof String) {
                                setBoolean(parameterIndex, "true".equalsIgnoreCase((String) parameterObj) || !"0".equalsIgnoreCase((String) parameterObj));
                                break;

                            } else if (parameterObj instanceof Number) {
                                int intValue = ((Number) parameterObj).intValue();
                                setBoolean(parameterIndex, intValue != 0);
                                break;

                            } else {
                                throw SQLError.createSQLException(
                                        Messages.getString("PreparedStatement.66", new Object[] { parameterObj.getClass().getName() }),
                                        SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
                            }

                        case BIT:
                        case TINYINT:
                        case TINYINT_UNSIGNED:
                        case SMALLINT:
                        case SMALLINT_UNSIGNED:
                        case INT:
                        case INT_UNSIGNED:
                        case BIGINT:
                        case BIGINT_UNSIGNED:
                        case FLOAT:
                        case FLOAT_UNSIGNED:
                        case DOUBLE:
                        case DOUBLE_UNSIGNED:
                        case DECIMAL:
                        case DECIMAL_UNSIGNED:
                            setNumericObject(parameterIndex, parameterObj, targetMysqlType, scale);
                            break;

                        case CHAR:
                        case ENUM:
                        case SET:
                        case VARCHAR:
                        case TINYTEXT:
                        case TEXT:
                        case MEDIUMTEXT:
                        case LONGTEXT:
                        case JSON:
                            if (parameterObj instanceof BigDecimal) {
                                setString(parameterIndex, (StringUtils.fixDecimalExponent(((BigDecimal) parameterObj).toPlainString())));
                            } else if (parameterObj instanceof java.sql.Clob) {
                                setClob(parameterIndex, (java.sql.Clob) parameterObj);
                            } else {
                                setString(parameterIndex, parameterObj.toString());
                            }
                            break;

                        case BINARY:
                        case GEOMETRY:
                        case VARBINARY:
                        case TINYBLOB:
                        case BLOB:
                        case MEDIUMBLOB:
                        case LONGBLOB:
                            if (parameterObj instanceof byte[]) {
                                setBytes(parameterIndex, (byte[]) parameterObj);
                            } else if (parameterObj instanceof java.sql.Blob) {
                                setBlob(parameterIndex, (java.sql.Blob) parameterObj);
                            } else {
                                setBytes(parameterIndex, StringUtils.getBytes(parameterObj.toString(), this.charEncoding));
                            }

                            break;

                        case DATE:
                        case TIMESTAMP:

                            java.util.Date parameterAsDate;

                            if (parameterObj instanceof String) {
                                ParsePosition pp = new ParsePosition(0);
                                java.text.DateFormat sdf = new java.text.SimpleDateFormat(getDateTimePattern((String) parameterObj, false), Locale.US);
                                parameterAsDate = sdf.parse((String) parameterObj, pp);
                            } else {
                                parameterAsDate = (java.util.Date) parameterObj;
                            }

                            switch (targetMysqlType) {
                                case DATE:

                                    if (parameterAsDate instanceof java.sql.Date) {
                                        setDate(parameterIndex, (java.sql.Date) parameterAsDate);
                                    } else {
                                        setDate(parameterIndex, new java.sql.Date(parameterAsDate.getTime()));
                                    }

                                    break;

                                case TIMESTAMP:

                                    if (parameterAsDate instanceof java.sql.Timestamp) {
                                        setTimestamp(parameterIndex, (java.sql.Timestamp) parameterAsDate);
                                    } else {
                                        setTimestamp(parameterIndex, new java.sql.Timestamp(parameterAsDate.getTime()));
                                    }

                                    break;

                                default:
                                    break;
                            }

                            break;

                        case TIME:
                            if (parameterObj instanceof String) {
                                java.text.DateFormat sdf = new java.text.SimpleDateFormat(getDateTimePattern((String) parameterObj, true), Locale.US);
                                setTime(parameterIndex, new java.sql.Time(sdf.parse((String) parameterObj).getTime()));
                            } else if (parameterObj instanceof Timestamp) {
                                Timestamp xT = (Timestamp) parameterObj;
                                setTime(parameterIndex, new java.sql.Time(xT.getTime()));
                            } else {
                                setTime(parameterIndex, (java.sql.Time) parameterObj);
                            }

                            break;

                        case UNKNOWN:
                            setSerializableObject(parameterIndex, parameterObj);
                            break;

                        default:
                            throw SQLError.createSQLException(Messages.getString("PreparedStatement.16"), SQLError.SQL_STATE_GENERAL_ERROR,
                                    getExceptionInterceptor());
                    }
                } catch (SQLException ex) {
                    throw ex;
                } catch (FeatureNotAvailableException nae) {
                    throw SQLError.createSQLFeatureNotSupportedException(Messages.getString("Statement.UnsupportedSQLType") + JDBCType.valueOf(targetSqlType),
                            SQLError.SQL_STATE_DRIVER_NOT_CAPABLE, getExceptionInterceptor());
                } catch (Exception ex) {
                    throw SQLError.createSQLException(
                            Messages.getString("PreparedStatement.17") + parameterObj.getClass().toString() + Messages.getString("PreparedStatement.18")
                                    + ex.getClass().getName() + Messages.getString("PreparedStatement.19") + ex.getMessage(),
                            SQLError.SQL_STATE_GENERAL_ERROR, ex, getExceptionInterceptor());
                }
            }
        }
    }

    public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            setObject(parameterIndex, x, targetSqlType.getVendorTypeNumber(), scaleOrLength);
        }
    }

    protected int setOneBatchedParameterSet(java.sql.PreparedStatement batchedStatement, int batchedParamIndex, Object paramSet) throws SQLException {
        BatchParams paramArg = (BatchParams) paramSet;

        boolean[] isNullBatch = paramArg.isNull;
        boolean[] isStreamBatch = paramArg.isStream;

        for (int j = 0; j < isNullBatch.length; j++) {
            if (isNullBatch[j]) {
                batchedStatement.setNull(batchedParamIndex++, MysqlType.NULL.getJdbcType());
            } else {
                if (isStreamBatch[j]) {
                    batchedStatement.setBinaryStream(batchedParamIndex++, paramArg.parameterStreams[j], paramArg.streamLengths[j]);
                } else {
                    ((com.mysql.cj.jdbc.PreparedStatement) batchedStatement).setBytesNoEscapeNoQuotes(batchedParamIndex++, paramArg.parameterStrings[j]);
                }
            }
        }

        return batchedParamIndex;
    }

    /**
     * JDBC 2.0 Set a REF(&lt;structured-type&gt;) parameter.
     * 
     * @param i
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            an object representing data of an SQL REF Type
     * 
     * @throws SQLException
     *             if a database error occurs
     * @throws SQLFeatureNotSupportedException
     */
    public void setRef(int i, Ref x) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    /**
     * Sets the value for the placeholder as a serialized Java object (used by
     * various forms of setObject()
     * 
     * @param parameterIndex
     * @param parameterObj
     * 
     * @throws SQLException
     */
    private final void setSerializableObject(int parameterIndex, Object parameterObj) throws SQLException {
        try {
            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            ObjectOutputStream objectOut = new ObjectOutputStream(bytesOut);
            objectOut.writeObject(parameterObj);
            objectOut.flush();
            objectOut.close();
            bytesOut.flush();
            bytesOut.close();

            byte[] buf = bytesOut.toByteArray();
            ByteArrayInputStream bytesIn = new ByteArrayInputStream(buf);
            setBinaryStream(parameterIndex, bytesIn, buf.length);
            this.parameterTypes[parameterIndex - 1 + getParameterIndexOffset()] = MysqlType.BINARY;
        } catch (Exception ex) {
            throw SQLError.createSQLException(Messages.getString("PreparedStatement.54") + ex.getClass().getName(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, ex,
                    getExceptionInterceptor());
        }
    }

    /**
     * Set a parameter to a Java short value. The driver converts this to a SQL
     * SMALLINT value when it sends it to the database.
     * 
     * @param parameterIndex
     *            the first parameter is 1...
     * @param x
     *            the parameter value
     * 
     * @exception SQLException
     *                if a database access error occurs
     */
    public void setShort(int parameterIndex, short x) throws SQLException {
        setInternal(parameterIndex, String.valueOf(x));

        this.parameterTypes[parameterIndex - 1 + getParameterIndexOffset()] = MysqlType.SMALLINT;
    }

    /**
     * Set a parameter to a Java String value. The driver converts this to a SQL
     * VARCHAR or LONGVARCHAR value (depending on the arguments size relative to
     * the driver's limits on VARCHARs) when it sends it to the database.
     * 
     * @param parameterIndex
     *            the first parameter is 1...
     * @param x
     *            the parameter value
     * 
     * @exception SQLException
     *                if a database access error occurs
     */
    public void setString(int parameterIndex, String x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            // if the passed string is null, then set this column to null
            if (x == null) {
                setNull(parameterIndex, MysqlType.VARCHAR);
            } else {
                checkClosed();

                int stringLength = x.length();

                if (this.connection.isNoBackslashEscapesSet()) {
                    // Scan for any nasty chars

                    boolean needsHexEscape = isEscapeNeededForString(x, stringLength);

                    if (!needsHexEscape) {
                        byte[] parameterAsBytes = null;

                        StringBuilder quotedString = new StringBuilder(x.length() + 2);
                        quotedString.append('\'');
                        quotedString.append(x);
                        quotedString.append('\'');

                        if (!this.isLoadDataQuery) {
                            parameterAsBytes = StringUtils.getBytes(quotedString.toString(), this.charEncoding);
                        } else {
                            // Send with platform character encoding
                            parameterAsBytes = StringUtils.getBytes(quotedString.toString());
                        }

                        setInternal(parameterIndex, parameterAsBytes);
                    } else {
                        byte[] parameterAsBytes = null;

                        if (!this.isLoadDataQuery) {
                            parameterAsBytes = StringUtils.getBytes(x, this.charEncoding);
                        } else {
                            // Send with platform character encoding
                            parameterAsBytes = StringUtils.getBytes(x);
                        }

                        setBytes(parameterIndex, parameterAsBytes);
                    }

                    return;
                }

                String parameterAsString = x;
                boolean needsQuoted = true;

                if (this.isLoadDataQuery || isEscapeNeededForString(x, stringLength)) {
                    needsQuoted = false; // saves an allocation later

                    StringBuilder buf = new StringBuilder((int) (x.length() * 1.1));

                    buf.append('\'');

                    //
                    // Note: buf.append(char) is _faster_ than appending in blocks, because the block append requires a System.arraycopy().... go figure...
                    //

                    for (int i = 0; i < stringLength; ++i) {
                        char c = x.charAt(i);

                        switch (c) {
                            case 0: /* Must be escaped for 'mysql' */
                                buf.append('\\');
                                buf.append('0');

                                break;

                            case '\n': /* Must be escaped for logs */
                                buf.append('\\');
                                buf.append('n');

                                break;

                            case '\r':
                                buf.append('\\');
                                buf.append('r');

                                break;

                            case '\\':
                                buf.append('\\');
                                buf.append('\\');

                                break;

                            case '\'':
                                buf.append('\\');
                                buf.append('\'');

                                break;

                            case '"': /* Better safe than sorry */
                                if (this.usingAnsiMode) {
                                    buf.append('\\');
                                }

                                buf.append('"');

                                break;

                            case '\032': /* This gives problems on Win32 */
                                buf.append('\\');
                                buf.append('Z');

                                break;

                            case '\u00a5':
                            case '\u20a9':
                                // escape characters interpreted as backslash by mysql
                                if (this.charsetEncoder != null) {
                                    CharBuffer cbuf = CharBuffer.allocate(1);
                                    ByteBuffer bbuf = ByteBuffer.allocate(1);
                                    cbuf.put(c);
                                    cbuf.position(0);
                                    this.charsetEncoder.encode(cbuf, bbuf, true);
                                    if (bbuf.get(0) == '\\') {
                                        buf.append('\\');
                                    }
                                }
                                buf.append(c);
                                break;

                            default:
                                buf.append(c);
                        }
                    }

                    buf.append('\'');

                    parameterAsString = buf.toString();
                }

                byte[] parameterAsBytes = null;

                if (!this.isLoadDataQuery) {
                    if (needsQuoted) {
                        parameterAsBytes = StringUtils.getBytesWrapped(parameterAsString, '\'', '\'', this.charEncoding);
                    } else {
                        parameterAsBytes = StringUtils.getBytes(parameterAsString, this.charEncoding);
                    }
                } else {
                    // Send with platform character encoding
                    parameterAsBytes = StringUtils.getBytes(parameterAsString);
                }

                setInternal(parameterIndex, parameterAsBytes);

                this.parameterTypes[parameterIndex - 1 + getParameterIndexOffset()] = MysqlType.VARCHAR;
            }
        }
    }

    private boolean isEscapeNeededForString(String x, int stringLength) {
        boolean needsHexEscape = false;

        for (int i = 0; i < stringLength; ++i) {
            char c = x.charAt(i);

            switch (c) {
                case 0: /* Must be escaped for 'mysql' */

                    needsHexEscape = true;
                    break;

                case '\n': /* Must be escaped for logs */
                    needsHexEscape = true;

                    break;

                case '\r':
                    needsHexEscape = true;
                    break;

                case '\\':
                    needsHexEscape = true;

                    break;

                case '\'':
                    needsHexEscape = true;

                    break;

                case '"': /* Better safe than sorry */
                    needsHexEscape = true;

                    break;

                case '\032': /* This gives problems on Win32 */
                    needsHexEscape = true;
                    break;
            }

            if (needsHexEscape) {
                break; // no need to scan more
            }
        }
        return needsHexEscape;
    }

    /**
     * Set a parameter to a java.sql.Time value. The driver converts this to a
     * SQL TIME value when it sends it to the database.
     * 
     * @param parameterIndex
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the parameter value
     * @param cal
     *            the cal specifying the timezone
     * 
     * @throws SQLException
     *             if a database-access error occurs.
     */
    public void setTime(int parameterIndex, java.sql.Time x, Calendar cal) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            setTimeInternal(parameterIndex, x, cal.getTimeZone());
        }
    }

    /**
     * Set a parameter to a java.sql.Time value. The driver converts this to a
     * SQL TIME value when it sends it to the database.
     * 
     * @param parameterIndex
     *            the first parameter is 1...));
     * @param x
     *            the parameter value
     * 
     * @throws java.sql.SQLException
     *             if a database access error occurs
     */
    public void setTime(int parameterIndex, Time x) throws java.sql.SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            setTimeInternal(parameterIndex, x, this.session.getDefaultTimeZone());
        }
    }

    /**
     * Set a parameter to a java.sql.Time value. The driver converts this to a
     * SQL TIME value when it sends it to the database, using the given
     * timezone.
     * 
     * @param parameterIndex
     *            the first parameter is 1...));
     * @param x
     *            the parameter value
     * @param tz
     *            the timezone to use
     * 
     * @throws java.sql.SQLException
     *             if a database access error occurs
     */
    private void setTimeInternal(int parameterIndex, Time x, TimeZone tz) throws java.sql.SQLException {
        if (x == null) {
            setNull(parameterIndex, MysqlType.TIME);
        } else {
            checkClosed();

            if (this.tdf == null) {
                this.tdf = new SimpleDateFormat("''HH:mm:ss''", Locale.US);
            }

            this.tdf.setTimeZone(tz);

            setInternal(parameterIndex, this.tdf.format(x));

            this.parameterTypes[parameterIndex - 1 + getParameterIndexOffset()] = MysqlType.TIME;
        }
    }

    /**
     * Set a parameter to a java.sql.Timestamp value. The driver converts this
     * to a SQL TIMESTAMP value when it sends it to the database.
     * 
     * @param parameterIndex
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the parameter value
     * @param cal
     *            the calendar specifying the timezone to use
     * 
     * @throws SQLException
     *             if a database-access error occurs.
     */
    public void setTimestamp(int parameterIndex, java.sql.Timestamp x, Calendar cal) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            setTimestampInternal(parameterIndex, x, cal.getTimeZone());
        }
    }

    /**
     * Set a parameter to a java.sql.Timestamp value. The driver converts this
     * to a SQL TIMESTAMP value when it sends it to the database.
     * 
     * @param parameterIndex
     *            the first parameter is 1...
     * @param x
     *            the parameter value
     * 
     * @throws java.sql.SQLException
     *             if a database access error occurs
     */
    public void setTimestamp(int parameterIndex, Timestamp x) throws java.sql.SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            setTimestampInternal(parameterIndex, x, this.session.getDefaultTimeZone());
        }
    }

    /**
     * Set a parameter to a java.sql.Timestamp value. The driver converts this
     * to a SQL TIMESTAMP value when it sends it to the database.
     * 
     * @param parameterIndex
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the parameter value
     * @param tz
     *            the timezone to use
     * 
     * @throws SQLException
     *             if a database-access error occurs.
     */
    private void setTimestampInternal(int parameterIndex, Timestamp x, TimeZone tz) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, MysqlType.TIMESTAMP);
        } else {
            if (!this.sendFractionalSeconds.getValue()) {
                x = TimeUtil.truncateFractionalSeconds(x);
            }

            this.parameterTypes[parameterIndex - 1 + getParameterIndexOffset()] = MysqlType.TIMESTAMP;

            if (this.tsdf == null) {
                this.tsdf = new SimpleDateFormat("''yyyy-MM-dd HH:mm:ss", Locale.US);
            }

            this.tsdf.setTimeZone(tz);

            StringBuffer buf = new StringBuffer();
            buf.append(this.tsdf.format(x));
            if (this.session.serverSupportsFracSecs()) {
                buf.append('.');
                buf.append(TimeUtil.formatNanos(x.getNanos(), true));
            }
            buf.append('\'');

            setInternal(parameterIndex, buf.toString());
        }
    }

    /**
     * When a very large Unicode value is input to a LONGVARCHAR parameter, it
     * may be more practical to send it via a java.io.InputStream. JDBC will
     * read the data from the stream as needed, until it reaches end-of-file.
     * The JDBC driver will do any necessary conversion from UNICODE to the
     * database char format.
     * 
     * <P>
     * <B>Note:</B> This stream object can either be a standard Java stream object or your own subclass that implements the standard interface.
     * </p>
     * 
     * @param parameterIndex
     *            the first parameter is 1...
     * @param x
     *            the parameter value
     * @param length
     *            the number of bytes to read from the stream
     * 
     * @throws SQLException
     *             if a database access error occurs
     * 
     * @deprecated
     */
    @Deprecated
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, MysqlType.TEXT);
        } else {
            setBinaryStream(parameterIndex, x, length);

            this.parameterTypes[parameterIndex - 1 + getParameterIndexOffset()] = MysqlType.TEXT; // TODO was Types.CLOB
        }
    }

    /**
     * @see PreparedStatement#setURL(int, URL)
     */
    public void setURL(int parameterIndex, URL arg) throws SQLException {
        if (arg == null) {
            setNull(parameterIndex, MysqlType.VARCHAR);
        } else {
            setString(parameterIndex, arg.toString());

            this.parameterTypes[parameterIndex - 1 + getParameterIndexOffset()] = MysqlType.VARCHAR; // TODO was Types.DATALINK
        }
    }

    private final void streamToBytes(PacketPayload packet, InputStream in, boolean escape, int streamLength, boolean useLength) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            try {
                if (this.streamConvertBuf == null) {
                    this.streamConvertBuf = new byte[4096];
                }

                boolean hexEscape = false;

                try {
                    if (this.connection.isNoBackslashEscapesSet()) {
                        hexEscape = true;
                    }
                } catch (RuntimeException ex) {
                    throw SQLError.createSQLException(ex.toString(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, ex, null);
                }

                if (streamLength == -1) {
                    useLength = false;
                }

                int bc = -1;

                if (useLength) {
                    bc = readblock(in, this.streamConvertBuf, streamLength);
                } else {
                    bc = readblock(in, this.streamConvertBuf);
                }

                int lengthLeftToRead = streamLength - bc;

                if (hexEscape) {
                    packet.writeBytes(StringLengthDataType.STRING_FIXED, StringUtils.getBytes("x"));
                } else {
                    packet.writeBytes(StringLengthDataType.STRING_FIXED, StringUtils.getBytes("_binary"));
                }

                if (escape) {
                    packet.writeInteger(IntegerDataType.INT1, (byte) '\'');
                }

                while (bc > 0) {
                    if (hexEscape) {
                        hexEscapeBlock(this.streamConvertBuf, packet, bc);
                    } else if (escape) {
                        escapeblockFast(this.streamConvertBuf, packet, bc);
                    } else {
                        packet.writeBytes(StringLengthDataType.STRING_FIXED, this.streamConvertBuf, 0, bc);
                    }

                    if (useLength) {
                        bc = readblock(in, this.streamConvertBuf, lengthLeftToRead);

                        if (bc > 0) {
                            lengthLeftToRead -= bc;
                        }
                    } else {
                        bc = readblock(in, this.streamConvertBuf);
                    }
                }

                if (escape) {
                    packet.writeInteger(IntegerDataType.INT1, (byte) '\'');
                }
            } finally {
                if (this.autoClosePStmtStreams.getValue()) {
                    try {
                        in.close();
                    } catch (IOException ioEx) {
                    }

                    in = null;
                }
            }
        }
    }

    private final byte[] streamToBytes(InputStream in, boolean escape, int streamLength, boolean useLength) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            in.mark(Integer.MAX_VALUE); // we may need to read this same stream several times, so we need to reset it at the end.
            try {
                if (this.streamConvertBuf == null) {
                    this.streamConvertBuf = new byte[4096];
                }
                if (streamLength == -1) {
                    useLength = false;
                }

                ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();

                int bc = -1;

                if (useLength) {
                    bc = readblock(in, this.streamConvertBuf, streamLength);
                } else {
                    bc = readblock(in, this.streamConvertBuf);
                }

                int lengthLeftToRead = streamLength - bc;

                if (escape) {
                    bytesOut.write('_');
                    bytesOut.write('b');
                    bytesOut.write('i');
                    bytesOut.write('n');
                    bytesOut.write('a');
                    bytesOut.write('r');
                    bytesOut.write('y');
                    bytesOut.write('\'');
                }

                while (bc > 0) {
                    if (escape) {
                        escapeblockFast(this.streamConvertBuf, bytesOut, bc);
                    } else {
                        bytesOut.write(this.streamConvertBuf, 0, bc);
                    }

                    if (useLength) {
                        bc = readblock(in, this.streamConvertBuf, lengthLeftToRead);

                        if (bc > 0) {
                            lengthLeftToRead -= bc;
                        }
                    } else {
                        bc = readblock(in, this.streamConvertBuf);
                    }
                }

                if (escape) {
                    bytesOut.write('\'');
                }

                return bytesOut.toByteArray();
            } finally {
                try {
                    in.reset();
                } catch (IOException e) {
                }
                if (this.autoClosePStmtStreams.getValue()) {
                    try {
                        in.close();
                    } catch (IOException ioEx) {
                    }

                    in = null;
                }
            }
        }
    }

    /**
     * Returns this PreparedStatement represented as a string.
     * 
     * @return this PreparedStatement represented as a string.
     */
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append(super.toString());
        buf.append(": ");

        try {
            buf.append(asSql());
        } catch (SQLException sqlEx) {
            buf.append("EXCEPTION: " + sqlEx.toString());
        }

        return buf.toString();
    }

    /**
     * For calling stored functions, this will be -1 as we don't really count
     * the first '?' parameter marker, it's only syntax, but JDBC counts it
     * as #1, otherwise it will return 0
     */

    protected int getParameterIndexOffset() {
        return 0;
    }

    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        setAsciiStream(parameterIndex, x, -1);
    }

    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        setAsciiStream(parameterIndex, x, (int) length);
        this.parameterTypes[parameterIndex - 1 + getParameterIndexOffset()] = MysqlType.TEXT; // TODO was Types.CLOB, check; use length to find right TEXT type
    }

    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        setBinaryStream(parameterIndex, x, -1);
    }

    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        setBinaryStream(parameterIndex, x, (int) length);
    }

    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        setBinaryStream(parameterIndex, inputStream);
    }

    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        setCharacterStream(parameterIndex, reader, -1);
    }

    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        setCharacterStream(parameterIndex, reader, (int) length);

    }

    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        setCharacterStream(parameterIndex, reader);

    }

    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        setCharacterStream(parameterIndex, reader, length);
    }

    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        setNCharacterStream(parameterIndex, value, -1);
    }

    /**
     * Set a parameter to a Java String value. The driver converts this to a SQL
     * VARCHAR or LONGVARCHAR value with introducer _utf8 (depending on the
     * arguments size relative to the driver's limits on VARCHARs) when it sends
     * it to the database. If charset is set as utf8, this method just call setString.
     * 
     * @param parameterIndex
     *            the first parameter is 1...
     * @param x
     *            the parameter value
     * 
     * @exception SQLException
     *                if a database access error occurs
     */
    public void setNString(int parameterIndex, String x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (this.charEncoding.equalsIgnoreCase("UTF-8") || this.charEncoding.equalsIgnoreCase("utf8")) {
                setString(parameterIndex, x);
                return;
            }

            // if the passed string is null, then set this column to null
            if (x == null) {
                setNull(parameterIndex, MysqlType.VARCHAR); // was Types.CHAR
            } else {
                int stringLength = x.length();
                // Ignore sql_mode=NO_BACKSLASH_ESCAPES in current implementation.

                // Add introducer _utf8 for NATIONAL CHARACTER
                StringBuilder buf = new StringBuilder((int) (x.length() * 1.1 + 4));
                buf.append("_utf8");
                buf.append('\'');

                //
                // Note: buf.append(char) is _faster_ than appending in blocks, because the block append requires a System.arraycopy().... go figure...
                //

                for (int i = 0; i < stringLength; ++i) {
                    char c = x.charAt(i);

                    switch (c) {
                        case 0: /* Must be escaped for 'mysql' */
                            buf.append('\\');
                            buf.append('0');

                            break;

                        case '\n': /* Must be escaped for logs */
                            buf.append('\\');
                            buf.append('n');

                            break;

                        case '\r':
                            buf.append('\\');
                            buf.append('r');

                            break;

                        case '\\':
                            buf.append('\\');
                            buf.append('\\');

                            break;

                        case '\'':
                            buf.append('\\');
                            buf.append('\'');

                            break;

                        case '"': /* Better safe than sorry */
                            if (this.usingAnsiMode) {
                                buf.append('\\');
                            }

                            buf.append('"');

                            break;

                        case '\032': /* This gives problems on Win32 */
                            buf.append('\\');
                            buf.append('Z');

                            break;

                        default:
                            buf.append(c);
                    }
                }

                buf.append('\'');

                String parameterAsString = buf.toString();

                byte[] parameterAsBytes = null;

                if (!this.isLoadDataQuery) {
                    parameterAsBytes = StringUtils.getBytes(parameterAsString, "UTF-8");
                } else {
                    // Send with platform character encoding
                    parameterAsBytes = StringUtils.getBytes(parameterAsString);
                }

                setInternal(parameterIndex, parameterAsBytes);

                this.parameterTypes[parameterIndex - 1 + getParameterIndexOffset()] = MysqlType.VARCHAR; // TODO was Types.NVARCHAR
            }
        }
    }

    /**
     * JDBC 2.0 When a very large UNICODE value is input to a LONGVARCHAR
     * parameter, it may be more practical to send it via a java.io.Reader. JDBC
     * will read the data from the stream as needed, until it reaches
     * end-of-file. The JDBC driver will do any necessary conversion from
     * UNICODE to the database char format.
     * 
     * <P>
     * <B>Note:</B> This stream object can either be a standard Java stream object or your own subclass that implements the standard interface.
     * </p>
     * 
     * @param parameterIndex
     *            the first parameter is 1, the second is 2, ...
     * @param reader
     *            the java reader which contains the UNICODE data
     * @param length
     *            the number of characters in the stream
     * 
     * @exception SQLException
     *                if a database-access error occurs.
     */
    public void setNCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            try {
                if (reader == null) {
                    setNull(parameterIndex, MysqlType.TEXT);

                } else {
                    char[] c = null;
                    int len = 0;

                    boolean useLength = this.useStreamLengthsInPrepStmts.getValue();

                    // Ignore "clobCharacterEncoding" because utf8 should be used this time.

                    if (useLength && (length != -1)) {
                        c = new char[(int) length];  // can't take more than Integer.MAX_VALUE

                        int numCharsRead = readFully(reader, c, (int) length); // blocks until all read
                        setNString(parameterIndex, new String(c, 0, numCharsRead));

                    } else {
                        c = new char[4096];

                        StringBuilder buf = new StringBuilder();

                        while ((len = reader.read(c)) != -1) {
                            buf.append(c, 0, len);
                        }

                        setNString(parameterIndex, buf.toString());
                    }

                    this.parameterTypes[parameterIndex - 1 + getParameterIndexOffset()] = MysqlType.TEXT; // TODO was Types.NCLOB; use length to find right TEXT type
                }
            } catch (java.io.IOException ioEx) {
                throw SQLError.createSQLException(ioEx.toString(), SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
            }
        }
    }

    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        setNCharacterStream(parameterIndex, reader);
    }

    /**
     * Set a NCLOB parameter.
     * 
     * @param parameterIndex
     *            the first parameter is 1, the second is 2, ...
     * @param reader
     *            the java reader which contains the UNICODE data
     * @param length
     *            the number of characters in the stream
     * 
     * @throws SQLException
     *             if a database error occurs
     */
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        if (reader == null) {
            setNull(parameterIndex, MysqlType.TEXT);
        } else {
            setNCharacterStream(parameterIndex, reader, length);
        }
    }

    public ParameterBindings getParameterBindings() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            return new EmulatedPreparedStatementBindings();
        }
    }

    class EmulatedPreparedStatementBindings implements ParameterBindings {

        private ResultSetImpl bindingsAsRs;
        private boolean[] parameterIsNull;

        EmulatedPreparedStatementBindings() throws SQLException {
            List<Row> rows = new ArrayList<Row>();
            this.parameterIsNull = new boolean[PreparedStatement.this.parameterCount];
            System.arraycopy(PreparedStatement.this.isNull, 0, this.parameterIsNull, 0, PreparedStatement.this.parameterCount);
            byte[][] rowData = new byte[PreparedStatement.this.parameterCount][];
            Field[] typeMetadata = new Field[PreparedStatement.this.parameterCount];

            for (int i = 0; i < PreparedStatement.this.parameterCount; i++) {
                if (PreparedStatement.this.batchCommandIndex == -1) {
                    rowData[i] = getBytesRepresentation(i);
                } else {
                    rowData[i] = getBytesRepresentationForBatch(i, PreparedStatement.this.batchCommandIndex);
                }

                int charsetIndex = 0;

                switch (PreparedStatement.this.parameterTypes[i]) {
                    case BINARY:
                    case BLOB:
                    case GEOMETRY:
                    case LONGBLOB:
                    case MEDIUMBLOB:
                    case TINYBLOB:
                    case UNKNOWN:
                    case VARBINARY:
                        charsetIndex = CharsetMapping.MYSQL_COLLATION_INDEX_binary;
                        break;
                    default:
                        try {
                            charsetIndex = CharsetMapping
                                    .getCollationIndexForJavaEncoding(
                                            PreparedStatement.this.session.getPropertySet()
                                                    .getStringReadableProperty(PropertyDefinitions.PNAME_characterEncoding).getValue(),
                                            PreparedStatement.this.session.getServerVersion());
                        } catch (RuntimeException ex) {
                            throw SQLError.createSQLException(ex.toString(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, ex, null);
                        }
                        break;
                }

                Field parameterMetadata = new Field(null, "parameter_" + (i + 1), charsetIndex, PreparedStatement.this.charEncoding,
                        PreparedStatement.this.parameterTypes[i], rowData[i].length);
                typeMetadata[i] = parameterMetadata;
            }

            rows.add(new ByteArrayRow(rowData, getExceptionInterceptor()));

            this.bindingsAsRs = PreparedStatement.this.resultSetFactory.createFromResultsetRows(ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    new ResultsetRowsStatic(rows, new MysqlaColumnDefinition(typeMetadata)));
            this.bindingsAsRs.next();
        }

        public Array getArray(int parameterIndex) throws SQLException {
            return this.bindingsAsRs.getArray(parameterIndex);
        }

        public InputStream getAsciiStream(int parameterIndex) throws SQLException {
            return this.bindingsAsRs.getAsciiStream(parameterIndex);
        }

        public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
            return this.bindingsAsRs.getBigDecimal(parameterIndex);
        }

        public InputStream getBinaryStream(int parameterIndex) throws SQLException {
            return this.bindingsAsRs.getBinaryStream(parameterIndex);
        }

        public java.sql.Blob getBlob(int parameterIndex) throws SQLException {
            return this.bindingsAsRs.getBlob(parameterIndex);
        }

        public boolean getBoolean(int parameterIndex) throws SQLException {
            return this.bindingsAsRs.getBoolean(parameterIndex);
        }

        public byte getByte(int parameterIndex) throws SQLException {
            return this.bindingsAsRs.getByte(parameterIndex);
        }

        public byte[] getBytes(int parameterIndex) throws SQLException {
            return this.bindingsAsRs.getBytes(parameterIndex);
        }

        public Reader getCharacterStream(int parameterIndex) throws SQLException {
            return this.bindingsAsRs.getCharacterStream(parameterIndex);
        }

        public java.sql.Clob getClob(int parameterIndex) throws SQLException {
            return this.bindingsAsRs.getClob(parameterIndex);
        }

        public Date getDate(int parameterIndex) throws SQLException {
            return this.bindingsAsRs.getDate(parameterIndex);
        }

        public double getDouble(int parameterIndex) throws SQLException {
            return this.bindingsAsRs.getDouble(parameterIndex);
        }

        public float getFloat(int parameterIndex) throws SQLException {
            return this.bindingsAsRs.getFloat(parameterIndex);
        }

        public int getInt(int parameterIndex) throws SQLException {
            return this.bindingsAsRs.getInt(parameterIndex);
        }

        public BigInteger getBigInteger(int parameterIndex) throws SQLException {
            return this.bindingsAsRs.getBigInteger(parameterIndex);
        }

        public long getLong(int parameterIndex) throws SQLException {
            return this.bindingsAsRs.getLong(parameterIndex);
        }

        public Reader getNCharacterStream(int parameterIndex) throws SQLException {
            return this.bindingsAsRs.getCharacterStream(parameterIndex);
        }

        public Reader getNClob(int parameterIndex) throws SQLException {
            return this.bindingsAsRs.getCharacterStream(parameterIndex);
        }

        public Object getObject(int parameterIndex) throws SQLException {
            checkBounds(parameterIndex, 0);

            if (this.parameterIsNull[parameterIndex - 1]) {
                return null;
            }

            // we can't rely on the default mapping for JDBC's ResultSet.getObject() for numerics, they're not one-to-one with PreparedStatement.setObject

            switch (PreparedStatement.this.parameterTypes[parameterIndex - 1]) {
                case TINYINT:
                case TINYINT_UNSIGNED:
                    return Byte.valueOf(getByte(parameterIndex));
                case SMALLINT:
                case SMALLINT_UNSIGNED:
                    return Short.valueOf(getShort(parameterIndex));
                case INT:
                case INT_UNSIGNED:
                    return Integer.valueOf(getInt(parameterIndex));
                case BIGINT:
                    return Long.valueOf(getLong(parameterIndex));
                case BIGINT_UNSIGNED:
                    return getBigInteger(parameterIndex);
                case FLOAT:
                case FLOAT_UNSIGNED:
                    return Float.valueOf(getFloat(parameterIndex));
                case DOUBLE:
                case DOUBLE_UNSIGNED:
                    return Double.valueOf(getDouble(parameterIndex));
                default:
                    return this.bindingsAsRs.getObject(parameterIndex);
            }
        }

        public Ref getRef(int parameterIndex) throws SQLException {
            return this.bindingsAsRs.getRef(parameterIndex);
        }

        public short getShort(int parameterIndex) throws SQLException {
            return this.bindingsAsRs.getShort(parameterIndex);
        }

        public String getString(int parameterIndex) throws SQLException {
            return this.bindingsAsRs.getString(parameterIndex);
        }

        public Time getTime(int parameterIndex) throws SQLException {
            return this.bindingsAsRs.getTime(parameterIndex);
        }

        public Timestamp getTimestamp(int parameterIndex) throws SQLException {
            return this.bindingsAsRs.getTimestamp(parameterIndex);
        }

        public URL getURL(int parameterIndex) throws SQLException {
            return this.bindingsAsRs.getURL(parameterIndex);
        }

        public boolean isNull(int parameterIndex) throws SQLException {
            checkBounds(parameterIndex, 0);

            return this.parameterIsNull[parameterIndex - 1];
        }
    }

    public String getPreparedSql() {
        synchronized (checkClosed().getConnectionMutex()) {
            if (this.rewrittenBatchSize == 0) {
                return this.originalSql;
            }

            try {
                return this.parseInfo.getSqlForBatch(this.parseInfo);
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public int getUpdateCount() throws SQLException {
        int count = super.getUpdateCount();

        if (containsOnDuplicateKeyUpdateInSQL() && this.compensateForOnDuplicateKeyUpdate) {
            if (count == 2 || count == 0) {
                count = 1;
            }
        }

        return count;
    }

    protected static boolean canRewrite(String sql, boolean isOnDuplicateKeyUpdate, int locationOfOnDuplicateKeyUpdate, int statementStartPos) {
        // Needs to be INSERT or REPLACE.
        // Can't have INSERT ... SELECT or INSERT ... ON DUPLICATE KEY UPDATE with an id=LAST_INSERT_ID(...).

        if (StringUtils.startsWithIgnoreCaseAndWs(sql, "INSERT", statementStartPos)) {
            if (StringUtils.indexOfIgnoreCase(statementStartPos, sql, "SELECT", "\"'`", "\"'`", StringUtils.SEARCH_MODE__MRK_COM_WS) != -1) {
                return false;
            }
            if (isOnDuplicateKeyUpdate) {
                int updateClausePos = StringUtils.indexOfIgnoreCase(locationOfOnDuplicateKeyUpdate, sql, " UPDATE ");
                if (updateClausePos != -1) {
                    return StringUtils.indexOfIgnoreCase(updateClausePos, sql, "LAST_INSERT_ID", "\"'`", "\"'`", StringUtils.SEARCH_MODE__MRK_COM_WS) == -1;
                }
            }
            return true;
        }

        return StringUtils.startsWithIgnoreCaseAndWs(sql, "REPLACE", statementStartPos)
                && StringUtils.indexOfIgnoreCase(statementStartPos, sql, "SELECT", "\"'`", "\"'`", StringUtils.SEARCH_MODE__MRK_COM_WS) == -1;
    }

    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    /**
     * Set a NCLOB parameter.
     * 
     * @param i
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            an object representing a NCLOB
     * 
     * @throws SQLException
     *             if a database error occurs
     */
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        if (value == null) {
            setNull(parameterIndex, MysqlType.TEXT);
        } else {
            setNCharacterStream(parameterIndex, value.getCharacterStream(), value.length());
        }
    }

    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        if (xmlObject == null) {
            setNull(parameterIndex, MysqlType.VARCHAR);
        } else {
            // FIXME: Won't work for Non-MYSQL SQLXMLs
            setCharacterStream(parameterIndex, ((MysqlSQLXML) xmlObject).serializeAsCharacterStream());
        }
    }

    /**
     * JDBC 4.2
     * Same as PreparedStatement.executeUpdate() but returns long instead of int.
     */
    public long executeLargeUpdate() throws SQLException {
        return executeUpdateInternal(true, false);
    }
}
