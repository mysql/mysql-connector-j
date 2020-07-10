/*
 * Copyright (c) 2017, 2020, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.mysql.cj;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.util.StringUtils;

/**
 * Represents the "parsed" state of a prepared query, with the statement broken up into its static and dynamic (where parameters are bound) parts.
 */
public class ParseInfo {

    protected static final String[] ON_DUPLICATE_KEY_UPDATE_CLAUSE = new String[] { "ON", "DUPLICATE", "KEY", "UPDATE" };

    private char firstStmtChar = 0;

    private boolean foundLoadData = false;

    long lastUsed = 0;

    int statementLength = 0;

    int statementStartPos = 0;

    boolean canRewriteAsMultiValueInsert = false;

    byte[][] staticSql = null;

    boolean hasPlaceholders = false;

    public int numberOfQueries = 1;

    boolean isOnDuplicateKeyUpdate = false;

    int locationOfOnDuplicateKeyUpdate = -1;

    String valuesClause;

    boolean parametersInDuplicateKeyClause = false;

    String charEncoding;

    private ParseInfo batchHead;

    private ParseInfo batchValues;

    private ParseInfo batchODKUClause;

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

    public ParseInfo(String sql, Session session, String encoding) {
        this(sql, session, encoding, true);
    }

    public ParseInfo(String sql, Session session, String encoding, boolean buildRewriteInfo) {

        try {
            if (sql == null) {
                throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("PreparedStatement.61"),
                        session.getExceptionInterceptor());
            }

            this.charEncoding = encoding;
            this.lastUsed = System.currentTimeMillis();

            String quotedIdentifierString = session.getIdentifierQuoteString();

            char quotedIdentifierChar = 0;

            if ((quotedIdentifierString != null) && !quotedIdentifierString.equals(" ") && (quotedIdentifierString.length() > 0)) {
                quotedIdentifierChar = quotedIdentifierString.charAt(0);
            }

            this.statementLength = sql.length();

            ArrayList<int[]> endpointList = new ArrayList<>();
            boolean inQuotes = false;
            char quoteChar = 0;
            boolean inQuotedId = false;
            int lastParmEnd = 0;
            int i;

            boolean noBackslashEscapes = session.getServerSession().isNoBackslashEscapesSet();

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
                                session.getPropertySet().getBooleanProperty(PropertyKey.dontCheckOnDuplicateKeyUpdateInSQL).getValue(),
                                session.getPropertySet().getBooleanProperty(PropertyKey.rewriteBatchedStatements).getValue(),
                                session.getServerSession().isNoBackslashEscapesSet());
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
                    //  only respect quotes when not in a quoted identifier

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

                if (!inQuotes && !inQuotedId) {
                    if ((c == '?')) {
                        endpointList.add(new int[] { lastParmEnd, i });
                        lastParmEnd = i + 1;

                        if (this.isOnDuplicateKeyUpdate && i > this.locationOfOnDuplicateKeyUpdate) {
                            this.parametersInDuplicateKeyClause = true;
                        }
                    } else if (c == ';') {
                        int j = i + 1;
                        if (j < this.statementLength) {
                            for (; j < this.statementLength; j++) {
                                if (!Character.isWhitespace(sql.charAt(j))) {
                                    break;
                                }
                            }
                            if (j < this.statementLength) {
                                this.numberOfQueries++;
                            }
                            i = j - 1;
                        }
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
            this.hasPlaceholders = this.staticSql.length > 1;

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
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("PreparedStatement.62", new Object[] { sql }), oobEx,
                    session.getExceptionInterceptor());
        }

        if (buildRewriteInfo) {
            this.canRewriteAsMultiValueInsert = this.numberOfQueries == 1 && !this.parametersInDuplicateKeyClause
                    && canRewrite(sql, this.isOnDuplicateKeyUpdate, this.locationOfOnDuplicateKeyUpdate, this.statementStartPos);
            if (this.canRewriteAsMultiValueInsert && session.getPropertySet().getBooleanProperty(PropertyKey.rewriteBatchedStatements).getValue()) {
                buildRewriteBatchedParams(sql, session, encoding);
            }
        }

    }

    public byte[][] getStaticSql() {
        return this.staticSql;
    }

    public String getValuesClause() {
        return this.valuesClause;
    }

    public int getLocationOfOnDuplicateKeyUpdate() {
        return this.locationOfOnDuplicateKeyUpdate;
    }

    public boolean canRewriteAsMultiValueInsertAtSqlLevel() {
        return this.canRewriteAsMultiValueInsert;
    }

    public boolean containsOnDuplicateKeyUpdateInSQL() {
        return this.isOnDuplicateKeyUpdate;
    }

    private void buildRewriteBatchedParams(String sql, Session session, String encoding) {
        this.valuesClause = extractValuesClause(sql, session.getIdentifierQuoteString());
        String odkuClause = this.isOnDuplicateKeyUpdate ? sql.substring(this.locationOfOnDuplicateKeyUpdate) : null;

        String headSql = null;

        if (this.isOnDuplicateKeyUpdate) {
            headSql = sql.substring(0, this.locationOfOnDuplicateKeyUpdate);
        } else {
            headSql = sql;
        }

        this.batchHead = new ParseInfo(headSql, session, encoding, false);
        this.batchValues = new ParseInfo("," + this.valuesClause, session, encoding, false);
        this.batchODKUClause = null;

        if (odkuClause != null && odkuClause.length() > 0) {
            this.batchODKUClause = new ParseInfo("," + this.valuesClause + " " + odkuClause, session, encoding, false);
        }
    }

    private String extractValuesClause(String sql, String quoteCharStr) {
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

        int endOfValuesClause = this.isOnDuplicateKeyUpdate ? this.locationOfOnDuplicateKeyUpdate : sql.length();

        return sql.substring(indexOfFirstParen, endOfValuesClause);
    }

    /**
     * Returns a ParseInfo for a multi-value INSERT for a batch of size numBatch (without parsing!).
     * 
     * @param numBatch
     *            number of batched parameters
     * @return {@link ParseInfo}
     */
    public synchronized ParseInfo getParseInfoForBatch(int numBatch) {
        AppendingBatchVisitor apv = new AppendingBatchVisitor();
        buildInfoForBatch(numBatch, apv);

        ParseInfo batchParseInfo = new ParseInfo(apv.getStaticSqlStrings(), this.firstStmtChar, this.foundLoadData, this.isOnDuplicateKeyUpdate,
                this.locationOfOnDuplicateKeyUpdate, this.statementLength, this.statementStartPos);

        return batchParseInfo;
    }

    /**
     * Returns a preparable SQL string for the number of batched parameters; used by server-side prepared statements
     * when re-writing batch INSERTs.
     * 
     * @param numBatch
     *            number of batched parameters
     * @return SQL string
     * @throws UnsupportedEncodingException
     *             if an error occurs
     */
    public String getSqlForBatch(int numBatch) throws UnsupportedEncodingException {
        ParseInfo batchInfo = getParseInfoForBatch(numBatch);

        return batchInfo.getSqlForBatch();
    }

    /**
     * Used for filling in the SQL for getPreparedSql() - for debugging
     * 
     * @return sql string
     * @throws UnsupportedEncodingException
     *             if an error occurs
     */
    public String getSqlForBatch() throws UnsupportedEncodingException {
        int size = 0;
        final byte[][] sqlStrings = this.staticSql;
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
     * 
     * @param numBatch
     *            number of batched parameters
     * @param visitor
     *            visitor
     */
    private void buildInfoForBatch(int numBatch, BatchVisitor visitor) {
        if (!this.hasPlaceholders) {
            if (numBatch == 1) {
                // ParseInfo for a multi-value INSERT that doesn't have any placeholder may require two or more batches (depends on if ODKU is present or not).
                // The original sql should be able to handle it.
                visitor.append(this.staticSql[0]);

                return;
            }

            // Without placeholders, only the values segment of the query needs repeating.

            final byte[] headStaticSql = this.batchHead.staticSql[0];
            visitor.append(headStaticSql).increment();

            int numValueRepeats = numBatch - 1; // First one is in the "head".
            if (this.batchODKUClause != null) {
                numValueRepeats--; // Last one is in the ODKU clause.
            }

            final byte[] valuesStaticSql = this.batchValues.staticSql[0];
            for (int i = 0; i < numValueRepeats; i++) {
                visitor.mergeWithLast(valuesStaticSql).increment();
            }

            if (this.batchODKUClause != null) {
                final byte[] batchOdkuStaticSql = this.batchODKUClause.staticSql[0];
                visitor.mergeWithLast(batchOdkuStaticSql).increment();
            }

            return;
        }

        // Placeholders require assembling all the parts in each segment of the query and repeat them as needed.

        // Add the head section except the last part.
        final byte[][] headStaticSql = this.batchHead.staticSql;
        final int headStaticSqlLength = headStaticSql.length;
        byte[] endOfHead = headStaticSql[headStaticSqlLength - 1];

        for (int i = 0; i < headStaticSqlLength - 1; i++) {
            visitor.append(headStaticSql[i]).increment();
        }

        // Repeat the values section as many times as needed.
        int numValueRepeats = numBatch - 1; // First one is in the "head".
        if (this.batchODKUClause != null) {
            numValueRepeats--; // Last one is in the ODKU clause.
        }

        final byte[][] valuesStaticSql = this.batchValues.staticSql;
        final int valuesStaticSqlLength = valuesStaticSql.length;
        byte[] beginOfValues = valuesStaticSql[0];
        byte[] endOfValues = valuesStaticSql[valuesStaticSqlLength - 1];

        for (int i = 0; i < numValueRepeats; i++) {
            visitor.merge(endOfValues, beginOfValues).increment();
            for (int j = 1; j < valuesStaticSqlLength - 1; j++) {
                visitor.append(valuesStaticSql[j]).increment();
            }
        }

        // Append the last value and/or ending.
        if (this.batchODKUClause != null) {
            final byte[][] batchOdkuStaticSql = this.batchODKUClause.staticSql;
            final int batchOdkuStaticSqlLength = batchOdkuStaticSql.length;
            byte[] beginOfOdku = batchOdkuStaticSql[0];
            byte[] endOfOdku = batchOdkuStaticSql[batchOdkuStaticSqlLength - 1];

            if (numBatch > 1) {
                visitor.merge(numValueRepeats > 0 ? endOfValues : endOfHead, beginOfOdku).increment();
                for (int i = 1; i < batchOdkuStaticSqlLength; i++) {
                    visitor.append(batchOdkuStaticSql[i]).increment();
                }
            } else {
                visitor.append(endOfOdku).increment();
            }
        } else {
            visitor.append(endOfHead);
        }
    }

    protected static int findStartOfStatement(String sql) {
        int statementStartPos = 0;

        if (StringUtils.startsWithIgnoreCaseAndWs(sql, "/*")) {
            statementStartPos = sql.indexOf("*/");

            if (statementStartPos == -1) {
                statementStartPos = 0;
            } else {
                statementStartPos += 2;
            }
        } else if (StringUtils.startsWithIgnoreCaseAndWs(sql, "--") || StringUtils.startsWithIgnoreCaseAndWs(sql, "#")) {
            statementStartPos = sql.indexOf('\n');

            if (statementStartPos == -1) {
                statementStartPos = sql.indexOf('\r');

                if (statementStartPos == -1) {
                    statementStartPos = 0;
                }
            }
        }

        return statementStartPos;
    }

    public static int getOnDuplicateKeyLocation(String sql, boolean dontCheckOnDuplicateKeyUpdateInSQL, boolean rewriteBatchedStatements,
            boolean noBackslashEscapes) {
        return dontCheckOnDuplicateKeyUpdateInSQL && !rewriteBatchedStatements ? -1 : StringUtils.indexOfIgnoreCase(0, sql, ON_DUPLICATE_KEY_UPDATE_CLAUSE,
                "\"'`", "\"'`", noBackslashEscapes ? StringUtils.SEARCH_MODE__MRK_COM_WS : StringUtils.SEARCH_MODE__ALL);
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

    public boolean isFoundLoadData() {
        return this.foundLoadData;
    }

    public char getFirstStmtChar() {
        return this.firstStmtChar;
    }

}
