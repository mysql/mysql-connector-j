/*
 * Copyright (c) 2017, 2021, Oracle and/or its affiliates.
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
import com.mysql.cj.util.SearchMode;
import com.mysql.cj.util.StringInspector;
import com.mysql.cj.util.StringUtils;

/**
 * Represents the "parsed" state of a prepared query, with the statement broken up into its static and dynamic (where parameters are bound) parts.
 */
public class ParseInfo {
    private static final String OPENING_MARKERS = "`'\"";
    private static final String CLOSING_MARKERS = "`'\"";
    private static final String OVERRIDING_MARKERS = "";

    private static final String[] ON_DUPLICATE_KEY_UPDATE_CLAUSE = new String[] { "ON", "DUPLICATE", "KEY", "UPDATE" };
    private static final String[] LOAD_DATA_CLAUSE = new String[] { "LOAD", "DATA" };

    private String charEncoding;
    private int statementLength = 0;
    private int statementStartPos = 0;
    private char firstStmtChar = 0;
    private QueryReturnType queryReturnType = null;
    private boolean hasParameters = false;
    private boolean parametersInDuplicateKeyClause = false;
    private boolean isLoadData = false;
    private boolean isOnDuplicateKeyUpdate = false;
    private int locationOfOnDuplicateKeyUpdate = -1;

    private int numberOfQueries = 1;

    private boolean canRewriteAsMultiValueInsert = false;
    private String valuesClause;
    private ParseInfo batchHead;
    private ParseInfo batchValues;
    private ParseInfo batchODKUClause;

    private byte[][] staticSql = null;

    private ParseInfo(byte[][] staticSql, char firstStmtChar, QueryReturnType queryReturnType, boolean isLoadData, boolean isOnDuplicateKeyUpdate,
            int locationOfOnDuplicateKeyUpdate, int statementLength, int statementStartPos) {
        this.firstStmtChar = firstStmtChar;
        this.queryReturnType = queryReturnType;
        this.isLoadData = isLoadData;
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
            this.statementLength = sql.length();

            boolean noBackslashEscapes = session.getServerSession().isNoBackslashEscapesSet();
            this.queryReturnType = getQueryReturnType(sql, noBackslashEscapes);

            // Skip comments at the beginning of statements, as frameworks such as Hibernate use them to aid in debugging.
            this.statementStartPos = indexOfStartOfStatement(sql, session.getServerSession().isNoBackslashEscapesSet());
            if (this.statementStartPos == -1) {
                this.statementStartPos = this.statementLength;
            }

            // Determine what kind of statement we're doing (_S_elect, _I_nsert, etc.)
            int statementKeywordPos = StringUtils.indexOfNextAlphanumericChar(this.statementStartPos, sql, OPENING_MARKERS, CLOSING_MARKERS, OVERRIDING_MARKERS,
                    noBackslashEscapes ? SearchMode.__MRK_COM_MYM_HNT_WS : SearchMode.__BSE_MRK_COM_MYM_HNT_WS);
            if (statementKeywordPos >= 0) {
                this.firstStmtChar = Character.toUpperCase(sql.charAt(statementKeywordPos));
            }

            // Check if this is a LOAD DATA statement.
            this.isLoadData = this.firstStmtChar == 'L' && StringUtils.indexOfIgnoreCase(this.statementStartPos, sql, LOAD_DATA_CLAUSE, OPENING_MARKERS,
                    CLOSING_MARKERS, noBackslashEscapes ? SearchMode.__MRK_COM_MYM_HNT_WS : SearchMode.__FULL) == this.statementStartPos;

            // Check if "ON DUPLICATE KEY UPDATE" is present. No need to search if not an INSERT statement.
            if (this.firstStmtChar == 'I' && StringUtils.startsWithIgnoreCaseAndWs(sql, "INSERT", this.statementStartPos)) {
                this.locationOfOnDuplicateKeyUpdate = getOnDuplicateKeyLocation(sql,
                        session.getPropertySet().getBooleanProperty(PropertyKey.dontCheckOnDuplicateKeyUpdateInSQL).getValue(),
                        session.getPropertySet().getBooleanProperty(PropertyKey.rewriteBatchedStatements).getValue(),
                        session.getServerSession().isNoBackslashEscapesSet());
                this.isOnDuplicateKeyUpdate = this.locationOfOnDuplicateKeyUpdate != -1;
            }

            StringInspector strInspector = new StringInspector(sql, this.statementStartPos, OPENING_MARKERS, CLOSING_MARKERS, OVERRIDING_MARKERS,
                    noBackslashEscapes ? SearchMode.__MRK_COM_MYM_HNT_WS : SearchMode.__FULL);
            int pos = this.statementStartPos;
            int prevParamEnd = 0;
            ArrayList<int[]> endpointList = new ArrayList<>();
            while ((pos = strInspector.indexOfNextNonWsChar()) >= 0) {
                if (strInspector.getChar() == '?') {
                    endpointList.add(new int[] { prevParamEnd, pos });
                    prevParamEnd = pos + 1;

                    if (this.isOnDuplicateKeyUpdate && pos > this.locationOfOnDuplicateKeyUpdate) {
                        this.parametersInDuplicateKeyClause = true;
                    }
                    strInspector.incrementPosition();

                } else if (strInspector.getChar() == ';') {
                    strInspector.incrementPosition();
                    pos = strInspector.indexOfNextNonWsChar();
                    if (pos > 0) {
                        this.numberOfQueries++;
                    }

                } else {
                    strInspector.incrementPosition();
                }
            }

            endpointList.add(new int[] { prevParamEnd, this.statementLength });
            this.staticSql = new byte[endpointList.size()][];
            this.hasParameters = this.staticSql.length > 1;

            for (int i = 0; i < this.staticSql.length; i++) {
                int[] ep = endpointList.get(i);
                int end = ep[1];
                int begin = ep[0];
                int len = end - begin;

                if (this.isLoadData) {
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
        } catch (Exception oobEx) {
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

    public int getNumberOfQueries() {
        return this.numberOfQueries;
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

    public QueryReturnType getQueryReturnType() {
        return this.queryReturnType;
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

        int indexOfFirstEqualsChar = StringUtils.indexOfIgnoreCase(valuesSearchStart, sql, "=", quoteCharStr, quoteCharStr, SearchMode.__MRK_COM_MYM_HNT_WS);

        while (indexOfValues == -1) {
            // "VALUE" is a synonym of "VALUES" clause, so checking for the first one
            if (quoteCharStr.length() > 0) {
                indexOfValues = StringUtils.indexOfIgnoreCase(valuesSearchStart, sql, "VALUE", quoteCharStr, quoteCharStr, SearchMode.__MRK_COM_MYM_HNT_WS);
            } else {
                indexOfValues = StringUtils.indexOfIgnoreCase(valuesSearchStart, sql, "VALUE");
            }

            if (indexOfFirstEqualsChar > 0 && indexOfValues > indexOfFirstEqualsChar) {
                // VALUES clause always precedes the first '=' occurrence, otherwise it's a values() function
                indexOfValues = -1;
            }

            // TODO: this doesn't support queries like "INSERT INTO t /* foo */VALUES/* bar */(...)" although its valid. Replace by StringInspector. 
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

        ParseInfo batchParseInfo = new ParseInfo(apv.getStaticSqlStrings(), this.firstStmtChar, this.queryReturnType, this.isLoadData,
                this.isOnDuplicateKeyUpdate, this.locationOfOnDuplicateKeyUpdate, this.statementLength, this.statementStartPos);

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
     * Builds a ParseInfo for the given batch size, without parsing. We use a visitor pattern here, because the if {}s make computing a size for the resultant
     * byte[][] too complex, and we don't necessarily want to use a List for this, because the size can be dynamic, and thus we'll not be able to guess a good
     * initial size for an array-based list, and it's not efficient to convert a LinkedList to an array.
     * 
     * @param numBatch
     *            number of batched parameters
     * @param visitor
     *            visitor
     */
    private void buildInfoForBatch(int numBatch, BatchVisitor visitor) {
        if (!this.hasParameters) {
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

    public boolean isLoadData() {
        return this.isLoadData;
    }

    public char getFirstStmtChar() {
        return this.firstStmtChar;
    }

    public static int indexOfStartOfStatement(String sql, boolean noBackslashEscapes) {
        return StringUtils.indexOfNextNonWsChar(0, sql, OPENING_MARKERS, CLOSING_MARKERS, OVERRIDING_MARKERS,
                noBackslashEscapes ? SearchMode.__MRK_COM_MYM_HNT_WS : SearchMode.__BSE_MRK_COM_MYM_HNT_WS);
    }

    public static int indexOfStatementKeyword(String sql, boolean noBackslashEscapes) {
        return StringUtils.indexOfNextAlphanumericChar(0, sql, OPENING_MARKERS, CLOSING_MARKERS, OVERRIDING_MARKERS,
                noBackslashEscapes ? SearchMode.__MRK_COM_MYM_HNT_WS : SearchMode.__BSE_MRK_COM_MYM_HNT_WS);
    }

    public static char firstCharOfStatementUc(String sql, boolean noBackslashEscapes) {
        int statementKeywordPos = indexOfStatementKeyword(sql, noBackslashEscapes);
        if (statementKeywordPos == -1) {
            return Character.MIN_VALUE;
        }
        return Character.toUpperCase(sql.charAt(statementKeywordPos));
    }

    /**
     * Checks whether the given query is safe to run in a read-only session. In case of doubt it is assumed to be safe.
     * 
     * @param sql
     *            the SQL to check
     * @param noBackslashEscapes
     *            whether backslash escapes are disabled or not
     * @return
     *         <code>true</code> if the query is read-only safe, <code>false</code> otherwise.
     */
    public static boolean isReadOnlySafeQuery(String sql, boolean noBackslashEscapes) {
        /*
         * Read-only unsafe statements:
         * - ALTER; CHANGE; CREATE; DELETE; DROP; GRANT; IMPORT; INSERT; INSTALL; LOAD; OPTIMIZE; RENAME; REPAIR; REPLACE; RESET; REVOKE; TRUNCATE; UNINSTALL;
         * - UPDATE; WITH ... DELETE|UPDATE
         * 
         * Read-only safe statements:
         * - ANALYZE; BEGIN; BINLOG; CACHE; CALL; CHECK; CHECKSUM; CLONE; COMMIT; DEALLOCATE; DESC; DESCRIBE; EXECUTE; EXPLAIN; FLUSH; GET; HANDLER; HELP; KILL;
         * - LOCK; PREPARE; PURGE; RELEASE; RESIGNAL; ROLLBACK; SAVEPOINT; SELECT; SET; SHOW; SIGNAL; START; STOP; TABLE; UNLOCK; USE; VALUES;
         * - WITH ... [SELECT|TABLE|VALUES]; XA
         */
        int statementKeywordPos = indexOfStatementKeyword(sql, noBackslashEscapes);
        if (statementKeywordPos == -1) {
            return true; // Assume it's safe.
        }
        char firstStatementChar = Character.toUpperCase(sql.charAt(statementKeywordPos));
        if (firstStatementChar == 'A' && StringUtils.startsWithIgnoreCaseAndWs(sql, "ALTER", statementKeywordPos)) {
            return false;
        } else if (firstStatementChar == 'C' && (StringUtils.startsWithIgnoreCaseAndWs(sql, "CHANGE", statementKeywordPos)
                || StringUtils.startsWithIgnoreCaseAndWs(sql, "CREATE", statementKeywordPos))) {
            return false;
        } else if (firstStatementChar == 'D' && (StringUtils.startsWithIgnoreCaseAndWs(sql, "DELETE", statementKeywordPos)
                || StringUtils.startsWithIgnoreCaseAndWs(sql, "DROP", statementKeywordPos))) {
            return false;
        } else if (firstStatementChar == 'G' && StringUtils.startsWithIgnoreCaseAndWs(sql, "GRANT", statementKeywordPos)) {
            return false;
        } else if (firstStatementChar == 'I' && (StringUtils.startsWithIgnoreCaseAndWs(sql, "IMPORT", statementKeywordPos)
                || StringUtils.startsWithIgnoreCaseAndWs(sql, "INSERT", statementKeywordPos)
                || StringUtils.startsWithIgnoreCaseAndWs(sql, "INSTALL", statementKeywordPos))) {
            return false;
        } else if (firstStatementChar == 'L' && StringUtils.startsWithIgnoreCaseAndWs(sql, "LOAD", statementKeywordPos)) {
            return false;
        } else if (firstStatementChar == 'O' && StringUtils.startsWithIgnoreCaseAndWs(sql, "OPTIMIZE", statementKeywordPos)) {
            return false;
        } else if (firstStatementChar == 'R' && (StringUtils.startsWithIgnoreCaseAndWs(sql, "RENAME", statementKeywordPos)
                || StringUtils.startsWithIgnoreCaseAndWs(sql, "REPAIR", statementKeywordPos)
                || StringUtils.startsWithIgnoreCaseAndWs(sql, "REPLACE", statementKeywordPos)
                || StringUtils.startsWithIgnoreCaseAndWs(sql, "RESET", statementKeywordPos)
                || StringUtils.startsWithIgnoreCaseAndWs(sql, "REVOKE", statementKeywordPos))) {
            return false;
        } else if (firstStatementChar == 'T' && StringUtils.startsWithIgnoreCaseAndWs(sql, "TRUNCATE", statementKeywordPos)) {
            return false;
        } else if (firstStatementChar == 'U' && (StringUtils.startsWithIgnoreCaseAndWs(sql, "UNINSTALL", statementKeywordPos)
                || StringUtils.startsWithIgnoreCaseAndWs(sql, "UPDATE", statementKeywordPos))) {
            return false;
        } else if (firstStatementChar == 'W' && StringUtils.startsWithIgnoreCaseAndWs(sql, "WITH", statementKeywordPos)) {
            String context = getContextForWithStatement(sql, noBackslashEscapes);
            return context == null || !context.equalsIgnoreCase("DELETE") && !context.equalsIgnoreCase("UPDATE");
        }
        return true; // Assume it's safe by default.
    }

    /**
     * Returns the type of return that can be expected from executing the given query.
     * 
     * @param sql
     *            the SQL to check
     * @param noBackslashEscapes
     *            whether backslash escapes are disabled or not
     * @return
     *         The return type that can be expected from the given query, one of the elements of {@link QueryReturnType}.
     */
    public static QueryReturnType getQueryReturnType(String sql, boolean noBackslashEscapes) {
        /*
         * Statements that return results:
         * - ANALYZE; CHECK/CHECKSUM; DESC/DESCRIBE; EXPLAIN; HELP; OPTIMIZE; REPAIR; SELECT; SHOW; TABLE; VALUES; WITH ... SELECT|TABLE|VALUES ...; XA RECOVER;
         * 
         * Statements that may return results:
         * - CALL; EXECUTE;
         * 
         * Statements that do not return results:
         * - ALTER; BINLOG; CACHE; CHANGE; CLONE; COMMIT; CREATE; DEALLOCATE; DELETE; DO; DROP; FLUSH; GET; GRANT; HANDLER; IMPORT; INSERT; INSTALL; KILL; LOAD;
         * - LOCK; PREPARE; PURGE; RELEASE; RENAME; REPLACE; RESET; RESIGNAL; RESTART; REVOKE; ROLLBACK; SAVEPOINT; SET; SHUTDOWN; SIGNAL; START; STOP;
         * - TRUNCATE; UNINSTALL; UNLOCK; UPDATE; USE; WITH ... DELETE|UPDATE ...; XA [!RECOVER];
         */
        int statementKeywordPos = indexOfStatementKeyword(sql, noBackslashEscapes);
        if (statementKeywordPos == -1) {
            return QueryReturnType.NONE;
        }
        char firstStatementChar = Character.toUpperCase(sql.charAt(statementKeywordPos));
        if (firstStatementChar == 'A' && StringUtils.startsWithIgnoreCaseAndWs(sql, "ANALYZE", statementKeywordPos)) {
            return QueryReturnType.PRODUCES_RESULT_SET;
        } else if (firstStatementChar == 'C' && StringUtils.startsWithIgnoreCaseAndWs(sql, "CALL", statementKeywordPos)) {
            return QueryReturnType.MAY_PRODUCE_RESULT_SET;
        } else if (firstStatementChar == 'C' && StringUtils.startsWithIgnoreCaseAndWs(sql, "CHECK", statementKeywordPos)) { // Also matches "CHECKSUM".
            return QueryReturnType.PRODUCES_RESULT_SET;
        } else if (firstStatementChar == 'D' && StringUtils.startsWithIgnoreCaseAndWs(sql, "DESC", statementKeywordPos)) { // Also matches "DESCRIBE".
            return QueryReturnType.PRODUCES_RESULT_SET;
        } else if (firstStatementChar == 'E' && StringUtils.startsWithIgnoreCaseAndWs(sql, "EXPLAIN", statementKeywordPos)) {
            return QueryReturnType.PRODUCES_RESULT_SET;
        } else if (firstStatementChar == 'E' && StringUtils.startsWithIgnoreCaseAndWs(sql, "EXECUTE", statementKeywordPos)) {
            return QueryReturnType.MAY_PRODUCE_RESULT_SET;
        } else if (firstStatementChar == 'H' && StringUtils.startsWithIgnoreCaseAndWs(sql, "HELP", statementKeywordPos)) {
            return QueryReturnType.PRODUCES_RESULT_SET;
        } else if (firstStatementChar == 'O' && StringUtils.startsWithIgnoreCaseAndWs(sql, "OPTIMIZE", statementKeywordPos)) {
            return QueryReturnType.PRODUCES_RESULT_SET;
        } else if (firstStatementChar == 'R' && StringUtils.startsWithIgnoreCaseAndWs(sql, "REPAIR", statementKeywordPos)) {
            return QueryReturnType.PRODUCES_RESULT_SET;
        } else if (firstStatementChar == 'S' && (StringUtils.startsWithIgnoreCaseAndWs(sql, "SELECT", statementKeywordPos)
                || StringUtils.startsWithIgnoreCaseAndWs(sql, "SHOW", statementKeywordPos))) {
            return QueryReturnType.PRODUCES_RESULT_SET;
        } else if (firstStatementChar == 'T' && StringUtils.startsWithIgnoreCaseAndWs(sql, "TABLE", statementKeywordPos)) {
            return QueryReturnType.PRODUCES_RESULT_SET;
        } else if (firstStatementChar == 'V' && StringUtils.startsWithIgnoreCaseAndWs(sql, "VALUES", statementKeywordPos)) {
            return QueryReturnType.PRODUCES_RESULT_SET;
        } else if (firstStatementChar == 'W' && StringUtils.startsWithIgnoreCaseAndWs(sql, "WITH", statementKeywordPos)) {
            String context = getContextForWithStatement(sql, noBackslashEscapes);
            if (context == null) {
                return QueryReturnType.MAY_PRODUCE_RESULT_SET;
            } else if (context.equalsIgnoreCase("SELECT") || context.equalsIgnoreCase("TABLE") || context.equalsIgnoreCase("VALUES")) {
                return QueryReturnType.PRODUCES_RESULT_SET;
            } else {
                return QueryReturnType.DOES_NOT_PRODUCE_RESULT_SET;
            }
        } else if (firstStatementChar == 'X' && StringUtils.indexOfIgnoreCase(statementKeywordPos, sql, new String[] { "XA", "RECOVER" }, OPENING_MARKERS,
                CLOSING_MARKERS, noBackslashEscapes ? SearchMode.__MRK_COM_MYM_HNT_WS : SearchMode.__FULL) == statementKeywordPos) {
            return QueryReturnType.PRODUCES_RESULT_SET;
        }
        return QueryReturnType.DOES_NOT_PRODUCE_RESULT_SET;
    }

    /**
     * Returns the context of the WITH statement. The context can be: SELECT, TABLE, VALUES, UPDATE or DELETE.
     * 
     * @param sql
     *            the query to search
     * @param noBackslashEscapes
     *            whether backslash escapes are disabled or not
     * @return
     *         the context of the WITH statement or null if failed to find it
     */
    private static String getContextForWithStatement(String sql, boolean noBackslashEscapes) {
        // Must remove all comments first.
        String commentsFreeSql = StringUtils.stripCommentsAndHints(sql, OPENING_MARKERS, CLOSING_MARKERS, !noBackslashEscapes);

        // Iterate through statement words, skipping all sub-queries sections enclosed by parens.
        StringInspector strInspector = new StringInspector(commentsFreeSql, OPENING_MARKERS + "(", CLOSING_MARKERS + ")", OPENING_MARKERS,
                noBackslashEscapes ? SearchMode.__MRK_COM_MYM_HNT_WS : SearchMode.__BSE_MRK_COM_MYM_HNT_WS);
        boolean asFound = false;
        while (true) {
            int nws = strInspector.indexOfNextNonWsChar();
            if (nws == -1) { // No more parts to analyze.
                return null;
            }
            int ws = strInspector.indexOfNextWsChar();
            if (ws == -1) { // End of query.
                ws = commentsFreeSql.length();
            }
            String section = commentsFreeSql.substring(nws, ws);
            if (!asFound && section.equalsIgnoreCase("AS")) {
                asFound = true; // Since the subquery part is skipped, this must be followed by a "," or the context statement.
            } else if (asFound) {
                if (section.equalsIgnoreCase(",")) {
                    asFound = false; // Another CTE is expected.
                } else {
                    return section;
                }
            }
        }
    }

    public static int getOnDuplicateKeyLocation(String sql, boolean dontCheckOnDuplicateKeyUpdateInSQL, boolean rewriteBatchedStatements,
            boolean noBackslashEscapes) {
        return dontCheckOnDuplicateKeyUpdateInSQL && !rewriteBatchedStatements ? -1
                : StringUtils.indexOfIgnoreCase(0, sql, ON_DUPLICATE_KEY_UPDATE_CLAUSE, OPENING_MARKERS, CLOSING_MARKERS,
                        noBackslashEscapes ? SearchMode.__MRK_COM_MYM_HNT_WS : SearchMode.__BSE_MRK_COM_MYM_HNT_WS);
    }

    protected static boolean canRewrite(String sql, boolean isOnDuplicateKeyUpdate, int locationOfOnDuplicateKeyUpdate, int statementStartPos) {
        // Needs to be INSERT or REPLACE.
        // Can't have INSERT ... SELECT or INSERT ... ON DUPLICATE KEY UPDATE with an id=LAST_INSERT_ID(...).

        if (StringUtils.startsWithIgnoreCaseAndWs(sql, "INSERT", statementStartPos)) {
            if (StringUtils.indexOfIgnoreCase(statementStartPos, sql, "SELECT", OPENING_MARKERS, CLOSING_MARKERS, SearchMode.__MRK_COM_MYM_HNT_WS) != -1) {
                return false;
            }
            if (isOnDuplicateKeyUpdate) {
                int updateClausePos = StringUtils.indexOfIgnoreCase(locationOfOnDuplicateKeyUpdate, sql, " UPDATE ");
                if (updateClausePos != -1) {
                    return StringUtils.indexOfIgnoreCase(updateClausePos, sql, "LAST_INSERT_ID", OPENING_MARKERS, CLOSING_MARKERS,
                            SearchMode.__MRK_COM_MYM_HNT_WS) == -1;
                }
            }
            return true;
        }

        return StringUtils.startsWithIgnoreCaseAndWs(sql, "REPLACE", statementStartPos)
                && StringUtils.indexOfIgnoreCase(statementStartPos, sql, "SELECT", OPENING_MARKERS, CLOSING_MARKERS, SearchMode.__MRK_COM_MYM_HNT_WS) == -1;
    }
}
