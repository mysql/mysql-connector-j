/*
 * Copyright (c) 2017, 2024, Oracle and/or its affiliates.
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

package com.mysql.cj;

import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.Message;
import com.mysql.cj.util.StringUtils;

public class ClientPreparedQuery extends AbstractQuery implements PreparedQuery {

    protected QueryInfo queryInfo;

    protected QueryBindings queryBindings = null;

    /** The SQL that was passed in to 'prepare' */
    protected String originalSql = null;

    /** The number of parameters in this PreparedStatement */
    protected int parameterCount;

    /** Command index of currently executing batch command. */
    protected int batchCommandIndex = -1;

    protected RuntimeProperty<Boolean> autoClosePStmtStreams;
    protected RuntimeProperty<Boolean> useStreamLengthsInPrepStmts;

    public ClientPreparedQuery(NativeSession sess) {
        super(sess);
        this.autoClosePStmtStreams = this.session.getPropertySet().getBooleanProperty(PropertyKey.autoClosePStmtStreams);
        this.useStreamLengthsInPrepStmts = this.session.getPropertySet().getBooleanProperty(PropertyKey.useStreamLengthsInPrepStmts);
    }

    @Override
    public void closeQuery() {
        super.closeQuery();
    }

    @Override
    public QueryInfo getQueryInfo() {
        return this.queryInfo;
    }

    @Override
    public void setQueryInfo(QueryInfo queryInfo) {
        this.queryInfo = queryInfo;
    }

    @Override
    public String getOriginalSql() {
        return this.originalSql;
    }

    @Override
    public void setOriginalSql(String originalSql) {
        this.originalSql = originalSql;
    }

    @Override
    public int getParameterCount() {
        return this.parameterCount;
    }

    @Override
    public void setParameterCount(int parameterCount) {
        this.parameterCount = parameterCount;
    }

    @Override
    public QueryBindings getQueryBindings() {
        return this.queryBindings;
    }

    @Override
    public void setQueryBindings(QueryBindings queryBindings) {
        this.queryBindings = queryBindings;
    }

    @Override
    public int getBatchCommandIndex() {
        return this.batchCommandIndex;
    }

    @Override
    public void setBatchCommandIndex(int batchCommandIndex) {
        this.batchCommandIndex = batchCommandIndex;
    }

    /**
     * Computes the optimum number of batched parameter lists to send
     * without overflowing max_allowed_packet.
     *
     * @param numBatchedArgs
     *            original batch size
     * @return computed batch size
     */
    @Override
    public int computeBatchSize(int numBatchedArgs) {
        long[] combinedValues = computeMaxParameterSetSizeAndBatchSize(numBatchedArgs);

        long maxSizeOfParameterSet = combinedValues[0];
        long sizeOfEntireBatch = combinedValues[1];

        if (sizeOfEntireBatch < this.maxAllowedPacket.getValue() - this.originalSql.length()) {
            return numBatchedArgs;
        }

        return (int) Math.max(1, (this.maxAllowedPacket.getValue() - this.originalSql.length()) / maxSizeOfParameterSet);
    }

    /**
     * Method checkNullOrEmptyQuery.
     *
     * @param sql
     *            the SQL to check
     *
     * @throws WrongArgumentException
     *             if query is null or empty.
     */
    @Override
    public void checkNullOrEmptyQuery(String sql) {
        if (sql == null) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("PreparedQuery.0"), this.session.getExceptionInterceptor());
        }

        if (sql.length() == 0) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("PreparedQuery.1"), this.session.getExceptionInterceptor());
        }
    }

    @Override
    public String asSql() {
        StringBuilder buf = new StringBuilder();
        Object batchArg = null;
        if (this.batchCommandIndex != -1) {
            batchArg = this.batchedArgs.get(this.batchCommandIndex);
        }

        byte[][] staticSqlStrings = this.queryInfo.getStaticSqlParts();
        for (int i = 0; i < this.parameterCount; ++i) {
            buf.append(this.charEncoding != null ? StringUtils.toString(staticSqlStrings[i], this.charEncoding) : StringUtils.toString(staticSqlStrings[i]));
            String val = null;
            if (batchArg != null && batchArg instanceof String) {
                buf.append((String) batchArg);
                continue;
            }
            val = this.batchCommandIndex == -1 ? this.queryBindings == null ? null : this.queryBindings.getBindValues()[i].getString()
                    : ((QueryBindings) batchArg).getBindValues()[i].getString();
            buf.append(val == null ? "** NOT SPECIFIED **" : val);
        }
        buf.append(this.charEncoding != null ? StringUtils.toString(staticSqlStrings[this.parameterCount], this.charEncoding)
                : StringUtils.toAsciiString(staticSqlStrings[this.parameterCount]));
        return buf.toString();
    }

    /**
     * Computes the maximum parameter set size, and entire batch size given
     * the number of arguments in the batch.
     *
     * @param numBatchedArgs
     *            number of batched arguments
     * @return new long[] { maxSizeOfParameterSet, sizeOfEntireBatch }
     */
    protected long[] computeMaxParameterSetSizeAndBatchSize(int numBatchedArgs) {
        long sizeOfEntireBatch = 1 /* com_query */;
        long maxSizeOfParameterSet = 0;

        if (this.session.getServerSession().supportsQueryAttributes()) {
            sizeOfEntireBatch += 9 /* parameter_count */ + 1 /* parameter_set_count */;
            sizeOfEntireBatch += (this.queryAttributesBindings.getCount() + 7) / 8 /* null_bitmap */ + 1 /* new_params_bind_flag */;
            for (int i = 0; i < this.queryAttributesBindings.getCount(); i++) {
                BindValue queryAttribute = this.queryAttributesBindings.getAttributeValue(i);
                sizeOfEntireBatch += 2 /* parameter_type */ + queryAttribute.getName().length() /* parameter_name */ + queryAttribute.getBinaryLength();
            }
        }

        for (int i = 0; i < numBatchedArgs; i++) {
            long sizeOfParameterSet = 0;

            BindValue[] bindValues = ((QueryBindings) this.batchedArgs.get(i)).getBindValues();
            for (int j = 0; j < bindValues.length; j++) {
                sizeOfParameterSet += bindValues[j].getTextLength();
            }

            //
            // Account for static part of values clause
            // This is a little naive, because the ?s will be replaced but it gives us some padding, and is less housekeeping to ignore them. We're looking
            // for a "fuzzy" value here anyway
            //
            sizeOfParameterSet += this.queryInfo.getValuesClauseLength() != -1 ? this.queryInfo.getValuesClauseLength() + 1 : this.originalSql.length() + 1;
            sizeOfEntireBatch += sizeOfParameterSet;

            if (sizeOfParameterSet > maxSizeOfParameterSet) {
                maxSizeOfParameterSet = sizeOfParameterSet;
            }
        }

        return new long[] { maxSizeOfParameterSet, sizeOfEntireBatch };
    }

    @SuppressWarnings("unchecked")
    @Override
    public <M extends Message> M fillSendPacket(QueryBindings bindings) {
        return (M) this.session.getProtocol().getMessageBuilder().buildComQuery(this.session.getSharedSendPacket(), this.session, this, bindings,
                this.charEncoding);
    }

}
