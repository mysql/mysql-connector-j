/*
  Copyright (c) 2017, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.mysqla;

import com.mysql.cj.api.BindValue;
import com.mysql.cj.api.PreparedQuery;
import com.mysql.cj.api.QueryBindings;
import com.mysql.cj.api.conf.ReadableProperty;
import com.mysql.cj.core.conf.PropertyDefinitions;

public class ClientPreparedQuery extends SimpleQuery implements PreparedQuery {

    protected ParseInfo parseInfo;

    protected QueryBindings queryBindings = null;

    /** The SQL that was passed in to 'prepare' */
    private String originalSql = null;

    /** The number of parameters in this PreparedStatement */
    protected int parameterCount;

    /** Is this query a LOAD DATA query? */
    protected boolean isLoadDataQuery = false;

    protected ReadableProperty<Boolean> autoClosePStmtStreams;

    public ClientPreparedQuery(MysqlaSession sess, int statementId) {
        super(sess);
        setId(statementId);

        this.autoClosePStmtStreams = this.session.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_autoClosePStmtStreams);
    }

    public String getOriginalSql() {
        return this.originalSql;
    }

    public void setOriginalSql(String originalSql) {
        this.originalSql = originalSql;
    }

    public int getParameterCount() {
        return this.parameterCount;
    }

    public void setParameterCount(int parameterCount) {
        this.parameterCount = parameterCount;
    }

    public boolean isLoadDataQuery() {
        return this.isLoadDataQuery;
    }

    public void setLoadDataQuery(boolean isLoadDataQuery) {
        this.isLoadDataQuery = isLoadDataQuery;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends QueryBindings> T getQueryBindings() {
        return (T) this.queryBindings;
    }

    @Override
    public <T extends QueryBindings> void setQueryBindings(T queryBindings) {
        this.queryBindings = queryBindings;
    }

    /**
     * Computes the optimum number of batched parameter lists to send
     * without overflowing max_allowed_packet.
     * 
     * @param numBatchedArgs
     */
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
     * Computes the maximum parameter set size, and entire batch size given
     * the number of arguments in the batch.
     * 
     */
    protected long[] computeMaxParameterSetSizeAndBatchSize(int numBatchedArgs) {
        long sizeOfEntireBatch = 0;
        long maxSizeOfParameterSet = 0;

        for (int i = 0; i < numBatchedArgs; i++) {
            QueryBindings qBindings = (QueryBindings) this.batchedArgs.get(i);

            BindValue[] bindValues = qBindings.getBindValues();

            long sizeOfParameterSet = 0;

            for (int j = 0; j < bindValues.length; j++) {
                if (!bindValues[j].isNull()) {

                    if (bindValues[j].isStream()) {
                        int streamLength = bindValues[j].getStreamLength();

                        if (streamLength != -1) {
                            sizeOfParameterSet += streamLength * 2; // for safety in escaping
                        } else {
                            int paramLength = qBindings.getBindValues()[j].getParameterValue().length;
                            sizeOfParameterSet += paramLength;
                        }
                    } else {
                        sizeOfParameterSet += qBindings.getBindValues()[j].getParameterValue().length;
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

            if (this.parseInfo.getValuesClause() != null) {
                sizeOfParameterSet += this.parseInfo.getValuesClause().length() + 1;
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

    public ParseInfo getParseInfo() {
        return this.parseInfo;
    }

    public void setParseInfo(ParseInfo parseInfo) {
        this.parseInfo = parseInfo;
    }
}
