/*
 * Copyright (c) 2017, 2018, Oracle and/or its affiliates. All rights reserved.
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

import com.mysql.cj.util.StringUtils;

//TODO should not be protocol-specific

public class ClientPreparedQuery extends AbstractPreparedQuery<ClientPreparedQueryBindings> {

    public ClientPreparedQuery(NativeSession sess) {
        super(sess);
    }

    /**
     * Computes the maximum parameter set size, and entire batch size given
     * the number of arguments in the batch.
     * 
     */
    @Override
    protected long[] computeMaxParameterSetSizeAndBatchSize(int numBatchedArgs) {
        long sizeOfEntireBatch = 0;
        long maxSizeOfParameterSet = 0;

        for (int i = 0; i < numBatchedArgs; i++) {
            ClientPreparedQueryBindings qBindings = (ClientPreparedQueryBindings) this.batchedArgs.get(i);

            BindValue[] bindValues = qBindings.getBindValues();

            long sizeOfParameterSet = 0;

            for (int j = 0; j < bindValues.length; j++) {
                if (!bindValues[j].isNull()) {

                    if (bindValues[j].isStream()) {
                        int streamLength = bindValues[j].getStreamLength();

                        if (streamLength != -1) {
                            sizeOfParameterSet += streamLength * 2; // for safety in escaping
                        } else {
                            int paramLength = qBindings.getBindValues()[j].getByteValue().length;
                            sizeOfParameterSet += paramLength;
                        }
                    } else {
                        sizeOfParameterSet += qBindings.getBindValues()[j].getByteValue().length;
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

    /**
     * @param parameterIndex
     *            parameter index
     * @return bytes
     */
    public byte[] getBytesRepresentation(int parameterIndex) {
        BindValue bv = this.queryBindings.getBindValues()[parameterIndex];

        if (bv.isStream()) {
            return streamToBytes(bv.getStreamValue(), false, bv.getStreamLength(), this.useStreamLengthsInPrepStmts.getValue());
        }

        byte[] parameterVal = bv.getByteValue();

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

    /**
     * Get bytes representation for a parameter in a statement batch.
     * 
     * @param parameterIndex
     *            parameter index
     * @param commandIndex
     *            command index
     * @return bytes
     */
    public byte[] getBytesRepresentationForBatch(int parameterIndex, int commandIndex) {
        Object batchedArg = this.batchedArgs.get(commandIndex);
        if (batchedArg instanceof String) {
            return StringUtils.getBytes((String) batchedArg, this.charEncoding);
        }

        BindValue bv = ((ClientPreparedQueryBindings) batchedArg).getBindValues()[parameterIndex];
        if (bv.isStream()) {
            return streamToBytes(bv.getStreamValue(), false, bv.getStreamLength(), this.useStreamLengthsInPrepStmts.getValue());
        }
        byte parameterVal[] = bv.getByteValue();
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
