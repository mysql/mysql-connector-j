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

//TODO should not be protocol-specific

public class ClientPreparedQuery extends AbstractPreparedQuery<ClientPreparedQueryBindings> {

    public ClientPreparedQuery(NativeSession sess) {
        super(sess);
    }

    /**
     * Computes the maximum parameter set size, and entire batch size given
     * the number of arguments in the batch.
     */
    @Override
    protected long[] computeMaxParameterSetSizeAndBatchSize(int numBatchedArgs) {
        long sizeOfEntireBatch = 1 /* com_query */;
        long maxSizeOfParameterSet = 0;

        if (this.session.getServerSession().supportsQueryAttributes()) {
            sizeOfEntireBatch += 9 /* parameter_count */ + 1 /* parameter_set_count */;
            sizeOfEntireBatch += (this.queryAttributesBindings.getCount() + 7) / 8 /* null_bitmap */ + 1 /* new_params_bind_flag */;
            for (int i = 0; i < this.queryAttributesBindings.getCount(); i++) {
                QueryAttributesBindValue queryAttribute = this.queryAttributesBindings.getAttributeValue(i);
                sizeOfEntireBatch += 2 /* parameter_type */ + queryAttribute.getName().length() /* parameter_name */ + queryAttribute.getBoundLength();
            }
        }

        for (int i = 0; i < numBatchedArgs; i++) {
            ClientPreparedQueryBindings qBindings = (ClientPreparedQueryBindings) this.batchedArgs.get(i);

            BindValue[] bindValues = qBindings.getBindValues();

            long sizeOfParameterSet = 0;

            for (int j = 0; j < bindValues.length; j++) {
                if (!bindValues[j].isNull()) {

                    if (bindValues[j].isStream()) {
                        long streamLength = bindValues[j].getStreamLength();

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
}
