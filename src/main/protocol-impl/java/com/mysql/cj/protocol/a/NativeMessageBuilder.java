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

package com.mysql.cj.protocol.a;

import java.util.List;

import com.mysql.cj.BindValue;
import com.mysql.cj.Constants;
import com.mysql.cj.MessageBuilder;
import com.mysql.cj.Messages;
import com.mysql.cj.MysqlType;
import com.mysql.cj.NativeQueryAttributesBindings;
import com.mysql.cj.NativeQueryBindValue;
import com.mysql.cj.PreparedQuery;
import com.mysql.cj.Query;
import com.mysql.cj.QueryAttributesBindings;
import com.mysql.cj.QueryBindings;
import com.mysql.cj.Session;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.CJOperationNotSupportedException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.protocol.a.NativeConstants.IntegerDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringLengthDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringSelfDataType;
import com.mysql.cj.util.StringUtils;

public class NativeMessageBuilder implements MessageBuilder<NativePacketPayload> {

    private boolean supportsQueryAttributes = true;

    public NativeMessageBuilder(boolean supportsQueryAttributes) {
        this.supportsQueryAttributes = supportsQueryAttributes;
    }

    @Override
    public NativePacketPayload buildSqlStatement(String statement) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    @Override
    public NativePacketPayload buildSqlStatement(String statement, List<Object> args) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    @Override
    public NativePacketPayload buildClose() {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    public NativePacketPayload buildComQuery(NativePacketPayload sharedPacket, Session sess, byte[] query) {
        NativePacketPayload packet = sharedPacket != null ? sharedPacket : new NativePacketPayload(query.length + 1);
        packet.writeInteger(IntegerDataType.INT1, NativeConstants.COM_QUERY);

        if (this.supportsQueryAttributes) {
            // CLIENT_QUERY_ATTRIBUTES capability has been negotiated but, since this method is used solely to run queries internally and it is not bound to any
            // Statement object, no query attributes are ever set. It remains to inject telemetry context propagation query attribute if telemetry is enabled.

            BindValue queryAttribute = new NativeQueryBindValue(sess); // Required for telemetry context propagation.
            sess.getTelemetryHandler().propagateContext((k, v) -> {
                queryAttribute.setName(k);
                queryAttribute.setBinding(v, MysqlType.CHAR, 0, null);
            });

            if (queryAttribute.getName() == null) { // Telemetry context propagation attribute wasn't set.
                packet.writeInteger(IntegerDataType.INT_LENENC, 0); // parameter_count (always 0)
                packet.writeInteger(IntegerDataType.INT_LENENC, 1); // parameter_set_count (always 1)

            } else {
                packet.writeInteger(IntegerDataType.INT_LENENC, 1); // parameter_count (always 1, for telemetry context propagation)
                packet.writeInteger(IntegerDataType.INT_LENENC, 1); // parameter_set_count (always 1)
                packet.writeInteger(IntegerDataType.INT1, 0); // null_bitmap (always 0, no nulls)
                packet.writeInteger(IntegerDataType.INT1, 1); // new_params_bind_flag (always 1)
                packet.writeInteger(IntegerDataType.INT2, queryAttribute.getFieldType()); // param_type_and_flag
                packet.writeBytes(StringSelfDataType.STRING_LENENC, queryAttribute.getName().getBytes()); // parameter_name
                queryAttribute.writeAsQueryAttribute(packet); // parameter_value
            }

        }

        packet.writeBytes(StringLengthDataType.STRING_FIXED, query);
        return packet;
    }

    public NativePacketPayload buildComQuery(NativePacketPayload sharedPacket, Session sess, String query) {
        return buildComQuery(sharedPacket, sess, StringUtils.getBytes(query));
    }

    public NativePacketPayload buildComQuery(NativePacketPayload sharedPacket, Session sess, String query, String encoding) {
        return buildComQuery(sharedPacket, sess, StringUtils.getBytes(query, encoding));
    }

    @Override
    public NativePacketPayload buildComQuery(NativePacketPayload sharedPacket, Session sess, String query, Query callingQuery, String characterEncoding) {
        String statementComment = sess.getQueryComment();
        byte[] commentAsBytes = StringUtils.getBytes(statementComment, characterEncoding);

        QueryAttributesBindings queryAttributesBindings = null;
        if (!this.supportsQueryAttributes && callingQuery != null && callingQuery.getQueryAttributesBindings().getCount() > 0) {
            sess.getLog().logWarn(Messages.getString("QueryAttributes.SetButNotSupported"));
        }
        if (this.supportsQueryAttributes && callingQuery != null) {
            queryAttributesBindings = callingQuery.getQueryAttributesBindings();
        } else {
            queryAttributesBindings = new NativeQueryAttributesBindings(sess); // Required for telemetry context propagation.
        }

        boolean contextPropagationAttributeWasInjected = false;
        final NativePacketPayload sendPacket;
        if (sharedPacket != null) {
            sendPacket = sharedPacket;
        } else {
            // Compute packet length. It's not possible to know exactly how many bytes will be obtained from the query, but UTF-8 max encoding length is 4, so
            // pad it (4 * query) + space for headers
            int packLength = 1 /* COM_QUERY */ + query.length() * 4 + 2;

            if (commentAsBytes.length > 0) {
                packLength += commentAsBytes.length;
                packLength += 6; // for "/*[space]" + "[space]*/"
            }

            if (this.supportsQueryAttributes) {
                if (!queryAttributesBindings.containsAttribute(sess.getTelemetryHandler().getContextPropagationKey())) {
                    sess.getTelemetryHandler().propagateContext(queryAttributesBindings::setAttribute);
                    contextPropagationAttributeWasInjected = true;
                }
                if (queryAttributesBindings.getCount() > 0) {
                    packLength += 9 /* parameter_count */ + 1 /* parameter_set_count */;
                    packLength += (queryAttributesBindings.getCount() + 7) / 8 /* null_bitmap */ + 1 /* new_params_bind_flag */;
                    for (int i = 0; i < queryAttributesBindings.getCount(); i++) {
                        BindValue queryAttribute = queryAttributesBindings.getAttributeValue(i);
                        packLength += 2 /* parameter_type */ + queryAttribute.getName().length() /* parameter_name */ + queryAttribute.getBinaryLength();
                    }
                } else {
                    packLength += 1 /* parameter_count */ + 1 /* parameter_set_count */;
                }
            }

            sendPacket = new NativePacketPayload(packLength);
        }

        sendPacket.setPosition(0);
        sendPacket.writeInteger(IntegerDataType.INT1, NativeConstants.COM_QUERY);

        if (this.supportsQueryAttributes) {
            if (queryAttributesBindings != null && queryAttributesBindings.getCount() > 0) {
                sendPacket.writeInteger(IntegerDataType.INT_LENENC, queryAttributesBindings.getCount());
                sendPacket.writeInteger(IntegerDataType.INT_LENENC, 1); // parameter_set_count (always 1)
                byte[] nullBitsBuffer = new byte[(queryAttributesBindings.getCount() + 7) / 8];
                for (int i = 0; i < queryAttributesBindings.getCount(); i++) {
                    if (queryAttributesBindings.getAttributeValue(i).isNull()) {
                        nullBitsBuffer[i >>> 3] |= 1 << (i & 7);
                    }
                }
                sendPacket.writeBytes(StringLengthDataType.STRING_VAR, nullBitsBuffer);
                sendPacket.writeInteger(IntegerDataType.INT1, 1); // new_params_bind_flag (always 1)
                queryAttributesBindings.runThroughAll(a -> {
                    sendPacket.writeInteger(IntegerDataType.INT2, a.getFieldType());
                    sendPacket.writeBytes(StringSelfDataType.STRING_LENENC, a.getName().getBytes());
                });
                queryAttributesBindings.runThroughAll(a -> {
                    if (!a.isNull()) {
                        a.writeAsQueryAttribute(sendPacket);
                    }
                });
            } else {
                sendPacket.writeInteger(IntegerDataType.INT_LENENC, 0);
                sendPacket.writeInteger(IntegerDataType.INT_LENENC, 1); // parameter_set_count (always 1)
            }
            if (contextPropagationAttributeWasInjected) {
                queryAttributesBindings.removeAttribute(sess.getTelemetryHandler().getContextPropagationKey());
            }
        }
        sendPacket.setTag("QUERY");

        if (commentAsBytes.length > 0) {
            sendPacket.writeBytes(StringLengthDataType.STRING_FIXED, Constants.SLASH_STAR_SPACE_AS_BYTES);
            sendPacket.writeBytes(StringLengthDataType.STRING_FIXED, commentAsBytes);
            sendPacket.writeBytes(StringLengthDataType.STRING_FIXED, Constants.SPACE_STAR_SLASH_SPACE_AS_BYTES);
        }

        if (!sess.getServerSession().getCharsetSettings().doesPlatformDbCharsetMatches() && StringUtils.startsWithIgnoreCaseAndWs(query, "LOAD DATA")) {
            sendPacket.writeBytes(StringLengthDataType.STRING_FIXED, StringUtils.getBytes(query));
        } else {
            sendPacket.writeBytes(StringLengthDataType.STRING_FIXED, StringUtils.getBytes(query, characterEncoding));
        }
        return sendPacket;
    }

    @Override
    public NativePacketPayload buildComQuery(NativePacketPayload sharedPacket, Session sess, PreparedQuery preparedQuery, QueryBindings bindings,
            String characterEncoding) {
        NativePacketPayload sendPacket = sharedPacket != null ? sharedPacket : new NativePacketPayload(9);
        QueryAttributesBindings queryAttributesBindings = preparedQuery.getQueryAttributesBindings();
        BindValue[] bindValues = bindings.getBindValues();

        sendPacket.writeInteger(IntegerDataType.INT1, NativeConstants.COM_QUERY);

        boolean contextPropagationAttributeWasInjected = false;
        if (this.supportsQueryAttributes) {
            if (!queryAttributesBindings.containsAttribute(sess.getTelemetryHandler().getContextPropagationKey())) {
                sess.getTelemetryHandler().propagateContext(queryAttributesBindings::setAttribute);
                contextPropagationAttributeWasInjected = true;
            }
            if (queryAttributesBindings.getCount() > 0) {
                sendPacket.writeInteger(IntegerDataType.INT_LENENC, queryAttributesBindings.getCount());
                sendPacket.writeInteger(IntegerDataType.INT_LENENC, 1); // parameter_set_count (always 1)
                byte[] nullBitsBuffer = new byte[(queryAttributesBindings.getCount() + 7) / 8];
                for (int i = 0; i < queryAttributesBindings.getCount(); i++) {
                    if (queryAttributesBindings.getAttributeValue(i).isNull()) {
                        nullBitsBuffer[i >>> 3] |= 1 << (i & 7);
                    }
                }
                sendPacket.writeBytes(StringLengthDataType.STRING_VAR, nullBitsBuffer);
                sendPacket.writeInteger(IntegerDataType.INT1, 1); // new_params_bind_flag (always 1)
                queryAttributesBindings.runThroughAll(a -> {
                    sendPacket.writeInteger(IntegerDataType.INT2, a.getFieldType());
                    sendPacket.writeBytes(StringSelfDataType.STRING_LENENC, a.getName().getBytes());
                });
                queryAttributesBindings.runThroughAll(a -> {
                    if (!a.isNull()) {
                        a.writeAsQueryAttribute(sendPacket);
                    }
                });
            } else {
                sendPacket.writeInteger(IntegerDataType.INT_LENENC, 0);
                sendPacket.writeInteger(IntegerDataType.INT_LENENC, 1); // parameter_set_count (always 1)
            }
            if (contextPropagationAttributeWasInjected) {
                queryAttributesBindings.removeAttribute(sess.getTelemetryHandler().getContextPropagationKey());
            }
        } else if (queryAttributesBindings.getCount() > 0) {
            sess.getLog().logWarn(Messages.getString("QueryAttributes.SetButNotSupported"));
        }
        sendPacket.setTag("QUERY");

        boolean useStreamLengths = sess.getPropertySet().getBooleanProperty(PropertyKey.useStreamLengthsInPrepStmts).getValue();

        // Try and get this allocation as close as possible for BLOBs.
        int ensurePacketSize = 0;

        String statementComment = sess.getQueryComment();
        byte[] commentAsBytes = null;

        if (statementComment != null) {
            commentAsBytes = StringUtils.getBytes(statementComment, characterEncoding);
            ensurePacketSize += commentAsBytes.length;
            ensurePacketSize += 6; // for /*[space] [space]*/
        }

        for (int i = 0; i < bindValues.length; i++) {
            if (bindValues[i].isStream() && useStreamLengths) {
                ensurePacketSize += bindValues[i].getScaleOrLength();
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

        byte[][] staticSqlStrings = preparedQuery.getQueryInfo().getStaticSqlParts();
        for (int i = 0; i < bindValues.length; i++) {
            bindings.checkParameterSet(i);
            sendPacket.writeBytes(StringLengthDataType.STRING_FIXED, staticSqlStrings[i]);
            bindValues[i].writeAsText(sendPacket);
        }

        sendPacket.writeBytes(StringLengthDataType.STRING_FIXED, staticSqlStrings[bindValues.length]);
        return sendPacket;
    }

    public NativePacketPayload buildComInitDb(NativePacketPayload sharedPacket, byte[] dbName) {
        NativePacketPayload packet = sharedPacket != null ? sharedPacket : new NativePacketPayload(dbName.length + 1);
        packet.writeInteger(IntegerDataType.INT1, NativeConstants.COM_INIT_DB);
        packet.writeBytes(StringLengthDataType.STRING_FIXED, dbName);
        return packet;
    }

    public NativePacketPayload buildComInitDb(NativePacketPayload sharedPacket, String dbName) {
        return buildComInitDb(sharedPacket, StringUtils.getBytes(dbName));
    }

    public NativePacketPayload buildComShutdown(NativePacketPayload sharedPacket) {
        NativePacketPayload packet = sharedPacket != null ? sharedPacket : new NativePacketPayload(1);
        packet.writeInteger(IntegerDataType.INT1, NativeConstants.COM_SHUTDOWN);
        return packet;
    }

    public NativePacketPayload buildComSetOption(NativePacketPayload sharedPacket, int val) {
        NativePacketPayload packet = sharedPacket != null ? sharedPacket : new NativePacketPayload(3);
        packet.writeInteger(IntegerDataType.INT1, NativeConstants.COM_SET_OPTION);
        packet.writeInteger(IntegerDataType.INT2, val);
        return packet;
    }

    public NativePacketPayload buildComPing(NativePacketPayload sharedPacket) {
        NativePacketPayload packet = sharedPacket != null ? sharedPacket : new NativePacketPayload(1);
        packet.writeInteger(IntegerDataType.INT1, NativeConstants.COM_PING);
        return packet;
    }

    public NativePacketPayload buildComQuit(NativePacketPayload sharedPacket) {
        NativePacketPayload packet = sharedPacket != null ? sharedPacket : new NativePacketPayload(1);
        packet.writeInteger(IntegerDataType.INT1, NativeConstants.COM_QUIT);
        return packet;
    }

    public NativePacketPayload buildComStmtPrepare(NativePacketPayload sharedPacket, byte[] query) {
        NativePacketPayload packet = sharedPacket != null ? sharedPacket : new NativePacketPayload(query.length + 1);
        packet.writeInteger(IntegerDataType.INT1, NativeConstants.COM_STMT_PREPARE);
        packet.writeBytes(StringLengthDataType.STRING_FIXED, query);
        return packet;
    }

    public NativePacketPayload buildComStmtPrepare(NativePacketPayload sharedPacket, String queryString, String characterEncoding) {
        return buildComStmtPrepare(sharedPacket, StringUtils.getBytes(queryString, characterEncoding));
    }

    public NativePacketPayload buildComStmtClose(NativePacketPayload sharedPacket, long serverStatementId) {
        NativePacketPayload packet = sharedPacket != null ? sharedPacket : new NativePacketPayload(5);
        packet.writeInteger(IntegerDataType.INT1, NativeConstants.COM_STMT_CLOSE);
        packet.writeInteger(IntegerDataType.INT4, serverStatementId);
        return packet;
    }

    public NativePacketPayload buildComStmtReset(NativePacketPayload sharedPacket, long serverStatementId) {
        NativePacketPayload packet = sharedPacket != null ? sharedPacket : new NativePacketPayload(5);
        packet.writeInteger(IntegerDataType.INT1, NativeConstants.COM_STMT_RESET);
        packet.writeInteger(IntegerDataType.INT4, serverStatementId);
        return packet;
    }

    public NativePacketPayload buildComStmtFetch(NativePacketPayload sharedPacket, long serverStatementId, long numRowsToFetch) {
        NativePacketPayload packet = sharedPacket != null ? sharedPacket : new NativePacketPayload(9);
        packet.writeInteger(IntegerDataType.INT1, NativeConstants.COM_STMT_FETCH);
        packet.writeInteger(IntegerDataType.INT4, serverStatementId);
        packet.writeInteger(IntegerDataType.INT4, numRowsToFetch);
        return packet;
    }

    public NativePacketPayload buildComStmtSendLongData(NativePacketPayload sharedPacket, long serverStatementId, int parameterIndex, byte[] longData) {
        NativePacketPayload packet = buildComStmtSendLongDataHeader(sharedPacket, serverStatementId, parameterIndex);
        packet.writeBytes(StringLengthDataType.STRING_FIXED, longData);
        return packet;
    }

    public NativePacketPayload buildComStmtSendLongDataHeader(NativePacketPayload sharedPacket, long serverStatementId, int parameterIndex) {
        NativePacketPayload packet = sharedPacket != null ? sharedPacket : new NativePacketPayload(9);
        packet.writeInteger(IntegerDataType.INT1, NativeConstants.COM_STMT_SEND_LONG_DATA);
        packet.writeInteger(IntegerDataType.INT4, serverStatementId);
        packet.writeInteger(IntegerDataType.INT2, parameterIndex);
        return packet;
    }

    public NativePacketPayload buildComStmtExecute(NativePacketPayload sharedPacket, long serverStatementId, byte flags, boolean sendQueryAttributes,
            PreparedQuery preparedQuery) {
        NativePacketPayload packet = sharedPacket != null ? sharedPacket : new NativePacketPayload(5);

        Session sess = preparedQuery.getSession();
        int parameterCount = preparedQuery.getParameterCount();
        QueryBindings queryBindings = preparedQuery.getQueryBindings();
        BindValue[] parameterBindings = queryBindings.getBindValues();
        QueryAttributesBindings queryAttributesBindings = preparedQuery.getQueryAttributesBindings();

        packet.writeInteger(IntegerDataType.INT1, NativeConstants.COM_STMT_EXECUTE);
        packet.writeInteger(IntegerDataType.INT4, serverStatementId);
        packet.writeInteger(IntegerDataType.INT1, flags);
        packet.writeInteger(IntegerDataType.INT4, 1); // placeholder for parameter iterations

        boolean contextPropagationAttributeWasInjected = false;
        int parametersAndAttributesCount = parameterCount;
        if (this.supportsQueryAttributes) {
            if (sendQueryAttributes) {
                if (!queryAttributesBindings.containsAttribute(sess.getTelemetryHandler().getContextPropagationKey())) {
                    sess.getTelemetryHandler().propagateContext(queryAttributesBindings::setAttribute);
                    contextPropagationAttributeWasInjected = true;
                }
                parametersAndAttributesCount += queryAttributesBindings.getCount();
            }
            if (sendQueryAttributes || parametersAndAttributesCount > 0) {
                // Servers between 8.0.23 and 8.0.25 don't expect a 'parameter_count' value if the statement was prepared without parameters.
                packet.writeInteger(IntegerDataType.INT_LENENC, parametersAndAttributesCount);
            }
        }

        if (parametersAndAttributesCount > 0) {
            /* Reserve place for null-marker bytes */
            int nullCount = (parametersAndAttributesCount + 7) / 8;
            int nullBitsPosition = packet.getPosition();
            for (int i = 0; i < nullCount; i++) {
                packet.writeInteger(IntegerDataType.INT1, 0);
            }
            byte[] nullBitsBuffer = new byte[nullCount];

            // In case if buffers (type) changed or there are query attributes to send.
            if (queryBindings.getSendTypesToServer().get() || sendQueryAttributes && queryAttributesBindings.getCount() > 0) {
                packet.writeInteger(IntegerDataType.INT1, 1);

                // Store types of parameters in the first packet that is sent to the server.
                for (int i = 0; i < parameterCount; i++) {
                    packet.writeInteger(IntegerDataType.INT2, parameterBindings[i].getFieldType());
                    if (this.supportsQueryAttributes) {
                        packet.writeBytes(StringSelfDataType.STRING_LENENC, "".getBytes()); // Parameters have no names.
                    }
                }

                if (sendQueryAttributes) {
                    queryAttributesBindings.runThroughAll(a -> {
                        packet.writeInteger(IntegerDataType.INT2, a.getFieldType());
                        packet.writeBytes(StringSelfDataType.STRING_LENENC, a.getName().getBytes());
                    });
                }
            } else {
                packet.writeInteger(IntegerDataType.INT1, 0);
            }

            // Store the parameter values.
            for (int i = 0; i < parameterCount; i++) {
                if (!parameterBindings[i].isStream()) {
                    if (!parameterBindings[i].isNull()) {
                        parameterBindings[i].writeAsBinary(packet);
                    } else {
                        nullBitsBuffer[i >>> 3] |= 1 << (i & 7);
                    }
                }
            }

            if (sendQueryAttributes) {
                for (int i = 0; i < queryAttributesBindings.getCount(); i++) {
                    if (queryAttributesBindings.getAttributeValue(i).isNull()) {
                        int b = i + parameterCount;
                        nullBitsBuffer[b >>> 3] |= 1 << (b & 7);
                    }
                }
                queryAttributesBindings.runThroughAll(a -> {
                    if (!a.isNull()) {
                        a.writeAsQueryAttribute(packet);
                    }
                });
            }

            // Go back and write the NULL flags to the beginning of the packet
            int endPosition = packet.getPosition();
            packet.setPosition(nullBitsPosition);
            packet.writeBytes(StringLengthDataType.STRING_FIXED, nullBitsBuffer);
            packet.setPosition(endPosition);
        }

        if (contextPropagationAttributeWasInjected) {
            queryAttributesBindings.removeAttribute(sess.getTelemetryHandler().getContextPropagationKey());
        }

        return packet;
    }

    public NativePacketPayload buildComResetConnection(NativePacketPayload sharedPacket) {
        NativePacketPayload packet = sharedPacket != null ? sharedPacket : new NativePacketPayload(1);
        packet.writeInteger(IntegerDataType.INT1, NativeConstants.COM_RESET_CONNECTION);
        return packet;
    }

}
