/*
 * Copyright (c) 2015, 2017, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.x.io;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import com.google.protobuf.ByteString;
import com.mysql.cj.api.x.io.XpluginStatementCommand;
import com.mysql.cj.core.authentication.Security;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.x.protobuf.MysqlxConnection.Capabilities;
import com.mysql.cj.x.protobuf.MysqlxConnection.CapabilitiesSet;
import com.mysql.cj.x.protobuf.MysqlxConnection.Capability;
import com.mysql.cj.x.protobuf.MysqlxCrud.Collection;
import com.mysql.cj.x.protobuf.MysqlxCrud.Column;
import com.mysql.cj.x.protobuf.MysqlxCrud.DataModel;
import com.mysql.cj.x.protobuf.MysqlxCrud.Delete;
import com.mysql.cj.x.protobuf.MysqlxCrud.Find;
import com.mysql.cj.x.protobuf.MysqlxCrud.Insert;
import com.mysql.cj.x.protobuf.MysqlxCrud.Insert.TypedRow;
import com.mysql.cj.x.protobuf.MysqlxCrud.Limit;
import com.mysql.cj.x.protobuf.MysqlxCrud.Order;
import com.mysql.cj.x.protobuf.MysqlxCrud.Projection;
import com.mysql.cj.x.protobuf.MysqlxCrud.Update;
import com.mysql.cj.x.protobuf.MysqlxCrud.UpdateOperation;
import com.mysql.cj.x.protobuf.MysqlxCrud.UpdateOperation.UpdateType;
import com.mysql.cj.x.protobuf.MysqlxDatatypes.Any;
import com.mysql.cj.x.protobuf.MysqlxDatatypes.Object.ObjectField;
import com.mysql.cj.x.protobuf.MysqlxDatatypes.Scalar;
import com.mysql.cj.x.protobuf.MysqlxExpr.ColumnIdentifier;
import com.mysql.cj.x.protobuf.MysqlxExpr.Expr;
import com.mysql.cj.x.protobuf.MysqlxSession.AuthenticateContinue;
import com.mysql.cj.x.protobuf.MysqlxSession.AuthenticateStart;
import com.mysql.cj.x.protobuf.MysqlxSql.StmtExecute;
import com.mysql.cj.xdevapi.CreateIndexParams;
import com.mysql.cj.xdevapi.CreateIndexParams.IndexField;
import com.mysql.cj.xdevapi.ExprUtil;
import com.mysql.cj.xdevapi.FilterParams;
import com.mysql.cj.xdevapi.FindParams;
import com.mysql.cj.xdevapi.InsertParams;
import com.mysql.cj.xdevapi.UpdateParams;
import com.mysql.cj.xdevapi.UpdateSpec;

public class MessageBuilder {
    // "mysqlx" namespace for StmtExecute messages is supported starting from MySQL 5.7.14
    // the previous "xplugin" namespace isn't compatible with c/J 6.0.4+
    private static final String XPLUGIN_NAMESPACE = "mysqlx";

    public MessageBuilder() {
    }

    public CapabilitiesSet buildCapabilitiesSet(String name, Object value) {
        Any v = ExprUtil.argObjectToScalarAny(value);
        Capability cap = Capability.newBuilder().setName(name).setValue(v).build();
        Capabilities caps = Capabilities.newBuilder().addCapabilities(cap).build();
        return CapabilitiesSet.newBuilder().setCapabilities(caps).build();
    }

    public StmtExecute buildCreateCollectionIndex(String schemaName, String collectionName, CreateIndexParams params) {
        com.mysql.cj.x.protobuf.MysqlxDatatypes.Object.Builder builder = com.mysql.cj.x.protobuf.MysqlxDatatypes.Object.newBuilder();
        builder.addFld(ObjectField.newBuilder().setKey("name").setValue(ExprUtil.buildAny(params.getIndexName())))
                .addFld(ObjectField.newBuilder().setKey("collection").setValue(ExprUtil.buildAny(collectionName)))
                .addFld(ObjectField.newBuilder().setKey("schema").setValue(ExprUtil.buildAny(schemaName)))
                .addFld(ObjectField.newBuilder().setKey("unique").setValue(ExprUtil.buildAny(false)));
        if (params.getIndexType() != null) {
            builder.addFld(ObjectField.newBuilder().setKey("type").setValue(ExprUtil.buildAny(params.getIndexType())));
        }

        com.mysql.cj.x.protobuf.MysqlxDatatypes.Array.Builder abuilder = com.mysql.cj.x.protobuf.MysqlxDatatypes.Array.newBuilder();
        for (IndexField indexField : params.getFields()) {
            com.mysql.cj.x.protobuf.MysqlxDatatypes.Object.Builder fld = com.mysql.cj.x.protobuf.MysqlxDatatypes.Object.newBuilder()
                    .addFld(ObjectField.newBuilder().setKey("member").setValue(ExprUtil.buildAny(indexField.getField())))
                    .addFld(ObjectField.newBuilder().setKey("type").setValue(ExprUtil.buildAny(indexField.getType())))
                    .addFld(ObjectField.newBuilder().setKey("required").setValue(ExprUtil.buildAny(indexField.isRequired())));
            if ("GEOJSON".equalsIgnoreCase(indexField.getType())) {
                if (indexField.getOptions() != null) {
                    fld.addFld(ObjectField.newBuilder().setKey("options").setValue(Any.newBuilder().setType(Any.Type.SCALAR)
                            .setScalar(Scalar.newBuilder().setType(Scalar.Type.V_UINT).setVUnsignedInt(indexField.getOptions())).build()));
                }
                if (indexField.getSrid() != null) {
                    fld.addFld(ObjectField.newBuilder().setKey("srid").setValue(Any.newBuilder().setType(Any.Type.SCALAR)
                            .setScalar(Scalar.newBuilder().setType(Scalar.Type.V_UINT).setVUnsignedInt(indexField.getSrid())).build()));
                }
            }
            abuilder.addValue(Any.newBuilder().setType(Any.Type.OBJECT).setObj(fld));
        }

        builder.addFld(ObjectField.newBuilder().setKey("constraint").setValue(Any.newBuilder().setType(Any.Type.ARRAY).setArray(abuilder)));
        return buildXpluginCommand(XpluginStatementCommand.XPLUGIN_STMT_CREATE_COLLECTION_INDEX,
                Any.newBuilder().setType(Any.Type.OBJECT).setObj(builder).build());
    }

    public StmtExecute buildDropCollectionIndex(String schemaName, String collectionName, String indexName) {
        return buildXpluginCommand(XpluginStatementCommand.XPLUGIN_STMT_DROP_COLLECTION_INDEX,
                Any.newBuilder().setType(Any.Type.OBJECT)
                        .setObj(com.mysql.cj.x.protobuf.MysqlxDatatypes.Object.newBuilder()
                                .addFld(ObjectField.newBuilder().setKey("name").setValue(ExprUtil.buildAny(indexName)))
                                .addFld(ObjectField.newBuilder().setKey("collection").setValue(ExprUtil.buildAny(collectionName)))
                                .addFld(ObjectField.newBuilder().setKey("schema").setValue(ExprUtil.buildAny(schemaName)))

                        ).build());
    }

    /**
     * Build a <i>StmtExecute</i> message for an xplugin command.
     *
     * @param command
     *            the xplugin command to send
     * @param args
     *            the arguments to the command
     * @return {@link StmtExecute}
     */
    public StmtExecute buildXpluginCommand(XpluginStatementCommand command, Any... args) {
        StmtExecute.Builder builder = StmtExecute.newBuilder();

        builder.setNamespace(XPLUGIN_NAMESPACE);
        // TODO: encoding (character_set_client?)
        builder.setStmt(ByteString.copyFromUtf8(command.commandName));
        Arrays.stream(args).forEach(a -> builder.addArgs(a));

        return builder.build();
    }

    /**
     * Build a <i>StmtExecute</i> message for a SQL statement.
     * 
     * @param statement
     *            SQL statement string
     * @param args
     *            list of {@link Any} arguments
     * @return {@link StmtExecute}
     */
    public StmtExecute buildSqlStatement(String statement, List<Any> args) {
        StmtExecute.Builder builder = StmtExecute.newBuilder();
        if (args != null) {
            builder.addAllArgs(args);
        }
        // TODO: encoding (character_set_client?)
        builder.setStmt(ByteString.copyFromUtf8(statement));
        return builder.build();
    }

    @SuppressWarnings("unchecked")
    public Find buildFind(FindParams findParams) {
        Find.Builder builder = Find.newBuilder().setCollection((Collection) findParams.getCollection());
        builder.setDataModel(findParams.isRelational() ? DataModel.TABLE : DataModel.DOCUMENT);
        if (findParams.getFields() != null) {
            builder.addAllProjection((List<Projection>) findParams.getFields());
        }
        if (findParams.getGrouping() != null) {
            builder.addAllGrouping((List<Expr>) findParams.getGrouping());
        }
        if (findParams.getGroupingCriteria() != null) {
            builder.setGroupingCriteria((Expr) findParams.getGroupingCriteria());
        }
        if (findParams.getLock() != null) {
            builder.setLocking(findParams.getLock());
        }
        applyFilterParams(findParams, builder::addAllOrder, builder::setLimit, builder::setCriteria, builder::addAllArgs);
        return builder.build();
    }

    public Update buildDocUpdate(FilterParams filterParams, List<UpdateSpec> updates) {
        Update.Builder builder = Update.newBuilder().setCollection((Collection) filterParams.getCollection());
        updates.forEach(u -> {
            UpdateOperation.Builder opBuilder = UpdateOperation.newBuilder();
            opBuilder.setOperation((UpdateType) u.getUpdateType());
            opBuilder.setSource((ColumnIdentifier) u.getSource());
            if (u.getValue() != null) {
                opBuilder.setValue((Expr) u.getValue());
            }
            builder.addOperation(opBuilder.build());
        });
        applyFilterParams(filterParams, builder::addAllOrder, builder::setLimit, builder::setCriteria, builder::addAllArgs);
        return builder.build();
    }

    // TODO: low-level tests of this method
    @SuppressWarnings("unchecked")
    public Update buildRowUpdate(FilterParams filterParams, UpdateParams updateParams) {
        Update.Builder builder = Update.newBuilder().setDataModel(DataModel.TABLE).setCollection((Collection) filterParams.getCollection());
        ((Map<ColumnIdentifier, Expr>) updateParams.getUpdates()).entrySet().stream()
                .map(e -> UpdateOperation.newBuilder().setOperation(UpdateType.SET).setSource(e.getKey()).setValue(e.getValue()).build())
                .forEach(builder::addOperation);
        applyFilterParams(filterParams, builder::addAllOrder, builder::setLimit, builder::setCriteria, builder::addAllArgs);
        return builder.build();
    }

    public Delete buildDelete(FilterParams filterParams) {
        Delete.Builder builder = Delete.newBuilder().setCollection((Collection) filterParams.getCollection());
        applyFilterParams(filterParams, builder::addAllOrder, builder::setLimit, builder::setCriteria, builder::addAllArgs);
        return builder.build();
    }

    public Insert buildDocInsert(String schemaName, String collectionName, List<String> json, boolean upsert) {
        Insert.Builder builder = Insert.newBuilder().setCollection(ExprUtil.buildCollection(schemaName, collectionName));
        if (upsert != builder.getUpsert()) {
            builder.setUpsert(upsert);
        }
        json.stream().map(str -> TypedRow.newBuilder().addField(ExprUtil.argObjectToExpr(str, false)).build()).forEach(builder::addRow);
        return builder.build();
    }

    @SuppressWarnings("unchecked")
    public Insert buildRowInsert(String schemaName, String tableName, InsertParams insertParams) {
        Insert.Builder builder = Insert.newBuilder().setDataModel(DataModel.TABLE).setCollection(ExprUtil.buildCollection(schemaName, tableName));
        if (insertParams.getProjection() != null) {
            builder.addAllProjection((List<Column>) insertParams.getProjection());
        }
        builder.addAllRow((List<TypedRow>) insertParams.getRows());
        return builder.build();
    }

    /**
     * Apply the given filter params to the builder object (represented by the method args). Abstract the process of setting the filter params on the operation
     * message builder.
     *
     * @param filterParams
     *            the filter params to apply
     * @param setOrder
     *            the "builder.addAllOrder()" method reference
     * @param setLimit
     *            the "builder.setLimit()" method reference
     * @param setCriteria
     *            the "builder.setCriteria()" method reference
     * @param setArgs
     *            the "builder.addAllArgs()" method reference
     */
    @SuppressWarnings("unchecked")
    private static void applyFilterParams(FilterParams filterParams, Consumer<List<Order>> setOrder, Consumer<Limit> setLimit, Consumer<Expr> setCriteria,
            Consumer<List<Scalar>> setArgs) {
        filterParams.verifyAllArgsBound();
        if (filterParams.getOrder() != null) {
            setOrder.accept((List<Order>) filterParams.getOrder());
        }
        if (filterParams.getLimit() != null) {
            Limit.Builder lb = Limit.newBuilder().setRowCount(filterParams.getLimit());
            if (filterParams.getOffset() != null) {
                lb.setOffset(filterParams.getOffset());
            }
            setLimit.accept(lb.build());
        }
        if (filterParams.getCriteria() != null) {
            setCriteria.accept((Expr) filterParams.getCriteria());
        }
        if (filterParams.getArgs() != null) {
            setArgs.accept((List<Scalar>) filterParams.getArgs());
        }
    }

    public AuthenticateContinue buildMysql41AuthContinue(String user, String password, byte[] salt, String database) {
        // TODO: encoding for all this?
        String encoding = "UTF8";
        byte[] userBytes = user == null ? new byte[] {} : StringUtils.getBytes(user, encoding);
        byte[] passwordBytes = password == null || password.length() == 0 ? new byte[] {} : StringUtils.getBytes(password, encoding);
        byte[] databaseBytes = database == null ? new byte[] {} : StringUtils.getBytes(database, encoding);

        byte[] hashedPassword = passwordBytes;
        if (password != null && password.length() > 0) {
            hashedPassword = Security.scramble411(passwordBytes, salt);
            // protocol dictates *-prefixed hex string as hashed password
            hashedPassword = String.format("*%040x", new java.math.BigInteger(1, hashedPassword)).getBytes();
        }

        // this is what would happen in the SASL provider but we don't need the overhead of all the plumbing.
        byte[] reply = new byte[databaseBytes.length + userBytes.length + hashedPassword.length + 2];

        // reply is length-prefixed when sent so we just separate fields by \0
        System.arraycopy(databaseBytes, 0, reply, 0, databaseBytes.length);
        int pos = databaseBytes.length;
        reply[pos++] = 0;
        System.arraycopy(userBytes, 0, reply, pos, userBytes.length);
        pos += userBytes.length;
        reply[pos++] = 0;
        System.arraycopy(hashedPassword, 0, reply, pos, hashedPassword.length);

        AuthenticateContinue.Builder builder = AuthenticateContinue.newBuilder();
        builder.setAuthData(ByteString.copyFrom(reply));
        return builder.build();
    }

    public AuthenticateStart buildPlainAuthStart(String user, String password, String database) {
        // SASL requests information from the app through callbacks. We provide the username and password by these callbacks. This implementation works for
        // PLAIN and would also work for CRAM-MD5. Additional standardized methods may require additional callbacks.
        CallbackHandler callbackHandler = new CallbackHandler() {
            public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
                for (Callback c : callbacks) {
                    if (NameCallback.class.isAssignableFrom(c.getClass())) {
                        // we get a name callback and provide the username
                        ((NameCallback) c).setName(user);
                    } else if (PasswordCallback.class.isAssignableFrom(c.getClass())) {
                        // we get  password callback and provide the password
                        ((PasswordCallback) c).setPassword(password.toCharArray());
                    } else {
                        // otherwise, thrown an exception
                        throw new UnsupportedCallbackException(c);
                    }
                }
            }
        };
        try {
            // now we create the client object we use which can handle PLAIN mechanism for "X Protocol" to "serverName"
            String[] mechanisms = new String[] { "PLAIN" };
            String authorizationId = database == null || database.trim().length() == 0 ? null : database; // as per protocol spec
            String protocol = "X Protocol";
            Map<String, ?> props = null;
            // TODO: >> serverName. Is this of any use in our X Protocol exchange? Should be defined to be blank or something.
            String serverName = "<unknown>";
            SaslClient saslClient = Sasl.createSaslClient(mechanisms, authorizationId, protocol, serverName, props, callbackHandler);

            // now just pass the details to the X Protocol auth start message
            AuthenticateStart.Builder authStartBuilder = AuthenticateStart.newBuilder();
            authStartBuilder.setMechName("PLAIN");
            // saslClient will build the SASL response message
            authStartBuilder.setAuthData(ByteString.copyFrom(saslClient.evaluateChallenge(null)));

            return authStartBuilder.build();
        } catch (SaslException ex) {
            // TODO: better exception, should introduce a new exception class for auth?
            throw new RuntimeException(ex);
        }
    }

    public AuthenticateStart buildExternalAuthStart(String database) {
        CallbackHandler callbackHandler = new CallbackHandler() {
            public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
                for (Callback c : callbacks) {
                    if (NameCallback.class.isAssignableFrom(c.getClass())) {
                        // TODO ((NameCallback) c).setName(user);
                        throw new UnsupportedCallbackException(c);
                    } else if (PasswordCallback.class.isAssignableFrom(c.getClass())) {
                        // TODO ((PasswordCallback) c).setPassword(password.toCharArray());
                        throw new UnsupportedCallbackException(c);
                    } else {
                        throw new UnsupportedCallbackException(c);
                    }
                }
            }
        };
        try {
            // now we create the client object we use which can handle EXTERNAL mechanism for "X Protocol" to "serverName"
            String[] mechanisms = new String[] { "EXTERNAL" };
            String authorizationId = database == null || database.trim().length() == 0 ? null : database; // as per protocol spec
            String protocol = "X Protocol";
            Map<String, ?> props = null;
            // TODO: >> serverName. Is this of any use in our X Protocol exchange? Should be defined to be blank or something.
            String serverName = "<unknown>";
            SaslClient saslClient = Sasl.createSaslClient(mechanisms, authorizationId, protocol, serverName, props, callbackHandler);

            // now just pass the details to the X Protocol auth start message
            AuthenticateStart.Builder authStartBuilder = AuthenticateStart.newBuilder();
            authStartBuilder.setMechName("EXTERNAL");
            // saslClient will build the SASL response message
            authStartBuilder.setAuthData(ByteString.copyFrom(saslClient.evaluateChallenge(null)));

            return authStartBuilder.build();
        } catch (SaslException ex) {
            // TODO: better exception, should introduce a new exception class for auth?
            throw new RuntimeException(ex);
        }
    }

    //    public AuthenticateStart buildSha256MemoryStart(String database) { // TODO String user, String password, 
    //        CallbackHandler callbackHandler = new CallbackHandler() {
    //            public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
    //                for (Callback c : callbacks) {
    //                    if (NameCallback.class.isAssignableFrom(c.getClass())) {
    //                        // TODO ((NameCallback) c).setName(user);
    //                        throw new UnsupportedCallbackException(c);
    //                    } else if (PasswordCallback.class.isAssignableFrom(c.getClass())) {
    //                        // TODO ((PasswordCallback) c).setPassword(password.toCharArray());
    //                        throw new UnsupportedCallbackException(c);
    //                    } else {
    //                        throw new UnsupportedCallbackException(c);
    //                    }
    //                }
    //            }
    //        };
    //        try {
    //            // now we create the client object we use which can handle EXTERNAL mechanism for "X Protocol" to "serverName"
    //            String[] mechanisms = new String[] { "SHA256_MEMORY" };
    //            String authorizationId = database == null || database.trim().length() == 0 ? null : database; // as per protocol spec
    //            String protocol = "X Protocol";
    //            Map<String, ?> props = null;
    //            // TODO: >> serverName. Is this of any use in our X Protocol exchange? Should be defined to be blank or something.
    //            String serverName = "<unknown>";
    //            SaslClient saslClient = Sasl.createSaslClient(mechanisms, authorizationId, protocol, serverName, props, callbackHandler);
    //
    //            // now just pass the details to the X Protocol auth start message
    //            AuthenticateStart.Builder authStartBuilder = AuthenticateStart.newBuilder();
    //            authStartBuilder.setMechName("SHA256_MEMORY");
    //            // saslClient will build the SASL response message
    //            authStartBuilder.setAuthData(ByteString.copyFrom(saslClient.evaluateChallenge(null)));
    //
    //            return authStartBuilder.build();
    //        } catch (SaslException ex) {
    //            // TODO: better exception, should introduce a new exception class for auth?
    //            throw new RuntimeException(ex);
    //        }
    //    }
    //
    //    public AuthenticateContinue buildSha256MemoryAuthContinue(String user, String password, byte[] salt, String database) {
    //        // TODO: encoding for all this?
    //        String encoding = "UTF8";
    //        byte[] userBytes = user == null ? new byte[] {} : StringUtils.getBytes(user, encoding);
    //        byte[] passwordBytes = password == null || password.length() == 0 ? new byte[] {} : StringUtils.getBytes(password, encoding);
    //        byte[] databaseBytes = database == null ? new byte[] {} : StringUtils.getBytes(database, encoding);
    //
    //        byte[] hashedPassword = passwordBytes;
    //        if (password != null && password.length() > 0) {
    //            try {
    //                hashedPassword = //Security.scramble411(passwordBytes, salt);
    //
    //                        Security.scrambleCachingSha2(StringUtils.getBytes(password, encoding), salt);
    //            } catch (DigestException e) {
    //                // TODO Auto-generated catch block
    //                e.printStackTrace();
    //            }
    //
    //            // protocol dictates *-prefixed hex string as hashed password
    //            //hashedPassword = String.format("*%040x", new java.math.BigInteger(1, hashedPassword)).getBytes();
    //
    //            String hexPassword = StringUtils.toHexString(hashedPassword, hashedPassword.length);
    //            hashedPassword = hexPassword.getBytes();
    //        }
    //
    //        // this is what would happen in the SASL provider but we don't need the overhead of all the plumbing.
    //        byte[] reply = new byte[databaseBytes.length + userBytes.length + hashedPassword.length + 2];
    //
    //        // reply is length-prefixed when sent so we just separate fields by \0
    //        System.arraycopy(databaseBytes, 0, reply, 0, databaseBytes.length);
    //        int pos = databaseBytes.length;
    //        reply[pos++] = 0;
    //        System.arraycopy(userBytes, 0, reply, pos, userBytes.length);
    //        pos += userBytes.length;
    //        reply[pos++] = 0;
    //        System.arraycopy(hashedPassword, 0, reply, pos, hashedPassword.length);
    //
    //        AuthenticateContinue.Builder builder = AuthenticateContinue.newBuilder();
    //        builder.setAuthData(ByteString.copyFrom(reply));
    //        return builder.build();
    //    }
    //    public static String toHexString(byte[] byteBuffer, int length) {
    //        StringBuilder outputBuilder = new StringBuilder(length * 2);
    //        for (int i = 0; i < length; i++) {
    //            String hexVal = Integer.toHexString(byteBuffer[i] & 0xff);
    //            if (hexVal.length() == 1) {
    //                hexVal = "0" + hexVal;
    //            }
    //            outputBuilder.append(hexVal);
    //        }
    //        return outputBuilder.toString();
    //    }

}
