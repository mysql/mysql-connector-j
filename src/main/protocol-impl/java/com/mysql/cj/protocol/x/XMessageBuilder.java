/*
 * Copyright (c) 2015, 2024, Oracle and/or its affiliates.
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

package com.mysql.cj.protocol.x;

import java.security.DigestException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import com.google.protobuf.ByteString;
import com.mysql.cj.MessageBuilder;
import com.mysql.cj.Messages;
import com.mysql.cj.PreparedQuery;
import com.mysql.cj.Query;
import com.mysql.cj.QueryBindings;
import com.mysql.cj.Session;
import com.mysql.cj.exceptions.CJOperationNotSupportedException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.protocol.Security;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.x.protobuf.MysqlxConnection.Capabilities;
import com.mysql.cj.x.protobuf.MysqlxConnection.CapabilitiesGet;
import com.mysql.cj.x.protobuf.MysqlxConnection.CapabilitiesSet;
import com.mysql.cj.x.protobuf.MysqlxConnection.Capability;
import com.mysql.cj.x.protobuf.MysqlxCrud.Collection;
import com.mysql.cj.x.protobuf.MysqlxCrud.Column;
import com.mysql.cj.x.protobuf.MysqlxCrud.DataModel;
import com.mysql.cj.x.protobuf.MysqlxCrud.Delete;
import com.mysql.cj.x.protobuf.MysqlxCrud.Find;
import com.mysql.cj.x.protobuf.MysqlxCrud.Find.RowLock;
import com.mysql.cj.x.protobuf.MysqlxCrud.Find.RowLockOptions;
import com.mysql.cj.x.protobuf.MysqlxCrud.Insert;
import com.mysql.cj.x.protobuf.MysqlxCrud.Insert.TypedRow;
import com.mysql.cj.x.protobuf.MysqlxCrud.Limit;
import com.mysql.cj.x.protobuf.MysqlxCrud.LimitExpr;
import com.mysql.cj.x.protobuf.MysqlxCrud.Order;
import com.mysql.cj.x.protobuf.MysqlxCrud.Projection;
import com.mysql.cj.x.protobuf.MysqlxCrud.Update;
import com.mysql.cj.x.protobuf.MysqlxCrud.UpdateOperation;
import com.mysql.cj.x.protobuf.MysqlxCrud.UpdateOperation.UpdateType;
import com.mysql.cj.x.protobuf.MysqlxDatatypes;
import com.mysql.cj.x.protobuf.MysqlxDatatypes.Any;
import com.mysql.cj.x.protobuf.MysqlxDatatypes.Object.Builder;
import com.mysql.cj.x.protobuf.MysqlxDatatypes.Object.ObjectField;
import com.mysql.cj.x.protobuf.MysqlxDatatypes.Scalar;
import com.mysql.cj.x.protobuf.MysqlxExpect;
import com.mysql.cj.x.protobuf.MysqlxExpr.ColumnIdentifier;
import com.mysql.cj.x.protobuf.MysqlxExpr.Expr;
import com.mysql.cj.x.protobuf.MysqlxPrepare.Deallocate;
import com.mysql.cj.x.protobuf.MysqlxPrepare.Execute;
import com.mysql.cj.x.protobuf.MysqlxPrepare.Prepare;
import com.mysql.cj.x.protobuf.MysqlxPrepare.Prepare.OneOfMessage;
import com.mysql.cj.x.protobuf.MysqlxSession.AuthenticateContinue;
import com.mysql.cj.x.protobuf.MysqlxSession.AuthenticateStart;
import com.mysql.cj.x.protobuf.MysqlxSession.Close;
import com.mysql.cj.x.protobuf.MysqlxSession.Reset;
import com.mysql.cj.x.protobuf.MysqlxSql.StmtExecute;
import com.mysql.cj.xdevapi.CreateIndexParams;
import com.mysql.cj.xdevapi.CreateIndexParams.IndexField;
import com.mysql.cj.xdevapi.ExprUtil;
import com.mysql.cj.xdevapi.FilterParams;
import com.mysql.cj.xdevapi.InsertParams;
import com.mysql.cj.xdevapi.Schema.CreateCollectionOptions;
import com.mysql.cj.xdevapi.Schema.ModifyCollectionOptions;
import com.mysql.cj.xdevapi.UpdateParams;
import com.mysql.cj.xdevapi.UpdateSpec;

public class XMessageBuilder implements MessageBuilder<XMessage> {

    private static final String XPLUGIN_NAMESPACE = "mysqlx";

    public XMessage buildCapabilitiesGet() {
        return new XMessage(CapabilitiesGet.getDefaultInstance());
    }

    @SuppressWarnings("unchecked")
    public XMessage buildCapabilitiesSet(Map<String, Object> keyValuePair) {
        Capabilities.Builder capsB = Capabilities.newBuilder();
        keyValuePair.forEach((k, v) -> {
            Any val;
            if (XServerCapabilities.KEY_SESSION_CONNECT_ATTRS.equals(k) || XServerCapabilities.KEY_COMPRESSION.equals(k)) {
                MysqlxDatatypes.Object.Builder attrB = MysqlxDatatypes.Object.newBuilder();
                ((Map<String, Object>) v)
                        .forEach((name, value) -> attrB.addFld(ObjectField.newBuilder().setKey(name).setValue(ExprUtil.argObjectToScalarAny(value)).build()));
                val = Any.newBuilder().setType(Any.Type.OBJECT).setObj(attrB).build();

            } else {
                val = ExprUtil.argObjectToScalarAny(v);
            }
            Capability cap = Capability.newBuilder().setName(k).setValue(val).build();
            capsB.addCapabilities(cap);
        });
        return new XMessage(CapabilitiesSet.newBuilder().setCapabilities(capsB).build());
    }

    /**
     * Build an {@link XMessage} for a non-prepared doc insert operation.
     *
     * @param schemaName
     *            the schema name
     * @param collectionName
     *            the collection name
     * @param json
     *            the documents to insert
     * @param upsert
     *            Whether this is an upsert operation or not
     * @return
     *         an {@link XMessage} instance
     */
    public XMessage buildDocInsert(String schemaName, String collectionName, List<String> json, boolean upsert) {
        Insert.Builder builder = Insert.newBuilder().setCollection(ExprUtil.buildCollection(schemaName, collectionName));
        if (upsert != builder.getUpsert()) {
            builder.setUpsert(upsert);
        }
        json.stream().map(str -> TypedRow.newBuilder().addField(ExprUtil.argObjectToExpr(str, false)).build()).forEach(builder::addRow);
        return new XMessage(builder.build());
    }

    /**
     * Initialize a {@link com.mysql.cj.x.protobuf.MysqlxCrud.Insert.Builder} for table data model with common data for prepared and non-prepared executions.
     *
     * @param schemaName
     *            the schema name
     * @param tableName
     *            the table name
     * @param insertParams
     *            the parameters to insert
     * @return
     *         an initialized {@link com.mysql.cj.x.protobuf.MysqlxCrud.Insert.Builder} instance
     */
    @SuppressWarnings("unchecked")
    private Insert.Builder commonRowInsertBuilder(String schemaName, String tableName, InsertParams insertParams) {
        Insert.Builder builder = Insert.newBuilder().setDataModel(DataModel.TABLE).setCollection(ExprUtil.buildCollection(schemaName, tableName));
        if (insertParams.getProjection() != null) {
            builder.addAllProjection((List<Column>) insertParams.getProjection());
        }
        return builder;
    }

    /**
     * Build an {@link XMessage} for a non-prepared row insert operation.
     *
     * @param schemaName
     *            the schema name
     * @param tableName
     *            the table name
     * @param insertParams
     *            the parameters to insert
     * @return
     *         an {@link XMessage} instance
     */
    @SuppressWarnings("unchecked")
    public XMessage buildRowInsert(String schemaName, String tableName, InsertParams insertParams) {
        Insert.Builder builder = commonRowInsertBuilder(schemaName, tableName, insertParams);
        builder.addAllRow((List<TypedRow>) insertParams.getRows());
        return new XMessage(builder.build());
    }

    /**
     * Initialize an {@link com.mysql.cj.x.protobuf.MysqlxCrud.Update.Builder} for collection data model with common data for prepared and non-prepared
     * executions.
     *
     * @param filterParams
     *            the filter parameters
     * @param updates
     *            the updates specifications to perform
     * @return
     *         an initialized {@link com.mysql.cj.x.protobuf.MysqlxCrud.Update.Builder} instance
     */
    private Update.Builder commonDocUpdateBuilder(FilterParams filterParams, List<UpdateSpec> updates) {
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
        return builder;
    }

    /**
     * Build an {@link XMessage} for a non-prepared doc update operation.
     *
     * @param filterParams
     *            the filter parameters
     * @param updates
     *            the updates specifications to perform
     * @return
     *         an {@link XMessage} instance
     */
    public XMessage buildDocUpdate(FilterParams filterParams, List<UpdateSpec> updates) {
        Update.Builder builder = commonDocUpdateBuilder(filterParams, updates);
        applyFilterParams(filterParams, builder::addAllOrder, builder::setLimit, builder::setCriteria, builder::addAllArgs);
        return new XMessage(builder.build());
    }

    /**
     * Build an {@link XMessage} for a prepared doc update operation.
     *
     * @param preparedStatementId
     *            the prepared statement id
     * @param filterParams
     *            the filter parameters
     * @param updates
     *            the updates specifications to perform
     * @return
     *         an {@link XMessage} instance
     */
    public XMessage buildPrepareDocUpdate(int preparedStatementId, FilterParams filterParams, List<UpdateSpec> updates) {
        Update.Builder updateBuilder = commonDocUpdateBuilder(filterParams, updates);
        applyFilterParams(filterParams, updateBuilder::addAllOrder, updateBuilder::setLimitExpr, updateBuilder::setCriteria);

        Prepare.Builder builder = Prepare.newBuilder().setStmtId(preparedStatementId);
        builder.setStmt(OneOfMessage.newBuilder().setType(OneOfMessage.Type.UPDATE).setUpdate(updateBuilder.build()).build());
        return new XMessage(builder.build());
    }

    /**
     * Initialize an {@link com.mysql.cj.x.protobuf.MysqlxCrud.Update.Builder} for table data model with common data for prepared and non-prepared executions.
     *
     * @param filterParams
     *            the filter parameters
     * @param updateParams
     *            the update parameters
     * @return
     *         an initialized {@link com.mysql.cj.x.protobuf.MysqlxCrud.Update.Builder} instance
     */
    @SuppressWarnings("unchecked")
    private Update.Builder commonRowUpdateBuilder(FilterParams filterParams, UpdateParams updateParams) {
        Update.Builder builder = Update.newBuilder().setDataModel(DataModel.TABLE).setCollection((Collection) filterParams.getCollection());
        ((Map<ColumnIdentifier, Expr>) updateParams.getUpdates()).entrySet().stream()
                .map(e -> UpdateOperation.newBuilder().setOperation(UpdateType.SET).setSource(e.getKey()).setValue(e.getValue()).build())
                .forEach(builder::addOperation);
        return builder;
    }

    /**
     * Build an {@link XMessage} for a non-prepared row update operation.
     *
     * @param filterParams
     *            the filter parameters
     * @param updateParams
     *            the update parameters
     * @return
     *         an {@link XMessage} instance
     */
    public XMessage buildRowUpdate(FilterParams filterParams, UpdateParams updateParams) {
        Update.Builder builder = commonRowUpdateBuilder(filterParams, updateParams);
        applyFilterParams(filterParams, builder::addAllOrder, builder::setLimit, builder::setCriteria, builder::addAllArgs);
        return new XMessage(builder.build());
    }

    /**
     * Build an {@link XMessage} for a prepared row update operation.
     *
     * @param preparedStatementId
     *            the prepared statement id
     * @param filterParams
     *            the filter parameters
     * @param updateParams
     *            the update parameters
     * @return
     *         an {@link XMessage} instance
     */
    public XMessage buildPrepareRowUpdate(int preparedStatementId, FilterParams filterParams, UpdateParams updateParams) {
        Update.Builder updateBuilder = commonRowUpdateBuilder(filterParams, updateParams);
        applyFilterParams(filterParams, updateBuilder::addAllOrder, updateBuilder::setLimitExpr, updateBuilder::setCriteria);

        Prepare.Builder builder = Prepare.newBuilder().setStmtId(preparedStatementId);
        builder.setStmt(OneOfMessage.newBuilder().setType(OneOfMessage.Type.UPDATE).setUpdate(updateBuilder.build()).build());
        return new XMessage(builder.build());
    }

    /**
     * Initialize a {@link com.mysql.cj.x.protobuf.MysqlxCrud.Find.Builder} for collection data model with common data for prepared and non-prepared executions.
     *
     * @param filterParams
     *            the filter parameters
     * @return
     *         an initialized {@link com.mysql.cj.x.protobuf.MysqlxCrud.Find.Builder} instance
     */
    @SuppressWarnings("unchecked")
    private Find.Builder commonFindBuilder(FilterParams filterParams) {
        Find.Builder builder = Find.newBuilder().setCollection((Collection) filterParams.getCollection());
        builder.setDataModel(filterParams.isRelational() ? DataModel.TABLE : DataModel.DOCUMENT);
        if (filterParams.getFields() != null) {
            builder.addAllProjection((List<Projection>) filterParams.getFields());
        }
        if (filterParams.getGrouping() != null) {
            builder.addAllGrouping((List<Expr>) filterParams.getGrouping());
        }
        if (filterParams.getGroupingCriteria() != null) {
            builder.setGroupingCriteria((Expr) filterParams.getGroupingCriteria());
        }
        if (filterParams.getLock() != null) {
            builder.setLocking(RowLock.forNumber(filterParams.getLock().asNumber()));
        }
        if (filterParams.getLockOption() != null) {
            builder.setLockingOptions(RowLockOptions.forNumber(filterParams.getLockOption().asNumber()));
        }
        return builder;
    }

    /**
     * Build an {@link XMessage} for a non-prepared find operation.
     *
     * @param filterParams
     *            the filter parameters
     * @return
     *         an {@link XMessage} instance
     */
    public XMessage buildFind(FilterParams filterParams) {
        Find.Builder builder = commonFindBuilder(filterParams);
        applyFilterParams(filterParams, builder::addAllOrder, builder::setLimit, builder::setCriteria, builder::addAllArgs);
        return new XMessage(builder.build());
    }

    /**
     * Build an {@link XMessage} for a prepared find operation.
     *
     * @param preparedStatementId
     *            the prepared statement id
     * @param filterParams
     *            the filter parameters
     * @return
     *         an {@link XMessage} instance
     */
    public XMessage buildPrepareFind(int preparedStatementId, FilterParams filterParams) {
        Find.Builder findBuilder = commonFindBuilder(filterParams);
        applyFilterParams(filterParams, findBuilder::addAllOrder, findBuilder::setLimitExpr, findBuilder::setCriteria);

        Prepare.Builder builder = Prepare.newBuilder().setStmtId(preparedStatementId);
        builder.setStmt(OneOfMessage.newBuilder().setType(OneOfMessage.Type.FIND).setFind(findBuilder.build()).build());
        return new XMessage(builder.build());
    }

    /**
     * Initialize a {@link com.mysql.cj.x.protobuf.MysqlxCrud.Delete.Builder} with common data for prepared and non-prepared executions.
     *
     * @param filterParams
     *            the filter parameters
     * @return
     *         an initialized {@link com.mysql.cj.x.protobuf.MysqlxCrud.Delete.Builder} instance
     */
    private Delete.Builder commonDeleteBuilder(FilterParams filterParams) {
        Delete.Builder builder = Delete.newBuilder().setCollection((Collection) filterParams.getCollection());
        return builder;
    }

    /**
     * Build an {@link XMessage} for a non-prepared delete operation.
     *
     * @param filterParams
     *            the filter parameters
     * @return
     *         an {@link XMessage} instance
     */
    public XMessage buildDelete(FilterParams filterParams) {
        Delete.Builder builder = commonDeleteBuilder(filterParams);
        applyFilterParams(filterParams, builder::addAllOrder, builder::setLimit, builder::setCriteria, builder::addAllArgs);
        return new XMessage(builder.build());
    }

    /**
     * Build an {@link XMessage} for a prepared delete operation.
     *
     * @param preparedStatementId
     *            the prepared statement id
     * @param filterParams
     *            the filter parameters
     * @return
     *         an {@link XMessage} instance
     */
    public XMessage buildPrepareDelete(int preparedStatementId, FilterParams filterParams) {
        Delete.Builder deleteBuilder = commonDeleteBuilder(filterParams);
        applyFilterParams(filterParams, deleteBuilder::addAllOrder, deleteBuilder::setLimitExpr, deleteBuilder::setCriteria);

        Prepare.Builder builder = Prepare.newBuilder().setStmtId(preparedStatementId);
        builder.setStmt(OneOfMessage.newBuilder().setType(OneOfMessage.Type.DELETE).setDelete(deleteBuilder.build()).build());
        return new XMessage(builder.build());
    }

    /**
     * Initialize a {@link com.mysql.cj.x.protobuf.MysqlxSql.StmtExecute.Builder} with common data for prepared and non-prepared executions.
     *
     * @param statement
     *            the SQL statement
     * @return
     *         an initialized {@link com.mysql.cj.x.protobuf.MysqlxSql.StmtExecute.Builder} instance
     */
    private StmtExecute.Builder commonSqlStatementBuilder(String statement) {
        StmtExecute.Builder builder = StmtExecute.newBuilder();
        // TODO: encoding (character_set_client?)
        builder.setStmt(ByteString.copyFromUtf8(statement));
        return builder;
    }

    /**
     * Build a <i>StmtExecute</i> message for a SQL statement.
     *
     * @param statement
     *            SQL statement string
     * @return {@link XMessage} wrapping {@link StmtExecute}
     */
    @Override
    public XMessage buildSqlStatement(String statement) {
        return buildSqlStatement(statement, null);
    }

    /**
     * Build a <i>StmtExecute</i> message for a SQL statement.
     *
     * @param statement
     *            SQL statement string
     * @param args
     *            list of {@link Object} arguments
     * @return {@link XMessage} wrapping {@link StmtExecute}
     */
    @Override
    public XMessage buildSqlStatement(String statement, List<Object> args) {
        StmtExecute.Builder builder = commonSqlStatementBuilder(statement);
        if (args != null) {
            builder.addAllArgs(args.stream().map(ExprUtil::argObjectToScalarAny).collect(Collectors.toList()));
        }
        return new XMessage(builder.build());
    }

    /**
     * Build a <i>Prepare</i> message for a SQL statement.
     *
     * @param preparedStatementId
     *            the prepared statement id
     * @param statement
     *            SQL statement string
     * @return {@link XMessage} wrapping {@link StmtExecute}
     */
    public XMessage buildPrepareSqlStatement(int preparedStatementId, String statement) {
        StmtExecute.Builder stmtExecBuilder = commonSqlStatementBuilder(statement);

        Prepare.Builder builder = Prepare.newBuilder().setStmtId(preparedStatementId);
        builder.setStmt(OneOfMessage.newBuilder().setType(OneOfMessage.Type.STMT).setStmtExecute(stmtExecBuilder.build()).build());
        return new XMessage(builder.build());
    }

    /**
     * Apply the given filter params to the builder object (represented by the setter methods).
     *
     * Abstract the process of setting the filter params on the operation message builder.
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

    /**
     * Apply the given filter params to the builder object (represented by the setter methods) using the variant that takes a <code>LimitExpr</code> and no
     * <code>Args</code>. This variant is suitable for building prepared statements prepare messages.
     *
     * Abstract the process of setting the filter params on the operation message builder.
     *
     * @param filterParams
     *            the filter params to apply
     * @param setOrder
     *            the "builder.addAllOrder()" method reference
     * @param setLimit
     *            the "builder.setLimitExp()" method reference
     * @param setCriteria
     *            the "builder.setCriteria()" method reference
     */
    @SuppressWarnings("unchecked")
    private static void applyFilterParams(FilterParams filterParams, Consumer<List<Order>> setOrder, Consumer<LimitExpr> setLimit, Consumer<Expr> setCriteria) {
        if (filterParams.getOrder() != null) {
            setOrder.accept((List<Order>) filterParams.getOrder());
        }
        Object argsList = filterParams.getArgs();
        int numberOfArgs = argsList == null ? 0 : ((List<Scalar>) argsList).size();
        if (filterParams.getLimit() != null) {
            LimitExpr.Builder lb = LimitExpr.newBuilder().setRowCount(ExprUtil.buildPlaceholderExpr(numberOfArgs));
            if (filterParams.supportsOffset()) {
                lb.setOffset(ExprUtil.buildPlaceholderExpr(numberOfArgs + 1));
            }
            setLimit.accept(lb.build());
        }
        if (filterParams.getCriteria() != null) {
            setCriteria.accept((Expr) filterParams.getCriteria());
        }
    }

    /**
     * Build an {@link XMessage} for executing a prepared statement with the given filters.
     *
     * @param preparedStatementId
     *            the prepared statement id
     * @param filterParams
     *            the filter parameter values
     * @return
     *         an {@link XMessage} instance
     */
    @SuppressWarnings("unchecked")
    public XMessage buildPrepareExecute(int preparedStatementId, FilterParams filterParams) {
        Execute.Builder builder = Execute.newBuilder().setStmtId(preparedStatementId);
        if (filterParams.getArgs() != null) {
            builder.addAllArgs(((List<Scalar>) filterParams.getArgs()).stream().map(s -> Any.newBuilder().setType(Any.Type.SCALAR).setScalar(s).build())
                    .collect(Collectors.toList()));
        }
        if (filterParams.getLimit() != null) {
            builder.addArgs(ExprUtil.anyOf(ExprUtil.scalarOf(filterParams.getLimit())));
            if (filterParams.supportsOffset()) {
                builder.addArgs(ExprUtil.anyOf(ExprUtil.scalarOf(filterParams.getOffset() != null ? filterParams.getOffset() : 0)));
            }
        }
        return new XMessage(builder.build());
    }

    /**
     * Build an {@link XMessage} for deallocating a prepared statement.
     *
     * @param preparedStatementId
     *            the prepared statement id
     * @return
     *         an {@link XMessage} instance
     */
    public XMessage buildPrepareDeallocate(int preparedStatementId) {
        Deallocate.Builder builder = Deallocate.newBuilder().setStmtId(preparedStatementId);
        return new XMessage(builder.build());
    }

    public XMessage buildCreateCollection(String schemaName, String collectionName, CreateCollectionOptions options) {
        if (schemaName == null) {
            throw new XProtocolError(Messages.getString("CreateTableStatement.0", new String[] { "schemaName" }));
        }
        if (collectionName == null) {
            throw new XProtocolError(Messages.getString("CreateTableStatement.0", new String[] { "collectionName" }));
        }

        Builder argsBuilder = com.mysql.cj.x.protobuf.MysqlxDatatypes.Object.newBuilder()
                .addFld(ObjectField.newBuilder().setKey("name").setValue(ExprUtil.buildAny(collectionName)))
                .addFld(ObjectField.newBuilder().setKey("schema").setValue(ExprUtil.buildAny(schemaName)));

        Builder optBuilder = com.mysql.cj.x.protobuf.MysqlxDatatypes.Object.newBuilder();

        boolean hasOptions = false;
        if (options.getReuseExisting() != null) {
            hasOptions = true;
            optBuilder.addFld(ObjectField.newBuilder().setKey("reuse_existing").setValue(ExprUtil.buildAny(options.getReuseExisting())));
        }

        if (options.getValidation() != null) {
            hasOptions = true;
            Builder validationBuilder = com.mysql.cj.x.protobuf.MysqlxDatatypes.Object.newBuilder();
            if (options.getValidation().getSchema() != null) {
                validationBuilder.addFld(ObjectField.newBuilder().setKey("schema").setValue(ExprUtil.buildAny(options.getValidation().getSchema())));
            }
            if (options.getValidation().getLevel() != null) {
                validationBuilder
                        .addFld(ObjectField.newBuilder().setKey("level").setValue(ExprUtil.buildAny(options.getValidation().getLevel().name().toLowerCase())));
            }
            optBuilder.addFld(ObjectField.newBuilder().setKey("validation").setValue(Any.newBuilder().setType(Any.Type.OBJECT).setObj(validationBuilder)));
        }
        if (hasOptions) {
            argsBuilder.addFld(ObjectField.newBuilder().setKey("options").setValue(Any.newBuilder().setType(Any.Type.OBJECT).setObj(optBuilder)));
        }

        return new XMessage(buildXpluginCommand(XpluginStatementCommand.XPLUGIN_STMT_CREATE_COLLECTION,
                Any.newBuilder().setType(Any.Type.OBJECT).setObj(argsBuilder).build()));
    }

    public XMessage buildModifyCollectionOptions(String schemaName, String collectionName, ModifyCollectionOptions options) {
        if (schemaName == null) {
            throw new XProtocolError(Messages.getString("CreateTableStatement.0", new String[] { "schemaName" }));
        }
        if (collectionName == null) {
            throw new XProtocolError(Messages.getString("CreateTableStatement.0", new String[] { "collectionName" }));
        }

        Builder argsBuilder = com.mysql.cj.x.protobuf.MysqlxDatatypes.Object.newBuilder()
                .addFld(ObjectField.newBuilder().setKey("name").setValue(ExprUtil.buildAny(collectionName)))
                .addFld(ObjectField.newBuilder().setKey("schema").setValue(ExprUtil.buildAny(schemaName)));

        Builder optBuilder = com.mysql.cj.x.protobuf.MysqlxDatatypes.Object.newBuilder();

        if (options != null && options.getValidation() != null) {
            Builder validationBuilder = com.mysql.cj.x.protobuf.MysqlxDatatypes.Object.newBuilder();
            if (options.getValidation().getSchema() != null) {
                validationBuilder.addFld(ObjectField.newBuilder().setKey("schema").setValue(ExprUtil.buildAny(options.getValidation().getSchema())));
            }
            if (options.getValidation().getLevel() != null) {
                validationBuilder
                        .addFld(ObjectField.newBuilder().setKey("level").setValue(ExprUtil.buildAny(options.getValidation().getLevel().name().toLowerCase())));
            }
            optBuilder.addFld(ObjectField.newBuilder().setKey("validation").setValue(Any.newBuilder().setType(Any.Type.OBJECT).setObj(validationBuilder)));
        }

        argsBuilder.addFld(ObjectField.newBuilder().setKey("options").setValue(Any.newBuilder().setType(Any.Type.OBJECT).setObj(optBuilder)));

        return new XMessage(buildXpluginCommand(XpluginStatementCommand.XPLUGIN_STMT_MODIFY_COLLECTION_OPTIONS,
                Any.newBuilder().setType(Any.Type.OBJECT).setObj(argsBuilder).build()));
    }

    public XMessage buildCreateCollection(String schemaName, String collectionName) {
        if (schemaName == null) {
            throw new XProtocolError(Messages.getString("CreateTableStatement.0", new String[] { "schemaName" }));
        }
        if (collectionName == null) {
            throw new XProtocolError(Messages.getString("CreateTableStatement.0", new String[] { "collectionName" }));
        }
        return new XMessage(buildXpluginCommand(XpluginStatementCommand.XPLUGIN_STMT_CREATE_COLLECTION,
                Any.newBuilder().setType(Any.Type.OBJECT)
                        .setObj(com.mysql.cj.x.protobuf.MysqlxDatatypes.Object.newBuilder()
                                .addFld(ObjectField.newBuilder().setKey("name").setValue(ExprUtil.buildAny(collectionName)))
                                .addFld(ObjectField.newBuilder().setKey("schema").setValue(ExprUtil.buildAny(schemaName))))
                        .build()));
    }

    public XMessage buildDropCollection(String schemaName, String collectionName) {
        // TODO this works for tables too
        if (schemaName == null) {
            throw new XProtocolError(Messages.getString("CreateTableStatement.0", new String[] { "schemaName" }));
        }
        if (collectionName == null) {
            throw new XProtocolError(Messages.getString("CreateTableStatement.0", new String[] { "collectionName" }));
        }
        return new XMessage(buildXpluginCommand(XpluginStatementCommand.XPLUGIN_STMT_DROP_COLLECTION,
                Any.newBuilder().setType(Any.Type.OBJECT)
                        .setObj(com.mysql.cj.x.protobuf.MysqlxDatatypes.Object.newBuilder()
                                .addFld(ObjectField.newBuilder().setKey("name").setValue(ExprUtil.buildAny(collectionName)))
                                .addFld(ObjectField.newBuilder().setKey("schema").setValue(ExprUtil.buildAny(schemaName))))
                        .build()));
    }

    @Override
    public XMessage buildClose() {
        return new XMessage(Close.getDefaultInstance());
    }

    /**
     * List the objects in the given schema. Returns a table as so:
     *
     * <pre>
     * | name                | type       |
     * |---------------------+------------|
     * | CollectionTest      | COLLECTION |
     * | some_view           | VIEW       |
     * | xprotocol_test_test | TABLE      |
     * </pre>
     *
     * .
     *
     * @param schemaName
     *            schema name
     * @param pattern
     *            object name pattern
     * @return XMessage
     */
    public XMessage buildListObjects(String schemaName, String pattern) {
        if (schemaName == null) {
            throw new XProtocolError(Messages.getString("CreateTableStatement.0", new String[] { "schemaName" }));
        }

        Builder obj = com.mysql.cj.x.protobuf.MysqlxDatatypes.Object.newBuilder()
                .addFld(ObjectField.newBuilder().setKey("schema").setValue(ExprUtil.buildAny(schemaName)));

        if (pattern != null) {
            obj.addFld(ObjectField.newBuilder().setKey("pattern").setValue(ExprUtil.buildAny(pattern)));
        }

        return new XMessage(
                buildXpluginCommand(XpluginStatementCommand.XPLUGIN_STMT_LIST_OBJECTS, Any.newBuilder().setType(Any.Type.OBJECT).setObj(obj).build()));
    }

    public XMessage buildEnableNotices(String... notices) {
        com.mysql.cj.x.protobuf.MysqlxDatatypes.Array.Builder abuilder = com.mysql.cj.x.protobuf.MysqlxDatatypes.Array.newBuilder();
        for (String notice : notices) {
            abuilder.addValue(ExprUtil.buildAny(notice));
        }
        return new XMessage(buildXpluginCommand(XpluginStatementCommand.XPLUGIN_STMT_ENABLE_NOTICES,
                Any.newBuilder().setType(Any.Type.OBJECT)
                        .setObj(com.mysql.cj.x.protobuf.MysqlxDatatypes.Object.newBuilder()
                                .addFld(ObjectField.newBuilder().setKey("notice").setValue(Any.newBuilder().setType(Any.Type.ARRAY).setArray(abuilder))))
                        .build()));
    }

    public XMessage buildDisableNotices(String... notices) {
        com.mysql.cj.x.protobuf.MysqlxDatatypes.Array.Builder abuilder = com.mysql.cj.x.protobuf.MysqlxDatatypes.Array.newBuilder();
        for (String notice : notices) {
            abuilder.addValue(ExprUtil.buildAny(notice));
        }
        return new XMessage(buildXpluginCommand(XpluginStatementCommand.XPLUGIN_STMT_DISABLE_NOTICES,
                Any.newBuilder().setType(Any.Type.OBJECT)
                        .setObj(com.mysql.cj.x.protobuf.MysqlxDatatypes.Object.newBuilder()
                                .addFld(ObjectField.newBuilder().setKey("notice").setValue(Any.newBuilder().setType(Any.Type.ARRAY).setArray(abuilder))))
                        .build()));
    }

    /**
     * List the notices the server allows subscribing to. Returns a table as so:
     *
     * <pre>
     * | notice (string)     | enabled (int) |
     * |---------------------+---------------|
     * | warnings            | 1             |
     * </pre>
     *
     * @return XMessage
     */
    public XMessage buildListNotices() {
        return new XMessage(buildXpluginCommand(XpluginStatementCommand.XPLUGIN_STMT_LIST_NOTICES));
    }

    public XMessage buildCreateCollectionIndex(String schemaName, String collectionName, CreateIndexParams params) {
        com.mysql.cj.x.protobuf.MysqlxDatatypes.Object.Builder builder = com.mysql.cj.x.protobuf.MysqlxDatatypes.Object.newBuilder();
        builder.addFld(ObjectField.newBuilder().setKey("name").setValue(ExprUtil.buildAny(params.getIndexName())))
                .addFld(ObjectField.newBuilder().setKey("collection").setValue(ExprUtil.buildAny(collectionName)))
                .addFld(ObjectField.newBuilder().setKey("schema").setValue(ExprUtil.buildAny(schemaName)))
                .addFld(ObjectField.newBuilder().setKey("unique").setValue(ExprUtil.buildAny(false)));
        if (params.getIndexType() != null) {
            builder.addFld(ObjectField.newBuilder().setKey("type").setValue(ExprUtil.buildAny(params.getIndexType())));
        }

        com.mysql.cj.x.protobuf.MysqlxDatatypes.Array.Builder aBuilder = com.mysql.cj.x.protobuf.MysqlxDatatypes.Array.newBuilder();
        for (IndexField indexField : params.getFields()) {
            com.mysql.cj.x.protobuf.MysqlxDatatypes.Object.Builder fBuilder = com.mysql.cj.x.protobuf.MysqlxDatatypes.Object.newBuilder()
                    .addFld(ObjectField.newBuilder().setKey("member").setValue(ExprUtil.buildAny(indexField.getField())))
                    .addFld(ObjectField.newBuilder().setKey("type").setValue(ExprUtil.buildAny(indexField.getType())));
            if (indexField.isRequired() != null) {
                fBuilder.addFld(ObjectField.newBuilder().setKey("required").setValue(ExprUtil.buildAny(indexField.isRequired())));
            }
            if (indexField.getOptions() != null) {
                fBuilder.addFld(ObjectField.newBuilder().setKey("options").setValue(Any.newBuilder().setType(Any.Type.SCALAR)
                        .setScalar(Scalar.newBuilder().setType(Scalar.Type.V_UINT).setVUnsignedInt(indexField.getOptions())).build()));
            }
            if (indexField.getSrid() != null) {
                fBuilder.addFld(ObjectField.newBuilder().setKey("srid").setValue(Any.newBuilder().setType(Any.Type.SCALAR)
                        .setScalar(Scalar.newBuilder().setType(Scalar.Type.V_UINT).setVUnsignedInt(indexField.getSrid())).build()));
            }
            if (indexField.isArray() != null) {
                fBuilder.addFld(ObjectField.newBuilder().setKey("array").setValue(ExprUtil.buildAny(indexField.isArray())));
            }
            aBuilder.addValue(Any.newBuilder().setType(Any.Type.OBJECT).setObj(fBuilder));
        }

        builder.addFld(ObjectField.newBuilder().setKey("constraint").setValue(Any.newBuilder().setType(Any.Type.ARRAY).setArray(aBuilder)));
        return new XMessage(buildXpluginCommand(XpluginStatementCommand.XPLUGIN_STMT_CREATE_COLLECTION_INDEX,
                Any.newBuilder().setType(Any.Type.OBJECT).setObj(builder).build()));
    }

    public XMessage buildDropCollectionIndex(String schemaName, String collectionName, String indexName) {
        return new XMessage(buildXpluginCommand(XpluginStatementCommand.XPLUGIN_STMT_DROP_COLLECTION_INDEX,
                Any.newBuilder().setType(Any.Type.OBJECT)
                        .setObj(com.mysql.cj.x.protobuf.MysqlxDatatypes.Object.newBuilder()
                                .addFld(ObjectField.newBuilder().setKey("name").setValue(ExprUtil.buildAny(indexName)))
                                .addFld(ObjectField.newBuilder().setKey("collection").setValue(ExprUtil.buildAny(collectionName)))
                                .addFld(ObjectField.newBuilder().setKey("schema").setValue(ExprUtil.buildAny(schemaName)))

                        ).build()));
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
    private StmtExecute buildXpluginCommand(XpluginStatementCommand command, Any... args) {
        StmtExecute.Builder builder = StmtExecute.newBuilder();

        builder.setNamespace(XPLUGIN_NAMESPACE);
        // TODO: encoding (character_set_client?)
        builder.setStmt(ByteString.copyFromUtf8(command.commandName));
        Arrays.stream(args).forEach(a -> builder.addArgs(a));

        return builder.build();
    }

    public XMessage buildSha256MemoryAuthStart() {
        return new XMessage(AuthenticateStart.newBuilder().setMechName("SHA256_MEMORY").build());
    }

    public XMessage buildSha256MemoryAuthContinue(String user, String password, byte[] nonce, String database) {
        // TODO: encoding for all this?
        String encoding = "UTF8";
        byte[] databaseBytes = database == null ? new byte[] {} : StringUtils.getBytes(database, encoding);
        byte[] userBytes = user == null ? new byte[] {} : StringUtils.getBytes(user, encoding);
        byte[] passwordBytes = password == null || password.length() == 0 ? new byte[] {} : StringUtils.getBytes(password, encoding);

        byte[] hashedPassword = passwordBytes;
        try {
            hashedPassword = Security.scrambleCachingSha2(passwordBytes, nonce);
        } catch (DigestException e) {
            throw new RuntimeException(e);
        }

        hashedPassword = StringUtils.toHexString(hashedPassword, hashedPassword.length).getBytes();

        byte[] reply = new byte[databaseBytes.length + userBytes.length + hashedPassword.length + 2];
        System.arraycopy(databaseBytes, 0, reply, 0, databaseBytes.length);
        int pos = databaseBytes.length;
        reply[pos++] = 0;
        System.arraycopy(userBytes, 0, reply, pos, userBytes.length);
        pos += userBytes.length;
        reply[pos++] = 0;
        System.arraycopy(hashedPassword, 0, reply, pos, hashedPassword.length);

        AuthenticateContinue.Builder builder = AuthenticateContinue.newBuilder();
        builder.setAuthData(ByteString.copyFrom(reply));
        return new XMessage(builder.build());
    }

    public XMessage buildMysql41AuthStart() {
        return new XMessage(AuthenticateStart.newBuilder().setMechName("MYSQL41").build());
    }

    public XMessage buildMysql41AuthContinue(String user, String password, byte[] salt, String database) {
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
        return new XMessage(builder.build());
    }

    public XMessage buildPlainAuthStart(String user, String password, String database) {
        // SASL requests information from the app through callbacks. We provide the username and password by these callbacks. This implementation works for
        // PLAIN and would also work for CRAM-MD5. Additional standardized methods may require additional callbacks.
        CallbackHandler callbackHandler = callbacks -> {
            for (Callback c : callbacks) {
                if (NameCallback.class.isAssignableFrom(c.getClass())) {
                    // we get a name callback and provide the username
                    ((NameCallback) c).setName(user);
                } else if (PasswordCallback.class.isAssignableFrom(c.getClass())) {
                    // we get  password callback and provide the password
                    ((PasswordCallback) c).setPassword(password == null ? new char[0] : password.toCharArray());
                } else {
                    // otherwise, thrown an exception
                    throw new UnsupportedCallbackException(c);
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

            return new XMessage(authStartBuilder.build());
        } catch (SaslException ex) {
            // TODO: better exception, should introduce a new exception class for auth?
            throw new RuntimeException(ex);
        }
    }

    public XMessage buildExternalAuthStart(String database) {
        CallbackHandler callbackHandler = callbacks -> {
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

            return new XMessage(authStartBuilder.build());
        } catch (SaslException ex) {
            // TODO: better exception, should introduce a new exception class for auth?
            throw new RuntimeException(ex);
        }
    }

    public XMessage buildSessionResetAndClose() {
        return new XMessage(Reset.newBuilder().build());
    }

    public XMessage buildSessionResetKeepOpen() {
        return new XMessage(Reset.newBuilder().setKeepOpen(true).build());
    }

    public XMessage buildExpectOpen() {
        return new XMessage(MysqlxExpect.Open.newBuilder().addCond(MysqlxExpect.Open.Condition.newBuilder()
                .setConditionKey(MysqlxExpect.Open.Condition.Key.EXPECT_FIELD_EXIST_VALUE).setConditionValue(ByteString.copyFromUtf8("6.1"))).build());
    }

    @Override
    public XMessage buildComQuery(XMessage sharedPacket, Session sess, String query, Query callingQuery, String characterEncoding) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    @Override
    public XMessage buildComQuery(XMessage sharedPacket, Session sess, PreparedQuery preparedQuery, QueryBindings bindings, String characterEncoding) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

}
