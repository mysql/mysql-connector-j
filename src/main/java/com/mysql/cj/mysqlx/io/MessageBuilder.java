/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.mysqlx.io;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import com.google.protobuf.ByteString;
import com.mysql.cj.mysqlx.CreateIndexParams;
import com.mysql.cj.mysqlx.ExprUtil;
import com.mysql.cj.mysqlx.FilterParams;
import com.mysql.cj.mysqlx.FindParams;
import com.mysql.cj.mysqlx.UpdateParams;
import com.mysql.cj.mysqlx.UpdateSpec;
import com.mysql.cj.mysqlx.protobuf.MysqlxCrud.Collection;
import com.mysql.cj.mysqlx.protobuf.MysqlxCrud.DataModel;
import com.mysql.cj.mysqlx.protobuf.MysqlxCrud.Delete;
import com.mysql.cj.mysqlx.protobuf.MysqlxCrud.Find;
import com.mysql.cj.mysqlx.protobuf.MysqlxCrud.Limit;
import com.mysql.cj.mysqlx.protobuf.MysqlxCrud.Order;
import com.mysql.cj.mysqlx.protobuf.MysqlxCrud.Projection;
import com.mysql.cj.mysqlx.protobuf.MysqlxCrud.Update;
import com.mysql.cj.mysqlx.protobuf.MysqlxCrud.UpdateOperation;
import com.mysql.cj.mysqlx.protobuf.MysqlxCrud.UpdateOperation.UpdateType;
import com.mysql.cj.mysqlx.protobuf.MysqlxDatatypes.Any;
import com.mysql.cj.mysqlx.protobuf.MysqlxDatatypes.Scalar;
import com.mysql.cj.mysqlx.protobuf.MysqlxExpr.ColumnIdentifier;
import com.mysql.cj.mysqlx.protobuf.MysqlxExpr.Expr;
import com.mysql.cj.mysqlx.protobuf.MysqlxSql.StmtExecute;

public class MessageBuilder {
    // "xplugin" namespace for StmtExecute messages
    private static final String XPLUGIN_NAMESPACE = "xplugin";

    public static enum XpluginStatementCommand {
        XPLUGIN_STMT_CREATE_COLLECTION("create_collection"), XPLUGIN_STMT_CREATE_COLLECTION_INDEX("create_collection_index"), XPLUGIN_STMT_DROP_COLLECTION(
                "drop_collection"), XPLUGIN_STMT_DROP_COLLECTION_INDEX("drop_collection_index"), XPLUGIN_STMT_PING("ping"), XPLUGIN_STMT_LIST_OBJECTS(
                "list_objects"), XPLUGIN_STMT_ENABLE_NOTICES("enable_notices"), XPLUGIN_STMT_DISABLE_NOTICES("disable_notices"), XPLUGIN_STMT_LIST_NOTICES(
                "list_notices");

        public String commandName;

        private XpluginStatementCommand(String commandName) {
            this.commandName = commandName;
        }
    }

    public MessageBuilder() {
    }

    public StmtExecute buildCreateCollectionIndex(String schemaName, String collectionName, CreateIndexParams params) {
        // TODO: check for 0-field params?
        Any[] args = new Any[4 + (3 * params.getDocPaths().size())];
        args[0] = ExprUtil.buildAny(schemaName);
        args[1] = ExprUtil.buildAny(collectionName);
        args[2] = ExprUtil.buildAny(params.getIndexName());
        args[3] = ExprUtil.buildAny(params.isUnique());
        int argPos = 4;
        for (int i = 0; i < params.getDocPaths().size(); ++i) {
            args[argPos++] = ExprUtil.buildAny(params.getDocPaths().get(i));
            args[argPos++] = ExprUtil.buildAny(params.getTypes().get(i));
            args[argPos++] = ExprUtil.buildAny(params.getNotNulls().get(i));
        }
        return buildXpluginCommand(XpluginStatementCommand.XPLUGIN_STMT_CREATE_COLLECTION_INDEX, args);
    }

    public StmtExecute buildDropCollectionIndex(String schemaName, String collectionName, String indexName) {
        Any[] args = new Any[3];
        args[0] = ExprUtil.buildAny(schemaName);
        args[1] = ExprUtil.buildAny(collectionName);
        args[2] = ExprUtil.buildAny(indexName);
        return buildXpluginCommand(XpluginStatementCommand.XPLUGIN_STMT_DROP_COLLECTION_INDEX, args);
    }

    /**
     * Build a <i>StmtExecute</i> message for an xplugin command.
     *
     * @param command
     *            the xplugin command to send
     * @param args
     *            the arguments to the command
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
}
