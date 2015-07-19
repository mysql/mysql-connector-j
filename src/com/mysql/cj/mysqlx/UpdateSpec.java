package com.mysql.cj.mysqlx;

import com.mysql.cj.mysqlx.protobuf.MysqlxCrud.UpdateOperation;
import com.mysql.cj.mysqlx.protobuf.MysqlxExpr.ColumnIdentifier;
import com.mysql.cj.mysqlx.protobuf.MysqlxExpr.Expr;

public class UpdateSpec {
    public static enum UpdateType {
        // these must mirror exactly the names of UpdateOperation.UpdateType in mysqlx_crud.proto
        ITEM_REMOVE, ITEM_SET, ITEM_REPLACE, ITEM_MERGE,
        ARRAY_INSERT, ARRAY_APPEND
    }

    private UpdateOperation.UpdateType updateType;
    private ColumnIdentifier source;
    private Expr value;

    public UpdateSpec(UpdateType updateType, String source) {
        this.updateType = UpdateOperation.UpdateType.valueOf(updateType.name());
        this.source = ColumnIdentifier.newBuilder().addAllDocumentPath(new ExprParser(source).documentPath()).build();
    }

    public Object getUpdateType() {
        return this.updateType;
    }

    public Object getSource() {
        return this.source;
    }

    public UpdateSpec setValue(Object value) {
        // TODO: this needs to be a JSON string so we need to escape & quote it properly. Alfredo says this is going to be changed so we can distinguish between
        // JSON docs and strings
        if (value != null && value.getClass() == String.class) {
            value = "\"" + value + "\"";
        }
        this.value = ExprUtil.buildLiteralExpr(ExprUtil.argObjectToAny(value));
        return this;
    }

    public Object getValue() {
        return this.value;
    }
}
