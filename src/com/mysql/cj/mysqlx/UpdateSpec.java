package com.mysql.cj.mysqlx;

import com.mysql.cj.core.exceptions.WrongArgumentException;
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
        // accomodate parser's documentPath() handling by removing "@"
        if (source.length() > 0 && source.charAt(0) == '@') {
            source = source.substring(1);
        } else {
            throw new WrongArgumentException("Update source should be a document path starting with `@'");
        }
        this.source = ColumnIdentifier.newBuilder().addAllDocumentPath(new ExprParser(source).documentPath()).build();
    }

    public Object getUpdateType() {
        return this.updateType;
    }

    public Object getSource() {
        return this.source;
    }

    public UpdateSpec setValue(Object value) {
        this.value = ExprUtil.argObjectToExpr(value);
        return this;
    }

    public Object getValue() {
        return this.value;
    }
}
