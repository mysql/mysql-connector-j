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

import java.io.IOException;

import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.protocol.ProtocolEntityFactory;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.protocol.a.NativePacketPayload;
import com.mysql.cj.util.TestUtils;

//TODO should not be protocol-specific

public class ServerPreparedQueryTestcaseGenerator extends ServerPreparedQuery {

    public ServerPreparedQueryTestcaseGenerator(NativeSession sess) {
        super(sess);
    }

    @Override
    public void closeQuery() {
        dumpCloseForTestcase();
        super.closeQuery();
    }

    private void dumpCloseForTestcase() {
        StringBuilder buf = new StringBuilder();
        this.session.getProtocol().generateQueryCommentBlock(buf);
        buf.append("DEALLOCATE PREPARE debug_stmt_");
        buf.append(this.statementId);
        buf.append(";\n");

        TestUtils.dumpTestcaseQuery(buf.toString());
    }

    @Override
    public void serverPrepare(String sql) throws IOException {
        dumpPrepareForTestcase();
        super.serverPrepare(sql);
    }

    private void dumpPrepareForTestcase() {
        StringBuilder buf = new StringBuilder(this.getOriginalSql().length() + 64);

        this.session.getProtocol().generateQueryCommentBlock(buf);

        buf.append("PREPARE debug_stmt_");
        buf.append(this.statementId);
        buf.append(" FROM \"");
        buf.append(this.getOriginalSql());
        buf.append("\";\n");

        TestUtils.dumpTestcaseQuery(buf.toString());
    }

    @Override
    public <T extends Resultset> T serverExecute(int maxRowsToRetrieve, boolean createStreamingResultSet, ColumnDefinition metadata,
            ProtocolEntityFactory<T, NativePacketPayload> resultSetFactory) {
        dumpExecuteForTestcase();
        return super.serverExecute(maxRowsToRetrieve, createStreamingResultSet, metadata, resultSetFactory);
    }

    private void dumpExecuteForTestcase() {
        StringBuilder buf = new StringBuilder();

        for (int i = 0; i < this.getParameterCount(); i++) {
            this.session.getProtocol().generateQueryCommentBlock(buf);

            buf.append("SET @debug_stmt_param");
            buf.append(this.statementId);
            buf.append("_");
            buf.append(i);
            buf.append("=");

            BindValue bv = this.queryBindings.getBindValues()[i];
            buf.append(bv.isNull() ? "NULL" : bv.getString());

            buf.append(";\n");
        }

        this.session.getProtocol().generateQueryCommentBlock(buf);

        buf.append("EXECUTE debug_stmt_");
        buf.append(this.statementId);

        if (this.getParameterCount() > 0) {
            buf.append(" USING ");
            for (int i = 0; i < this.getParameterCount(); i++) {
                if (i > 0) {
                    buf.append(", ");
                }

                buf.append("@debug_stmt_param");
                buf.append(this.statementId);
                buf.append("_");
                buf.append(i);

            }
        }

        buf.append(";\n");

        TestUtils.dumpTestcaseQuery(buf.toString());
    }

}
