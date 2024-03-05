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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.ProtocolEntity;
import com.mysql.cj.protocol.ResultBuilder;
import com.mysql.cj.protocol.x.Notice.XSessionStateChanged;
import com.mysql.cj.protocol.x.Notice.XWarning;

/**
 * Result builder producing a {@link StatementExecuteOk} instance. Handles state necessary to accumulate noticed and build a {@link StatementExecuteOk}
 * response.
 */
public class StatementExecuteOkBuilder implements ResultBuilder<StatementExecuteOk> {

    private long rowsAffected = 0;
    private Long lastInsertId = null;
    private List<String> generatedIds = Collections.emptyList();
    private List<com.mysql.cj.protocol.Warning> warnings = new ArrayList<>();

    @Override
    public boolean addProtocolEntity(ProtocolEntity entity) {
        if (entity instanceof Notice) {
            addNotice((Notice) entity);
            return false;

        } else if (entity instanceof FetchDoneEntity) {
            return false;

        } else if (entity instanceof StatementExecuteOk) {
            return true;
        }
        throw ExceptionFactory.createException(WrongArgumentException.class, "Unexpected protocol entity " + entity);
    }

    @Override
    public StatementExecuteOk build() {
        return new StatementExecuteOk(this.rowsAffected, this.lastInsertId, this.generatedIds, this.warnings);
    }

    private void addNotice(Notice notice) {
        if (notice instanceof XWarning) {
            this.warnings.add((XWarning) notice);

        } else if (notice instanceof XSessionStateChanged) {
            switch (((XSessionStateChanged) notice).getParamType()) {
                case Notice.SessionStateChanged_GENERATED_INSERT_ID:
                    this.lastInsertId = ((XSessionStateChanged) notice).getValue().getVUnsignedInt(); // TODO: handle > 2^63-1?
                    break;
                case Notice.SessionStateChanged_ROWS_AFFECTED:
                    this.rowsAffected = ((XSessionStateChanged) notice).getValue().getVUnsignedInt(); // TODO: handle > 2^63-1?
                    break;
                case Notice.SessionStateChanged_GENERATED_DOCUMENT_IDS:
                    this.generatedIds = ((XSessionStateChanged) notice).getValueList().stream().map(v -> v.getVOctets().getValue().toStringUtf8())
                            .collect(Collectors.toList());
                    break;
                case Notice.SessionStateChanged_PRODUCED_MESSAGE:
                case Notice.SessionStateChanged_CURRENT_SCHEMA:
                case Notice.SessionStateChanged_ACCOUNT_EXPIRED:
                case Notice.SessionStateChanged_ROWS_FOUND:
                case Notice.SessionStateChanged_ROWS_MATCHED:
                case Notice.SessionStateChanged_TRX_COMMITTED:
                case Notice.SessionStateChanged_TRX_ROLLEDBACK:
                case Notice.SessionStateChanged_CLIENT_ID_ASSIGNED:
                default:
                    // TODO do something with notices, expose them to client
            }
        }
    }

}
