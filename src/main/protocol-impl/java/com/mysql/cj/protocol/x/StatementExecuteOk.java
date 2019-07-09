/*
 * Copyright (c) 2015, 2019, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.protocol.x;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import com.mysql.cj.protocol.ProtocolEntity;
import com.mysql.cj.protocol.Warning;
import com.mysql.cj.xdevapi.Result;
import com.mysql.cj.xdevapi.WarningImpl;

/**
 * ProtocolEntity representing a {@link StatementExecuteOk} message.
 */
public class StatementExecuteOk implements ProtocolEntity, Result {
    private long rowsAffected = 0;
    private Long lastInsertId = null;
    private List<String> generatedIds;
    private List<Warning> warnings;

    public StatementExecuteOk() {
        this.generatedIds = Collections.emptyList();
        this.warnings = new ArrayList<>();
    }

    public StatementExecuteOk(long rowsAffected, Long lastInsertId, List<String> generatedIds, List<Warning> warnings) {
        this.rowsAffected = rowsAffected;
        this.lastInsertId = lastInsertId;
        this.generatedIds = Collections.unmodifiableList(generatedIds);
        this.warnings = warnings; // should NOT be null
    }

    public long getAffectedItemsCount() {
        return this.rowsAffected;
    }

    public Long getLastInsertId() {
        return this.lastInsertId;
    }

    public List<String> getGeneratedIds() {
        return this.generatedIds;
    }

    @Override
    public int getWarningsCount() {
        return this.warnings.size();
    }

    @Override
    public Iterator<com.mysql.cj.xdevapi.Warning> getWarnings() {
        return this.warnings.stream().map(w -> (com.mysql.cj.xdevapi.Warning) new WarningImpl(w)).collect(Collectors.toList()).iterator();
    }

}
