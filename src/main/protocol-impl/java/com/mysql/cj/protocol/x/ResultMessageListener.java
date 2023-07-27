/*
 * Copyright (c) 2015, 2023, Oracle and/or its affiliates.
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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.google.protobuf.GeneratedMessageV3;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.MessageListener;
import com.mysql.cj.protocol.ProtocolEntity;
import com.mysql.cj.protocol.ProtocolEntityFactory;
import com.mysql.cj.protocol.ResultBuilder;
import com.mysql.cj.x.protobuf.Mysqlx.Error;

/**
 * A {@link MessageListener} to handle result data and propagate it to a {@link ResultBuilder}.
 */
public class ResultMessageListener<R> implements MessageListener<XMessage> {

    private ResultBuilder<?> resultBuilder;
    private CompletableFuture<R> future;

    private Map<Class<? extends GeneratedMessageV3>, ProtocolEntityFactory<? extends ProtocolEntity, XMessage>> messageToProtocolEntityFactory = new HashMap<>();

    public ResultMessageListener(
            Map<Class<? extends GeneratedMessageV3>, ProtocolEntityFactory<? extends ProtocolEntity, XMessage>> messageToProtocolEntityFactory,
            ResultBuilder<R> resultBuilder, CompletableFuture<R> future) {
        this.messageToProtocolEntityFactory = messageToProtocolEntityFactory;
        this.resultBuilder = resultBuilder;
        this.future = future;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean processMessage(XMessage message) {
        Class<? extends GeneratedMessageV3> msgClass = (Class<? extends GeneratedMessageV3>) message.getMessage().getClass();

        if (Error.class.equals(msgClass)) {
            this.future.completeExceptionally(new XProtocolError(Error.class.cast(message.getMessage())));

        } else if (!this.messageToProtocolEntityFactory.containsKey(msgClass)) {
            this.future.completeExceptionally(new WrongArgumentException("Unhandled msg class (" + msgClass + ") + msg=" + message.getMessage()));

        } else {
            if (!this.resultBuilder.addProtocolEntity(this.messageToProtocolEntityFactory.get(msgClass).createFromMessage(message))) {
                return false;
            }
            this.future.complete((R) this.resultBuilder.build());
        }

        return true; /* done reading */
    }

    @Override
    public void error(Throwable ex) {
        this.future.completeExceptionally(ex);
    }

}
