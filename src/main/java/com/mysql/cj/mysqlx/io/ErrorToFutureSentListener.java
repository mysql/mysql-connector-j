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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.MessageLite;

import com.mysql.cj.core.exceptions.CJCommunicationsException;
import com.mysql.cj.core.exceptions.CJPacketTooBigException;
import com.mysql.cj.mysqlx.io.AsyncMessageWriter.SentListener;
import com.mysql.cj.mysqlx.protobuf.Mysqlx.ClientMessages;

/**
 * Base class that propagates any error to the given future allowing only implementation of the success callback.
 */
public abstract class ErrorToFutureSentListener implements SentListener {
    private CompletableFuture<?> future;

    public ErrorToFutureSentListener(CompletableFuture<?> future) {
        this.future = future;
    }

    public void error(Throwable ex) {
        this.future.completeExceptionally(ex);
    }
}
