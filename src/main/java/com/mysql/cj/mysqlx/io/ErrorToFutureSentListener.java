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

import java.util.concurrent.CompletableFuture;

import com.mysql.cj.mysqlx.io.AsyncMessageWriter.SentListener;

/**
 * Base class that propagates any error to the given future allowing only implementation of the success callback.
 */
public class ErrorToFutureSentListener implements SentListener {
    private CompletableFuture<?> future;
    private SentListener successCallback;

    public ErrorToFutureSentListener(CompletableFuture<?> future, SentListener successCallback) {
        this.future = future;
        this.successCallback = successCallback;
    }

    public void completed() {
        this.successCallback.completed();
    }

    public void error(Throwable ex) {
        this.future.completeExceptionally(ex);
    }
}
