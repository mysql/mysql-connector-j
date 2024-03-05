/*
 * Copyright (c) 2020, 2024, Oracle and/or its affiliates.
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

package com.mysql.cj.callback;

/**
 * A functional interface for implementing callback handlers.
 *
 * The single method {@link MysqlCallbackHandler#handle(MysqlCallback)} is called by the classes where the callback handler is passed when they need to share
 * data with the caller. The kind of data to exchange depends on the implementations of {@link MysqlCallback} they choose to use.
 */
@FunctionalInterface
public interface MysqlCallbackHandler {

    /**
     * Exchanges information between the caller of this method and the owner of the {@link MysqlCallbackHandler} instance. The method can be called multiple
     * times with different callback implementations to exchange different types of information. A typical {@link MysqlCallbackHandler} implementation looks
     * like:
     *
     * <pre>
     *
     * private MysqlCallbackHandler callbackHandler = (cb) -&gt; {
     *     if (cb instanceof UsernameCallback) {
     *         this.username = ((UsernameCallback) cb).getUsername();
     *     }
     * };
     * </pre>
     *
     * @param cb
     *            the {@link MysqlCallback} to process
     */
    void handle(MysqlCallback cb);

}
