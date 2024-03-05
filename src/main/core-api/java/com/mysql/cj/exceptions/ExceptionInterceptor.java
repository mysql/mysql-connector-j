/*
 * Copyright (c) 2008, 2024, Oracle and/or its affiliates.
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

package com.mysql.cj.exceptions;

import java.util.Properties;

import com.mysql.cj.log.Log;

public interface ExceptionInterceptor {

    /**
     * Called once per connection that wants to use the extension
     *
     * The properties are the same ones passed in in the URL or arguments to
     * Driver.connect() or DriverManager.getConnection().
     *
     * @param props
     *            configuration values as passed to the connection. Note that
     *            in order to support javax.sql.DataSources, configuration properties specific
     *            to an interceptor <strong>must</strong> be passed via setURL() on the
     *            DataSource. Extension properties are not exposed via
     *            accessor/mutator methods on DataSources.
     * @param log
     *            logger instance
     * @return {@link ExceptionInterceptor}
     */

    ExceptionInterceptor init(Properties props, Log log);

    /**
     * Called by the driver when this extension should release any resources
     * it is holding and cleanup internally before the connection is
     * closed.
     */
    void destroy();

    Exception interceptException(Exception sqlEx);

}
