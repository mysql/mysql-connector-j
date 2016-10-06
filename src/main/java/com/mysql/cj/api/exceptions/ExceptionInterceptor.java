/*
  Copyright (c) 2008, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.api.exceptions;

import java.util.Properties;

import com.mysql.cj.api.log.Log;

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
