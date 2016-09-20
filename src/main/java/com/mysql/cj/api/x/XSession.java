/*
  Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.api.x;

/**
 * X DevAPI introduces a new, high-level database session concept that is called XSession. When working with X DevAPI it is important to understand this new
 * XSession concept which is different from working with traditional low-level MySQL connections.
 * 
 * An application using the XSession class can be run against a single MySQL server or large number of MySQL servers forming a sharding cluster with no code
 * changes. When a low-level MySQL connection to a single MySQL instance is needed this is still supported by using a {@link NodeSession}.
 */
public interface XSession extends BaseSession {

    /**
     * Create a "virtual" {@link NodeSession} instance, the one which shares the connection to the router with the XSession used to create it.
     * <p>
     * The lifespan of a virtual NodeSession is bound to the lifespan of the XSession that created it.
     * A virtual NodeSession offers the same functionality as a normal NodeSession.
     * <p>
     * XSession.close() will also close any virtual NodeSession instance bound to this XSession.
     * A virtual NodeSession.close() closes only the NodeSession, the parent XSession remains valid.
     * <p>
     * Sharing the connection to the router also means that any pending result of virtual NodeSession is flushed before starting a new XSession command
     * and vice versa.
     */
    NodeSession bindToDefaultShard();
}
