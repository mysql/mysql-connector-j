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

package com.mysql.cj.mysqlx.devapi;

import java.util.Properties;

import com.mysql.cj.api.x.NodeSession;
import com.mysql.cj.api.x.XSession;
import com.mysql.cj.core.exceptions.ConnectionIsClosedException;

public class SessionImpl extends AbstractSession implements XSession {

    public SessionImpl(Properties properties) {
        super(properties);
    }

    @Override
    public NodeSession bindToDefaultShard() {
        // Throws RuntimeError if the XSession is not connected to a farm or the connection to a MySQLd node of the shard fails
        if (!isOpen()) {
            throw new ConnectionIsClosedException("Can't bind NodeSession to closed XSession.");
        }

        // TODO add binding to the _shard_ when available in X Protocol

        return new VirtualNodeSession(this);
    }
}
