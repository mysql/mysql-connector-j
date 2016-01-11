/*
  Copyright (c) 2013, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.fabric.xmlrpc.exceptions;

import java.sql.SQLException;

import com.mysql.fabric.xmlrpc.base.Fault;
import com.mysql.fabric.xmlrpc.base.Struct;

public class MySQLFabricException extends SQLException {

    static final long serialVersionUID = -8776763137552613517L;

    public MySQLFabricException() {
        super();
    }

    public MySQLFabricException(Fault fault) {
        super((String) ((Struct) fault.getValue().getValue()).getMember().get(1).getValue().getValue(), "",
                (Integer) ((Struct) fault.getValue().getValue()).getMember().get(0).getValue().getValue());
    }

    public MySQLFabricException(String reason, String SQLState, int vendorCode) {
        super(reason, SQLState, vendorCode);
    }

    public MySQLFabricException(String reason, String SQLState) {
        super(reason, SQLState);
    }

    public MySQLFabricException(String reason) {
        super(reason);
    }
}
