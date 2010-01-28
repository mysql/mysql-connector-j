/*
 Copyright  2005 MySQL AB, 2008 Sun Microsystems
 All rights reserved. Use is subject to license terms.

  The MySQL Connector/J is licensed under the terms of the GPL,
  like most MySQL Connectors. There are special exceptions to the
  terms and conditions of the GPL as it is applied to this software,
  see the FLOSS License Exception available on mysql.com.

  This program is free software; you can redistribute it and/or
  modify it under the terms of the GNU General Public License as
  published by the Free Software Foundation; version 2 of the
  License.

  This program is distributed in the hope that it will be useful,  
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  02110-1301 USA

*/

package com.mysql.jdbc.exceptions;

public class MySQLSyntaxErrorException extends MySQLNonTransientException {

	public MySQLSyntaxErrorException() {
		super();
	}

	public MySQLSyntaxErrorException(String reason, String SQLState, int vendorCode) {
		super(reason, SQLState, vendorCode);
	}

	public MySQLSyntaxErrorException(String reason, String SQLState) {
		super(reason, SQLState);
	}

	public MySQLSyntaxErrorException(String reason) {
		super(reason);
	}
}
