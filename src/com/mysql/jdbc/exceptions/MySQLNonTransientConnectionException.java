/*
 Copyright  2005 MySQL AB, 2008 Sun Microsystems

 This program is free software; you can redistribute it and/or modify
 it under the terms of version 2 of the GNU General Public License as 
 published by the Free Software Foundation.

 There are special exceptions to the terms and conditions of the GPL 
 as it is applied to this software. View the full text of the 
 exception in file EXCEPTIONS-CONNECTOR-J in the directory of this 
 software distribution.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

*/

package com.mysql.jdbc.exceptions;

public class MySQLNonTransientConnectionException extends
		MySQLNonTransientException {

	public MySQLNonTransientConnectionException() {
		super();
	}

	public MySQLNonTransientConnectionException(String reason, String SQLState, int vendorCode) {
		super(reason, SQLState, vendorCode);
	}

	public MySQLNonTransientConnectionException(String reason, String SQLState) {
		super(reason, SQLState);
	}

	public MySQLNonTransientConnectionException(String reason) {
		super(reason);
	}
}
