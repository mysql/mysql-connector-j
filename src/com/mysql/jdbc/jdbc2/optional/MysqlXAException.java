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

package com.mysql.jdbc.jdbc2.optional;

import javax.transaction.xa.XAException;

/**
 * The stock XAException class isn't too friendly (i.e. no
 * error messages), so we extend it a bit.
 */
class MysqlXAException extends XAException {
	private static final long serialVersionUID = -9075817535836563004L;
	
	private String message;
	private String xidAsString;
	
	public MysqlXAException(int errorCode, String message, String xidAsString) {
		super(errorCode);
		this.message = message;
		this.xidAsString = xidAsString;
	}
	
	public MysqlXAException(String message, String xidAsString) {
		super();
		
		this.message = message;
		this.xidAsString = xidAsString;
	}

	public String getMessage() {
		String superMessage = super.getMessage();
		StringBuffer returnedMessage = new StringBuffer();
		
		if (superMessage != null) {
			returnedMessage.append(superMessage);
			returnedMessage.append(":");
		}
		
		if (this.message != null) {
			returnedMessage.append(this.message);
		}
		
		return returnedMessage.toString();
	}
}