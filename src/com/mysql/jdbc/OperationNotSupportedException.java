/*
 * Created on Sep 23, 2004
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package com.mysql.jdbc;

import java.sql.SQLException;

class OperationNotSupportedException extends SQLException {
	OperationNotSupportedException() {
		super(
				Messages.getString("RowDataDynamic.10"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT); //$NON-NLS-1$
	}
}