/*
 Copyright  2002-2004 MySQL AB, 2008 Sun Microsystems

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
package com.mysql.jdbc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * A java.io.OutputStream used to write ASCII data into Blobs and Clobs
 * 
 * @author Mark Matthews
 */
class WatchableOutputStream extends ByteArrayOutputStream {
	// ~ Instance fields
	// --------------------------------------------------------

	private OutputStreamWatcher watcher;

	// ~ Methods
	// ----------------------------------------------------------------

	/**
	 * @see java.io.OutputStream#close()
	 */
	public void close() throws IOException {
		super.close();

		if (this.watcher != null) {
			this.watcher.streamClosed(this);
		}
	}

	/**
	 * DOCUMENT ME!
	 * 
	 * @param watcher
	 *            DOCUMENT ME!
	 */
	public void setWatcher(OutputStreamWatcher watcher) {
		this.watcher = watcher;
	}
}
