/*
 Copyright  2007 MySQL AB, 2008 Sun Microsystems

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

import java.sql.SQLException;
import java.util.Iterator;

import com.mysql.jdbc.DatabaseMetaData.IteratorWithCleanup;

public abstract class IterateBlock {
	IteratorWithCleanup iteratorWithCleanup;
	Iterator javaIterator;
	boolean stopIterating = false;
	
	IterateBlock(IteratorWithCleanup i) {
		this.iteratorWithCleanup = i;
		this.javaIterator = null;
	}
	
	IterateBlock(Iterator i) {
		this.javaIterator = i;
		this.iteratorWithCleanup = null;
	}

	public void doForAll() throws SQLException {
		if (this.iteratorWithCleanup != null) {
			try {
				while (this.iteratorWithCleanup.hasNext()) {
					forEach(this.iteratorWithCleanup.next());
					
					if (this.stopIterating) {
						break;
					}
				}
			} finally {
				this.iteratorWithCleanup.close();
			}
		} else {
			while (this.javaIterator.hasNext()) {
				forEach(this.javaIterator.next());
				
				if (this.stopIterating) {
					break;
				}
			}
		}
	}

	abstract void forEach(Object each) throws SQLException;
	
	public final boolean fullIteration() {
		return !this.stopIterating;
	}
}