/*
   Copyright (C) 2002 MySQL AB
   
      This program is free software; you can redistribute it and/or modify
      it under the terms of the GNU General Public License as published by
      the Free Software Foundation; either version 2 of the License, or
      (at your option) any later version.
   
      This program is distributed in the hope that it will be useful,
      but WITHOUT ANY WARRANTY; without even the implied warranty of
      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
      GNU General Public License for more details.
   
      You should have received a copy of the GNU General Public License
      along with this program; if not, write to the Free Software
      Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
      
 */
 
package com.mysql.jdbc;

import java.io.CharArrayWriter;

/**
 * A java.io.Writer used to write unicode data into
 * Blobs and Clobs
 * 
 * @author Mark Matthews
 */
class WatchableWriter extends CharArrayWriter {

    private WriterWatcher watcher;
	
	/**
	 * @see java.io.Writer#close()
	 */
	public void close() {
        super.close();
        
        // Send data to watcher
        
        if (this.watcher != null) {
            this.watcher.writerClosed(toCharArray());
        }
	}
    
    public void setWatcher(WriterWatcher watcher) {
        this.watcher = watcher;
    }

}
