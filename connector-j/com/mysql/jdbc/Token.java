/*
 * MM JDBC Drivers for MySQL
 *
 * $Id: Token.java,v 1.2 2002/04/21 03:03:47 mark_matthews Exp $
 *
 * Copyright (C) 1998 Mark Matthews <mmatthew@worldserver.com>
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 * 
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the
 * Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA  02111-1307, USA.
 *
 * See the COPYING file located in the top-level-directory of
 * the archive of this library for complete text of license.
 */

package com.mysql.jdbc;

/**
 * Token used in the EscapeProccesor.doConcat() method
 */

class Token {
	boolean quoted = false;
	String Value = "";

	public Token(String Value, boolean quoted) {
		this.Value = Value;
		this.quoted = quoted;
	}
}
