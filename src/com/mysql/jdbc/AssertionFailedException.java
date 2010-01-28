/*
 Copyright  2002-2004 MySQL AB, 2008 Sun Microsystems
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
package com.mysql.jdbc;

/**
 * Assertions for empty code paths that should never be executed.
 * 
 * @author Mark Matthews
 * 
 * @version $Id: AssertionFailedException.java,v 1.1.2.1 2005/05/13 18:58:37
 *          mmatthews Exp $
 */
public class AssertionFailedException extends RuntimeException {
	// ~ Constructors
	// -----------------------------------------------------------

	/**
	 * Convenience method.
	 * 
	 * @param ex
	 *            the exception that should never have been thrown.
	 * @throws AssertionFailedException
	 *             for the exception ex.
	 */
	public static void shouldNotHappen(Exception ex)
			throws AssertionFailedException {
		throw new AssertionFailedException(ex);
	}

	// ~ Methods
	// ----------------------------------------------------------------

	/**
	 * Creates an AssertionFailedException for the given exception that should
	 * never have been thrown.
	 * 
	 * @param ex
	 *            the exception that should never have been thrown.
	 */
	public AssertionFailedException(Exception ex) {
		super(Messages.getString("AssertionFailedException.0") + ex.toString() //$NON-NLS-1$
				+ Messages.getString("AssertionFailedException.1")); //$NON-NLS-1$
	}
}
