/*
  Copyright (c) 2002, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.core.exceptions;

import com.mysql.cj.core.Messages;

/**
 * Assertions for empty code paths that should never be executed.
 */
public class AssertionFailedException extends CJException {

    private static final long serialVersionUID = 5832552608575043403L;

    /**
     * Convenience method.
     * 
     * @param ex
     *            the exception that should never have been thrown.
     * @throws AssertionFailedException
     *             for the exception ex.
     */
    public static AssertionFailedException shouldNotHappen(Exception ex) throws AssertionFailedException {
        throw new AssertionFailedException(ex);
    }

    /**
     * Create (and caller should subsequently throw) an <code>AssertionFailedException</code>.
     *
     * <P>
     * Typical use is as follows:
     * 
     * <PRE>
     * if (something == null) {
     *     throw AssertionFailedException.shouldNotHappen("Something cannot be null");
     * }
     * </PRE>
     *
     * @return the exception. exception should be thrown by the caller to satisfy compiler checks for data-flow, etc
     */
    public static AssertionFailedException shouldNotHappen(String assertion) throws AssertionFailedException {
        return new AssertionFailedException(assertion);
    }

    /**
     * Creates an AssertionFailedException for the given exception that should
     * never have been thrown.
     * 
     * @param ex
     *            the exception that should never have been thrown.
     */
    public AssertionFailedException(Exception ex) {
        super(Messages.getString("AssertionFailedException.0") + ex.toString() + Messages.getString("AssertionFailedException.1"), ex);
    }

    /**
     * Creates an AssertionFailedException for the reason given.
     * 
     * @param assertion
     *            a description of the assertion that failed
     */
    public AssertionFailedException(String assertion) {
        super(Messages.getString("AssertionFailedException.2", new Object[] { assertion }));
    }
}
