/*
 * Copyright (c) 2002, 2018, Oracle and/or its affiliates. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.mysql.cj.exceptions;

import com.mysql.cj.Messages;

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
     * @return {@link AssertionFailedException}
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
     * @param assertion
     *            message
     * @return the exception. exception should be thrown by the caller to satisfy compiler checks for data-flow, etc
     * @throws AssertionFailedException
     *             if exception occurs
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
