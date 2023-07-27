/*
 * Copyright (c) 2015, 2023, Oracle and/or its affiliates.
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

package com.mysql.cj.conf;

import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.WrongArgumentException;

public class IntegerProperty extends AbstractRuntimeProperty<Integer> {

    private static final long serialVersionUID = 9208223182595760858L;

    public IntegerProperty(PropertyDefinition<Integer> propertyDefinition) {
        super(propertyDefinition);
    }

    @Override
    protected void checkRange(Integer val, String valueAsString, ExceptionInterceptor exceptionInterceptor) {
        if (val.intValue() < getPropertyDefinition().getLowerBound() || val.intValue() > getPropertyDefinition().getUpperBound()) {
            throw ExceptionFactory.createException(WrongArgumentException.class,
                    "The connection property '" + getPropertyDefinition().getName() + "' only accepts integer values in the range of "
                            + getPropertyDefinition().getLowerBound() + " - " + getPropertyDefinition().getUpperBound() + ", the value '"
                            + (valueAsString == null ? val.intValue() : valueAsString) + "' exceeds this range.",
                    exceptionInterceptor);
        }
    }

}
