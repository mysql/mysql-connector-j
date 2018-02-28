/*
 * Copyright (c) 2015, 2018, Oracle and/or its affiliates. All rights reserved.
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

public class IntegerPropertyDefinition extends AbstractPropertyDefinition<Integer> {

    private static final long serialVersionUID = 4151893695173946081L;

    protected int multiplier = 1;

    public IntegerPropertyDefinition(String name, String alias, int defaultValue, boolean isRuntimeModifiable, String description, String sinceVersion,
            String category, int orderInCategory) {
        super(name, alias, Integer.valueOf(defaultValue), isRuntimeModifiable, description, sinceVersion, category, orderInCategory);
    }

    public IntegerPropertyDefinition(String name, String alias, int defaultValue, boolean isRuntimeModifiable, String description, String sinceVersion,
            String category, int orderInCategory, int lowerBound, int upperBound) {
        super(name, alias, Integer.valueOf(defaultValue), isRuntimeModifiable, description, sinceVersion, category, orderInCategory, lowerBound, upperBound);
    }

    @Override
    public boolean isRangeBased() {
        return getUpperBound() != getLowerBound();
    }

    @Override
    public Integer parseObject(String value, ExceptionInterceptor exceptionInterceptor) {
        try {
            // Parse decimals, too
            int intValue = (int) (Double.valueOf(value).doubleValue() * this.multiplier);

            return intValue;

        } catch (NumberFormatException nfe) {
            throw ExceptionFactory.createException(WrongArgumentException.class,
                    "The connection property '" + getName() + "' only accepts integer values. The value '" + value + "' can not be converted to an integer.",
                    exceptionInterceptor);
        }
    }

    /**
     * Creates instance of ReadableIntegerProperty or ModifiableIntegerProperty depending on isRuntimeModifiable() result.
     * 
     * @return
     */
    @Override
    public RuntimeProperty<Integer> createRuntimeProperty() {
        return isRuntimeModifiable() ? new ModifiableIntegerProperty(this) : new ReadableIntegerProperty(this);
    }

}
