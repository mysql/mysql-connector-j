/*
  Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.core.conf;

import com.mysql.cj.api.conf.RuntimeProperty;
import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.exceptions.WrongArgumentException;

public class IntegerPropertyDefinition extends AbstractPropertyDefinition<Integer> {

    private static final long serialVersionUID = 4151893695173946081L;

    protected int multiplier = 1;

    public IntegerPropertyDefinition(String name, int defaultValue, boolean isRuntimeModifiable, String description, String sinceVersion, String category,
            int orderInCategory) {
        super(name, Integer.valueOf(defaultValue), isRuntimeModifiable, description, sinceVersion, category, orderInCategory);
    }

    public IntegerPropertyDefinition(String name, int defaultValue, boolean isRuntimeModifiable, String description, String sinceVersion, String category,
            int orderInCategory, int lowerBound, int upperBound) {
        super(name, Integer.valueOf(defaultValue), isRuntimeModifiable, description, sinceVersion, category, orderInCategory, lowerBound, upperBound);
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
