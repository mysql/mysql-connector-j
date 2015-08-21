/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

public class LongPropertyDefinition extends AbstractPropertyDefinition<Long> {

    private static final long serialVersionUID = -5264490959206230852L;

    public LongPropertyDefinition(String name, long defaultValue, boolean isRuntimeModifiable, String description, String sinceVersion, String category,
            int orderInCategory) {
        super(name, Long.valueOf(defaultValue), isRuntimeModifiable, description, sinceVersion, category, orderInCategory);
    }

    public LongPropertyDefinition(String name, long defaultValue, boolean isRuntimeModifiable, String description, String sinceVersion, String category,
            int orderInCategory, long lowerBound, long upperBound) {
        super(name, Long.valueOf(defaultValue), isRuntimeModifiable, description, sinceVersion, category, orderInCategory, (int) lowerBound, (int) upperBound);
    }

    @Override
    public Long parseObject(String value, ExceptionInterceptor exceptionInterceptor) {
        try {
            // Parse decimals, too
            return Double.valueOf(value).longValue();

        } catch (NumberFormatException nfe) {
            throw ExceptionFactory.createException(WrongArgumentException.class, "The connection property '" + getName()
                    + "' only accepts long integer values. The value '" + value + "' can not be converted to a long integer.", exceptionInterceptor);
        }
    }

    @Override
    public boolean isRangeBased() {
        return getUpperBound() != getLowerBound();
    }

    /**
     * Creates instance of ReadableLongProperty or ModifiableLongProperty depending on isRuntimeModifiable() result.
     * 
     * @return
     */
    @Override
    public RuntimeProperty<Long> createRuntimeProperty() {
        return isRuntimeModifiable() ? new ModifiableLongProperty(this) : new ReadableLongProperty(this);
    }

}
