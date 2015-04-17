/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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

import com.mysql.cj.api.exception.ExceptionInterceptor;
import com.mysql.cj.core.exception.ExceptionFactory;
import com.mysql.cj.core.exception.WrongArgumentException;

public class LongConnectionProperty extends IntegerConnectionProperty {

    private static final long serialVersionUID = 2564576071949370871L;

    LongConnectionProperty(String propertyNameToSet, long defaultValueToSet, long lowerBoundToSet, long upperBoundToSet, String descriptionToSet,
            String sinceVersionToSet, String category, int orderInCategory) {
        super(propertyNameToSet, Long.valueOf(defaultValueToSet), null, (int) lowerBoundToSet, (int) upperBoundToSet, descriptionToSet, sinceVersionToSet,
                category, orderInCategory);
    }

    public LongConnectionProperty(String propertyNameToSet, long defaultValueToSet, String descriptionToSet, String sinceVersionToSet, String category,
            int orderInCategory) {
        this(propertyNameToSet, defaultValueToSet, 0, 0, descriptionToSet, sinceVersionToSet, category, orderInCategory);
    }

    public void setValue(long longValue, ExceptionInterceptor exceptionInterceptor) throws Exception {
        setValue(longValue, null, exceptionInterceptor);
    }

    void setValue(long longValue, String valueAsString, ExceptionInterceptor exceptionInterceptor) {
        if (isRangeBased()) {
            if ((longValue < getLowerBound()) || (longValue > getUpperBound())) {
                throw ExceptionFactory.createException(WrongArgumentException.class, "The connection property '" + getPropertyName()
                        + "' only accepts long integer values in the range of " + getLowerBound() + " - " + getUpperBound() + ", the value '"
                        + (valueAsString == null ? longValue : valueAsString) + "' exceeds this range.", exceptionInterceptor);
            }
        }
        this.valueAsObject = Long.valueOf(longValue);
        this.updateCount++;
    }

    public long getValueAsLong() {
        return ((Long) this.valueAsObject).longValue();
    }

    @Override
    protected void initializeFrom(String extractedValue, ExceptionInterceptor exceptionInterceptor) {
        if (extractedValue != null) {
            try {
                // Parse decimals, too
                long longValue = Double.valueOf(extractedValue).longValue();

                setValue(longValue, extractedValue, exceptionInterceptor);
            } catch (NumberFormatException nfe) {
                throw ExceptionFactory.createException(WrongArgumentException.class, "The connection property '" + getPropertyName()
                        + "' only accepts long integer values. The value '" + extractedValue + "' can not be converted to a long integer.",
                        exceptionInterceptor);
            }
        } else {
            this.valueAsObject = this.defaultValue;
        }
        this.updateCount++;
    }

}
