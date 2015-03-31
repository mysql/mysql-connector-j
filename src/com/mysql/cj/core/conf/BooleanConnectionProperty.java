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

import java.io.Serializable;

import com.mysql.cj.api.ExceptionInterceptor;

public class BooleanConnectionProperty extends ConnectionProperty implements Serializable {

    private static final long serialVersionUID = 2816568198432199863L;

    /**
     * @param propertyNameToSet
     * @param defaultValueToSet
     * @param descriptionToSet
     * @param sinceVersionToSet
     */
    public BooleanConnectionProperty(String propertyNameToSet, boolean defaultValueToSet, String descriptionToSet, String sinceVersionToSet, String category,
            int orderInCategory) {
        super(propertyNameToSet, Boolean.valueOf(defaultValueToSet), null, 0, 0, descriptionToSet, sinceVersionToSet, category, orderInCategory);
    }

    @Override
    public String[] getAllowableValues() {
        return new String[] { "true", "false", "yes", "no" };
    }

    public boolean getValueAsBoolean() {
        return ((Boolean) this.valueAsObject).booleanValue();
    }

    @Override
    protected boolean hasValueConstraints() {
        return true;
    }

    @Override
    public void initializeFrom(String extractedValue, ExceptionInterceptor exceptionInterceptor) {
        if (extractedValue != null) {
            validateStringValues(extractedValue, exceptionInterceptor);

            this.valueAsObject = Boolean.valueOf(extractedValue.equalsIgnoreCase("TRUE") || extractedValue.equalsIgnoreCase("YES"));
        } else {
            this.valueAsObject = this.defaultValue;
        }
        this.updateCount++;
    }

    @Override
    protected boolean isRangeBased() {
        return false;
    }

    public void setValue(boolean valueFlag) {
        this.valueAsObject = Boolean.valueOf(valueFlag);
        this.updateCount++;
    }
}
