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

public class StringConnectionProperty extends ConnectionProperty implements Serializable {

    private static final long serialVersionUID = -4622859572601878754L;

    public StringConnectionProperty(String propertyNameToSet, String defaultValueToSet, String descriptionToSet, String sinceVersionToSet, String category,
            int orderInCategory) {
        this(propertyNameToSet, defaultValueToSet, null, descriptionToSet, sinceVersionToSet, category, orderInCategory);
    }

    /**
     * @param propertyNameToSet
     * @param defaultValueToSet
     * @param allowableValuesToSet
     * @param descriptionToSet
     * @param sinceVersionToSet
     */
    public StringConnectionProperty(String propertyNameToSet, String defaultValueToSet, String[] allowableValuesToSet, String descriptionToSet,
            String sinceVersionToSet, String category, int orderInCategory) {
        super(propertyNameToSet, defaultValueToSet, allowableValuesToSet, 0, 0, descriptionToSet, sinceVersionToSet, category, orderInCategory);
    }

    public String getValueAsString() {
        return (String) this.valueAsObject;
    }

    @Override
    protected boolean hasValueConstraints() {
        return (this.allowableValues != null) && (this.allowableValues.length > 0);
    }

    @Override
    protected void initializeFrom(String extractedValue, ExceptionInterceptor exceptionInterceptor) {
        if (extractedValue != null) {
            validateStringValues(extractedValue, exceptionInterceptor);

            this.valueAsObject = extractedValue;
        } else {
            this.valueAsObject = this.defaultValue;
        }
        this.updateCount++;
    }

    @Override
    protected boolean isRangeBased() {
        return false;
    }

    public void setValue(String valueFlag) {
        this.valueAsObject = valueFlag;
        this.updateCount++;
    }

}
