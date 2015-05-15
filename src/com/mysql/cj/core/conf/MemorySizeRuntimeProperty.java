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

import com.mysql.cj.api.conf.MemorySizeModifiableProperty;
import com.mysql.cj.api.conf.PropertyDefinition;
import com.mysql.cj.api.exception.ExceptionInterceptor;

public class MemorySizeRuntimeProperty extends IntegerRuntimeProperty implements MemorySizeModifiableProperty, Serializable {

    private static final long serialVersionUID = -8166011277756228978L;

    private String valueAsString;

    public MemorySizeRuntimeProperty(String propertyNameToSet) {
        super(propertyNameToSet);
    }

    protected MemorySizeRuntimeProperty(PropertyDefinition propertyDefinition) {
        super(propertyDefinition);
    }

    @Override
    public void setFromString(String value, ExceptionInterceptor exceptionInterceptor) {
        this.valueAsString = value;
        setValue(((MemorySizePropertyDefinition) getPropertyDefinition()).parseObject(value, exceptionInterceptor), value, exceptionInterceptor);
        this.updateCount++;
    }

    @Override
    public String getValueAsString() {
        return this.valueAsString;
    }

}
