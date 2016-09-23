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
import com.mysql.cj.core.util.StringUtils;

public class MemorySizePropertyDefinition extends IntegerPropertyDefinition {

    private static final long serialVersionUID = -6878680905514177949L;

    public MemorySizePropertyDefinition(String name, int defaultValue, boolean isRuntimeModifiable, String description, String sinceVersion, String category,
            int orderInCategory) {
        super(name, defaultValue, isRuntimeModifiable, description, sinceVersion, category, orderInCategory);
    }

    public MemorySizePropertyDefinition(String name, int defaultValue, boolean isRuntimeModifiable, String description, String sinceVersion, String category,
            int orderInCategory, int lowerBound, int upperBound) {
        super(name, defaultValue, isRuntimeModifiable, description, sinceVersion, category, orderInCategory, lowerBound, upperBound);
    }

    @Override
    public Integer parseObject(String value, ExceptionInterceptor exceptionInterceptor) {
        this.multiplier = 1;

        if (value.endsWith("k") || value.endsWith("K") || value.endsWith("kb") || value.endsWith("Kb") || value.endsWith("kB") || value.endsWith("KB")) {
            this.multiplier = 1024;
            int indexOfK = StringUtils.indexOfIgnoreCase(value, "k");
            value = value.substring(0, indexOfK);
        } else if (value.endsWith("m") || value.endsWith("M") || value.endsWith("mb") || value.endsWith("Mb") || value.endsWith("mB") || value.endsWith("MB")) {
            this.multiplier = 1024 * 1024;
            int indexOfM = StringUtils.indexOfIgnoreCase(value, "m");
            value = value.substring(0, indexOfM);
        } else if (value.endsWith("g") || value.endsWith("G") || value.endsWith("gb") || value.endsWith("Gb") || value.endsWith("gB") || value.endsWith("GB")) {
            this.multiplier = 1024 * 1024 * 1024;
            int indexOfG = StringUtils.indexOfIgnoreCase(value, "g");
            value = value.substring(0, indexOfG);
        }

        return super.parseObject(value, exceptionInterceptor);
    }

    /**
     * Creates instance of ReadableMemorySizeProperty or ModifiableMemorySizeProperty depending on isRuntimeModifiable() result.
     * 
     * @return
     */
    @Override
    public RuntimeProperty<Integer> createRuntimeProperty() {
        return isRuntimeModifiable() ? new ModifiableMemorySizeProperty(this) : new ReadableMemorySizeProperty(this);
    }

}
