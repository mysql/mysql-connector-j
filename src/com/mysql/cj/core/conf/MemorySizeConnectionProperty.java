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
import java.sql.SQLException;

import com.mysql.cj.api.ExceptionInterceptor;
import com.mysql.cj.core.util.StringUtils;

public class MemorySizeConnectionProperty extends IntegerConnectionProperty implements Serializable {

    private static final long serialVersionUID = -8166011277756228978L;

    private String valueAsString;

    public MemorySizeConnectionProperty(String propertyNameToSet, int defaultValueToSet, int lowerBoundToSet, int upperBoundToSet, String descriptionToSet,
            String sinceVersionToSet, String category, int orderInCategory) {
        super(propertyNameToSet, defaultValueToSet, lowerBoundToSet, upperBoundToSet, descriptionToSet, sinceVersionToSet, category, orderInCategory);
    }

    @Override
    protected void initializeFrom(String extractedValue, ExceptionInterceptor exceptionInterceptor) throws SQLException {
        this.valueAsString = extractedValue;
        this.multiplier = 1;

        if (extractedValue != null) {
            if (extractedValue.endsWith("k") || extractedValue.endsWith("K") || extractedValue.endsWith("kb") || extractedValue.endsWith("Kb")
                    || extractedValue.endsWith("kB") || extractedValue.endsWith("KB")) {
                this.multiplier = 1024;
                int indexOfK = StringUtils.indexOfIgnoreCase(extractedValue, "k");
                extractedValue = extractedValue.substring(0, indexOfK);
            } else if (extractedValue.endsWith("m") || extractedValue.endsWith("M") || extractedValue.endsWith("mb") || extractedValue.endsWith("Mb")
                    || extractedValue.endsWith("mB") || extractedValue.endsWith("MB")) {
                this.multiplier = 1024 * 1024;
                int indexOfM = StringUtils.indexOfIgnoreCase(extractedValue, "m");
                extractedValue = extractedValue.substring(0, indexOfM);
            } else if (extractedValue.endsWith("g") || extractedValue.endsWith("G") || extractedValue.endsWith("gb") || extractedValue.endsWith("Gb")
                    || extractedValue.endsWith("gB") || extractedValue.endsWith("GB")) {
                this.multiplier = 1024 * 1024 * 1024;
                int indexOfG = StringUtils.indexOfIgnoreCase(extractedValue, "g");
                extractedValue = extractedValue.substring(0, indexOfG);
            }
        }

        super.initializeFrom(extractedValue, exceptionInterceptor);
    }

    public void setValue(String value, ExceptionInterceptor exceptionInterceptor) throws SQLException {
        initializeFrom(value, exceptionInterceptor);
    }

    public String getValueAsString() {
        return this.valueAsString;
    }

}
