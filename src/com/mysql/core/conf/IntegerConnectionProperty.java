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

package com.mysql.core.conf;

import java.io.Serializable;
import java.sql.SQLException;

import com.mysql.api.ExceptionInterceptor;
import com.mysql.jdbc.exceptions.SQLError;

public class IntegerConnectionProperty extends ConnectionProperty implements Serializable {

    private static final long serialVersionUID = 4507602644049413720L;

    protected int multiplier = 1;

    public IntegerConnectionProperty(String propertyNameToSet, Object defaultValueToSet, String[] allowableValuesToSet, int lowerBoundToSet,
            int upperBoundToSet, String descriptionToSet, String sinceVersionToSet, String category, int orderInCategory) {
        super(propertyNameToSet, defaultValueToSet, allowableValuesToSet, lowerBoundToSet, upperBoundToSet, descriptionToSet, sinceVersionToSet, category,
                orderInCategory);
    }

    public IntegerConnectionProperty(String propertyNameToSet, int defaultValueToSet, int lowerBoundToSet, int upperBoundToSet, String descriptionToSet,
            String sinceVersionToSet, String category, int orderInCategory) {
        super(propertyNameToSet, Integer.valueOf(defaultValueToSet), null, lowerBoundToSet, upperBoundToSet, descriptionToSet, sinceVersionToSet, category,
                orderInCategory);
    }

    /**
     * @param propertyNameToSet
     * @param defaultValueToSet
     * @param descriptionToSet
     * @param sinceVersionToSet
     */

    public IntegerConnectionProperty(String propertyNameToSet, int defaultValueToSet, String descriptionToSet, String sinceVersionToSet, String category,
            int orderInCategory) {
        this(propertyNameToSet, defaultValueToSet, 0, 0, descriptionToSet, sinceVersionToSet, category, orderInCategory);
    }

    /**
     * @see com.mysql.jdbc.JdbcConnectionProperties.ConnectionProperty#getAllowableValues()
     */
    @Override
    protected String[] getAllowableValues() {
        return null;
    }

    /**
     * @see com.mysql.jdbc.JdbcConnectionProperties.ConnectionProperty#getLowerBound()
     */
    @Override
    protected int getLowerBound() {
        return this.lowerBound;
    }

    /**
     * @see com.mysql.jdbc.JdbcConnectionProperties.ConnectionProperty#getUpperBound()
     */
    @Override
    protected int getUpperBound() {
        return this.upperBound;
    }

    public int getValueAsInt() {
        return ((Integer) this.valueAsObject).intValue();
    }

    /**
     * @see com.mysql.jdbc.JdbcConnectionProperties.ConnectionProperty#hasValueConstraints()
     */
    @Override
    protected boolean hasValueConstraints() {
        return false;
    }

    /**
     * @see com.mysql.jdbc.JdbcConnectionProperties.ConnectionProperty#initializeFrom(java.lang.String)
     */
    @Override
    protected void initializeFrom(String extractedValue, ExceptionInterceptor exceptionInterceptor) throws SQLException {
        if (extractedValue != null) {
            try {
                // Parse decimals, too
                int intValue = (int) (Double.valueOf(extractedValue).doubleValue() * this.multiplier);

                setValue(intValue, extractedValue, exceptionInterceptor);
            } catch (NumberFormatException nfe) {
                throw SQLError.createSQLException("The connection property '" + getPropertyName() + "' only accepts integer values. The value '"
                        + extractedValue + "' can not be converted to an integer.", SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor);
            }
        } else {
            this.valueAsObject = this.defaultValue;
        }
        this.updateCount++;
    }

    /**
     * @see com.mysql.jdbc.JdbcConnectionProperties.ConnectionProperty#isRangeBased()
     */
    @Override
    protected boolean isRangeBased() {
        return getUpperBound() != getLowerBound();
    }

    public void setValue(int intValue, ExceptionInterceptor exceptionInterceptor) throws SQLException {
        setValue(intValue, null, exceptionInterceptor);
    }

    void setValue(int intValue, String valueAsString, ExceptionInterceptor exceptionInterceptor) throws SQLException {
        if (isRangeBased()) {
            if ((intValue < getLowerBound()) || (intValue > getUpperBound())) {
                throw SQLError.createSQLException("The connection property '" + getPropertyName() + "' only accepts integer values in the range of "
                        + getLowerBound() + " - " + getUpperBound() + ", the value '" + (valueAsString == null ? intValue : valueAsString)
                        + "' exceeds this range.", SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor);
            }
        }

        this.valueAsObject = Integer.valueOf(intValue);
        this.updateCount++;
    }
}
