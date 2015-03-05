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
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.StringRefAddr;

import com.mysql.api.ExceptionInterceptor;
import com.mysql.jdbc.exceptions.SQLError;

public abstract class ConnectionProperty implements Serializable {

    private static final long serialVersionUID = 5343886385944677653L;

    protected String[] allowableValues;

    String categoryName;

    protected Object defaultValue;

    protected int lowerBound;

    int order;

    String propertyName;

    public String sinceVersion;

    protected int upperBound;

    protected Object valueAsObject;

    public boolean required;

    public String description;

    protected int updateCount = 0;

    public ConnectionProperty() {
    }

    protected ConnectionProperty(String propertyNameToSet, Object defaultValueToSet, String[] allowableValuesToSet, int lowerBoundToSet, int upperBoundToSet,
            String descriptionToSet, String sinceVersionToSet, String category, int orderInCategory) {

        this.description = descriptionToSet;
        this.propertyName = propertyNameToSet;
        this.defaultValue = defaultValueToSet;
        this.valueAsObject = defaultValueToSet;
        this.allowableValues = allowableValuesToSet;
        this.lowerBound = lowerBoundToSet;
        this.upperBound = upperBoundToSet;
        this.required = false;
        this.sinceVersion = sinceVersionToSet;
        this.categoryName = category;
        this.order = orderInCategory;
    }

    protected String[] getAllowableValues() {
        return this.allowableValues;
    }

    /**
     * @return Returns the categoryName.
     */
    public String getCategoryName() {
        return this.categoryName;
    }

    public Object getDefaultValue() {
        return this.defaultValue;
    }

    protected int getLowerBound() {
        return this.lowerBound;
    }

    /**
     * @return Returns the order.
     */
    public int getOrder() {
        return this.order;
    }

    public String getPropertyName() {
        return this.propertyName;
    }

    protected int getUpperBound() {
        return this.upperBound;
    }

    public Object getValueAsObject() {
        return this.valueAsObject;
    }

    public int getUpdateCount() {
        return this.updateCount;
    }

    protected abstract boolean hasValueConstraints();

    public void initializeFrom(Properties extractFrom, ExceptionInterceptor exceptionInterceptor) throws SQLException {
        String extractedValue = extractFrom.getProperty(getPropertyName());
        extractFrom.remove(getPropertyName());
        initializeFrom(extractedValue, exceptionInterceptor);
    }

    public void initializeFrom(Reference ref, ExceptionInterceptor exceptionInterceptor) throws SQLException {
        RefAddr refAddr = ref.get(getPropertyName());

        if (refAddr != null) {
            String refContentAsString = (String) refAddr.getContent();

            initializeFrom(refContentAsString, exceptionInterceptor);
        }
    }

    protected abstract void initializeFrom(String extractedValue, ExceptionInterceptor exceptionInterceptor) throws SQLException;

    protected abstract boolean isRangeBased();

    /**
     * @param categoryName
     *            The categoryName to set.
     */
    void setCategoryName(String categoryName) {
        this.categoryName = categoryName;
    }

    /**
     * @param order
     *            The order to set.
     */
    void setOrder(int order) {
        this.order = order;
    }

    public void setValueAsObject(Object obj) {
        this.valueAsObject = obj;
        this.updateCount++;
    }

    public void storeTo(Reference ref) {
        if (getValueAsObject() != null) {
            ref.add(new StringRefAddr(getPropertyName(), getValueAsObject().toString()));
        }
    }

    public DriverPropertyInfo getAsDriverPropertyInfo() {
        DriverPropertyInfo dpi = new DriverPropertyInfo(this.propertyName, null);
        dpi.choices = getAllowableValues();
        dpi.value = (this.valueAsObject != null) ? this.valueAsObject.toString() : null;
        dpi.required = this.required;
        dpi.description = this.description;

        return dpi;
    }

    protected void validateStringValues(String valueToValidate, ExceptionInterceptor exceptionInterceptor) throws SQLException {
        String[] validateAgainst = getAllowableValues();

        if (valueToValidate == null) {
            return;
        }

        if ((validateAgainst == null) || (validateAgainst.length == 0)) {
            return;
        }

        for (int i = 0; i < validateAgainst.length; i++) {
            if ((validateAgainst[i] != null) && validateAgainst[i].equalsIgnoreCase(valueToValidate)) {
                return;
            }
        }

        StringBuilder errorMessageBuf = new StringBuilder();

        errorMessageBuf.append("The connection property '");
        errorMessageBuf.append(getPropertyName());
        errorMessageBuf.append("' only accepts values of the form: ");

        if (validateAgainst.length != 0) {
            errorMessageBuf.append("'");
            errorMessageBuf.append(validateAgainst[0]);
            errorMessageBuf.append("'");

            for (int i = 1; i < (validateAgainst.length - 1); i++) {
                errorMessageBuf.append(", ");
                errorMessageBuf.append("'");
                errorMessageBuf.append(validateAgainst[i]);
                errorMessageBuf.append("'");
            }

            errorMessageBuf.append(" or '");
            errorMessageBuf.append(validateAgainst[validateAgainst.length - 1]);
            errorMessageBuf.append("'");
        }

        errorMessageBuf.append(". The value '");
        errorMessageBuf.append(valueToValidate);
        errorMessageBuf.append("' is not in this set.");

        throw SQLError.createSQLException(errorMessageBuf.toString(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor);
    }
}
