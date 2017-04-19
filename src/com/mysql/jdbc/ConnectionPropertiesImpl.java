/*
  Copyright (c) 2002, 2017, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.jdbc;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.StringRefAddr;

import com.mysql.jdbc.log.Log;
import com.mysql.jdbc.log.StandardLogger;

/**
 * Represents configurable properties for Connections and DataSources. Can also expose properties as JDBC DriverPropertyInfo if required as well.
 */
public class ConnectionPropertiesImpl implements Serializable, ConnectionProperties {

    private static final long serialVersionUID = 4257801713007640580L;

    static class BooleanConnectionProperty extends ConnectionProperty implements Serializable {

        private static final long serialVersionUID = 2540132501709159404L;

        /**
         * @param propertyNameToSet
         * @param defaultValueToSet
         * @param descriptionToSet
         * @param sinceVersionToSet
         */
        BooleanConnectionProperty(String propertyNameToSet, boolean defaultValueToSet, String descriptionToSet, String sinceVersionToSet, String category,
                int orderInCategory) {
            super(propertyNameToSet, Boolean.valueOf(defaultValueToSet), null, 0, 0, descriptionToSet, sinceVersionToSet, category, orderInCategory);
        }

        /**
         * @see com.mysql.jdbc.ConnectionPropertiesImpl.ConnectionProperty#getAllowableValues()
         */
        @Override
        String[] getAllowableValues() {
            return new String[] { "true", "false", "yes", "no" };
        }

        boolean getValueAsBoolean() {
            return ((Boolean) this.valueAsObject).booleanValue();
        }

        /**
         * @see com.mysql.jdbc.ConnectionPropertiesImpl.ConnectionProperty#hasValueConstraints()
         */
        @Override
        boolean hasValueConstraints() {
            return true;
        }

        /**
         * @see com.mysql.jdbc.ConnectionPropertiesImpl.ConnectionProperty#initializeFrom(java.util.Properties)
         */
        @Override
        void initializeFrom(String extractedValue, ExceptionInterceptor exceptionInterceptor) throws SQLException {
            if (extractedValue != null) {
                validateStringValues(extractedValue, exceptionInterceptor);

                this.valueAsObject = Boolean.valueOf(extractedValue.equalsIgnoreCase("TRUE") || extractedValue.equalsIgnoreCase("YES"));
                this.wasExplicitlySet = true;
            } else {
                this.valueAsObject = this.defaultValue;
            }
            this.updateCount++;
        }

        /**
         * @see com.mysql.jdbc.ConnectionPropertiesImpl.ConnectionProperty#isRangeBased()
         */
        @Override
        boolean isRangeBased() {
            return false;
        }

        void setValue(boolean valueFlag) {
            this.valueAsObject = Boolean.valueOf(valueFlag);
            this.wasExplicitlySet = true;
            this.updateCount++;
        }
    }

    static abstract class ConnectionProperty implements Serializable {

        static final long serialVersionUID = -6644853639584478367L;

        String[] allowableValues;

        String categoryName;

        Object defaultValue;

        int lowerBound;

        int order;

        String propertyName;

        String sinceVersion;

        int upperBound;

        Object valueAsObject;

        boolean required;

        String description;

        int updateCount = 0;

        boolean wasExplicitlySet = false;

        public ConnectionProperty() {
        }

        ConnectionProperty(String propertyNameToSet, Object defaultValueToSet, String[] allowableValuesToSet, int lowerBoundToSet, int upperBoundToSet,
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

        String[] getAllowableValues() {
            return this.allowableValues;
        }

        /**
         * @return Returns the categoryName.
         */
        String getCategoryName() {
            return this.categoryName;
        }

        Object getDefaultValue() {
            return this.defaultValue;
        }

        int getLowerBound() {
            return this.lowerBound;
        }

        /**
         * @return Returns the order.
         */
        int getOrder() {
            return this.order;
        }

        String getPropertyName() {
            return this.propertyName;
        }

        int getUpperBound() {
            return this.upperBound;
        }

        Object getValueAsObject() {
            return this.valueAsObject;
        }

        int getUpdateCount() {
            return this.updateCount;
        }

        boolean isExplicitlySet() {
            return this.wasExplicitlySet;
        }

        abstract boolean hasValueConstraints();

        void initializeFrom(Properties extractFrom, ExceptionInterceptor exceptionInterceptor) throws SQLException {
            String extractedValue = extractFrom.getProperty(getPropertyName());
            extractFrom.remove(getPropertyName());
            initializeFrom(extractedValue, exceptionInterceptor);
        }

        void initializeFrom(Reference ref, ExceptionInterceptor exceptionInterceptor) throws SQLException {
            RefAddr refAddr = ref.get(getPropertyName());

            if (refAddr != null) {
                String refContentAsString = (String) refAddr.getContent();

                initializeFrom(refContentAsString, exceptionInterceptor);
            }
        }

        abstract void initializeFrom(String extractedValue, ExceptionInterceptor exceptionInterceptor) throws SQLException;

        abstract boolean isRangeBased();

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

        void setValueAsObject(Object obj) {
            this.valueAsObject = obj;
            this.updateCount++;
        }

        void storeTo(Reference ref) {
            if (getValueAsObject() != null) {
                ref.add(new StringRefAddr(getPropertyName(), getValueAsObject().toString()));
            }
        }

        DriverPropertyInfo getAsDriverPropertyInfo() {
            DriverPropertyInfo dpi = new DriverPropertyInfo(this.propertyName, null);
            dpi.choices = getAllowableValues();
            dpi.value = (this.valueAsObject != null) ? this.valueAsObject.toString() : null;
            dpi.required = this.required;
            dpi.description = this.description;

            return dpi;
        }

        void validateStringValues(String valueToValidate, ExceptionInterceptor exceptionInterceptor) throws SQLException {
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

    static class IntegerConnectionProperty extends ConnectionProperty implements Serializable {

        private static final long serialVersionUID = -3004305481796850832L;

        int multiplier = 1;

        public IntegerConnectionProperty(String propertyNameToSet, Object defaultValueToSet, String[] allowableValuesToSet, int lowerBoundToSet,
                int upperBoundToSet, String descriptionToSet, String sinceVersionToSet, String category, int orderInCategory) {
            super(propertyNameToSet, defaultValueToSet, allowableValuesToSet, lowerBoundToSet, upperBoundToSet, descriptionToSet, sinceVersionToSet, category,
                    orderInCategory);
        }

        IntegerConnectionProperty(String propertyNameToSet, int defaultValueToSet, int lowerBoundToSet, int upperBoundToSet, String descriptionToSet,
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

        IntegerConnectionProperty(String propertyNameToSet, int defaultValueToSet, String descriptionToSet, String sinceVersionToSet, String category,
                int orderInCategory) {
            this(propertyNameToSet, defaultValueToSet, 0, 0, descriptionToSet, sinceVersionToSet, category, orderInCategory);
        }

        /**
         * @see com.mysql.jdbc.ConnectionProperties.ConnectionProperty#getAllowableValues()
         */
        @Override
        String[] getAllowableValues() {
            return null;
        }

        /**
         * @see com.mysql.jdbc.ConnectionProperties.ConnectionProperty#getLowerBound()
         */
        @Override
        int getLowerBound() {
            return this.lowerBound;
        }

        /**
         * @see com.mysql.jdbc.ConnectionProperties.ConnectionProperty#getUpperBound()
         */
        @Override
        int getUpperBound() {
            return this.upperBound;
        }

        int getValueAsInt() {
            return ((Integer) this.valueAsObject).intValue();
        }

        /**
         * @see com.mysql.jdbc.ConnectionProperties.ConnectionProperty#hasValueConstraints()
         */
        @Override
        boolean hasValueConstraints() {
            return false;
        }

        /**
         * @see com.mysql.jdbc.ConnectionProperties.ConnectionProperty#initializeFrom(java.lang.String)
         */
        @Override
        void initializeFrom(String extractedValue, ExceptionInterceptor exceptionInterceptor) throws SQLException {
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
         * @see com.mysql.jdbc.ConnectionProperties.ConnectionProperty#isRangeBased()
         */
        @Override
        boolean isRangeBased() {
            return getUpperBound() != getLowerBound();
        }

        void setValue(int intValue, ExceptionInterceptor exceptionInterceptor) throws SQLException {
            setValue(intValue, null, exceptionInterceptor);
        }

        void setValue(int intValue, String valueAsString, ExceptionInterceptor exceptionInterceptor) throws SQLException {
            if (isRangeBased()) {
                if ((intValue < getLowerBound()) || (intValue > getUpperBound())) {
                    throw SQLError.createSQLException(
                            "The connection property '" + getPropertyName() + "' only accepts integer values in the range of " + getLowerBound() + " - "
                                    + getUpperBound() + ", the value '" + (valueAsString == null ? intValue : valueAsString) + "' exceeds this range.",
                            SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor);
                }
            }

            this.valueAsObject = Integer.valueOf(intValue);
            this.wasExplicitlySet = true;
            this.updateCount++;
        }
    }

    static public class LongConnectionProperty extends IntegerConnectionProperty {

        private static final long serialVersionUID = 6068572984340480895L;

        LongConnectionProperty(String propertyNameToSet, long defaultValueToSet, long lowerBoundToSet, long upperBoundToSet, String descriptionToSet,
                String sinceVersionToSet, String category, int orderInCategory) {
            super(propertyNameToSet, Long.valueOf(defaultValueToSet), null, (int) lowerBoundToSet, (int) upperBoundToSet, descriptionToSet, sinceVersionToSet,
                    category, orderInCategory);
        }

        LongConnectionProperty(String propertyNameToSet, long defaultValueToSet, String descriptionToSet, String sinceVersionToSet, String category,
                int orderInCategory) {
            this(propertyNameToSet, defaultValueToSet, 0, 0, descriptionToSet, sinceVersionToSet, category, orderInCategory);
        }

        void setValue(long longValue, ExceptionInterceptor exceptionInterceptor) throws SQLException {
            setValue(longValue, null, exceptionInterceptor);
        }

        void setValue(long longValue, String valueAsString, ExceptionInterceptor exceptionInterceptor) throws SQLException {
            if (isRangeBased()) {
                if ((longValue < getLowerBound()) || (longValue > getUpperBound())) {
                    throw SQLError.createSQLException(
                            "The connection property '" + getPropertyName() + "' only accepts long integer values in the range of " + getLowerBound() + " - "
                                    + getUpperBound() + ", the value '" + (valueAsString == null ? longValue : valueAsString) + "' exceeds this range.",
                            SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor);
                }
            }
            this.valueAsObject = Long.valueOf(longValue);
            this.wasExplicitlySet = true;
            this.updateCount++;
        }

        long getValueAsLong() {
            return ((Long) this.valueAsObject).longValue();
        }

        @Override
        void initializeFrom(String extractedValue, ExceptionInterceptor exceptionInterceptor) throws SQLException {
            if (extractedValue != null) {
                try {
                    // Parse decimals, too
                    long longValue = Double.valueOf(extractedValue).longValue();

                    setValue(longValue, extractedValue, exceptionInterceptor);
                } catch (NumberFormatException nfe) {
                    throw SQLError.createSQLException("The connection property '" + getPropertyName() + "' only accepts long integer values. The value '"
                            + extractedValue + "' can not be converted to a long integer.", SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor);
                }
            } else {
                this.valueAsObject = this.defaultValue;
            }
            this.updateCount++;
        }
    }

    static class MemorySizeConnectionProperty extends IntegerConnectionProperty implements Serializable {

        private static final long serialVersionUID = 7351065128998572656L;

        private String valueAsString;

        MemorySizeConnectionProperty(String propertyNameToSet, int defaultValueToSet, int lowerBoundToSet, int upperBoundToSet, String descriptionToSet,
                String sinceVersionToSet, String category, int orderInCategory) {
            super(propertyNameToSet, defaultValueToSet, lowerBoundToSet, upperBoundToSet, descriptionToSet, sinceVersionToSet, category, orderInCategory);
        }

        @Override
        void initializeFrom(String extractedValue, ExceptionInterceptor exceptionInterceptor) throws SQLException {
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

        void setValue(String value, ExceptionInterceptor exceptionInterceptor) throws SQLException {
            initializeFrom(value, exceptionInterceptor);
        }

        String getValueAsString() {
            return this.valueAsString;
        }
    }

    static class StringConnectionProperty extends ConnectionProperty implements Serializable {

        private static final long serialVersionUID = 5432127962785948272L;

        StringConnectionProperty(String propertyNameToSet, String defaultValueToSet, String descriptionToSet, String sinceVersionToSet, String category,
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
        StringConnectionProperty(String propertyNameToSet, String defaultValueToSet, String[] allowableValuesToSet, String descriptionToSet,
                String sinceVersionToSet, String category, int orderInCategory) {
            super(propertyNameToSet, defaultValueToSet, allowableValuesToSet, 0, 0, descriptionToSet, sinceVersionToSet, category, orderInCategory);
        }

        String getValueAsString() {
            return (String) this.valueAsObject;
        }

        /**
         * @see com.mysql.jdbc.ConnectionPropertiesImpl.ConnectionProperty#hasValueConstraints()
         */
        @Override
        boolean hasValueConstraints() {
            return (this.allowableValues != null) && (this.allowableValues.length > 0);
        }

        /**
         * @see com.mysql.jdbc.ConnectionPropertiesImpl.ConnectionProperty#initializeFrom(java.util.Properties)
         */
        @Override
        void initializeFrom(String extractedValue, ExceptionInterceptor exceptionInterceptor) throws SQLException {
            if (extractedValue != null) {
                validateStringValues(extractedValue, exceptionInterceptor);

                this.valueAsObject = extractedValue;
                this.wasExplicitlySet = true;
            } else {
                this.valueAsObject = this.defaultValue;
            }
            this.updateCount++;
        }

        /**
         * @see com.mysql.jdbc.ConnectionPropertiesImpl.ConnectionProperty#isRangeBased()
         */
        @Override
        boolean isRangeBased() {
            return false;
        }

        void setValue(String valueFlag) {
            this.valueAsObject = valueFlag;
            this.wasExplicitlySet = true;
            this.updateCount++;
        }
    }

    private static final String CONNECTION_AND_AUTH_CATEGORY = Messages.getString("ConnectionProperties.categoryConnectionAuthentication");

    private static final String NETWORK_CATEGORY = Messages.getString("ConnectionProperties.categoryNetworking");

    private static final String DEBUGING_PROFILING_CATEGORY = Messages.getString("ConnectionProperties.categoryDebuggingProfiling");

    private static final String HA_CATEGORY = Messages.getString("ConnectionProperties.categorryHA");

    private static final String MISC_CATEGORY = Messages.getString("ConnectionProperties.categoryMisc");

    private static final String PERFORMANCE_CATEGORY = Messages.getString("ConnectionProperties.categoryPerformance");

    private static final String SECURITY_CATEGORY = Messages.getString("ConnectionProperties.categorySecurity");

    private static final String[] PROPERTY_CATEGORIES = new String[] { CONNECTION_AND_AUTH_CATEGORY, NETWORK_CATEGORY, HA_CATEGORY, SECURITY_CATEGORY,
            PERFORMANCE_CATEGORY, DEBUGING_PROFILING_CATEGORY, MISC_CATEGORY };

    private static final ArrayList<java.lang.reflect.Field> PROPERTY_LIST = new ArrayList<java.lang.reflect.Field>();

    //
    // Yes, this looks goofy, but we're trying to avoid intern()ing here
    //
    private static final String STANDARD_LOGGER_NAME = StandardLogger.class.getName();

    protected static final String ZERO_DATETIME_BEHAVIOR_CONVERT_TO_NULL = "convertToNull";

    protected static final String ZERO_DATETIME_BEHAVIOR_EXCEPTION = "exception";

    protected static final String ZERO_DATETIME_BEHAVIOR_ROUND = "round";

    static {
        try {
            java.lang.reflect.Field[] declaredFields = ConnectionPropertiesImpl.class.getDeclaredFields();

            for (int i = 0; i < declaredFields.length; i++) {
                if (ConnectionPropertiesImpl.ConnectionProperty.class.isAssignableFrom(declaredFields[i].getType())) {
                    PROPERTY_LIST.add(declaredFields[i]);
                }
            }
        } catch (Exception ex) {
            RuntimeException rtEx = new RuntimeException();
            rtEx.initCause(ex);

            throw rtEx;
        }
    }

    public ExceptionInterceptor getExceptionInterceptor() {
        return null;
    }

    /**
     * Exposes all ConnectionPropertyInfo instances as DriverPropertyInfo
     * 
     * @param info
     *            the properties to load into these ConnectionPropertyInfo
     *            instances
     * @param slotsToReserve
     *            the number of DPI slots to reserve for 'standard' DPI
     *            properties (user, host, password, etc)
     * @return a list of all ConnectionPropertyInfo instances, as
     *         DriverPropertyInfo
     * @throws SQLException
     *             if an error occurs
     */
    protected static DriverPropertyInfo[] exposeAsDriverPropertyInfo(Properties info, int slotsToReserve) throws SQLException {
        return (new ConnectionPropertiesImpl() {
            private static final long serialVersionUID = 4257801713007640581L;
        }).exposeAsDriverPropertyInfoInternal(info, slotsToReserve);
    }

    private BooleanConnectionProperty allowLoadLocalInfile = new BooleanConnectionProperty("allowLoadLocalInfile", true,
            Messages.getString("ConnectionProperties.loadDataLocal"), "3.0.3", SECURITY_CATEGORY, Integer.MAX_VALUE);

    private BooleanConnectionProperty allowMultiQueries = new BooleanConnectionProperty("allowMultiQueries", false,
            Messages.getString("ConnectionProperties.allowMultiQueries"), "3.1.1", SECURITY_CATEGORY, 1);

    private BooleanConnectionProperty allowNanAndInf = new BooleanConnectionProperty("allowNanAndInf", false,
            Messages.getString("ConnectionProperties.allowNANandINF"), "3.1.5", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty allowUrlInLocalInfile = new BooleanConnectionProperty("allowUrlInLocalInfile", false,
            Messages.getString("ConnectionProperties.allowUrlInLoadLocal"), "3.1.4", SECURITY_CATEGORY, Integer.MAX_VALUE);

    private BooleanConnectionProperty alwaysSendSetIsolation = new BooleanConnectionProperty("alwaysSendSetIsolation", true,
            Messages.getString("ConnectionProperties.alwaysSendSetIsolation"), "3.1.7", PERFORMANCE_CATEGORY, Integer.MAX_VALUE);

    private BooleanConnectionProperty autoClosePStmtStreams = new BooleanConnectionProperty("autoClosePStmtStreams", false,
            Messages.getString("ConnectionProperties.autoClosePstmtStreams"), "3.1.12", MISC_CATEGORY, Integer.MIN_VALUE);

    private StringConnectionProperty replicationConnectionGroup = new StringConnectionProperty("replicationConnectionGroup", null,
            Messages.getString("ConnectionProperties.replicationConnectionGroup"), "5.1.27", HA_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty allowMasterDownConnections = new BooleanConnectionProperty("allowMasterDownConnections", false,
            Messages.getString("ConnectionProperties.allowMasterDownConnections"), "5.1.27", HA_CATEGORY, Integer.MAX_VALUE);

    private BooleanConnectionProperty allowSlaveDownConnections = new BooleanConnectionProperty("allowSlaveDownConnections", false,
            Messages.getString("ConnectionProperties.allowSlaveDownConnections"), "5.1.38", HA_CATEGORY, Integer.MAX_VALUE);

    private BooleanConnectionProperty readFromMasterWhenNoSlaves = new BooleanConnectionProperty("readFromMasterWhenNoSlaves", false,
            Messages.getString("ConnectionProperties.readFromMasterWhenNoSlaves"), "5.1.38", HA_CATEGORY, Integer.MAX_VALUE);

    private BooleanConnectionProperty autoDeserialize = new BooleanConnectionProperty("autoDeserialize", false,
            Messages.getString("ConnectionProperties.autoDeserialize"), "3.1.5", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty autoGenerateTestcaseScript = new BooleanConnectionProperty("autoGenerateTestcaseScript", false,
            Messages.getString("ConnectionProperties.autoGenerateTestcaseScript"), "3.1.9", DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE);

    private boolean autoGenerateTestcaseScriptAsBoolean = false;

    private BooleanConnectionProperty autoReconnect = new BooleanConnectionProperty("autoReconnect", false,
            Messages.getString("ConnectionProperties.autoReconnect"), "1.1", HA_CATEGORY, 0);

    private BooleanConnectionProperty autoReconnectForPools = new BooleanConnectionProperty("autoReconnectForPools", false,
            Messages.getString("ConnectionProperties.autoReconnectForPools"), "3.1.3", HA_CATEGORY, 1);

    private boolean autoReconnectForPoolsAsBoolean = false;

    private MemorySizeConnectionProperty blobSendChunkSize = new MemorySizeConnectionProperty("blobSendChunkSize", 1024 * 1024, 0, 0,
            Messages.getString("ConnectionProperties.blobSendChunkSize"), "3.1.9", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty autoSlowLog = new BooleanConnectionProperty("autoSlowLog", true, Messages.getString("ConnectionProperties.autoSlowLog"),
            "5.1.4", DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty blobsAreStrings = new BooleanConnectionProperty("blobsAreStrings", false,
            "Should the driver always treat BLOBs as Strings - specifically to work around dubious metadata returned by the server for GROUP BY clauses?",
            "5.0.8", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty functionsNeverReturnBlobs = new BooleanConnectionProperty("functionsNeverReturnBlobs", false,
            "Should the driver always treat data from functions returning BLOBs as Strings - specifically to work around dubious metadata "
                    + "returned by the server for GROUP BY clauses?",
            "5.0.8", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty cacheCallableStatements = new BooleanConnectionProperty("cacheCallableStmts", false,
            Messages.getString("ConnectionProperties.cacheCallableStatements"), "3.1.2", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty cachePreparedStatements = new BooleanConnectionProperty("cachePrepStmts", false,
            Messages.getString("ConnectionProperties.cachePrepStmts"), "3.0.10", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty cacheResultSetMetadata = new BooleanConnectionProperty("cacheResultSetMetadata", false,
            Messages.getString("ConnectionProperties.cacheRSMetadata"), "3.1.1", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);

    private boolean cacheResultSetMetaDataAsBoolean;

    private StringConnectionProperty serverConfigCacheFactory = new StringConnectionProperty("serverConfigCacheFactory",
            PerVmServerConfigCacheFactory.class.getName(), Messages.getString("ConnectionProperties.serverConfigCacheFactory"), "5.1.1", PERFORMANCE_CATEGORY,
            12);

    private BooleanConnectionProperty cacheServerConfiguration = new BooleanConnectionProperty("cacheServerConfiguration", false,
            Messages.getString("ConnectionProperties.cacheServerConfiguration"), "3.1.5", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);

    private IntegerConnectionProperty callableStatementCacheSize = new IntegerConnectionProperty("callableStmtCacheSize", 100, 0, Integer.MAX_VALUE,
            Messages.getString("ConnectionProperties.callableStmtCacheSize"), "3.1.2", PERFORMANCE_CATEGORY, 5);

    private BooleanConnectionProperty capitalizeTypeNames = new BooleanConnectionProperty("capitalizeTypeNames", true,
            Messages.getString("ConnectionProperties.capitalizeTypeNames"), "2.0.7", MISC_CATEGORY, Integer.MIN_VALUE);

    private StringConnectionProperty characterEncoding = new StringConnectionProperty("characterEncoding", null,
            Messages.getString("ConnectionProperties.characterEncoding"), "1.1g", MISC_CATEGORY, 5);

    private String characterEncodingAsString = null;

    protected boolean characterEncodingIsAliasForSjis = false;

    private StringConnectionProperty characterSetResults = new StringConnectionProperty("characterSetResults", null,
            Messages.getString("ConnectionProperties.characterSetResults"), "3.0.13", MISC_CATEGORY, 6);

    private StringConnectionProperty connectionAttributes = new StringConnectionProperty("connectionAttributes", null,
            Messages.getString("ConnectionProperties.connectionAttributes"), "5.1.25", MISC_CATEGORY, 7);

    private StringConnectionProperty clientInfoProvider = new StringConnectionProperty("clientInfoProvider", "com.mysql.jdbc.JDBC4CommentClientInfoProvider",
            Messages.getString("ConnectionProperties.clientInfoProvider"), "5.1.0", DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty clobberStreamingResults = new BooleanConnectionProperty("clobberStreamingResults", false,
            Messages.getString("ConnectionProperties.clobberStreamingResults"), "3.0.9", MISC_CATEGORY, Integer.MIN_VALUE);

    private StringConnectionProperty clobCharacterEncoding = new StringConnectionProperty("clobCharacterEncoding", null,
            Messages.getString("ConnectionProperties.clobCharacterEncoding"), "5.0.0", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty compensateOnDuplicateKeyUpdateCounts = new BooleanConnectionProperty("compensateOnDuplicateKeyUpdateCounts", false,
            Messages.getString("ConnectionProperties.compensateOnDuplicateKeyUpdateCounts"), "5.1.7", MISC_CATEGORY, Integer.MIN_VALUE);
    private StringConnectionProperty connectionCollation = new StringConnectionProperty("connectionCollation", null,
            Messages.getString("ConnectionProperties.connectionCollation"), "3.0.13", MISC_CATEGORY, 7);

    private StringConnectionProperty connectionLifecycleInterceptors = new StringConnectionProperty("connectionLifecycleInterceptors", null,
            Messages.getString("ConnectionProperties.connectionLifecycleInterceptors"), "5.1.4", CONNECTION_AND_AUTH_CATEGORY, Integer.MAX_VALUE);

    private IntegerConnectionProperty connectTimeout = new IntegerConnectionProperty("connectTimeout", 0, 0, Integer.MAX_VALUE,
            Messages.getString("ConnectionProperties.connectTimeout"), "3.0.1", CONNECTION_AND_AUTH_CATEGORY, 9);

    private BooleanConnectionProperty continueBatchOnError = new BooleanConnectionProperty("continueBatchOnError", true,
            Messages.getString("ConnectionProperties.continueBatchOnError"), "3.0.3", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty createDatabaseIfNotExist = new BooleanConnectionProperty("createDatabaseIfNotExist", false,
            Messages.getString("ConnectionProperties.createDatabaseIfNotExist"), "3.1.9", MISC_CATEGORY, Integer.MIN_VALUE);

    private IntegerConnectionProperty defaultFetchSize = new IntegerConnectionProperty("defaultFetchSize", 0,
            Messages.getString("ConnectionProperties.defaultFetchSize"), "3.1.9", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);

    // Think really long and hard about changing the default for this many, many applications have come to be acustomed to the latency profile of preparing
    // stuff client-side, rather than prepare (round-trip), execute (round-trip), close (round-trip).
    private BooleanConnectionProperty detectServerPreparedStmts = new BooleanConnectionProperty("useServerPrepStmts", false,
            Messages.getString("ConnectionProperties.useServerPrepStmts"), "3.1.0", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty dontTrackOpenResources = new BooleanConnectionProperty("dontTrackOpenResources", false,
            Messages.getString("ConnectionProperties.dontTrackOpenResources"), "3.1.7", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty dumpQueriesOnException = new BooleanConnectionProperty("dumpQueriesOnException", false,
            Messages.getString("ConnectionProperties.dumpQueriesOnException"), "3.1.3", DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty dynamicCalendars = new BooleanConnectionProperty("dynamicCalendars", false,
            Messages.getString("ConnectionProperties.dynamicCalendars"), "3.1.5", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty elideSetAutoCommits = new BooleanConnectionProperty("elideSetAutoCommits", false,
            Messages.getString("ConnectionProperties.eliseSetAutoCommit"), "3.1.3", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty emptyStringsConvertToZero = new BooleanConnectionProperty("emptyStringsConvertToZero", true,
            Messages.getString("ConnectionProperties.emptyStringsConvertToZero"), "3.1.8", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty emulateLocators = new BooleanConnectionProperty("emulateLocators", false,
            Messages.getString("ConnectionProperties.emulateLocators"), "3.1.0", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty emulateUnsupportedPstmts = new BooleanConnectionProperty("emulateUnsupportedPstmts", true,
            Messages.getString("ConnectionProperties.emulateUnsupportedPstmts"), "3.1.7", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty enablePacketDebug = new BooleanConnectionProperty("enablePacketDebug", false,
            Messages.getString("ConnectionProperties.enablePacketDebug"), "3.1.3", DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty enableQueryTimeouts = new BooleanConnectionProperty("enableQueryTimeouts", true,
            Messages.getString("ConnectionProperties.enableQueryTimeouts"), "5.0.6", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty explainSlowQueries = new BooleanConnectionProperty("explainSlowQueries", false,
            Messages.getString("ConnectionProperties.explainSlowQueries"), "3.1.2", DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE);

    private StringConnectionProperty exceptionInterceptors = new StringConnectionProperty("exceptionInterceptors", null,
            Messages.getString("ConnectionProperties.exceptionInterceptors"), "5.1.8", MISC_CATEGORY, Integer.MIN_VALUE);

    /** When failed-over, set connection to read-only? */
    private BooleanConnectionProperty failOverReadOnly = new BooleanConnectionProperty("failOverReadOnly", true,
            Messages.getString("ConnectionProperties.failoverReadOnly"), "3.0.12", HA_CATEGORY, 2);

    private BooleanConnectionProperty gatherPerformanceMetrics = new BooleanConnectionProperty("gatherPerfMetrics", false,
            Messages.getString("ConnectionProperties.gatherPerfMetrics"), "3.1.2", DEBUGING_PROFILING_CATEGORY, 1);

    private BooleanConnectionProperty generateSimpleParameterMetadata = new BooleanConnectionProperty("generateSimpleParameterMetadata", false,
            Messages.getString("ConnectionProperties.generateSimpleParameterMetadata"), "5.0.5", MISC_CATEGORY, Integer.MIN_VALUE);

    private boolean highAvailabilityAsBoolean = false;

    private BooleanConnectionProperty holdResultsOpenOverStatementClose = new BooleanConnectionProperty("holdResultsOpenOverStatementClose", false,
            Messages.getString("ConnectionProperties.holdRSOpenOverStmtClose"), "3.1.7", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty includeInnodbStatusInDeadlockExceptions = new BooleanConnectionProperty("includeInnodbStatusInDeadlockExceptions", false,
            Messages.getString("ConnectionProperties.includeInnodbStatusInDeadlockExceptions"), "5.0.7", DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty includeThreadDumpInDeadlockExceptions = new BooleanConnectionProperty("includeThreadDumpInDeadlockExceptions", false,
            Messages.getString("ConnectionProperties.includeThreadDumpInDeadlockExceptions"), "5.1.15", DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty includeThreadNamesAsStatementComment = new BooleanConnectionProperty("includeThreadNamesAsStatementComment", false,
            Messages.getString("ConnectionProperties.includeThreadNamesAsStatementComment"), "5.1.15", DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty ignoreNonTxTables = new BooleanConnectionProperty("ignoreNonTxTables", false,
            Messages.getString("ConnectionProperties.ignoreNonTxTables"), "3.0.9", MISC_CATEGORY, Integer.MIN_VALUE);

    private IntegerConnectionProperty initialTimeout = new IntegerConnectionProperty("initialTimeout", 2, 1, Integer.MAX_VALUE,
            Messages.getString("ConnectionProperties.initialTimeout"), "1.1", HA_CATEGORY, 5);

    private BooleanConnectionProperty isInteractiveClient = new BooleanConnectionProperty("interactiveClient", false,
            Messages.getString("ConnectionProperties.interactiveClient"), "3.1.0", CONNECTION_AND_AUTH_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty jdbcCompliantTruncation = new BooleanConnectionProperty("jdbcCompliantTruncation", true,
            Messages.getString("ConnectionProperties.jdbcCompliantTruncation"), "3.1.2", MISC_CATEGORY, Integer.MIN_VALUE);

    private boolean jdbcCompliantTruncationForReads = this.jdbcCompliantTruncation.getValueAsBoolean();

    protected MemorySizeConnectionProperty largeRowSizeThreshold = new MemorySizeConnectionProperty("largeRowSizeThreshold", 2048, 0, Integer.MAX_VALUE,
            Messages.getString("ConnectionProperties.largeRowSizeThreshold"), "5.1.1", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);

    private StringConnectionProperty loadBalanceStrategy = new StringConnectionProperty("loadBalanceStrategy", "random", null,
            Messages.getString("ConnectionProperties.loadBalanceStrategy"), "5.0.6", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);

    private StringConnectionProperty serverAffinityOrder = new StringConnectionProperty("serverAffinityOrder", "", null,
            Messages.getString("ConnectionProperties.serverAffinityOrder"), "5.1.4.", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);

    private IntegerConnectionProperty loadBalanceBlacklistTimeout = new IntegerConnectionProperty("loadBalanceBlacklistTimeout", 0, 0, Integer.MAX_VALUE,
            Messages.getString("ConnectionProperties.loadBalanceBlacklistTimeout"), "5.1.0", MISC_CATEGORY, Integer.MIN_VALUE);

    private IntegerConnectionProperty loadBalancePingTimeout = new IntegerConnectionProperty("loadBalancePingTimeout", 0, 0, Integer.MAX_VALUE,
            Messages.getString("ConnectionProperties.loadBalancePingTimeout"), "5.1.13", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty loadBalanceValidateConnectionOnSwapServer = new BooleanConnectionProperty("loadBalanceValidateConnectionOnSwapServer",
            false, Messages.getString("ConnectionProperties.loadBalanceValidateConnectionOnSwapServer"), "5.1.13", MISC_CATEGORY, Integer.MIN_VALUE);

    private StringConnectionProperty loadBalanceConnectionGroup = new StringConnectionProperty("loadBalanceConnectionGroup", null,
            Messages.getString("ConnectionProperties.loadBalanceConnectionGroup"), "5.1.13", MISC_CATEGORY, Integer.MIN_VALUE);

    private StringConnectionProperty loadBalanceExceptionChecker = new StringConnectionProperty("loadBalanceExceptionChecker",
            "com.mysql.jdbc.StandardLoadBalanceExceptionChecker", null, Messages.getString("ConnectionProperties.loadBalanceExceptionChecker"), "5.1.13",
            MISC_CATEGORY, Integer.MIN_VALUE);

    private StringConnectionProperty loadBalanceSQLStateFailover = new StringConnectionProperty("loadBalanceSQLStateFailover", null,
            Messages.getString("ConnectionProperties.loadBalanceSQLStateFailover"), "5.1.13", MISC_CATEGORY, Integer.MIN_VALUE);

    private StringConnectionProperty loadBalanceSQLExceptionSubclassFailover = new StringConnectionProperty("loadBalanceSQLExceptionSubclassFailover", null,
            Messages.getString("ConnectionProperties.loadBalanceSQLExceptionSubclassFailover"), "5.1.13", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty loadBalanceEnableJMX = new BooleanConnectionProperty("loadBalanceEnableJMX", false,
            Messages.getString("ConnectionProperties.loadBalanceEnableJMX"), "5.1.13", MISC_CATEGORY, Integer.MAX_VALUE);

    private IntegerConnectionProperty loadBalanceHostRemovalGracePeriod = new IntegerConnectionProperty("loadBalanceHostRemovalGracePeriod", 15000, 0,
            Integer.MAX_VALUE, Messages.getString("ConnectionProperties.loadBalanceHostRemovalGracePeriod"), "5.1.39", MISC_CATEGORY, Integer.MAX_VALUE);

    private StringConnectionProperty loadBalanceAutoCommitStatementRegex = new StringConnectionProperty("loadBalanceAutoCommitStatementRegex", null,
            Messages.getString("ConnectionProperties.loadBalanceAutoCommitStatementRegex"), "5.1.15", MISC_CATEGORY, Integer.MIN_VALUE);

    private IntegerConnectionProperty loadBalanceAutoCommitStatementThreshold = new IntegerConnectionProperty("loadBalanceAutoCommitStatementThreshold", 0, 0,
            Integer.MAX_VALUE, Messages.getString("ConnectionProperties.loadBalanceAutoCommitStatementThreshold"), "5.1.15", MISC_CATEGORY, Integer.MIN_VALUE);

    private StringConnectionProperty localSocketAddress = new StringConnectionProperty("localSocketAddress", null,
            Messages.getString("ConnectionProperties.localSocketAddress"), "5.0.5", CONNECTION_AND_AUTH_CATEGORY, Integer.MIN_VALUE);

    private MemorySizeConnectionProperty locatorFetchBufferSize = new MemorySizeConnectionProperty("locatorFetchBufferSize", 1024 * 1024, 0, Integer.MAX_VALUE,
            Messages.getString("ConnectionProperties.locatorFetchBufferSize"), "3.2.1", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);

    private StringConnectionProperty loggerClassName = new StringConnectionProperty("logger", STANDARD_LOGGER_NAME,
            Messages.getString("ConnectionProperties.logger", new Object[] { Log.class.getName(), STANDARD_LOGGER_NAME }), "3.1.1", DEBUGING_PROFILING_CATEGORY,
            0);

    private BooleanConnectionProperty logSlowQueries = new BooleanConnectionProperty("logSlowQueries", false,
            Messages.getString("ConnectionProperties.logSlowQueries"), "3.1.2", DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty logXaCommands = new BooleanConnectionProperty("logXaCommands", false,
            Messages.getString("ConnectionProperties.logXaCommands"), "5.0.5", DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty maintainTimeStats = new BooleanConnectionProperty("maintainTimeStats", true,
            Messages.getString("ConnectionProperties.maintainTimeStats"), "3.1.9", PERFORMANCE_CATEGORY, Integer.MAX_VALUE);

    private boolean maintainTimeStatsAsBoolean = true;

    private IntegerConnectionProperty maxQuerySizeToLog = new IntegerConnectionProperty("maxQuerySizeToLog", 2048, 0, Integer.MAX_VALUE,
            Messages.getString("ConnectionProperties.maxQuerySizeToLog"), "3.1.3", DEBUGING_PROFILING_CATEGORY, 4);

    private IntegerConnectionProperty maxReconnects = new IntegerConnectionProperty("maxReconnects", 3, 1, Integer.MAX_VALUE,
            Messages.getString("ConnectionProperties.maxReconnects"), "1.1", HA_CATEGORY, 4);

    private IntegerConnectionProperty retriesAllDown = new IntegerConnectionProperty("retriesAllDown", 120, 0, Integer.MAX_VALUE,
            Messages.getString("ConnectionProperties.retriesAllDown"), "5.1.6", HA_CATEGORY, 4);

    private IntegerConnectionProperty maxRows = new IntegerConnectionProperty("maxRows", -1, -1, Integer.MAX_VALUE,
            Messages.getString("ConnectionProperties.maxRows"), Messages.getString("ConnectionProperties.allVersions"), MISC_CATEGORY, Integer.MIN_VALUE);

    private int maxRowsAsInt = -1;

    private IntegerConnectionProperty metadataCacheSize = new IntegerConnectionProperty("metadataCacheSize", 50, 1, Integer.MAX_VALUE,
            Messages.getString("ConnectionProperties.metadataCacheSize"), "3.1.1", PERFORMANCE_CATEGORY, 5);

    private IntegerConnectionProperty netTimeoutForStreamingResults = new IntegerConnectionProperty("netTimeoutForStreamingResults", 600, 0, Integer.MAX_VALUE,
            Messages.getString("ConnectionProperties.netTimeoutForStreamingResults"), "5.1.0", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty noAccessToProcedureBodies = new BooleanConnectionProperty("noAccessToProcedureBodies", false,
            "When determining procedure parameter types for CallableStatements, and the connected user "
                    + " can't access procedure bodies through \"SHOW CREATE PROCEDURE\" or select on mysql.proc "
                    + " should the driver instead create basic metadata (all parameters reported as IN VARCHARs,"
                    + " but allowing registerOutParameter() to be called on them anyway) instead of throwing an exception?",
            "5.0.3", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty noDatetimeStringSync = new BooleanConnectionProperty("noDatetimeStringSync", false,
            Messages.getString("ConnectionProperties.noDatetimeStringSync"), "3.1.7", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty noTimezoneConversionForTimeType = new BooleanConnectionProperty("noTimezoneConversionForTimeType", false,
            Messages.getString("ConnectionProperties.noTzConversionForTimeType"), "5.0.0", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty noTimezoneConversionForDateType = new BooleanConnectionProperty("noTimezoneConversionForDateType", true,
            Messages.getString("ConnectionProperties.noTzConversionForDateType"), "5.1.35", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty cacheDefaultTimezone = new BooleanConnectionProperty("cacheDefaultTimezone", true,
            Messages.getString("ConnectionProperties.cacheDefaultTimezone"), "5.1.35", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty nullCatalogMeansCurrent = new BooleanConnectionProperty("nullCatalogMeansCurrent", true,
            Messages.getString("ConnectionProperties.nullCatalogMeansCurrent"), "3.1.8", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty nullNamePatternMatchesAll = new BooleanConnectionProperty("nullNamePatternMatchesAll", true,
            Messages.getString("ConnectionProperties.nullNamePatternMatchesAll"), "3.1.8", MISC_CATEGORY, Integer.MIN_VALUE);

    private IntegerConnectionProperty packetDebugBufferSize = new IntegerConnectionProperty("packetDebugBufferSize", 20, 0, Integer.MAX_VALUE,
            Messages.getString("ConnectionProperties.packetDebugBufferSize"), "3.1.3", DEBUGING_PROFILING_CATEGORY, 7);

    private BooleanConnectionProperty padCharsWithSpace = new BooleanConnectionProperty("padCharsWithSpace", false,
            Messages.getString("ConnectionProperties.padCharsWithSpace"), "5.0.6", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty paranoid = new BooleanConnectionProperty("paranoid", false, Messages.getString("ConnectionProperties.paranoid"), "3.0.1",
            SECURITY_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty pedantic = new BooleanConnectionProperty("pedantic", false, Messages.getString("ConnectionProperties.pedantic"), "3.0.0",
            MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty pinGlobalTxToPhysicalConnection = new BooleanConnectionProperty("pinGlobalTxToPhysicalConnection", false,
            Messages.getString("ConnectionProperties.pinGlobalTxToPhysicalConnection"), "5.0.1", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty populateInsertRowWithDefaultValues = new BooleanConnectionProperty("populateInsertRowWithDefaultValues", false,
            Messages.getString("ConnectionProperties.populateInsertRowWithDefaultValues"), "5.0.5", MISC_CATEGORY, Integer.MIN_VALUE);

    private IntegerConnectionProperty preparedStatementCacheSize = new IntegerConnectionProperty("prepStmtCacheSize", 25, 0, Integer.MAX_VALUE,
            Messages.getString("ConnectionProperties.prepStmtCacheSize"), "3.0.10", PERFORMANCE_CATEGORY, 10);

    private IntegerConnectionProperty preparedStatementCacheSqlLimit = new IntegerConnectionProperty("prepStmtCacheSqlLimit", 256, 1, Integer.MAX_VALUE,
            Messages.getString("ConnectionProperties.prepStmtCacheSqlLimit"), "3.0.10", PERFORMANCE_CATEGORY, 11);

    private StringConnectionProperty parseInfoCacheFactory = new StringConnectionProperty("parseInfoCacheFactory", PerConnectionLRUFactory.class.getName(),
            Messages.getString("ConnectionProperties.parseInfoCacheFactory"), "5.1.1", PERFORMANCE_CATEGORY, 12);

    private BooleanConnectionProperty processEscapeCodesForPrepStmts = new BooleanConnectionProperty("processEscapeCodesForPrepStmts", true,
            Messages.getString("ConnectionProperties.processEscapeCodesForPrepStmts"), "3.1.12", MISC_CATEGORY, Integer.MIN_VALUE);

    private StringConnectionProperty profilerEventHandler = new StringConnectionProperty("profilerEventHandler",
            "com.mysql.jdbc.profiler.LoggingProfilerEventHandler", Messages.getString("ConnectionProperties.profilerEventHandler"), "5.1.6",
            DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE);

    private StringConnectionProperty profileSql = new StringConnectionProperty("profileSql", null,
            Messages.getString("ConnectionProperties.profileSqlDeprecated"), "2.0.14", DEBUGING_PROFILING_CATEGORY, 3);

    private BooleanConnectionProperty profileSQL = new BooleanConnectionProperty("profileSQL", false, Messages.getString("ConnectionProperties.profileSQL"),
            "3.1.0", DEBUGING_PROFILING_CATEGORY, 1);

    private boolean profileSQLAsBoolean = false;

    private StringConnectionProperty propertiesTransform = new StringConnectionProperty(NonRegisteringDriver.PROPERTIES_TRANSFORM_KEY, null,
            Messages.getString("ConnectionProperties.connectionPropertiesTransform"), "3.1.4", CONNECTION_AND_AUTH_CATEGORY, Integer.MIN_VALUE);

    private IntegerConnectionProperty queriesBeforeRetryMaster = new IntegerConnectionProperty("queriesBeforeRetryMaster", 50, 0, Integer.MAX_VALUE,
            Messages.getString("ConnectionProperties.queriesBeforeRetryMaster"), "3.0.2", HA_CATEGORY, 7);

    private BooleanConnectionProperty queryTimeoutKillsConnection = new BooleanConnectionProperty("queryTimeoutKillsConnection", false,
            Messages.getString("ConnectionProperties.queryTimeoutKillsConnection"), "5.1.9", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty reconnectAtTxEnd = new BooleanConnectionProperty("reconnectAtTxEnd", false,
            Messages.getString("ConnectionProperties.reconnectAtTxEnd"), "3.0.10", HA_CATEGORY, 4);

    private boolean reconnectTxAtEndAsBoolean = false;

    private BooleanConnectionProperty relaxAutoCommit = new BooleanConnectionProperty("relaxAutoCommit", false,
            Messages.getString("ConnectionProperties.relaxAutoCommit"), "2.0.13", MISC_CATEGORY, Integer.MIN_VALUE);

    private IntegerConnectionProperty reportMetricsIntervalMillis = new IntegerConnectionProperty("reportMetricsIntervalMillis", 30000, 0, Integer.MAX_VALUE,
            Messages.getString("ConnectionProperties.reportMetricsIntervalMillis"), "3.1.2", DEBUGING_PROFILING_CATEGORY, 3);

    private BooleanConnectionProperty requireSSL = new BooleanConnectionProperty("requireSSL", false, Messages.getString("ConnectionProperties.requireSSL"),
            "3.1.0", SECURITY_CATEGORY, 3);

    private StringConnectionProperty resourceId = new StringConnectionProperty("resourceId", null, Messages.getString("ConnectionProperties.resourceId"),
            "5.0.1", HA_CATEGORY, Integer.MIN_VALUE);

    private IntegerConnectionProperty resultSetSizeThreshold = new IntegerConnectionProperty("resultSetSizeThreshold", 100,
            Messages.getString("ConnectionProperties.resultSetSizeThreshold"), "5.0.5", DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty retainStatementAfterResultSetClose = new BooleanConnectionProperty("retainStatementAfterResultSetClose", false,
            Messages.getString("ConnectionProperties.retainStatementAfterResultSetClose"), "3.1.11", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty rewriteBatchedStatements = new BooleanConnectionProperty("rewriteBatchedStatements", false,
            Messages.getString("ConnectionProperties.rewriteBatchedStatements"), "3.1.13", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty rollbackOnPooledClose = new BooleanConnectionProperty("rollbackOnPooledClose", true,
            Messages.getString("ConnectionProperties.rollbackOnPooledClose"), "3.0.15", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty roundRobinLoadBalance = new BooleanConnectionProperty("roundRobinLoadBalance", false,
            Messages.getString("ConnectionProperties.roundRobinLoadBalance"), "3.1.2", HA_CATEGORY, 5);

    private BooleanConnectionProperty runningCTS13 = new BooleanConnectionProperty("runningCTS13", false,
            Messages.getString("ConnectionProperties.runningCTS13"), "3.1.7", MISC_CATEGORY, Integer.MIN_VALUE);

    private IntegerConnectionProperty secondsBeforeRetryMaster = new IntegerConnectionProperty("secondsBeforeRetryMaster", 30, 0, Integer.MAX_VALUE,
            Messages.getString("ConnectionProperties.secondsBeforeRetryMaster"), "3.0.2", HA_CATEGORY, 8);

    private IntegerConnectionProperty selfDestructOnPingSecondsLifetime = new IntegerConnectionProperty("selfDestructOnPingSecondsLifetime", 0, 0,
            Integer.MAX_VALUE, Messages.getString("ConnectionProperties.selfDestructOnPingSecondsLifetime"), "5.1.6", HA_CATEGORY, Integer.MAX_VALUE);

    private IntegerConnectionProperty selfDestructOnPingMaxOperations = new IntegerConnectionProperty("selfDestructOnPingMaxOperations", 0, 0,
            Integer.MAX_VALUE, Messages.getString("ConnectionProperties.selfDestructOnPingMaxOperations"), "5.1.6", HA_CATEGORY, Integer.MAX_VALUE);

    private BooleanConnectionProperty replicationEnableJMX = new BooleanConnectionProperty("replicationEnableJMX", false,
            Messages.getString("ConnectionProperties.loadBalanceEnableJMX"), "5.1.27", HA_CATEGORY, Integer.MAX_VALUE);

    private StringConnectionProperty serverTimezone = new StringConnectionProperty("serverTimezone", null,
            Messages.getString("ConnectionProperties.serverTimezone"), "3.0.2", MISC_CATEGORY, Integer.MIN_VALUE);

    private StringConnectionProperty sessionVariables = new StringConnectionProperty("sessionVariables", null,
            Messages.getString("ConnectionProperties.sessionVariables"), "3.1.8", MISC_CATEGORY, Integer.MAX_VALUE);

    private IntegerConnectionProperty slowQueryThresholdMillis = new IntegerConnectionProperty("slowQueryThresholdMillis", 2000, 0, Integer.MAX_VALUE,
            Messages.getString("ConnectionProperties.slowQueryThresholdMillis"), "3.1.2", DEBUGING_PROFILING_CATEGORY, 9);

    private LongConnectionProperty slowQueryThresholdNanos = new LongConnectionProperty("slowQueryThresholdNanos", 0,
            Messages.getString("ConnectionProperties.slowQueryThresholdNanos"), "5.0.7", DEBUGING_PROFILING_CATEGORY, 10);

    private StringConnectionProperty socketFactoryClassName = new StringConnectionProperty("socketFactory", StandardSocketFactory.class.getName(),
            Messages.getString("ConnectionProperties.socketFactory"), "3.0.3", CONNECTION_AND_AUTH_CATEGORY, 4);

    private StringConnectionProperty socksProxyHost = new StringConnectionProperty("socksProxyHost", null,
            Messages.getString("ConnectionProperties.socksProxyHost"), "5.1.34", NETWORK_CATEGORY, 1);

    private IntegerConnectionProperty socksProxyPort = new IntegerConnectionProperty("socksProxyPort", SocksProxySocketFactory.SOCKS_DEFAULT_PORT, 0, 65535,
            Messages.getString("ConnectionProperties.socksProxyPort"), "5.1.34", NETWORK_CATEGORY, 2);

    private IntegerConnectionProperty socketTimeout = new IntegerConnectionProperty("socketTimeout", 0, 0, Integer.MAX_VALUE,
            Messages.getString("ConnectionProperties.socketTimeout"), "3.0.1", CONNECTION_AND_AUTH_CATEGORY, 10);

    private StringConnectionProperty statementInterceptors = new StringConnectionProperty("statementInterceptors", null,
            Messages.getString("ConnectionProperties.statementInterceptors"), "5.1.1", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty strictFloatingPoint = new BooleanConnectionProperty("strictFloatingPoint", false,
            Messages.getString("ConnectionProperties.strictFloatingPoint"), "3.0.0", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty strictUpdates = new BooleanConnectionProperty("strictUpdates", true,
            Messages.getString("ConnectionProperties.strictUpdates"), "3.0.4", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty overrideSupportsIntegrityEnhancementFacility = new BooleanConnectionProperty(
            "overrideSupportsIntegrityEnhancementFacility", false, Messages.getString("ConnectionProperties.overrideSupportsIEF"), "3.1.12", MISC_CATEGORY,
            Integer.MIN_VALUE);

    private BooleanConnectionProperty tcpNoDelay = new BooleanConnectionProperty(StandardSocketFactory.TCP_NO_DELAY_PROPERTY_NAME,
            Boolean.valueOf(StandardSocketFactory.TCP_NO_DELAY_DEFAULT_VALUE).booleanValue(), Messages.getString("ConnectionProperties.tcpNoDelay"), "5.0.7",
            NETWORK_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty tcpKeepAlive = new BooleanConnectionProperty(StandardSocketFactory.TCP_KEEP_ALIVE_PROPERTY_NAME,
            Boolean.valueOf(StandardSocketFactory.TCP_KEEP_ALIVE_DEFAULT_VALUE).booleanValue(), Messages.getString("ConnectionProperties.tcpKeepAlive"),
            "5.0.7", NETWORK_CATEGORY, Integer.MIN_VALUE);

    private IntegerConnectionProperty tcpRcvBuf = new IntegerConnectionProperty(StandardSocketFactory.TCP_RCV_BUF_PROPERTY_NAME,
            Integer.parseInt(StandardSocketFactory.TCP_RCV_BUF_DEFAULT_VALUE), 0, Integer.MAX_VALUE, Messages.getString("ConnectionProperties.tcpSoRcvBuf"),
            "5.0.7", NETWORK_CATEGORY, Integer.MIN_VALUE);

    private IntegerConnectionProperty tcpSndBuf = new IntegerConnectionProperty(StandardSocketFactory.TCP_SND_BUF_PROPERTY_NAME,
            Integer.parseInt(StandardSocketFactory.TCP_SND_BUF_DEFAULT_VALUE), 0, Integer.MAX_VALUE, Messages.getString("ConnectionProperties.tcpSoSndBuf"),
            "5.0.7", NETWORK_CATEGORY, Integer.MIN_VALUE);

    private IntegerConnectionProperty tcpTrafficClass = new IntegerConnectionProperty(StandardSocketFactory.TCP_TRAFFIC_CLASS_PROPERTY_NAME,
            Integer.parseInt(StandardSocketFactory.TCP_TRAFFIC_CLASS_DEFAULT_VALUE), 0, 255, Messages.getString("ConnectionProperties.tcpTrafficClass"),
            "5.0.7", NETWORK_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty tinyInt1isBit = new BooleanConnectionProperty("tinyInt1isBit", true,
            Messages.getString("ConnectionProperties.tinyInt1isBit"), "3.0.16", MISC_CATEGORY, Integer.MIN_VALUE);

    protected BooleanConnectionProperty traceProtocol = new BooleanConnectionProperty("traceProtocol", false,
            Messages.getString("ConnectionProperties.traceProtocol"), "3.1.2", DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty treatUtilDateAsTimestamp = new BooleanConnectionProperty("treatUtilDateAsTimestamp", true,
            Messages.getString("ConnectionProperties.treatUtilDateAsTimestamp"), "5.0.5", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty transformedBitIsBoolean = new BooleanConnectionProperty("transformedBitIsBoolean", false,
            Messages.getString("ConnectionProperties.transformedBitIsBoolean"), "3.1.9", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty useBlobToStoreUTF8OutsideBMP = new BooleanConnectionProperty("useBlobToStoreUTF8OutsideBMP", false,
            Messages.getString("ConnectionProperties.useBlobToStoreUTF8OutsideBMP"), "5.1.3", MISC_CATEGORY, 128);

    private StringConnectionProperty utf8OutsideBmpExcludedColumnNamePattern = new StringConnectionProperty("utf8OutsideBmpExcludedColumnNamePattern", null,
            Messages.getString("ConnectionProperties.utf8OutsideBmpExcludedColumnNamePattern"), "5.1.3", MISC_CATEGORY, 129);

    private StringConnectionProperty utf8OutsideBmpIncludedColumnNamePattern = new StringConnectionProperty("utf8OutsideBmpIncludedColumnNamePattern", null,
            Messages.getString("ConnectionProperties.utf8OutsideBmpIncludedColumnNamePattern"), "5.1.3", MISC_CATEGORY, 129);

    private BooleanConnectionProperty useCompression = new BooleanConnectionProperty("useCompression", false,
            Messages.getString("ConnectionProperties.useCompression"), "3.0.17", CONNECTION_AND_AUTH_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty useColumnNamesInFindColumn = new BooleanConnectionProperty("useColumnNamesInFindColumn", false,
            Messages.getString("ConnectionProperties.useColumnNamesInFindColumn"), "5.1.7", MISC_CATEGORY, Integer.MAX_VALUE);

    private StringConnectionProperty useConfigs = new StringConnectionProperty("useConfigs", null, Messages.getString("ConnectionProperties.useConfigs"),
            "3.1.5", CONNECTION_AND_AUTH_CATEGORY, Integer.MAX_VALUE);

    private BooleanConnectionProperty useCursorFetch = new BooleanConnectionProperty("useCursorFetch", false,
            Messages.getString("ConnectionProperties.useCursorFetch"), "5.0.0", PERFORMANCE_CATEGORY, Integer.MAX_VALUE);

    private BooleanConnectionProperty useDynamicCharsetInfo = new BooleanConnectionProperty("useDynamicCharsetInfo", true,
            Messages.getString("ConnectionProperties.useDynamicCharsetInfo"), "5.0.6", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty useDirectRowUnpack = new BooleanConnectionProperty("useDirectRowUnpack", true,
            "Use newer result set row unpacking code that skips a copy from network buffers "
                    + " to a MySQL packet instance and instead reads directly into the result set row data buffers.",
            "5.1.1", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty useFastIntParsing = new BooleanConnectionProperty("useFastIntParsing", true,
            Messages.getString("ConnectionProperties.useFastIntParsing"), "3.1.4", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty useFastDateParsing = new BooleanConnectionProperty("useFastDateParsing", true,
            Messages.getString("ConnectionProperties.useFastDateParsing"), "5.0.5", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty useHostsInPrivileges = new BooleanConnectionProperty("useHostsInPrivileges", true,
            Messages.getString("ConnectionProperties.useHostsInPrivileges"), "3.0.2", MISC_CATEGORY, Integer.MIN_VALUE);
    private BooleanConnectionProperty useInformationSchema = new BooleanConnectionProperty("useInformationSchema", false,
            Messages.getString("ConnectionProperties.useInformationSchema"), "5.0.0", MISC_CATEGORY, Integer.MIN_VALUE);
    private BooleanConnectionProperty useJDBCCompliantTimezoneShift = new BooleanConnectionProperty("useJDBCCompliantTimezoneShift", false,
            Messages.getString("ConnectionProperties.useJDBCCompliantTimezoneShift"), "5.0.0", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty useLocalSessionState = new BooleanConnectionProperty("useLocalSessionState", false,
            Messages.getString("ConnectionProperties.useLocalSessionState"), "3.1.7", PERFORMANCE_CATEGORY, 5);

    private BooleanConnectionProperty useLocalTransactionState = new BooleanConnectionProperty("useLocalTransactionState", false,
            Messages.getString("ConnectionProperties.useLocalTransactionState"), "5.1.7", PERFORMANCE_CATEGORY, 6);

    private BooleanConnectionProperty useLegacyDatetimeCode = new BooleanConnectionProperty("useLegacyDatetimeCode", true,
            Messages.getString("ConnectionProperties.useLegacyDatetimeCode"), "5.1.6", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty sendFractionalSeconds = new BooleanConnectionProperty("sendFractionalSeconds", true,
            Messages.getString("ConnectionProperties.sendFractionalSeconds"), "5.1.37", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty useNanosForElapsedTime = new BooleanConnectionProperty("useNanosForElapsedTime", false,
            Messages.getString("ConnectionProperties.useNanosForElapsedTime"), "5.0.7", DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty useOldAliasMetadataBehavior = new BooleanConnectionProperty("useOldAliasMetadataBehavior", false,
            Messages.getString("ConnectionProperties.useOldAliasMetadataBehavior"), "5.0.4", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty useOldUTF8Behavior = new BooleanConnectionProperty("useOldUTF8Behavior", false,
            Messages.getString("ConnectionProperties.useOldUtf8Behavior"), "3.1.6", MISC_CATEGORY, Integer.MIN_VALUE);

    private boolean useOldUTF8BehaviorAsBoolean = false;

    private BooleanConnectionProperty useOnlyServerErrorMessages = new BooleanConnectionProperty("useOnlyServerErrorMessages", true,
            Messages.getString("ConnectionProperties.useOnlyServerErrorMessages"), "3.0.15", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty useReadAheadInput = new BooleanConnectionProperty("useReadAheadInput", true,
            Messages.getString("ConnectionProperties.useReadAheadInput"), "3.1.5", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty useSqlStateCodes = new BooleanConnectionProperty("useSqlStateCodes", true,
            Messages.getString("ConnectionProperties.useSqlStateCodes"), "3.1.3", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty useSSL = new BooleanConnectionProperty("useSSL", false, Messages.getString("ConnectionProperties.useSSL"), "3.0.2",
            SECURITY_CATEGORY, 2);

    private BooleanConnectionProperty useSSPSCompatibleTimezoneShift = new BooleanConnectionProperty("useSSPSCompatibleTimezoneShift", false,
            Messages.getString("ConnectionProperties.useSSPSCompatibleTimezoneShift"), "5.0.5", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty useStreamLengthsInPrepStmts = new BooleanConnectionProperty("useStreamLengthsInPrepStmts", true,
            Messages.getString("ConnectionProperties.useStreamLengthsInPrepStmts"), "3.0.2", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty useTimezone = new BooleanConnectionProperty("useTimezone", false, Messages.getString("ConnectionProperties.useTimezone"),
            "3.0.2", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty useUltraDevWorkAround = new BooleanConnectionProperty("ultraDevHack", false,
            Messages.getString("ConnectionProperties.ultraDevHack"), "2.0.3", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty useUnbufferedInput = new BooleanConnectionProperty("useUnbufferedInput", true,
            Messages.getString("ConnectionProperties.useUnbufferedInput"), "3.0.11", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty useUnicode = new BooleanConnectionProperty("useUnicode", true, Messages.getString("ConnectionProperties.useUnicode"),
            "1.1g", MISC_CATEGORY, 0);

    // Cache these values, they are 'hot'
    private boolean useUnicodeAsBoolean = true;

    private BooleanConnectionProperty useUsageAdvisor = new BooleanConnectionProperty("useUsageAdvisor", false,
            Messages.getString("ConnectionProperties.useUsageAdvisor"), "3.1.1", DEBUGING_PROFILING_CATEGORY, 10);

    private boolean useUsageAdvisorAsBoolean = false;

    private BooleanConnectionProperty yearIsDateType = new BooleanConnectionProperty("yearIsDateType", true,
            Messages.getString("ConnectionProperties.yearIsDateType"), "3.1.9", MISC_CATEGORY, Integer.MIN_VALUE);

    private StringConnectionProperty zeroDateTimeBehavior = new StringConnectionProperty("zeroDateTimeBehavior", ZERO_DATETIME_BEHAVIOR_EXCEPTION,
            new String[] { ZERO_DATETIME_BEHAVIOR_EXCEPTION, ZERO_DATETIME_BEHAVIOR_ROUND, ZERO_DATETIME_BEHAVIOR_CONVERT_TO_NULL },
            Messages.getString("ConnectionProperties.zeroDateTimeBehavior",
                    new Object[] { ZERO_DATETIME_BEHAVIOR_EXCEPTION, ZERO_DATETIME_BEHAVIOR_ROUND, ZERO_DATETIME_BEHAVIOR_CONVERT_TO_NULL }),
            "3.1.4", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty useJvmCharsetConverters = new BooleanConnectionProperty("useJvmCharsetConverters", false,
            Messages.getString("ConnectionProperties.useJvmCharsetConverters"), "5.0.1", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty useGmtMillisForDatetimes = new BooleanConnectionProperty("useGmtMillisForDatetimes", false,
            Messages.getString("ConnectionProperties.useGmtMillisForDatetimes"), "3.1.12", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty dumpMetadataOnColumnNotFound = new BooleanConnectionProperty("dumpMetadataOnColumnNotFound", false,
            Messages.getString("ConnectionProperties.dumpMetadataOnColumnNotFound"), "3.1.13", DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE);

    // SSL Options

    private StringConnectionProperty clientCertificateKeyStoreUrl = new StringConnectionProperty("clientCertificateKeyStoreUrl", null,
            Messages.getString("ConnectionProperties.clientCertificateKeyStoreUrl"), "5.1.0", SECURITY_CATEGORY, 5);

    private StringConnectionProperty trustCertificateKeyStoreUrl = new StringConnectionProperty("trustCertificateKeyStoreUrl", null,
            Messages.getString("ConnectionProperties.trustCertificateKeyStoreUrl"), "5.1.0", SECURITY_CATEGORY, 8);

    private StringConnectionProperty clientCertificateKeyStoreType = new StringConnectionProperty("clientCertificateKeyStoreType", "JKS",
            Messages.getString("ConnectionProperties.clientCertificateKeyStoreType"), "5.1.0", SECURITY_CATEGORY, 6);

    private StringConnectionProperty clientCertificateKeyStorePassword = new StringConnectionProperty("clientCertificateKeyStorePassword", null,
            Messages.getString("ConnectionProperties.clientCertificateKeyStorePassword"), "5.1.0", SECURITY_CATEGORY, 7);

    private StringConnectionProperty trustCertificateKeyStoreType = new StringConnectionProperty("trustCertificateKeyStoreType", "JKS",
            Messages.getString("ConnectionProperties.trustCertificateKeyStoreType"), "5.1.0", SECURITY_CATEGORY, 9);

    private StringConnectionProperty trustCertificateKeyStorePassword = new StringConnectionProperty("trustCertificateKeyStorePassword", null,
            Messages.getString("ConnectionProperties.trustCertificateKeyStorePassword"), "5.1.0", SECURITY_CATEGORY, 10);

    private BooleanConnectionProperty verifyServerCertificate = new BooleanConnectionProperty("verifyServerCertificate", true,
            Messages.getString("ConnectionProperties.verifyServerCertificate"), "5.1.6", SECURITY_CATEGORY, 4);

    private BooleanConnectionProperty useAffectedRows = new BooleanConnectionProperty("useAffectedRows", false,
            Messages.getString("ConnectionProperties.useAffectedRows"), "5.1.7", MISC_CATEGORY, Integer.MIN_VALUE);

    private StringConnectionProperty passwordCharacterEncoding = new StringConnectionProperty("passwordCharacterEncoding", null,
            Messages.getString("ConnectionProperties.passwordCharacterEncoding"), "5.1.7", SECURITY_CATEGORY, Integer.MIN_VALUE);

    private IntegerConnectionProperty maxAllowedPacket = new IntegerConnectionProperty("maxAllowedPacket", -1,
            Messages.getString("ConnectionProperties.maxAllowedPacket"), "5.1.8", NETWORK_CATEGORY, Integer.MIN_VALUE);

    private StringConnectionProperty authenticationPlugins = new StringConnectionProperty("authenticationPlugins", null,
            Messages.getString("ConnectionProperties.authenticationPlugins"), "5.1.19", CONNECTION_AND_AUTH_CATEGORY, Integer.MIN_VALUE);

    private StringConnectionProperty disabledAuthenticationPlugins = new StringConnectionProperty("disabledAuthenticationPlugins", null,
            Messages.getString("ConnectionProperties.disabledAuthenticationPlugins"), "5.1.19", CONNECTION_AND_AUTH_CATEGORY, Integer.MIN_VALUE);

    private StringConnectionProperty defaultAuthenticationPlugin = new StringConnectionProperty("defaultAuthenticationPlugin",
            "com.mysql.jdbc.authentication.MysqlNativePasswordPlugin", Messages.getString("ConnectionProperties.defaultAuthenticationPlugin"), "5.1.19",
            CONNECTION_AND_AUTH_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty disconnectOnExpiredPasswords = new BooleanConnectionProperty("disconnectOnExpiredPasswords", true,
            Messages.getString("ConnectionProperties.disconnectOnExpiredPasswords"), "5.1.23", CONNECTION_AND_AUTH_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty getProceduresReturnsFunctions = new BooleanConnectionProperty("getProceduresReturnsFunctions", true,
            Messages.getString("ConnectionProperties.getProceduresReturnsFunctions"), "5.1.26", MISC_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty detectCustomCollations = new BooleanConnectionProperty("detectCustomCollations", false,
            Messages.getString("ConnectionProperties.detectCustomCollations"), "5.1.29", MISC_CATEGORY, Integer.MIN_VALUE);

    private StringConnectionProperty serverRSAPublicKeyFile = new StringConnectionProperty("serverRSAPublicKeyFile", null,
            Messages.getString("ConnectionProperties.serverRSAPublicKeyFile"), "5.1.31", SECURITY_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty allowPublicKeyRetrieval = new BooleanConnectionProperty("allowPublicKeyRetrieval", false,
            Messages.getString("ConnectionProperties.allowPublicKeyRetrieval"), "5.1.31", SECURITY_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty dontCheckOnDuplicateKeyUpdateInSQL = new BooleanConnectionProperty("dontCheckOnDuplicateKeyUpdateInSQL", false,
            Messages.getString("ConnectionProperties.dontCheckOnDuplicateKeyUpdateInSQL"), "5.1.32", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);

    private BooleanConnectionProperty readOnlyPropagatesToServer = new BooleanConnectionProperty("readOnlyPropagatesToServer", true,
            Messages.getString("ConnectionProperties.readOnlyPropagatesToServer"), "5.1.35", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);

    private StringConnectionProperty enabledSSLCipherSuites = new StringConnectionProperty("enabledSSLCipherSuites", null,
            Messages.getString("ConnectionProperties.enabledSSLCipherSuites"), "5.1.35", SECURITY_CATEGORY, 11);

    private BooleanConnectionProperty enableEscapeProcessing = new BooleanConnectionProperty("enableEscapeProcessing", true,
            Messages.getString("ConnectionProperties.enableEscapeProcessing"), "5.1.37", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);

    protected DriverPropertyInfo[] exposeAsDriverPropertyInfoInternal(Properties info, int slotsToReserve) throws SQLException {
        initializeProperties(info);

        int numProperties = PROPERTY_LIST.size();

        int listSize = numProperties + slotsToReserve;

        DriverPropertyInfo[] driverProperties = new DriverPropertyInfo[listSize];

        for (int i = slotsToReserve; i < listSize; i++) {
            java.lang.reflect.Field propertyField = PROPERTY_LIST.get(i - slotsToReserve);

            try {
                ConnectionProperty propToExpose = (ConnectionProperty) propertyField.get(this);

                if (info != null) {
                    propToExpose.initializeFrom(info, getExceptionInterceptor());
                }

                driverProperties[i] = propToExpose.getAsDriverPropertyInfo();
            } catch (IllegalAccessException iae) {
                throw SQLError.createSQLException(Messages.getString("ConnectionProperties.InternalPropertiesFailure"), SQLError.SQL_STATE_GENERAL_ERROR,
                        getExceptionInterceptor());
            }
        }

        return driverProperties;
    }

    protected Properties exposeAsProperties(Properties info) throws SQLException {
        if (info == null) {
            info = new Properties();
        }

        int numPropertiesToSet = PROPERTY_LIST.size();

        for (int i = 0; i < numPropertiesToSet; i++) {
            java.lang.reflect.Field propertyField = PROPERTY_LIST.get(i);

            try {
                ConnectionProperty propToGet = (ConnectionProperty) propertyField.get(this);

                Object propValue = propToGet.getValueAsObject();

                if (propValue != null) {
                    info.setProperty(propToGet.getPropertyName(), propValue.toString());
                }
            } catch (IllegalAccessException iae) {
                throw SQLError.createSQLException("Internal properties failure", SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
            }
        }

        return info;
    }

    class XmlMap {
        protected Map<Integer, Map<String, ConnectionProperty>> ordered = new TreeMap<Integer, Map<String, ConnectionProperty>>();
        protected Map<String, ConnectionProperty> alpha = new TreeMap<String, ConnectionProperty>();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#exposeAsXml()
     */
    public String exposeAsXml() throws SQLException {
        StringBuilder xmlBuf = new StringBuilder();
        xmlBuf.append("<ConnectionProperties>");

        int numPropertiesToSet = PROPERTY_LIST.size();

        int numCategories = PROPERTY_CATEGORIES.length;

        Map<String, XmlMap> propertyListByCategory = new HashMap<String, XmlMap>();

        for (int i = 0; i < numCategories; i++) {
            propertyListByCategory.put(PROPERTY_CATEGORIES[i], new XmlMap());
        }

        //
        // The following properties are not exposed as 'normal' properties, but they are settable nonetheless, so we need to have them documented, make sure
        // that they sort 'first' as #1 and #2 in the category
        //
        StringConnectionProperty userProp = new StringConnectionProperty(NonRegisteringDriver.USER_PROPERTY_KEY, null,
                Messages.getString("ConnectionProperties.Username"), Messages.getString("ConnectionProperties.allVersions"), CONNECTION_AND_AUTH_CATEGORY,
                Integer.MIN_VALUE + 1);
        StringConnectionProperty passwordProp = new StringConnectionProperty(NonRegisteringDriver.PASSWORD_PROPERTY_KEY, null,
                Messages.getString("ConnectionProperties.Password"), Messages.getString("ConnectionProperties.allVersions"), CONNECTION_AND_AUTH_CATEGORY,
                Integer.MIN_VALUE + 2);

        XmlMap connectionSortMaps = propertyListByCategory.get(CONNECTION_AND_AUTH_CATEGORY);
        TreeMap<String, ConnectionProperty> userMap = new TreeMap<String, ConnectionProperty>();
        userMap.put(userProp.getPropertyName(), userProp);

        connectionSortMaps.ordered.put(Integer.valueOf(userProp.getOrder()), userMap);

        TreeMap<String, ConnectionProperty> passwordMap = new TreeMap<String, ConnectionProperty>();
        passwordMap.put(passwordProp.getPropertyName(), passwordProp);

        connectionSortMaps.ordered.put(new Integer(passwordProp.getOrder()), passwordMap);

        try {
            for (int i = 0; i < numPropertiesToSet; i++) {
                java.lang.reflect.Field propertyField = PROPERTY_LIST.get(i);
                ConnectionProperty propToGet = (ConnectionProperty) propertyField.get(this);
                XmlMap sortMaps = propertyListByCategory.get(propToGet.getCategoryName());
                int orderInCategory = propToGet.getOrder();

                if (orderInCategory == Integer.MIN_VALUE) {
                    sortMaps.alpha.put(propToGet.getPropertyName(), propToGet);
                } else {
                    Integer order = Integer.valueOf(orderInCategory);
                    Map<String, ConnectionProperty> orderMap = sortMaps.ordered.get(order);

                    if (orderMap == null) {
                        orderMap = new TreeMap<String, ConnectionProperty>();
                        sortMaps.ordered.put(order, orderMap);
                    }

                    orderMap.put(propToGet.getPropertyName(), propToGet);
                }
            }

            for (int j = 0; j < numCategories; j++) {
                XmlMap sortMaps = propertyListByCategory.get(PROPERTY_CATEGORIES[j]);

                xmlBuf.append("\n <PropertyCategory name=\"");
                xmlBuf.append(PROPERTY_CATEGORIES[j]);
                xmlBuf.append("\">");

                for (Map<String, ConnectionProperty> orderedEl : sortMaps.ordered.values()) {
                    for (ConnectionProperty propToGet : orderedEl.values()) {
                        xmlBuf.append("\n  <Property name=\"");
                        xmlBuf.append(propToGet.getPropertyName());
                        xmlBuf.append("\" required=\"");
                        xmlBuf.append(propToGet.required ? "Yes" : "No");

                        xmlBuf.append("\" default=\"");

                        if (propToGet.getDefaultValue() != null) {
                            xmlBuf.append(propToGet.getDefaultValue());
                        }

                        xmlBuf.append("\" sortOrder=\"");
                        xmlBuf.append(propToGet.getOrder());
                        xmlBuf.append("\" since=\"");
                        xmlBuf.append(propToGet.sinceVersion);
                        xmlBuf.append("\">\n");
                        xmlBuf.append("    ");
                        String escapedDescription = propToGet.description;
                        escapedDescription = escapedDescription.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");

                        xmlBuf.append(escapedDescription);
                        xmlBuf.append("\n  </Property>");
                    }
                }

                for (ConnectionProperty propToGet : sortMaps.alpha.values()) {
                    xmlBuf.append("\n  <Property name=\"");
                    xmlBuf.append(propToGet.getPropertyName());
                    xmlBuf.append("\" required=\"");
                    xmlBuf.append(propToGet.required ? "Yes" : "No");

                    xmlBuf.append("\" default=\"");

                    if (propToGet.getDefaultValue() != null) {
                        xmlBuf.append(propToGet.getDefaultValue());
                    }

                    xmlBuf.append("\" sortOrder=\"alpha\" since=\"");
                    xmlBuf.append(propToGet.sinceVersion);
                    xmlBuf.append("\">\n");
                    xmlBuf.append("    ");
                    xmlBuf.append(propToGet.description);
                    xmlBuf.append("\n  </Property>");
                }

                xmlBuf.append("\n </PropertyCategory>");
            }
        } catch (IllegalAccessException iae) {
            throw SQLError.createSQLException("Internal properties failure", SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
        }

        xmlBuf.append("\n</ConnectionProperties>");

        return xmlBuf.toString();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getAllowLoadLocalInfile()
     */
    public boolean getAllowLoadLocalInfile() {
        return this.allowLoadLocalInfile.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getAllowMultiQueries()
     */
    public boolean getAllowMultiQueries() {
        return this.allowMultiQueries.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getAllowNanAndInf()
     */
    public boolean getAllowNanAndInf() {
        return this.allowNanAndInf.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getAllowUrlInLocalInfile()
     */
    public boolean getAllowUrlInLocalInfile() {
        return this.allowUrlInLocalInfile.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getAlwaysSendSetIsolation()
     */
    public boolean getAlwaysSendSetIsolation() {
        return this.alwaysSendSetIsolation.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getAutoDeserialize()
     */
    public boolean getAutoDeserialize() {
        return this.autoDeserialize.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getAutoGenerateTestcaseScript()
     */
    public boolean getAutoGenerateTestcaseScript() {
        return this.autoGenerateTestcaseScriptAsBoolean;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getAutoReconnectForPools()
     */
    public boolean getAutoReconnectForPools() {
        return this.autoReconnectForPoolsAsBoolean;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getBlobSendChunkSize()
     */
    public int getBlobSendChunkSize() {
        return this.blobSendChunkSize.getValueAsInt();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getCacheCallableStatements()
     */
    public boolean getCacheCallableStatements() {
        return this.cacheCallableStatements.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getCachePreparedStatements()
     */
    public boolean getCachePreparedStatements() {
        return ((Boolean) this.cachePreparedStatements.getValueAsObject()).booleanValue();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getCacheResultSetMetadata()
     */
    public boolean getCacheResultSetMetadata() {
        return this.cacheResultSetMetaDataAsBoolean;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getCacheServerConfiguration()
     */
    public boolean getCacheServerConfiguration() {
        return this.cacheServerConfiguration.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getCallableStatementCacheSize()
     */
    public int getCallableStatementCacheSize() {
        return this.callableStatementCacheSize.getValueAsInt();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getCapitalizeTypeNames()
     */
    public boolean getCapitalizeTypeNames() {
        return this.capitalizeTypeNames.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getCharacterSetResults()
     */
    public String getCharacterSetResults() {
        return this.characterSetResults.getValueAsString();
    }

    public String getConnectionAttributes() {
        return this.connectionAttributes.getValueAsString();
    }

    public void setConnectionAttributes(String val) {
        this.connectionAttributes.setValue(val);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getClobberStreamingResults()
     */
    public boolean getClobberStreamingResults() {
        return this.clobberStreamingResults.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getClobCharacterEncoding()
     */
    public String getClobCharacterEncoding() {
        return this.clobCharacterEncoding.getValueAsString();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getConnectionCollation()
     */
    public String getConnectionCollation() {
        return this.connectionCollation.getValueAsString();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getConnectTimeout()
     */
    public int getConnectTimeout() {
        return this.connectTimeout.getValueAsInt();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getContinueBatchOnError()
     */
    public boolean getContinueBatchOnError() {
        return this.continueBatchOnError.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getCreateDatabaseIfNotExist()
     */
    public boolean getCreateDatabaseIfNotExist() {
        return this.createDatabaseIfNotExist.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getDefaultFetchSize()
     */
    public int getDefaultFetchSize() {
        return this.defaultFetchSize.getValueAsInt();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getDontTrackOpenResources()
     */
    public boolean getDontTrackOpenResources() {
        return this.dontTrackOpenResources.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getDumpQueriesOnException()
     */
    public boolean getDumpQueriesOnException() {
        return this.dumpQueriesOnException.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getDynamicCalendars()
     */
    public boolean getDynamicCalendars() {
        return this.dynamicCalendars.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getElideSetAutoCommits()
     */
    public boolean getElideSetAutoCommits() {
        // Server Bug#66884 (SERVER_STATUS is always initiated with SERVER_STATUS_AUTOCOMMIT=1) invalidates this feature.
        return false;
        // TODO Turn this feature back on as soon as the server bug is fixed. Consider making it version specific.
        // return this.elideSetAutoCommits.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getEmptyStringsConvertToZero()
     */
    public boolean getEmptyStringsConvertToZero() {
        return this.emptyStringsConvertToZero.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getEmulateLocators()
     */
    public boolean getEmulateLocators() {
        return this.emulateLocators.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getEmulateUnsupportedPstmts()
     */
    public boolean getEmulateUnsupportedPstmts() {
        return this.emulateUnsupportedPstmts.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getEnablePacketDebug()
     */
    public boolean getEnablePacketDebug() {
        return this.enablePacketDebug.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getEncoding()
     */
    public String getEncoding() {
        return this.characterEncodingAsString;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getExplainSlowQueries()
     */
    public boolean getExplainSlowQueries() {
        return this.explainSlowQueries.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getFailOverReadOnly()
     */
    public boolean getFailOverReadOnly() {
        return this.failOverReadOnly.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getGatherPerformanceMetrics()
     */
    public boolean getGatherPerformanceMetrics() {
        return this.gatherPerformanceMetrics.getValueAsBoolean();
    }

    protected boolean getHighAvailability() {
        return this.highAvailabilityAsBoolean;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getHoldResultsOpenOverStatementClose()
     */
    public boolean getHoldResultsOpenOverStatementClose() {
        return this.holdResultsOpenOverStatementClose.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getIgnoreNonTxTables()
     */
    public boolean getIgnoreNonTxTables() {
        return this.ignoreNonTxTables.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getInitialTimeout()
     */
    public int getInitialTimeout() {
        return this.initialTimeout.getValueAsInt();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getInteractiveClient()
     */
    public boolean getInteractiveClient() {
        return this.isInteractiveClient.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getIsInteractiveClient()
     */
    public boolean getIsInteractiveClient() {
        return this.isInteractiveClient.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getJdbcCompliantTruncation()
     */
    public boolean getJdbcCompliantTruncation() {
        return this.jdbcCompliantTruncation.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getLocatorFetchBufferSize()
     */
    public int getLocatorFetchBufferSize() {
        return this.locatorFetchBufferSize.getValueAsInt();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getLogger()
     */
    public String getLogger() {
        return this.loggerClassName.getValueAsString();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getLoggerClassName()
     */
    public String getLoggerClassName() {
        return this.loggerClassName.getValueAsString();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getLogSlowQueries()
     */
    public boolean getLogSlowQueries() {
        return this.logSlowQueries.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getMaintainTimeStats()
     */
    public boolean getMaintainTimeStats() {
        return this.maintainTimeStatsAsBoolean;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getMaxQuerySizeToLog()
     */
    public int getMaxQuerySizeToLog() {
        return this.maxQuerySizeToLog.getValueAsInt();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getMaxReconnects()
     */
    public int getMaxReconnects() {
        return this.maxReconnects.getValueAsInt();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getMaxRows()
     */
    public int getMaxRows() {
        return this.maxRowsAsInt;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getMetadataCacheSize()
     */
    public int getMetadataCacheSize() {
        return this.metadataCacheSize.getValueAsInt();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getNoDatetimeStringSync()
     */
    public boolean getNoDatetimeStringSync() {
        return this.noDatetimeStringSync.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getNullCatalogMeansCurrent()
     */
    public boolean getNullCatalogMeansCurrent() {
        return this.nullCatalogMeansCurrent.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getNullNamePatternMatchesAll()
     */
    public boolean getNullNamePatternMatchesAll() {
        return this.nullNamePatternMatchesAll.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getPacketDebugBufferSize()
     */
    public int getPacketDebugBufferSize() {
        return this.packetDebugBufferSize.getValueAsInt();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getParanoid()
     */
    public boolean getParanoid() {
        return this.paranoid.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getPedantic()
     */
    public boolean getPedantic() {
        return this.pedantic.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getPreparedStatementCacheSize()
     */
    public int getPreparedStatementCacheSize() {
        return ((Integer) this.preparedStatementCacheSize.getValueAsObject()).intValue();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getPreparedStatementCacheSqlLimit()
     */
    public int getPreparedStatementCacheSqlLimit() {
        return ((Integer) this.preparedStatementCacheSqlLimit.getValueAsObject()).intValue();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getProfileSql()
     */
    public boolean getProfileSql() {
        return this.profileSQLAsBoolean;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getProfileSQL()
     */
    public boolean getProfileSQL() {
        return this.profileSQL.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getPropertiesTransform()
     */
    public String getPropertiesTransform() {
        return this.propertiesTransform.getValueAsString();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getQueriesBeforeRetryMaster()
     */
    public int getQueriesBeforeRetryMaster() {
        return this.queriesBeforeRetryMaster.getValueAsInt();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getReconnectAtTxEnd()
     */
    public boolean getReconnectAtTxEnd() {
        return this.reconnectTxAtEndAsBoolean;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getRelaxAutoCommit()
     */
    public boolean getRelaxAutoCommit() {
        return this.relaxAutoCommit.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getReportMetricsIntervalMillis()
     */
    public int getReportMetricsIntervalMillis() {
        return this.reportMetricsIntervalMillis.getValueAsInt();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getRequireSSL()
     */
    public boolean getRequireSSL() {
        return this.requireSSL.getValueAsBoolean();
    }

    public boolean getRetainStatementAfterResultSetClose() {
        return this.retainStatementAfterResultSetClose.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getRollbackOnPooledClose()
     */
    public boolean getRollbackOnPooledClose() {
        return this.rollbackOnPooledClose.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getRoundRobinLoadBalance()
     */
    public boolean getRoundRobinLoadBalance() {
        return this.roundRobinLoadBalance.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getRunningCTS13()
     */
    public boolean getRunningCTS13() {
        return this.runningCTS13.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getSecondsBeforeRetryMaster()
     */
    public int getSecondsBeforeRetryMaster() {
        return this.secondsBeforeRetryMaster.getValueAsInt();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getServerTimezone()
     */
    public String getServerTimezone() {
        return this.serverTimezone.getValueAsString();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getSessionVariables()
     */
    public String getSessionVariables() {
        return this.sessionVariables.getValueAsString();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getSlowQueryThresholdMillis()
     */
    public int getSlowQueryThresholdMillis() {
        return this.slowQueryThresholdMillis.getValueAsInt();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getSocketFactoryClassName()
     */
    public String getSocketFactoryClassName() {
        return this.socketFactoryClassName.getValueAsString();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getSocketTimeout()
     */
    public int getSocketTimeout() {
        return this.socketTimeout.getValueAsInt();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getStrictFloatingPoint()
     */
    public boolean getStrictFloatingPoint() {
        return this.strictFloatingPoint.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getStrictUpdates()
     */
    public boolean getStrictUpdates() {
        return this.strictUpdates.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getTinyInt1isBit()
     */
    public boolean getTinyInt1isBit() {
        return this.tinyInt1isBit.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getTraceProtocol()
     */
    public boolean getTraceProtocol() {
        return this.traceProtocol.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getTransformedBitIsBoolean()
     */
    public boolean getTransformedBitIsBoolean() {
        return this.transformedBitIsBoolean.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getUseCompression()
     */
    public boolean getUseCompression() {
        return this.useCompression.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getUseFastIntParsing()
     */
    public boolean getUseFastIntParsing() {
        return this.useFastIntParsing.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getUseHostsInPrivileges()
     */
    public boolean getUseHostsInPrivileges() {
        return this.useHostsInPrivileges.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getUseInformationSchema()
     */
    public boolean getUseInformationSchema() {
        return this.useInformationSchema.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getUseLocalSessionState()
     */
    public boolean getUseLocalSessionState() {
        return this.useLocalSessionState.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getUseOldUTF8Behavior()
     */
    public boolean getUseOldUTF8Behavior() {
        return this.useOldUTF8BehaviorAsBoolean;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getUseOnlyServerErrorMessages()
     */
    public boolean getUseOnlyServerErrorMessages() {
        return this.useOnlyServerErrorMessages.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getUseReadAheadInput()
     */
    public boolean getUseReadAheadInput() {
        return this.useReadAheadInput.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getUseServerPreparedStmts()
     */
    public boolean getUseServerPreparedStmts() {
        return this.detectServerPreparedStmts.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getUseSqlStateCodes()
     */
    public boolean getUseSqlStateCodes() {
        return this.useSqlStateCodes.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getUseSSL()
     */
    public boolean getUseSSL() {
        return this.useSSL.getValueAsBoolean();
    }

    /**
     * Was the value of useSSL set explicitly or just got from defaults.
     * 
     * @return
     */
    public boolean isUseSSLExplicit() {
        return this.useSSL.wasExplicitlySet;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getUseStreamLengthsInPrepStmts()
     */
    public boolean getUseStreamLengthsInPrepStmts() {
        return this.useStreamLengthsInPrepStmts.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getUseTimezone()
     */
    public boolean getUseTimezone() {
        return this.useTimezone.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getUseUltraDevWorkAround()
     */
    public boolean getUseUltraDevWorkAround() {
        return this.useUltraDevWorkAround.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getUseUnbufferedInput()
     */
    public boolean getUseUnbufferedInput() {
        return this.useUnbufferedInput.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getUseUnicode()
     */
    public boolean getUseUnicode() {
        return this.useUnicodeAsBoolean;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getUseUsageAdvisor()
     */
    public boolean getUseUsageAdvisor() {
        return this.useUsageAdvisorAsBoolean;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getYearIsDateType()
     */
    public boolean getYearIsDateType() {
        return this.yearIsDateType.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getZeroDateTimeBehavior()
     */
    public String getZeroDateTimeBehavior() {
        return this.zeroDateTimeBehavior.getValueAsString();
    }

    /**
     * Initializes driver properties that come from a JNDI reference (in the
     * case of a javax.sql.DataSource bound into some name service that doesn't
     * handle Java objects directly).
     * 
     * @param ref
     *            The JNDI Reference that holds RefAddrs for all properties
     * @throws SQLException
     */
    protected void initializeFromRef(Reference ref) throws SQLException {
        int numPropertiesToSet = PROPERTY_LIST.size();

        for (int i = 0; i < numPropertiesToSet; i++) {
            java.lang.reflect.Field propertyField = PROPERTY_LIST.get(i);

            try {
                ConnectionProperty propToSet = (ConnectionProperty) propertyField.get(this);

                if (ref != null) {
                    propToSet.initializeFrom(ref, getExceptionInterceptor());
                }
            } catch (IllegalAccessException iae) {
                throw SQLError.createSQLException("Internal properties failure", SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
            }
        }

        postInitialization();
    }

    /**
     * Initializes driver properties that come from URL or properties passed to
     * the driver manager.
     * 
     * @param info
     * @throws SQLException
     */
    protected void initializeProperties(Properties info) throws SQLException {
        if (info != null) {
            // For backwards-compatibility
            String profileSqlLc = info.getProperty("profileSql");

            if (profileSqlLc != null) {
                info.put("profileSQL", profileSqlLc);
            }

            Properties infoCopy = (Properties) info.clone();

            infoCopy.remove(NonRegisteringDriver.HOST_PROPERTY_KEY);
            infoCopy.remove(NonRegisteringDriver.USER_PROPERTY_KEY);
            infoCopy.remove(NonRegisteringDriver.PASSWORD_PROPERTY_KEY);
            infoCopy.remove(NonRegisteringDriver.DBNAME_PROPERTY_KEY);
            infoCopy.remove(NonRegisteringDriver.PORT_PROPERTY_KEY);
            infoCopy.remove("profileSql");

            int numPropertiesToSet = PROPERTY_LIST.size();

            for (int i = 0; i < numPropertiesToSet; i++) {
                java.lang.reflect.Field propertyField = PROPERTY_LIST.get(i);

                try {
                    ConnectionProperty propToSet = (ConnectionProperty) propertyField.get(this);

                    propToSet.initializeFrom(infoCopy, getExceptionInterceptor());
                } catch (IllegalAccessException iae) {
                    throw SQLError.createSQLException(Messages.getString("ConnectionProperties.unableToInitDriverProperties") + iae.toString(),
                            SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
                }
            }

            postInitialization();
        }
    }

    protected void postInitialization() throws SQLException {

        // Support 'old' profileSql capitalization
        if (this.profileSql.getValueAsObject() != null) {
            this.profileSQL.initializeFrom(this.profileSql.getValueAsObject().toString(), getExceptionInterceptor());
        }

        this.reconnectTxAtEndAsBoolean = ((Boolean) this.reconnectAtTxEnd.getValueAsObject()).booleanValue();

        // Adjust max rows
        if (this.getMaxRows() == 0) {
            // adjust so that it will become MysqlDefs.MAX_ROWS in execSQL()
            this.maxRows.setValueAsObject(Integer.valueOf(-1));
        }

        //
        // Check character encoding
        //
        String testEncoding = ((String) this.characterEncoding.getValueAsObject());

        if (testEncoding != null) {
            // Attempt to use the encoding, and bail out if it can't be used
            try {
                String testString = "abc";
                StringUtils.getBytes(testString, testEncoding);
            } catch (UnsupportedEncodingException UE) {
                throw SQLError.createSQLException(Messages.getString("ConnectionProperties.unsupportedCharacterEncoding", new Object[] { testEncoding }),
                        "0S100", getExceptionInterceptor());
            }
        }

        // Metadata caching is only supported on JDK-1.4 and newer because it relies on LinkedHashMap being present.
        // Check (and disable) if not supported
        if (((Boolean) this.cacheResultSetMetadata.getValueAsObject()).booleanValue()) {
            try {
                Class.forName("java.util.LinkedHashMap");
            } catch (ClassNotFoundException cnfe) {
                this.cacheResultSetMetadata.setValue(false);
            }
        }

        this.cacheResultSetMetaDataAsBoolean = this.cacheResultSetMetadata.getValueAsBoolean();
        this.useUnicodeAsBoolean = this.useUnicode.getValueAsBoolean();
        this.characterEncodingAsString = ((String) this.characterEncoding.getValueAsObject());
        this.highAvailabilityAsBoolean = this.autoReconnect.getValueAsBoolean();
        this.autoReconnectForPoolsAsBoolean = this.autoReconnectForPools.getValueAsBoolean();
        this.maxRowsAsInt = ((Integer) this.maxRows.getValueAsObject()).intValue();
        this.profileSQLAsBoolean = this.profileSQL.getValueAsBoolean();
        this.useUsageAdvisorAsBoolean = this.useUsageAdvisor.getValueAsBoolean();
        this.useOldUTF8BehaviorAsBoolean = this.useOldUTF8Behavior.getValueAsBoolean();
        this.autoGenerateTestcaseScriptAsBoolean = this.autoGenerateTestcaseScript.getValueAsBoolean();
        this.maintainTimeStatsAsBoolean = this.maintainTimeStats.getValueAsBoolean();
        this.jdbcCompliantTruncationForReads = getJdbcCompliantTruncation();

        if (getUseCursorFetch()) {
            // assume they want to use server-side prepared statements because they're required for this functionality
            setDetectServerPreparedStmts(true);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setAllowLoadLocalInfile(boolean)
     */
    public void setAllowLoadLocalInfile(boolean property) {
        this.allowLoadLocalInfile.setValue(property);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setAllowMultiQueries(boolean)
     */
    public void setAllowMultiQueries(boolean property) {
        this.allowMultiQueries.setValue(property);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setAllowNanAndInf(boolean)
     */
    public void setAllowNanAndInf(boolean flag) {
        this.allowNanAndInf.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setAllowUrlInLocalInfile(boolean)
     */
    public void setAllowUrlInLocalInfile(boolean flag) {
        this.allowUrlInLocalInfile.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setAlwaysSendSetIsolation(boolean)
     */
    public void setAlwaysSendSetIsolation(boolean flag) {
        this.alwaysSendSetIsolation.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setAutoDeserialize(boolean)
     */
    public void setAutoDeserialize(boolean flag) {
        this.autoDeserialize.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setAutoGenerateTestcaseScript(boolean)
     */
    public void setAutoGenerateTestcaseScript(boolean flag) {
        this.autoGenerateTestcaseScript.setValue(flag);
        this.autoGenerateTestcaseScriptAsBoolean = this.autoGenerateTestcaseScript.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setAutoReconnect(boolean)
     */
    public void setAutoReconnect(boolean flag) {
        this.autoReconnect.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setAutoReconnectForConnectionPools(boolean)
     */
    public void setAutoReconnectForConnectionPools(boolean property) {
        this.autoReconnectForPools.setValue(property);
        this.autoReconnectForPoolsAsBoolean = this.autoReconnectForPools.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setAutoReconnectForPools(boolean)
     */
    public void setAutoReconnectForPools(boolean flag) {
        this.autoReconnectForPools.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setBlobSendChunkSize(java.lang.String)
     */
    public void setBlobSendChunkSize(String value) throws SQLException {
        this.blobSendChunkSize.setValue(value, getExceptionInterceptor());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setCacheCallableStatements(boolean)
     */
    public void setCacheCallableStatements(boolean flag) {
        this.cacheCallableStatements.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setCachePreparedStatements(boolean)
     */
    public void setCachePreparedStatements(boolean flag) {
        this.cachePreparedStatements.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setCacheResultSetMetadata(boolean)
     */
    public void setCacheResultSetMetadata(boolean property) {
        this.cacheResultSetMetadata.setValue(property);
        this.cacheResultSetMetaDataAsBoolean = this.cacheResultSetMetadata.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setCacheServerConfiguration(boolean)
     */
    public void setCacheServerConfiguration(boolean flag) {
        this.cacheServerConfiguration.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setCallableStatementCacheSize(int)
     */
    public void setCallableStatementCacheSize(int size) throws SQLException {
        this.callableStatementCacheSize.setValue(size, getExceptionInterceptor());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setCapitalizeDBMDTypes(boolean)
     */
    public void setCapitalizeDBMDTypes(boolean property) {
        this.capitalizeTypeNames.setValue(property);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setCapitalizeTypeNames(boolean)
     */
    public void setCapitalizeTypeNames(boolean flag) {
        this.capitalizeTypeNames.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setCharacterEncoding(java.lang.String)
     */
    public void setCharacterEncoding(String encoding) {
        this.characterEncoding.setValue(encoding);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setCharacterSetResults(java.lang.String)
     */
    public void setCharacterSetResults(String characterSet) {
        this.characterSetResults.setValue(characterSet);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setClobberStreamingResults(boolean)
     */
    public void setClobberStreamingResults(boolean flag) {
        this.clobberStreamingResults.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setClobCharacterEncoding(java.lang.String)
     */
    public void setClobCharacterEncoding(String encoding) {
        this.clobCharacterEncoding.setValue(encoding);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setConnectionCollation(java.lang.String)
     */
    public void setConnectionCollation(String collation) {
        this.connectionCollation.setValue(collation);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setConnectTimeout(int)
     */
    public void setConnectTimeout(int timeoutMs) throws SQLException {
        this.connectTimeout.setValue(timeoutMs, getExceptionInterceptor());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setContinueBatchOnError(boolean)
     */
    public void setContinueBatchOnError(boolean property) {
        this.continueBatchOnError.setValue(property);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setCreateDatabaseIfNotExist(boolean)
     */
    public void setCreateDatabaseIfNotExist(boolean flag) {
        this.createDatabaseIfNotExist.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setDefaultFetchSize(int)
     */
    public void setDefaultFetchSize(int n) throws SQLException {
        this.defaultFetchSize.setValue(n, getExceptionInterceptor());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setDetectServerPreparedStmts(boolean)
     */
    public void setDetectServerPreparedStmts(boolean property) {
        this.detectServerPreparedStmts.setValue(property);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setDontTrackOpenResources(boolean)
     */
    public void setDontTrackOpenResources(boolean flag) {
        this.dontTrackOpenResources.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setDumpQueriesOnException(boolean)
     */
    public void setDumpQueriesOnException(boolean flag) {
        this.dumpQueriesOnException.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setDynamicCalendars(boolean)
     */
    public void setDynamicCalendars(boolean flag) {
        this.dynamicCalendars.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setElideSetAutoCommits(boolean)
     */
    public void setElideSetAutoCommits(boolean flag) {
        this.elideSetAutoCommits.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setEmptyStringsConvertToZero(boolean)
     */
    public void setEmptyStringsConvertToZero(boolean flag) {
        this.emptyStringsConvertToZero.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setEmulateLocators(boolean)
     */
    public void setEmulateLocators(boolean property) {
        this.emulateLocators.setValue(property);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setEmulateUnsupportedPstmts(boolean)
     */
    public void setEmulateUnsupportedPstmts(boolean flag) {
        this.emulateUnsupportedPstmts.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setEnablePacketDebug(boolean)
     */
    public void setEnablePacketDebug(boolean flag) {
        this.enablePacketDebug.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setEncoding(java.lang.String)
     */
    public void setEncoding(String property) {
        this.characterEncoding.setValue(property);
        this.characterEncodingAsString = this.characterEncoding.getValueAsString();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setExplainSlowQueries(boolean)
     */
    public void setExplainSlowQueries(boolean flag) {
        this.explainSlowQueries.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setFailOverReadOnly(boolean)
     */
    public void setFailOverReadOnly(boolean flag) {
        this.failOverReadOnly.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setGatherPerformanceMetrics(boolean)
     */
    public void setGatherPerformanceMetrics(boolean flag) {
        this.gatherPerformanceMetrics.setValue(flag);
    }

    protected void setHighAvailability(boolean property) {
        this.autoReconnect.setValue(property);
        this.highAvailabilityAsBoolean = this.autoReconnect.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setHoldResultsOpenOverStatementClose(boolean)
     */
    public void setHoldResultsOpenOverStatementClose(boolean flag) {
        this.holdResultsOpenOverStatementClose.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setIgnoreNonTxTables(boolean)
     */
    public void setIgnoreNonTxTables(boolean property) {
        this.ignoreNonTxTables.setValue(property);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setInitialTimeout(int)
     */
    public void setInitialTimeout(int property) throws SQLException {
        this.initialTimeout.setValue(property, getExceptionInterceptor());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setIsInteractiveClient(boolean)
     */
    public void setIsInteractiveClient(boolean property) {
        this.isInteractiveClient.setValue(property);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setJdbcCompliantTruncation(boolean)
     */
    public void setJdbcCompliantTruncation(boolean flag) {
        this.jdbcCompliantTruncation.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setLocatorFetchBufferSize(java.lang.String)
     */
    public void setLocatorFetchBufferSize(String value) throws SQLException {
        this.locatorFetchBufferSize.setValue(value, getExceptionInterceptor());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setLogger(java.lang.String)
     */
    public void setLogger(String property) {
        this.loggerClassName.setValueAsObject(property);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setLoggerClassName(java.lang.String)
     */
    public void setLoggerClassName(String className) {
        this.loggerClassName.setValue(className);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setLogSlowQueries(boolean)
     */
    public void setLogSlowQueries(boolean flag) {
        this.logSlowQueries.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setMaintainTimeStats(boolean)
     */
    public void setMaintainTimeStats(boolean flag) {
        this.maintainTimeStats.setValue(flag);
        this.maintainTimeStatsAsBoolean = this.maintainTimeStats.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setMaxQuerySizeToLog(int)
     */
    public void setMaxQuerySizeToLog(int sizeInBytes) throws SQLException {
        this.maxQuerySizeToLog.setValue(sizeInBytes, getExceptionInterceptor());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setMaxReconnects(int)
     */
    public void setMaxReconnects(int property) throws SQLException {
        this.maxReconnects.setValue(property, getExceptionInterceptor());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setMaxRows(int)
     */
    public void setMaxRows(int property) throws SQLException {
        this.maxRows.setValue(property, getExceptionInterceptor());
        this.maxRowsAsInt = this.maxRows.getValueAsInt();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setMetadataCacheSize(int)
     */
    public void setMetadataCacheSize(int value) throws SQLException {
        this.metadataCacheSize.setValue(value, getExceptionInterceptor());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setNoDatetimeStringSync(boolean)
     */
    public void setNoDatetimeStringSync(boolean flag) {
        this.noDatetimeStringSync.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setNullCatalogMeansCurrent(boolean)
     */
    public void setNullCatalogMeansCurrent(boolean value) {
        this.nullCatalogMeansCurrent.setValue(value);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setNullNamePatternMatchesAll(boolean)
     */
    public void setNullNamePatternMatchesAll(boolean value) {
        this.nullNamePatternMatchesAll.setValue(value);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setPacketDebugBufferSize(int)
     */
    public void setPacketDebugBufferSize(int size) throws SQLException {
        this.packetDebugBufferSize.setValue(size, getExceptionInterceptor());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setParanoid(boolean)
     */
    public void setParanoid(boolean property) {
        this.paranoid.setValue(property);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setPedantic(boolean)
     */
    public void setPedantic(boolean property) {
        this.pedantic.setValue(property);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setPreparedStatementCacheSize(int)
     */
    public void setPreparedStatementCacheSize(int cacheSize) throws SQLException {
        this.preparedStatementCacheSize.setValue(cacheSize, getExceptionInterceptor());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setPreparedStatementCacheSqlLimit(int)
     */
    public void setPreparedStatementCacheSqlLimit(int cacheSqlLimit) throws SQLException {
        this.preparedStatementCacheSqlLimit.setValue(cacheSqlLimit, getExceptionInterceptor());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setProfileSql(boolean)
     */
    public void setProfileSql(boolean property) {
        this.profileSQL.setValue(property);
        this.profileSQLAsBoolean = this.profileSQL.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setProfileSQL(boolean)
     */
    public void setProfileSQL(boolean flag) {
        this.profileSQL.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setPropertiesTransform(java.lang.String)
     */
    public void setPropertiesTransform(String value) {
        this.propertiesTransform.setValue(value);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setQueriesBeforeRetryMaster(int)
     */
    public void setQueriesBeforeRetryMaster(int property) throws SQLException {
        this.queriesBeforeRetryMaster.setValue(property, getExceptionInterceptor());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setReconnectAtTxEnd(boolean)
     */
    public void setReconnectAtTxEnd(boolean property) {
        this.reconnectAtTxEnd.setValue(property);
        this.reconnectTxAtEndAsBoolean = this.reconnectAtTxEnd.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setRelaxAutoCommit(boolean)
     */
    public void setRelaxAutoCommit(boolean property) {
        this.relaxAutoCommit.setValue(property);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setReportMetricsIntervalMillis(int)
     */
    public void setReportMetricsIntervalMillis(int millis) throws SQLException {
        this.reportMetricsIntervalMillis.setValue(millis, getExceptionInterceptor());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setRequireSSL(boolean)
     */
    public void setRequireSSL(boolean property) {
        this.requireSSL.setValue(property);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setRetainStatementAfterResultSetClose(boolean)
     */
    public void setRetainStatementAfterResultSetClose(boolean flag) {
        this.retainStatementAfterResultSetClose.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setRollbackOnPooledClose(boolean)
     */
    public void setRollbackOnPooledClose(boolean flag) {
        this.rollbackOnPooledClose.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setRoundRobinLoadBalance(boolean)
     */
    public void setRoundRobinLoadBalance(boolean flag) {
        this.roundRobinLoadBalance.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setRunningCTS13(boolean)
     */
    public void setRunningCTS13(boolean flag) {
        this.runningCTS13.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setSecondsBeforeRetryMaster(int)
     */
    public void setSecondsBeforeRetryMaster(int property) throws SQLException {
        this.secondsBeforeRetryMaster.setValue(property, getExceptionInterceptor());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setServerTimezone(java.lang.String)
     */
    public void setServerTimezone(String property) {
        this.serverTimezone.setValue(property);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setSessionVariables(java.lang.String)
     */
    public void setSessionVariables(String variables) {
        this.sessionVariables.setValue(variables);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setSlowQueryThresholdMillis(int)
     */
    public void setSlowQueryThresholdMillis(int millis) throws SQLException {
        this.slowQueryThresholdMillis.setValue(millis, getExceptionInterceptor());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setSocketFactoryClassName(java.lang.String)
     */
    public void setSocketFactoryClassName(String property) {
        this.socketFactoryClassName.setValue(property);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setSocketTimeout(int)
     */
    public void setSocketTimeout(int property) throws SQLException {
        this.socketTimeout.setValue(property, getExceptionInterceptor());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setStrictFloatingPoint(boolean)
     */
    public void setStrictFloatingPoint(boolean property) {
        this.strictFloatingPoint.setValue(property);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setStrictUpdates(boolean)
     */
    public void setStrictUpdates(boolean property) {
        this.strictUpdates.setValue(property);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setTinyInt1isBit(boolean)
     */
    public void setTinyInt1isBit(boolean flag) {
        this.tinyInt1isBit.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setTraceProtocol(boolean)
     */
    public void setTraceProtocol(boolean flag) {
        this.traceProtocol.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setTransformedBitIsBoolean(boolean)
     */
    public void setTransformedBitIsBoolean(boolean flag) {
        this.transformedBitIsBoolean.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setUseCompression(boolean)
     */
    public void setUseCompression(boolean property) {
        this.useCompression.setValue(property);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setUseFastIntParsing(boolean)
     */
    public void setUseFastIntParsing(boolean flag) {
        this.useFastIntParsing.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setUseHostsInPrivileges(boolean)
     */
    public void setUseHostsInPrivileges(boolean property) {
        this.useHostsInPrivileges.setValue(property);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setUseInformationSchema(boolean)
     */
    public void setUseInformationSchema(boolean flag) {
        this.useInformationSchema.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setUseLocalSessionState(boolean)
     */
    public void setUseLocalSessionState(boolean flag) {
        this.useLocalSessionState.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setUseOldUTF8Behavior(boolean)
     */
    public void setUseOldUTF8Behavior(boolean flag) {
        this.useOldUTF8Behavior.setValue(flag);
        this.useOldUTF8BehaviorAsBoolean = this.useOldUTF8Behavior.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setUseOnlyServerErrorMessages(boolean)
     */
    public void setUseOnlyServerErrorMessages(boolean flag) {
        this.useOnlyServerErrorMessages.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setUseReadAheadInput(boolean)
     */
    public void setUseReadAheadInput(boolean flag) {
        this.useReadAheadInput.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setUseServerPreparedStmts(boolean)
     */
    public void setUseServerPreparedStmts(boolean flag) {
        this.detectServerPreparedStmts.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setUseSqlStateCodes(boolean)
     */
    public void setUseSqlStateCodes(boolean flag) {
        this.useSqlStateCodes.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setUseSSL(boolean)
     */
    public void setUseSSL(boolean property) {
        this.useSSL.setValue(property);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setUseStreamLengthsInPrepStmts(boolean)
     */
    public void setUseStreamLengthsInPrepStmts(boolean property) {
        this.useStreamLengthsInPrepStmts.setValue(property);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setUseTimezone(boolean)
     */
    public void setUseTimezone(boolean property) {
        this.useTimezone.setValue(property);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setUseUltraDevWorkAround(boolean)
     */
    public void setUseUltraDevWorkAround(boolean property) {
        this.useUltraDevWorkAround.setValue(property);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setUseUnbufferedInput(boolean)
     */
    public void setUseUnbufferedInput(boolean flag) {
        this.useUnbufferedInput.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setUseUnicode(boolean)
     */
    public void setUseUnicode(boolean flag) {
        this.useUnicode.setValue(flag);
        this.useUnicodeAsBoolean = this.useUnicode.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setUseUsageAdvisor(boolean)
     */
    public void setUseUsageAdvisor(boolean useUsageAdvisorFlag) {
        this.useUsageAdvisor.setValue(useUsageAdvisorFlag);
        this.useUsageAdvisorAsBoolean = this.useUsageAdvisor.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setYearIsDateType(boolean)
     */
    public void setYearIsDateType(boolean flag) {
        this.yearIsDateType.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setZeroDateTimeBehavior(java.lang.String)
     */
    public void setZeroDateTimeBehavior(String behavior) {
        this.zeroDateTimeBehavior.setValue(behavior);
    }

    protected void storeToRef(Reference ref) throws SQLException {
        int numPropertiesToSet = PROPERTY_LIST.size();

        for (int i = 0; i < numPropertiesToSet; i++) {
            java.lang.reflect.Field propertyField = PROPERTY_LIST.get(i);

            try {
                ConnectionProperty propToStore = (ConnectionProperty) propertyField.get(this);

                if (ref != null) {
                    propToStore.storeTo(ref);
                }
            } catch (IllegalAccessException iae) {
                throw SQLError.createSQLException(Messages.getString("ConnectionProperties.errorNotExpected"), getExceptionInterceptor());
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#useUnbufferedInput()
     */
    public boolean useUnbufferedInput() {
        return this.useUnbufferedInput.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getUseCursorFetch()
     */
    public boolean getUseCursorFetch() {
        return this.useCursorFetch.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setUseCursorFetch(boolean)
     */
    public void setUseCursorFetch(boolean flag) {
        this.useCursorFetch.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getOverrideSupportsIntegrityEnhancementFacility()
     */
    public boolean getOverrideSupportsIntegrityEnhancementFacility() {
        return this.overrideSupportsIntegrityEnhancementFacility.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setOverrideSupportsIntegrityEnhancementFacility(boolean)
     */
    public void setOverrideSupportsIntegrityEnhancementFacility(boolean flag) {
        this.overrideSupportsIntegrityEnhancementFacility.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getNoTimezoneConversionForTimeType()
     */
    public boolean getNoTimezoneConversionForTimeType() {
        return this.noTimezoneConversionForTimeType.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setNoTimezoneConversionForTimeType(boolean)
     */
    public void setNoTimezoneConversionForTimeType(boolean flag) {
        this.noTimezoneConversionForTimeType.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getNoTimezoneConversionForDateType()
     */
    public boolean getNoTimezoneConversionForDateType() {
        return this.noTimezoneConversionForDateType.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setNoTimezoneConversionForDateType(boolean)
     */
    public void setNoTimezoneConversionForDateType(boolean flag) {
        this.noTimezoneConversionForDateType.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getCacheDefaultTimezone()
     */
    public boolean getCacheDefaultTimezone() {
        return this.cacheDefaultTimezone.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setCacheDefaultTimezone(boolean)
     */
    public void setCacheDefaultTimezone(boolean flag) {
        this.cacheDefaultTimezone.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getUseJDBCCompliantTimezoneShift()
     */
    public boolean getUseJDBCCompliantTimezoneShift() {
        return this.useJDBCCompliantTimezoneShift.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setUseJDBCCompliantTimezoneShift(boolean)
     */
    public void setUseJDBCCompliantTimezoneShift(boolean flag) {
        this.useJDBCCompliantTimezoneShift.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getAutoClosePStmtStreams()
     */
    public boolean getAutoClosePStmtStreams() {
        return this.autoClosePStmtStreams.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setAutoClosePStmtStreams(boolean)
     */
    public void setAutoClosePStmtStreams(boolean flag) {
        this.autoClosePStmtStreams.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getProcessEscapeCodesForPrepStmts()
     */
    public boolean getProcessEscapeCodesForPrepStmts() {
        return this.processEscapeCodesForPrepStmts.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setProcessEscapeCodesForPrepStmts(boolean)
     */
    public void setProcessEscapeCodesForPrepStmts(boolean flag) {
        this.processEscapeCodesForPrepStmts.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getUseGmtMillisForDatetimes()
     */
    public boolean getUseGmtMillisForDatetimes() {
        return this.useGmtMillisForDatetimes.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setUseGmtMillisForDatetimes(boolean)
     */
    public void setUseGmtMillisForDatetimes(boolean flag) {
        this.useGmtMillisForDatetimes.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getDumpMetadataOnColumnNotFound()
     */
    public boolean getDumpMetadataOnColumnNotFound() {
        return this.dumpMetadataOnColumnNotFound.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setDumpMetadataOnColumnNotFound(boolean)
     */
    public void setDumpMetadataOnColumnNotFound(boolean flag) {
        this.dumpMetadataOnColumnNotFound.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getResourceId()
     */
    public String getResourceId() {
        return this.resourceId.getValueAsString();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setResourceId(java.lang.String)
     */
    public void setResourceId(String resourceId) {
        this.resourceId.setValue(resourceId);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getRewriteBatchedStatements()
     */
    public boolean getRewriteBatchedStatements() {
        return this.rewriteBatchedStatements.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setRewriteBatchedStatements(boolean)
     */
    public void setRewriteBatchedStatements(boolean flag) {
        this.rewriteBatchedStatements.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getJdbcCompliantTruncationForReads()
     */
    public boolean getJdbcCompliantTruncationForReads() {
        return this.jdbcCompliantTruncationForReads;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setJdbcCompliantTruncationForReads(boolean)
     */
    public void setJdbcCompliantTruncationForReads(boolean jdbcCompliantTruncationForReads) {
        this.jdbcCompliantTruncationForReads = jdbcCompliantTruncationForReads;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getUseJvmCharsetConverters()
     */
    public boolean getUseJvmCharsetConverters() {
        return this.useJvmCharsetConverters.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setUseJvmCharsetConverters(boolean)
     */
    public void setUseJvmCharsetConverters(boolean flag) {
        this.useJvmCharsetConverters.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getPinGlobalTxToPhysicalConnection()
     */
    public boolean getPinGlobalTxToPhysicalConnection() {
        return this.pinGlobalTxToPhysicalConnection.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setPinGlobalTxToPhysicalConnection(boolean)
     */
    public void setPinGlobalTxToPhysicalConnection(boolean flag) {
        this.pinGlobalTxToPhysicalConnection.setValue(flag);
    }

    /*
     * "Aliases" which match the property names to make using
     * from datasources easier.
     */

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setGatherPerfMetrics(boolean)
     */
    public void setGatherPerfMetrics(boolean flag) {
        setGatherPerformanceMetrics(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getGatherPerfMetrics()
     */
    public boolean getGatherPerfMetrics() {
        return getGatherPerformanceMetrics();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setUltraDevHack(boolean)
     */
    public void setUltraDevHack(boolean flag) {
        setUseUltraDevWorkAround(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getUltraDevHack()
     */
    public boolean getUltraDevHack() {
        return getUseUltraDevWorkAround();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setInteractiveClient(boolean)
     */
    public void setInteractiveClient(boolean property) {
        setIsInteractiveClient(property);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setSocketFactory(java.lang.String)
     */
    public void setSocketFactory(String name) {
        setSocketFactoryClassName(name);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getSocketFactory()
     */
    public String getSocketFactory() {
        return getSocketFactoryClassName();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setUseServerPrepStmts(boolean)
     */
    public void setUseServerPrepStmts(boolean flag) {
        setUseServerPreparedStmts(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getUseServerPrepStmts()
     */
    public boolean getUseServerPrepStmts() {
        return getUseServerPreparedStmts();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setCacheCallableStmts(boolean)
     */
    public void setCacheCallableStmts(boolean flag) {
        setCacheCallableStatements(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getCacheCallableStmts()
     */
    public boolean getCacheCallableStmts() {
        return getCacheCallableStatements();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setCachePrepStmts(boolean)
     */
    public void setCachePrepStmts(boolean flag) {
        setCachePreparedStatements(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getCachePrepStmts()
     */
    public boolean getCachePrepStmts() {
        return getCachePreparedStatements();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setCallableStmtCacheSize(int)
     */
    public void setCallableStmtCacheSize(int cacheSize) throws SQLException {
        setCallableStatementCacheSize(cacheSize);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getCallableStmtCacheSize()
     */
    public int getCallableStmtCacheSize() {
        return getCallableStatementCacheSize();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setPrepStmtCacheSize(int)
     */
    public void setPrepStmtCacheSize(int cacheSize) throws SQLException {
        setPreparedStatementCacheSize(cacheSize);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getPrepStmtCacheSize()
     */
    public int getPrepStmtCacheSize() {
        return getPreparedStatementCacheSize();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setPrepStmtCacheSqlLimit(int)
     */
    public void setPrepStmtCacheSqlLimit(int sqlLimit) throws SQLException {
        setPreparedStatementCacheSqlLimit(sqlLimit);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getPrepStmtCacheSqlLimit()
     */
    public int getPrepStmtCacheSqlLimit() {
        return getPreparedStatementCacheSqlLimit();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getNoAccessToProcedureBodies()
     */
    public boolean getNoAccessToProcedureBodies() {
        return this.noAccessToProcedureBodies.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setNoAccessToProcedureBodies(boolean)
     */
    public void setNoAccessToProcedureBodies(boolean flag) {
        this.noAccessToProcedureBodies.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getUseOldAliasMetadataBehavior()
     */
    public boolean getUseOldAliasMetadataBehavior() {
        return this.useOldAliasMetadataBehavior.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setUseOldAliasMetadataBehavior(boolean)
     */
    public void setUseOldAliasMetadataBehavior(boolean flag) {
        this.useOldAliasMetadataBehavior.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getClientCertificateKeyStorePassword()
     */
    public String getClientCertificateKeyStorePassword() {
        return this.clientCertificateKeyStorePassword.getValueAsString();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setClientCertificateKeyStorePassword(java.lang.String)
     */
    public void setClientCertificateKeyStorePassword(String value) {
        this.clientCertificateKeyStorePassword.setValue(value);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getClientCertificateKeyStoreType()
     */
    public String getClientCertificateKeyStoreType() {
        return this.clientCertificateKeyStoreType.getValueAsString();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setClientCertificateKeyStoreType(java.lang.String)
     */
    public void setClientCertificateKeyStoreType(String value) {
        this.clientCertificateKeyStoreType.setValue(value);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getClientCertificateKeyStoreUrl()
     */
    public String getClientCertificateKeyStoreUrl() {
        return this.clientCertificateKeyStoreUrl.getValueAsString();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setClientCertificateKeyStoreUrl(java.lang.String)
     */
    public void setClientCertificateKeyStoreUrl(String value) {
        this.clientCertificateKeyStoreUrl.setValue(value);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getTrustCertificateKeyStorePassword()
     */
    public String getTrustCertificateKeyStorePassword() {
        return this.trustCertificateKeyStorePassword.getValueAsString();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setTrustCertificateKeyStorePassword(java.lang.String)
     */
    public void setTrustCertificateKeyStorePassword(String value) {
        this.trustCertificateKeyStorePassword.setValue(value);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getTrustCertificateKeyStoreType()
     */
    public String getTrustCertificateKeyStoreType() {
        return this.trustCertificateKeyStoreType.getValueAsString();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setTrustCertificateKeyStoreType(java.lang.String)
     */
    public void setTrustCertificateKeyStoreType(String value) {
        this.trustCertificateKeyStoreType.setValue(value);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getTrustCertificateKeyStoreUrl()
     */
    public String getTrustCertificateKeyStoreUrl() {
        return this.trustCertificateKeyStoreUrl.getValueAsString();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setTrustCertificateKeyStoreUrl(java.lang.String)
     */
    public void setTrustCertificateKeyStoreUrl(String value) {
        this.trustCertificateKeyStoreUrl.setValue(value);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getUseSSPSCompatibleTimezoneShift()
     */
    public boolean getUseSSPSCompatibleTimezoneShift() {
        return this.useSSPSCompatibleTimezoneShift.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setUseSSPSCompatibleTimezoneShift(boolean)
     */
    public void setUseSSPSCompatibleTimezoneShift(boolean flag) {
        this.useSSPSCompatibleTimezoneShift.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getTreatUtilDateAsTimestamp()
     */
    public boolean getTreatUtilDateAsTimestamp() {
        return this.treatUtilDateAsTimestamp.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setTreatUtilDateAsTimestamp(boolean)
     */
    public void setTreatUtilDateAsTimestamp(boolean flag) {
        this.treatUtilDateAsTimestamp.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getUseFastDateParsing()
     */
    public boolean getUseFastDateParsing() {
        return this.useFastDateParsing.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setUseFastDateParsing(boolean)
     */
    public void setUseFastDateParsing(boolean flag) {
        this.useFastDateParsing.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getLocalSocketAddress()
     */
    public String getLocalSocketAddress() {
        return this.localSocketAddress.getValueAsString();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setLocalSocketAddress(java.lang.String)
     */
    public void setLocalSocketAddress(String address) {
        this.localSocketAddress.setValue(address);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setUseConfigs(java.lang.String)
     */
    public void setUseConfigs(String configs) {
        this.useConfigs.setValue(configs);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getUseConfigs()
     */
    public String getUseConfigs() {
        return this.useConfigs.getValueAsString();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getGenerateSimpleParameterMetadata()
     */
    public boolean getGenerateSimpleParameterMetadata() {
        return this.generateSimpleParameterMetadata.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setGenerateSimpleParameterMetadata(boolean)
     */
    public void setGenerateSimpleParameterMetadata(boolean flag) {
        this.generateSimpleParameterMetadata.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getLogXaCommands()
     */
    public boolean getLogXaCommands() {
        return this.logXaCommands.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setLogXaCommands(boolean)
     */
    public void setLogXaCommands(boolean flag) {
        this.logXaCommands.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getResultSetSizeThreshold()
     */
    public int getResultSetSizeThreshold() {
        return this.resultSetSizeThreshold.getValueAsInt();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setResultSetSizeThreshold(int)
     */
    public void setResultSetSizeThreshold(int threshold) throws SQLException {
        this.resultSetSizeThreshold.setValue(threshold, getExceptionInterceptor());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getNetTimeoutForStreamingResults()
     */
    public int getNetTimeoutForStreamingResults() {
        return this.netTimeoutForStreamingResults.getValueAsInt();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setNetTimeoutForStreamingResults(int)
     */
    public void setNetTimeoutForStreamingResults(int value) throws SQLException {
        this.netTimeoutForStreamingResults.setValue(value, getExceptionInterceptor());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getEnableQueryTimeouts()
     */
    public boolean getEnableQueryTimeouts() {
        return this.enableQueryTimeouts.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setEnableQueryTimeouts(boolean)
     */
    public void setEnableQueryTimeouts(boolean flag) {
        this.enableQueryTimeouts.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getPadCharsWithSpace()
     */
    public boolean getPadCharsWithSpace() {
        return this.padCharsWithSpace.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setPadCharsWithSpace(boolean)
     */
    public void setPadCharsWithSpace(boolean flag) {
        this.padCharsWithSpace.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getUseDynamicCharsetInfo()
     */
    public boolean getUseDynamicCharsetInfo() {
        return this.useDynamicCharsetInfo.getValueAsBoolean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setUseDynamicCharsetInfo(boolean)
     */
    public void setUseDynamicCharsetInfo(boolean flag) {
        this.useDynamicCharsetInfo.setValue(flag);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#getClientInfoProvider()
     */
    public String getClientInfoProvider() {
        return this.clientInfoProvider.getValueAsString();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.IConnectionProperties#setClientInfoProvider(java.lang.String)
     */
    public void setClientInfoProvider(String classname) {
        this.clientInfoProvider.setValue(classname);
    }

    public boolean getPopulateInsertRowWithDefaultValues() {
        return this.populateInsertRowWithDefaultValues.getValueAsBoolean();
    }

    public void setPopulateInsertRowWithDefaultValues(boolean flag) {
        this.populateInsertRowWithDefaultValues.setValue(flag);
    }

    public String getLoadBalanceStrategy() {
        return this.loadBalanceStrategy.getValueAsString();
    }

    public void setLoadBalanceStrategy(String strategy) {
        this.loadBalanceStrategy.setValue(strategy);
    }

    public String getServerAffinityOrder() {
        return this.serverAffinityOrder.getValueAsString();
    }

    public void setServerAffinityOrder(String hostsList) {
        this.serverAffinityOrder.setValue(hostsList);
    }

    public boolean getTcpNoDelay() {
        return this.tcpNoDelay.getValueAsBoolean();
    }

    public void setTcpNoDelay(boolean flag) {
        this.tcpNoDelay.setValue(flag);
    }

    public boolean getTcpKeepAlive() {
        return this.tcpKeepAlive.getValueAsBoolean();
    }

    public void setTcpKeepAlive(boolean flag) {
        this.tcpKeepAlive.setValue(flag);
    }

    public int getTcpRcvBuf() {
        return this.tcpRcvBuf.getValueAsInt();
    }

    public void setTcpRcvBuf(int bufSize) throws SQLException {
        this.tcpRcvBuf.setValue(bufSize, getExceptionInterceptor());
    }

    public int getTcpSndBuf() {
        return this.tcpSndBuf.getValueAsInt();
    }

    public void setTcpSndBuf(int bufSize) throws SQLException {
        this.tcpSndBuf.setValue(bufSize, getExceptionInterceptor());
    }

    public int getTcpTrafficClass() {
        return this.tcpTrafficClass.getValueAsInt();
    }

    public void setTcpTrafficClass(int classFlags) throws SQLException {
        this.tcpTrafficClass.setValue(classFlags, getExceptionInterceptor());
    }

    public boolean getUseNanosForElapsedTime() {
        return this.useNanosForElapsedTime.getValueAsBoolean();
    }

    public void setUseNanosForElapsedTime(boolean flag) {
        this.useNanosForElapsedTime.setValue(flag);
    }

    public long getSlowQueryThresholdNanos() {
        return this.slowQueryThresholdNanos.getValueAsLong();
    }

    public void setSlowQueryThresholdNanos(long nanos) throws SQLException {
        this.slowQueryThresholdNanos.setValue(nanos, getExceptionInterceptor());
    }

    public String getStatementInterceptors() {
        return this.statementInterceptors.getValueAsString();
    }

    public void setStatementInterceptors(String value) {
        this.statementInterceptors.setValue(value);
    }

    public boolean getUseDirectRowUnpack() {
        return this.useDirectRowUnpack.getValueAsBoolean();
    }

    public void setUseDirectRowUnpack(boolean flag) {
        this.useDirectRowUnpack.setValue(flag);
    }

    public String getLargeRowSizeThreshold() {
        return this.largeRowSizeThreshold.getValueAsString();
    }

    public void setLargeRowSizeThreshold(String value) throws SQLException {
        this.largeRowSizeThreshold.setValue(value, getExceptionInterceptor());
    }

    public boolean getUseBlobToStoreUTF8OutsideBMP() {
        return this.useBlobToStoreUTF8OutsideBMP.getValueAsBoolean();
    }

    public void setUseBlobToStoreUTF8OutsideBMP(boolean flag) {
        this.useBlobToStoreUTF8OutsideBMP.setValue(flag);
    }

    public String getUtf8OutsideBmpExcludedColumnNamePattern() {
        return this.utf8OutsideBmpExcludedColumnNamePattern.getValueAsString();
    }

    public void setUtf8OutsideBmpExcludedColumnNamePattern(String regexPattern) {
        this.utf8OutsideBmpExcludedColumnNamePattern.setValue(regexPattern);
    }

    public String getUtf8OutsideBmpIncludedColumnNamePattern() {
        return this.utf8OutsideBmpIncludedColumnNamePattern.getValueAsString();
    }

    public void setUtf8OutsideBmpIncludedColumnNamePattern(String regexPattern) {
        this.utf8OutsideBmpIncludedColumnNamePattern.setValue(regexPattern);
    }

    public boolean getIncludeInnodbStatusInDeadlockExceptions() {
        return this.includeInnodbStatusInDeadlockExceptions.getValueAsBoolean();
    }

    public void setIncludeInnodbStatusInDeadlockExceptions(boolean flag) {
        this.includeInnodbStatusInDeadlockExceptions.setValue(flag);
    }

    public boolean getBlobsAreStrings() {
        return this.blobsAreStrings.getValueAsBoolean();
    }

    public void setBlobsAreStrings(boolean flag) {
        this.blobsAreStrings.setValue(flag);
    }

    public boolean getFunctionsNeverReturnBlobs() {
        return this.functionsNeverReturnBlobs.getValueAsBoolean();
    }

    public void setFunctionsNeverReturnBlobs(boolean flag) {
        this.functionsNeverReturnBlobs.setValue(flag);
    }

    public boolean getAutoSlowLog() {
        return this.autoSlowLog.getValueAsBoolean();
    }

    public void setAutoSlowLog(boolean flag) {
        this.autoSlowLog.setValue(flag);
    }

    public String getConnectionLifecycleInterceptors() {
        return this.connectionLifecycleInterceptors.getValueAsString();
    }

    public void setConnectionLifecycleInterceptors(String interceptors) {
        this.connectionLifecycleInterceptors.setValue(interceptors);
    }

    public String getProfilerEventHandler() {
        return this.profilerEventHandler.getValueAsString();
    }

    public void setProfilerEventHandler(String handler) {
        this.profilerEventHandler.setValue(handler);
    }

    public boolean getVerifyServerCertificate() {
        return this.verifyServerCertificate.getValueAsBoolean();
    }

    public void setVerifyServerCertificate(boolean flag) {
        this.verifyServerCertificate.setValue(flag);
    }

    public boolean getUseLegacyDatetimeCode() {
        return this.useLegacyDatetimeCode.getValueAsBoolean();
    }

    public void setUseLegacyDatetimeCode(boolean flag) {
        this.useLegacyDatetimeCode.setValue(flag);
    }

    public boolean getSendFractionalSeconds() {
        return this.sendFractionalSeconds.getValueAsBoolean();
    }

    public void setSendFractionalSeconds(boolean flag) {
        this.sendFractionalSeconds.setValue(flag);
    }

    public int getSelfDestructOnPingSecondsLifetime() {
        return this.selfDestructOnPingSecondsLifetime.getValueAsInt();
    }

    public void setSelfDestructOnPingSecondsLifetime(int seconds) throws SQLException {
        this.selfDestructOnPingSecondsLifetime.setValue(seconds, getExceptionInterceptor());
    }

    public int getSelfDestructOnPingMaxOperations() {
        return this.selfDestructOnPingMaxOperations.getValueAsInt();
    }

    public void setSelfDestructOnPingMaxOperations(int maxOperations) throws SQLException {
        this.selfDestructOnPingMaxOperations.setValue(maxOperations, getExceptionInterceptor());
    }

    public boolean getUseColumnNamesInFindColumn() {
        return this.useColumnNamesInFindColumn.getValueAsBoolean();
    }

    public void setUseColumnNamesInFindColumn(boolean flag) {
        this.useColumnNamesInFindColumn.setValue(flag);
    }

    public boolean getUseLocalTransactionState() {
        return this.useLocalTransactionState.getValueAsBoolean();
    }

    public void setUseLocalTransactionState(boolean flag) {
        this.useLocalTransactionState.setValue(flag);
    }

    public boolean getCompensateOnDuplicateKeyUpdateCounts() {
        return this.compensateOnDuplicateKeyUpdateCounts.getValueAsBoolean();
    }

    public void setCompensateOnDuplicateKeyUpdateCounts(boolean flag) {
        this.compensateOnDuplicateKeyUpdateCounts.setValue(flag);
    }

    public int getLoadBalanceBlacklistTimeout() {
        return this.loadBalanceBlacklistTimeout.getValueAsInt();
    }

    public void setLoadBalanceBlacklistTimeout(int loadBalanceBlacklistTimeout) throws SQLException {
        this.loadBalanceBlacklistTimeout.setValue(loadBalanceBlacklistTimeout, getExceptionInterceptor());
    }

    public int getLoadBalancePingTimeout() {
        return this.loadBalancePingTimeout.getValueAsInt();
    }

    public void setLoadBalancePingTimeout(int loadBalancePingTimeout) throws SQLException {
        this.loadBalancePingTimeout.setValue(loadBalancePingTimeout, getExceptionInterceptor());
    }

    public void setRetriesAllDown(int retriesAllDown) throws SQLException {
        this.retriesAllDown.setValue(retriesAllDown, getExceptionInterceptor());
    }

    public int getRetriesAllDown() {
        return this.retriesAllDown.getValueAsInt();
    }

    public void setUseAffectedRows(boolean flag) {
        this.useAffectedRows.setValue(flag);
    }

    public boolean getUseAffectedRows() {
        return this.useAffectedRows.getValueAsBoolean();
    }

    public void setPasswordCharacterEncoding(String characterSet) {
        this.passwordCharacterEncoding.setValue(characterSet);
    }

    public String getPasswordCharacterEncoding() {
        String encoding;
        if ((encoding = this.passwordCharacterEncoding.getValueAsString()) != null) {
            return encoding;
        }
        if (getUseUnicode() && (encoding = getEncoding()) != null) {
            return encoding;
        }
        return "UTF-8";
    }

    public void setExceptionInterceptors(String exceptionInterceptors) {
        this.exceptionInterceptors.setValue(exceptionInterceptors);
    }

    public String getExceptionInterceptors() {
        return this.exceptionInterceptors.getValueAsString();
    }

    public void setMaxAllowedPacket(int max) throws SQLException {
        this.maxAllowedPacket.setValue(max, getExceptionInterceptor());
    }

    public int getMaxAllowedPacket() {
        return this.maxAllowedPacket.getValueAsInt();
    }

    public boolean getQueryTimeoutKillsConnection() {
        return this.queryTimeoutKillsConnection.getValueAsBoolean();
    }

    public void setQueryTimeoutKillsConnection(boolean queryTimeoutKillsConnection) {
        this.queryTimeoutKillsConnection.setValue(queryTimeoutKillsConnection);
    }

    public boolean getLoadBalanceValidateConnectionOnSwapServer() {
        return this.loadBalanceValidateConnectionOnSwapServer.getValueAsBoolean();
    }

    public void setLoadBalanceValidateConnectionOnSwapServer(boolean loadBalanceValidateConnectionOnSwapServer) {
        this.loadBalanceValidateConnectionOnSwapServer.setValue(loadBalanceValidateConnectionOnSwapServer);
    }

    public String getLoadBalanceConnectionGroup() {
        return this.loadBalanceConnectionGroup.getValueAsString();
    }

    public void setLoadBalanceConnectionGroup(String loadBalanceConnectionGroup) {
        this.loadBalanceConnectionGroup.setValue(loadBalanceConnectionGroup);
    }

    public String getLoadBalanceExceptionChecker() {
        return this.loadBalanceExceptionChecker.getValueAsString();
    }

    public void setLoadBalanceExceptionChecker(String loadBalanceExceptionChecker) {
        this.loadBalanceExceptionChecker.setValue(loadBalanceExceptionChecker);
    }

    public String getLoadBalanceSQLStateFailover() {
        return this.loadBalanceSQLStateFailover.getValueAsString();
    }

    public void setLoadBalanceSQLStateFailover(String loadBalanceSQLStateFailover) {
        this.loadBalanceSQLStateFailover.setValue(loadBalanceSQLStateFailover);
    }

    public String getLoadBalanceSQLExceptionSubclassFailover() {
        return this.loadBalanceSQLExceptionSubclassFailover.getValueAsString();
    }

    public void setLoadBalanceSQLExceptionSubclassFailover(String loadBalanceSQLExceptionSubclassFailover) {
        this.loadBalanceSQLExceptionSubclassFailover.setValue(loadBalanceSQLExceptionSubclassFailover);
    }

    public boolean getLoadBalanceEnableJMX() {
        return this.loadBalanceEnableJMX.getValueAsBoolean();
    }

    public void setLoadBalanceEnableJMX(boolean loadBalanceEnableJMX) {
        this.loadBalanceEnableJMX.setValue(loadBalanceEnableJMX);
    }

    public void setLoadBalanceHostRemovalGracePeriod(int loadBalanceHostRemovalGracePeriod) throws SQLException {
        this.loadBalanceHostRemovalGracePeriod.setValue(loadBalanceHostRemovalGracePeriod, getExceptionInterceptor());
    }

    public int getLoadBalanceHostRemovalGracePeriod() {
        return this.loadBalanceHostRemovalGracePeriod.getValueAsInt();
    }

    public void setLoadBalanceAutoCommitStatementThreshold(int loadBalanceAutoCommitStatementThreshold) throws SQLException {
        this.loadBalanceAutoCommitStatementThreshold.setValue(loadBalanceAutoCommitStatementThreshold, getExceptionInterceptor());
    }

    public int getLoadBalanceAutoCommitStatementThreshold() {
        return this.loadBalanceAutoCommitStatementThreshold.getValueAsInt();
    }

    public void setLoadBalanceAutoCommitStatementRegex(String loadBalanceAutoCommitStatementRegex) {
        this.loadBalanceAutoCommitStatementRegex.setValue(loadBalanceAutoCommitStatementRegex);
    }

    public String getLoadBalanceAutoCommitStatementRegex() {
        return this.loadBalanceAutoCommitStatementRegex.getValueAsString();
    }

    public void setIncludeThreadDumpInDeadlockExceptions(boolean flag) {
        this.includeThreadDumpInDeadlockExceptions.setValue(flag);
    }

    public boolean getIncludeThreadDumpInDeadlockExceptions() {
        return this.includeThreadDumpInDeadlockExceptions.getValueAsBoolean();
    }

    public void setIncludeThreadNamesAsStatementComment(boolean flag) {
        this.includeThreadNamesAsStatementComment.setValue(flag);
    }

    public boolean getIncludeThreadNamesAsStatementComment() {
        return this.includeThreadNamesAsStatementComment.getValueAsBoolean();
    }

    public void setAuthenticationPlugins(String authenticationPlugins) {
        this.authenticationPlugins.setValue(authenticationPlugins);
    }

    public String getAuthenticationPlugins() {
        return this.authenticationPlugins.getValueAsString();
    }

    public void setDisabledAuthenticationPlugins(String disabledAuthenticationPlugins) {
        this.disabledAuthenticationPlugins.setValue(disabledAuthenticationPlugins);
    }

    public String getDisabledAuthenticationPlugins() {
        return this.disabledAuthenticationPlugins.getValueAsString();
    }

    public void setDefaultAuthenticationPlugin(String defaultAuthenticationPlugin) {
        this.defaultAuthenticationPlugin.setValue(defaultAuthenticationPlugin);
    }

    public String getDefaultAuthenticationPlugin() {
        return this.defaultAuthenticationPlugin.getValueAsString();
    }

    public void setParseInfoCacheFactory(String factoryClassname) {
        this.parseInfoCacheFactory.setValue(factoryClassname);
    }

    public String getParseInfoCacheFactory() {
        return this.parseInfoCacheFactory.getValueAsString();
    }

    public void setServerConfigCacheFactory(String factoryClassname) {
        this.serverConfigCacheFactory.setValue(factoryClassname);
    }

    public String getServerConfigCacheFactory() {
        return this.serverConfigCacheFactory.getValueAsString();
    }

    public void setDisconnectOnExpiredPasswords(boolean disconnectOnExpiredPasswords) {
        this.disconnectOnExpiredPasswords.setValue(disconnectOnExpiredPasswords);
    }

    public boolean getDisconnectOnExpiredPasswords() {
        return this.disconnectOnExpiredPasswords.getValueAsBoolean();
    }

    public String getReplicationConnectionGroup() {
        return this.replicationConnectionGroup.getValueAsString();
    }

    public void setReplicationConnectionGroup(String replicationConnectionGroup) {
        this.replicationConnectionGroup.setValue(replicationConnectionGroup);
    }

    public boolean getAllowMasterDownConnections() {
        return this.allowMasterDownConnections.getValueAsBoolean();
    }

    public void setAllowMasterDownConnections(boolean connectIfMasterDown) {
        this.allowMasterDownConnections.setValue(connectIfMasterDown);
    }

    public boolean getAllowSlaveDownConnections() {
        return this.allowSlaveDownConnections.getValueAsBoolean();
    }

    public void setAllowSlaveDownConnections(boolean connectIfSlaveDown) {
        this.allowSlaveDownConnections.setValue(connectIfSlaveDown);
    }

    public boolean getReadFromMasterWhenNoSlaves() {
        return this.readFromMasterWhenNoSlaves.getValueAsBoolean();
    }

    public void setReadFromMasterWhenNoSlaves(boolean useMasterIfSlavesDown) {
        this.readFromMasterWhenNoSlaves.setValue(useMasterIfSlavesDown);
    }

    public boolean getReplicationEnableJMX() {
        return this.replicationEnableJMX.getValueAsBoolean();
    }

    public void setReplicationEnableJMX(boolean replicationEnableJMX) {
        this.replicationEnableJMX.setValue(replicationEnableJMX);
    }

    public void setGetProceduresReturnsFunctions(boolean getProcedureReturnsFunctions) {
        this.getProceduresReturnsFunctions.setValue(getProcedureReturnsFunctions);
    }

    public boolean getGetProceduresReturnsFunctions() {
        return this.getProceduresReturnsFunctions.getValueAsBoolean();
    }

    public void setDetectCustomCollations(boolean detectCustomCollations) {
        this.detectCustomCollations.setValue(detectCustomCollations);
    }

    public boolean getDetectCustomCollations() {
        return this.detectCustomCollations.getValueAsBoolean();
    }

    public String getServerRSAPublicKeyFile() {
        return this.serverRSAPublicKeyFile.getValueAsString();
    }

    public void setServerRSAPublicKeyFile(String serverRSAPublicKeyFile) throws SQLException {
        if (this.serverRSAPublicKeyFile.getUpdateCount() > 0) {
            throw SQLError.createSQLException(Messages.getString("ConnectionProperties.dynamicChangeIsNotAllowed", new Object[] { "'serverRSAPublicKeyFile'" }),
                    SQLError.SQL_STATE_ILLEGAL_ARGUMENT, null);
        }
        this.serverRSAPublicKeyFile.setValue(serverRSAPublicKeyFile);
    }

    public boolean getAllowPublicKeyRetrieval() {
        return this.allowPublicKeyRetrieval.getValueAsBoolean();
    }

    public void setAllowPublicKeyRetrieval(boolean allowPublicKeyRetrieval) throws SQLException {
        if (this.allowPublicKeyRetrieval.getUpdateCount() > 0) {
            throw SQLError.createSQLException(
                    Messages.getString("ConnectionProperties.dynamicChangeIsNotAllowed", new Object[] { "'allowPublicKeyRetrieval'" }),
                    SQLError.SQL_STATE_ILLEGAL_ARGUMENT, null);
        }
        this.allowPublicKeyRetrieval.setValue(allowPublicKeyRetrieval);
    }

    public void setDontCheckOnDuplicateKeyUpdateInSQL(boolean dontCheckOnDuplicateKeyUpdateInSQL) {
        this.dontCheckOnDuplicateKeyUpdateInSQL.setValue(dontCheckOnDuplicateKeyUpdateInSQL);
    }

    public boolean getDontCheckOnDuplicateKeyUpdateInSQL() {
        return this.dontCheckOnDuplicateKeyUpdateInSQL.getValueAsBoolean();
    }

    public void setSocksProxyHost(String socksProxyHost) {
        this.socksProxyHost.setValue(socksProxyHost);
    }

    public String getSocksProxyHost() {
        return this.socksProxyHost.getValueAsString();
    }

    public void setSocksProxyPort(int socksProxyPort) throws SQLException {
        this.socksProxyPort.setValue(socksProxyPort, null);
    }

    public int getSocksProxyPort() {
        return this.socksProxyPort.getValueAsInt();
    }

    public boolean getReadOnlyPropagatesToServer() {
        return this.readOnlyPropagatesToServer.getValueAsBoolean();
    }

    public void setReadOnlyPropagatesToServer(boolean flag) {
        this.readOnlyPropagatesToServer.setValue(flag);
    }

    public String getEnabledSSLCipherSuites() {
        return this.enabledSSLCipherSuites.getValueAsString();
    }

    public void setEnabledSSLCipherSuites(String cipherSuites) {
        this.enabledSSLCipherSuites.setValue(cipherSuites);
    }

    public boolean getEnableEscapeProcessing() {
        return this.enableEscapeProcessing.getValueAsBoolean();
    }

    public void setEnableEscapeProcessing(boolean flag) {
        this.enableEscapeProcessing.setValue(flag);
    }
}
