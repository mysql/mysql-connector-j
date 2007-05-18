/*
 Copyright (C) 2002-2004 MySQL AB

 This program is free software; you can redistribute it and/or modify
 it under the terms of version 2 of the GNU General Public License as 
 published by the Free Software Foundation.

 There are special exceptions to the terms and conditions of the GPL 
 as it is applied to this software. View the full text of the 
 exception in file EXCEPTIONS-CONNECTOR-J in the directory of this 
 software distribution.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA



 */
package com.mysql.jdbc;

import com.mysql.jdbc.log.Log;
import com.mysql.jdbc.log.StandardLogger;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.StringRefAddr;

/**
 * Represents configurable properties for Connections and DataSources. Can also
 * expose properties as JDBC DriverPropertyInfo if required as well.
 * 
 * @author Mark Matthews
 * @version $Id: ConnectionProperties.java,v 1.1.2.2 2005/05/17 14:58:56
 *          mmatthews Exp $
 */
public class ConnectionPropertiesImpl implements Serializable, ConnectionProperties {
	
	private static final long serialVersionUID = 4257801713007640580L;

	class BooleanConnectionProperty extends ConnectionProperty implements Serializable {
	
		private static final long serialVersionUID = 2540132501709159404L;

		/**
		 * DOCUMENT ME!
		 * 
		 * @param propertyNameToSet
		 * @param defaultValueToSet
		 * @param descriptionToSet
		 *            DOCUMENT ME!
		 * @param sinceVersionToSet
		 *            DOCUMENT ME!
		 */
		BooleanConnectionProperty(String propertyNameToSet,
				boolean defaultValueToSet, String descriptionToSet,
				String sinceVersionToSet, String category, int orderInCategory) {
			super(propertyNameToSet, Boolean.valueOf(defaultValueToSet), null, 0,
					0, descriptionToSet, sinceVersionToSet, category,
					orderInCategory);
		}

		/**
		 * @see com.mysql.jdbc.ConnectionPropertiesImpl.ConnectionProperty#getAllowableValues()
		 */
		String[] getAllowableValues() {
			return new String[] { "true", "false", "yes", "no" };
		}

		boolean getValueAsBoolean() {
			return ((Boolean) this.valueAsObject).booleanValue();
		}

		/**
		 * @see com.mysql.jdbc.ConnectionPropertiesImpl.ConnectionProperty#hasValueConstraints()
		 */
		boolean hasValueConstraints() {
			return true;
		}

		/**
		 * @see com.mysql.jdbc.ConnectionPropertiesImpl.ConnectionProperty#initializeFrom(java.util.Properties)
		 */
		void initializeFrom(String extractedValue) throws SQLException {
			if (extractedValue != null) {
				validateStringValues(extractedValue);

				this.valueAsObject = Boolean.valueOf(extractedValue
						.equalsIgnoreCase("TRUE")
						|| extractedValue.equalsIgnoreCase("YES"));
			} else {
				this.valueAsObject = this.defaultValue;
			}
		}

		/**
		 * @see com.mysql.jdbc.ConnectionPropertiesImpl.ConnectionProperty#isRangeBased()
		 */
		boolean isRangeBased() {
			return false;
		}

		void setValue(boolean valueFlag) {
			this.valueAsObject = Boolean.valueOf(valueFlag);
		}
	}

	abstract class ConnectionProperty implements Serializable {
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
		
		public ConnectionProperty() {}
		
		ConnectionProperty(String propertyNameToSet, Object defaultValueToSet,
				String[] allowableValuesToSet, int lowerBoundToSet,
				int upperBoundToSet, String descriptionToSet,
				String sinceVersionToSet, String category, int orderInCategory) {
			
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

		abstract boolean hasValueConstraints();

		void initializeFrom(Properties extractFrom) throws SQLException {
			String extractedValue = extractFrom.getProperty(getPropertyName());
			extractFrom.remove(getPropertyName());
			initializeFrom(extractedValue);
		}

		void initializeFrom(Reference ref) throws SQLException {
			RefAddr refAddr = ref.get(getPropertyName());

			if (refAddr != null) {
				String refContentAsString = (String) refAddr.getContent();

				initializeFrom(refContentAsString);
			}
		}

		abstract void initializeFrom(String extractedValue) throws SQLException;

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
		}

		void storeTo(Reference ref) {
			if (getValueAsObject() != null) {
				ref.add(new StringRefAddr(getPropertyName(), getValueAsObject()
						.toString()));
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
		

		void validateStringValues(String valueToValidate) throws SQLException {
			String[] validateAgainst = getAllowableValues();

			if (valueToValidate == null) {
				return;
			}

			if ((validateAgainst == null) || (validateAgainst.length == 0)) {
				return;
			}

			for (int i = 0; i < validateAgainst.length; i++) {
				if ((validateAgainst[i] != null)
						&& validateAgainst[i].equalsIgnoreCase(valueToValidate)) {
					return;
				}
			}

			StringBuffer errorMessageBuf = new StringBuffer();

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
				errorMessageBuf
						.append(validateAgainst[validateAgainst.length - 1]);
				errorMessageBuf.append("'");
			}

			errorMessageBuf.append(". The value '");
			errorMessageBuf.append(valueToValidate);
			errorMessageBuf.append("' is not in this set.");

			throw SQLError.createSQLException(errorMessageBuf.toString(),
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
		}
	}

	class IntegerConnectionProperty extends ConnectionProperty implements Serializable {

		private static final long serialVersionUID = -3004305481796850832L;

		public IntegerConnectionProperty(String propertyNameToSet,
				Object defaultValueToSet, String[] allowableValuesToSet,
				int lowerBoundToSet, int upperBoundToSet,
				String descriptionToSet, String sinceVersionToSet,
				String category, int orderInCategory) {
			super(propertyNameToSet, defaultValueToSet, allowableValuesToSet,
					lowerBoundToSet, upperBoundToSet, descriptionToSet, sinceVersionToSet,
					category, orderInCategory);
		}

		int multiplier = 1;

		IntegerConnectionProperty(String propertyNameToSet,
				int defaultValueToSet, int lowerBoundToSet,
				int upperBoundToSet, String descriptionToSet,
				String sinceVersionToSet, String category, int orderInCategory) {
			super(propertyNameToSet, new Integer(defaultValueToSet), null,
					lowerBoundToSet, upperBoundToSet, descriptionToSet,
					sinceVersionToSet, category, orderInCategory);
		}

		/**
		 * DOCUMENT ME!
		 * 
		 * @param propertyNameToSet
		 * @param defaultValueToSet
		 * @param descriptionToSet
		 * @param sinceVersionToSet
		 *            DOCUMENT ME!
		 */

		IntegerConnectionProperty(String propertyNameToSet,
				int defaultValueToSet, String descriptionToSet,
				String sinceVersionToSet, String category, int orderInCategory) {
			this(propertyNameToSet, defaultValueToSet, 0, 0, descriptionToSet,
					sinceVersionToSet, category, orderInCategory);
		}

		/**
		 * @see com.mysql.jdbc.ConnectionProperties.ConnectionProperty#getAllowableValues()
		 */
		String[] getAllowableValues() {
			return null;
		}

		/**
		 * @see com.mysql.jdbc.ConnectionProperties.ConnectionProperty#getLowerBound()
		 */
		int getLowerBound() {
			return this.lowerBound;
		}

		/**
		 * @see com.mysql.jdbc.ConnectionProperties.ConnectionProperty#getUpperBound()
		 */
		int getUpperBound() {
			return this.upperBound;
		}

		int getValueAsInt() {
			return ((Integer) this.valueAsObject).intValue();
		}

		/**
		 * @see com.mysql.jdbc.ConnectionProperties.ConnectionProperty#hasValueConstraints()
		 */
		boolean hasValueConstraints() {
			return false;
		}

		/**
		 * @see com.mysql.jdbc.ConnectionProperties.ConnectionProperty#initializeFrom(java.lang.String)
		 */
		void initializeFrom(String extractedValue) throws SQLException {
			if (extractedValue != null) {
				try {
					// Parse decimals, too
					int intValue = Double.valueOf(extractedValue).intValue();

					/*
					 * if (isRangeBased()) { if ((intValue < getLowerBound()) ||
					 * (intValue > getUpperBound())) { throw new
					 * SQLException("The connection property '" +
					 * getPropertyName() + "' only accepts integer values in the
					 * range of " + getLowerBound() + " - " + getUpperBound() + ",
					 * the value '" + extractedValue + "' exceeds this range.",
					 * SQLError.SQL_STATE_ILLEGAL_ARGUMENT); } }
					 */
					this.valueAsObject = new Integer(intValue * multiplier);
				} catch (NumberFormatException nfe) {
					throw SQLError.createSQLException("The connection property '"
							+ getPropertyName()
							+ "' only accepts integer values. The value '"
							+ extractedValue
							+ "' can not be converted to an integer.",
							SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
				}
			} else {
				this.valueAsObject = this.defaultValue;
			}
		}

		/**
		 * @see com.mysql.jdbc.ConnectionProperties.ConnectionProperty#isRangeBased()
		 */
		boolean isRangeBased() {
			return getUpperBound() != getLowerBound();
		}

		void setValue(int valueFlag) {
			this.valueAsObject = new Integer(valueFlag);
		}
	}
	
	public class LongConnectionProperty extends IntegerConnectionProperty {

		private static final long serialVersionUID = 6068572984340480895L;

		LongConnectionProperty(String propertyNameToSet,
				long defaultValueToSet, long lowerBoundToSet,
				long upperBoundToSet, String descriptionToSet,
				String sinceVersionToSet, String category, int orderInCategory) {
			super(propertyNameToSet, new Long(defaultValueToSet), null,
					(int)lowerBoundToSet, (int)upperBoundToSet, descriptionToSet,
					sinceVersionToSet, category, orderInCategory);
		}
		

		LongConnectionProperty(String propertyNameToSet,
				long defaultValueToSet, String descriptionToSet,
				String sinceVersionToSet, String category, int orderInCategory) {
			this(propertyNameToSet,
				defaultValueToSet, 0,
				0, descriptionToSet,
				sinceVersionToSet, category, orderInCategory);
		}
		
		void setValue(long value) {
			this.valueAsObject = new Long(value);
		}
		
		long getValueAsLong() {
			return ((Long) this.valueAsObject).longValue();
		}
		
		void initializeFrom(String extractedValue) throws SQLException {
			if (extractedValue != null) {
				try {
					// Parse decimals, too
					long longValue = Double.valueOf(extractedValue).longValue();

					this.valueAsObject = new Long(longValue);
				} catch (NumberFormatException nfe) {
					throw SQLError.createSQLException("The connection property '"
							+ getPropertyName()
							+ "' only accepts long integer values. The value '"
							+ extractedValue
							+ "' can not be converted to a long integer.",
							SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
				}
			} else {
				this.valueAsObject = this.defaultValue;
			}
		}
	}
	
	class MemorySizeConnectionProperty extends IntegerConnectionProperty implements Serializable {

		private static final long serialVersionUID = 7351065128998572656L;

		MemorySizeConnectionProperty(String propertyNameToSet,
				int defaultValueToSet, int lowerBoundToSet,
				int upperBoundToSet, String descriptionToSet,
				String sinceVersionToSet, String category, int orderInCategory) {
			super(propertyNameToSet, defaultValueToSet, lowerBoundToSet,
					upperBoundToSet, descriptionToSet, sinceVersionToSet,
					category, orderInCategory);
			// TODO Auto-generated constructor stub
		}

		void initializeFrom(String extractedValue) throws SQLException {
			if (extractedValue != null) {
				if (extractedValue.endsWith("k")
						|| extractedValue.endsWith("K")
						|| extractedValue.endsWith("kb")
						|| extractedValue.endsWith("Kb")
						|| extractedValue.endsWith("kB")) {
					multiplier = 1024;
					int indexOfK = StringUtils.indexOfIgnoreCase(
							extractedValue, "k");
					extractedValue = extractedValue.substring(0, indexOfK);
				} else if (extractedValue.endsWith("m")
						|| extractedValue.endsWith("M")
						|| extractedValue.endsWith("G")
						|| extractedValue.endsWith("mb")
						|| extractedValue.endsWith("Mb")
						|| extractedValue.endsWith("mB")) {
					multiplier = 1024 * 1024;
					int indexOfM = StringUtils.indexOfIgnoreCase(
							extractedValue, "m");
					extractedValue = extractedValue.substring(0, indexOfM);
				} else if (extractedValue.endsWith("g")
						|| extractedValue.endsWith("G")
						|| extractedValue.endsWith("gb")
						|| extractedValue.endsWith("Gb")
						|| extractedValue.endsWith("gB")) {
					multiplier = 1024 * 1024 * 1024;
					int indexOfG = StringUtils.indexOfIgnoreCase(
							extractedValue, "g");
					extractedValue = extractedValue.substring(0, indexOfG);
				}
			}

			super.initializeFrom(extractedValue);
		}

		void setValue(String value) throws SQLException {
			initializeFrom(value);
		}
	}

	class StringConnectionProperty extends ConnectionProperty implements Serializable {
	
		private static final long serialVersionUID = 5432127962785948272L;

		StringConnectionProperty(String propertyNameToSet,
				String defaultValueToSet, String descriptionToSet,
				String sinceVersionToSet, String category, int orderInCategory) {
			this(propertyNameToSet, defaultValueToSet, null, descriptionToSet,
					sinceVersionToSet, category, orderInCategory);
		}

		/**
		 * DOCUMENT ME!
		 * 
		 * @param propertyNameToSet
		 * @param defaultValueToSet
		 * @param allowableValuesToSet
		 * @param descriptionToSet
		 * @param sinceVersionToSet
		 *            DOCUMENT ME!
		 */
		StringConnectionProperty(String propertyNameToSet,
				String defaultValueToSet, String[] allowableValuesToSet,
				String descriptionToSet, String sinceVersionToSet,
				String category, int orderInCategory) {
			super(propertyNameToSet, defaultValueToSet, allowableValuesToSet,
					0, 0, descriptionToSet, sinceVersionToSet, category,
					orderInCategory);
		}

		String getValueAsString() {
			return (String) this.valueAsObject;
		}

		/**
		 * @see com.mysql.jdbc.ConnectionPropertiesImpl.ConnectionProperty#hasValueConstraints()
		 */
		boolean hasValueConstraints() {
			return (this.allowableValues != null)
					&& (this.allowableValues.length > 0);
		}

		/**
		 * @see com.mysql.jdbc.ConnectionPropertiesImpl.ConnectionProperty#initializeFrom(java.util.Properties)
		 */
		void initializeFrom(String extractedValue) throws SQLException {
			if (extractedValue != null) {
				validateStringValues(extractedValue);

				this.valueAsObject = extractedValue;
			} else {
				this.valueAsObject = this.defaultValue;
			}
		}

		/**
		 * @see com.mysql.jdbc.ConnectionPropertiesImpl.ConnectionProperty#isRangeBased()
		 */
		boolean isRangeBased() {
			return false;
		}

		void setValue(String valueFlag) {
			this.valueAsObject = valueFlag;
		}
	}

	private static final String CONNECTION_AND_AUTH_CATEGORY = "Connection/Authentication";

	private static final String NETWORK_CATEGORY = "Networking";
	
	private static final String DEBUGING_PROFILING_CATEGORY = "Debuging/Profiling";

	private static final String HA_CATEGORY = "High Availability and Clustering";

	private static final String MISC_CATEGORY = "Miscellaneous";

	private static final String PERFORMANCE_CATEGORY = "Performance Extensions";
	
	private static final String SECURITY_CATEGORY = "Security";
	
	private static final String[] PROPERTY_CATEGORIES = new String[] {
		CONNECTION_AND_AUTH_CATEGORY, NETWORK_CATEGORY,
		HA_CATEGORY, SECURITY_CATEGORY,
		PERFORMANCE_CATEGORY, DEBUGING_PROFILING_CATEGORY, MISC_CATEGORY };
	
	private static final ArrayList PROPERTY_LIST = new ArrayList();

	//
	// Yes, this looks goofy, but we're trying to avoid intern()ing here
	//
	private static final String STANDARD_LOGGER_NAME = StandardLogger.class.getName();

	protected static final String ZERO_DATETIME_BEHAVIOR_CONVERT_TO_NULL = "convertToNull";

	protected static final String ZERO_DATETIME_BEHAVIOR_EXCEPTION = "exception";

	protected static final String ZERO_DATETIME_BEHAVIOR_ROUND = "round";

	static {
		try {
			java.lang.reflect.Field[] declaredFields = ConnectionPropertiesImpl.class
					.getDeclaredFields();

			for (int i = 0; i < declaredFields.length; i++) {
				if (ConnectionPropertiesImpl.ConnectionProperty.class
						.isAssignableFrom(declaredFields[i].getType())) {
					PROPERTY_LIST.add(declaredFields[i]);
				}
			}
		} catch (Exception ex) {
			throw new RuntimeException(ex.toString());
		}
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
	protected static DriverPropertyInfo[] exposeAsDriverPropertyInfo(
			Properties info, int slotsToReserve) throws SQLException {
		return (new ConnectionPropertiesImpl() {
		}).exposeAsDriverPropertyInfoInternal(info, slotsToReserve);
	}

	private BooleanConnectionProperty allowLoadLocalInfile = new BooleanConnectionProperty(
			"allowLoadLocalInfile",
			true,
			"Should the driver allow use of 'LOAD DATA LOCAL INFILE...' (defaults to 'true').",
			"3.0.3", SECURITY_CATEGORY, Integer.MAX_VALUE);

	private BooleanConnectionProperty allowMultiQueries = new BooleanConnectionProperty(
			"allowMultiQueries",
			false,
			"Allow the use of ';' to delimit multiple queries during one statement (true/false), defaults to 'false'",
			"3.1.1", SECURITY_CATEGORY, 1);

	private BooleanConnectionProperty allowNanAndInf = new BooleanConnectionProperty(
			"allowNanAndInf",
			false,
			"Should the driver allow NaN or +/- INF values in PreparedStatement.setDouble()?",
			"3.1.5", MISC_CATEGORY, Integer.MIN_VALUE);

	private BooleanConnectionProperty allowUrlInLocalInfile = new BooleanConnectionProperty(
			"allowUrlInLocalInfile",
			false,
			"Should the driver allow URLs in 'LOAD DATA LOCAL INFILE' statements?",
			"3.1.4", SECURITY_CATEGORY, Integer.MAX_VALUE);

	private BooleanConnectionProperty alwaysSendSetIsolation = new BooleanConnectionProperty(
			"alwaysSendSetIsolation",
			true,
			"Should the driver always communicate with the database when "
					+ " Connection.setTransactionIsolation() is called? "
					+ "If set to false, the driver will only communicate with the "
					+ "database when the requested transaction isolation is different "
					+ "than the whichever is newer, the last value that was set via "
					+ "Connection.setTransactionIsolation(), or the value that was read from "
					+ "the server when the connection was established.",
			"3.1.7", PERFORMANCE_CATEGORY, Integer.MAX_VALUE);

	private BooleanConnectionProperty autoClosePStmtStreams = new BooleanConnectionProperty(
			"autoClosePStmtStreams", 
			false,
			"Should the driver automatically call .close() on streams/readers passed as "
			+ "arguments via set*() methods?",
			"3.1.12",
			MISC_CATEGORY,
			Integer.MIN_VALUE);
	
	private BooleanConnectionProperty autoDeserialize = new BooleanConnectionProperty(
			"autoDeserialize",
			false,
			"Should the driver automatically detect and de-serialize objects stored in BLOB fields?",
			"3.1.5", MISC_CATEGORY, Integer.MIN_VALUE);

	private BooleanConnectionProperty autoGenerateTestcaseScript = new BooleanConnectionProperty(
			"autoGenerateTestcaseScript", false,
			"Should the driver dump the SQL it is executing, including server-side "
					+ "prepared statements to STDERR?", "3.1.9",
			DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE);

	private boolean autoGenerateTestcaseScriptAsBoolean = false;

	private BooleanConnectionProperty autoReconnect = new BooleanConnectionProperty(
			"autoReconnect",
			false,
			"Should the driver try to re-establish stale and/or dead connections? "
					+ "  If enabled the driver will throw an exception for a queries issued on a stale or dead connection, "
					+ " which belong to the current transaction, but will attempt reconnect before the next query issued on the "
					+ "connection in a new transaction. The use of this feature "
					+ "is not recommended, because it has side effects related to session state and data consistency when applications don't"
					+ "handle SQLExceptions properly, and is only designed to be used "
					+ "when you are unable to configure your application to handle SQLExceptions resulting from dead and"
					+ "stale connections properly. Alternatively, investigate setting the MySQL server variable \"wait_timeout\""
					+ "to some high value rather than the default of 8 hours.",
			"1.1", HA_CATEGORY, 0);

	private BooleanConnectionProperty autoReconnectForPools = new BooleanConnectionProperty(
			"autoReconnectForPools",
			false,
			"Use a reconnection strategy appropriate for connection pools (defaults to 'false')",
			"3.1.3", HA_CATEGORY, 1);

	private boolean autoReconnectForPoolsAsBoolean = false;

	private MemorySizeConnectionProperty blobSendChunkSize = new MemorySizeConnectionProperty(
			"blobSendChunkSize",
			1024 * 1024,
			1,
			Integer.MAX_VALUE,
			"Chunk to use when sending BLOB/CLOBs via ServerPreparedStatements",
			"3.1.9", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);

	private BooleanConnectionProperty cacheCallableStatements = new BooleanConnectionProperty(
			"cacheCallableStmts", false,
			"Should the driver cache the parsing stage of CallableStatements",
			"3.1.2", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);

	private BooleanConnectionProperty cachePreparedStatements = new BooleanConnectionProperty(
			"cachePrepStmts",
			false,
			"Should the driver cache the parsing stage of PreparedStatements of client-side "
					+ "prepared statements, the \"check\" for suitability of server-side prepared "
					+ " and server-side prepared statements themselves?",
			"3.0.10", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);

	private BooleanConnectionProperty cacheResultSetMetadata = new BooleanConnectionProperty(
			"cacheResultSetMetadata",
			false,
			"Should the driver cache ResultSetMetaData for Statements and PreparedStatements? (Req. JDK-1.4+, true/false, default 'false')",
			"3.1.1", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);

	private boolean cacheResultSetMetaDataAsBoolean;

	private BooleanConnectionProperty cacheServerConfiguration = new BooleanConnectionProperty(
			"cacheServerConfiguration",
			false,
			"Should the driver cache the results of "
					+ "'SHOW VARIABLES' and 'SHOW COLLATION' on a per-URL basis?",
			"3.1.5", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);

	private IntegerConnectionProperty callableStatementCacheSize = new IntegerConnectionProperty(
			"callableStmtCacheSize",
			100,
			0,
			Integer.MAX_VALUE,
			"If 'cacheCallableStmts' is enabled, how many callable statements should be cached?",
			"3.1.2", PERFORMANCE_CATEGORY, 5);

	private BooleanConnectionProperty capitalizeTypeNames = new BooleanConnectionProperty(
			"capitalizeTypeNames",
			false,
			"Capitalize type names in DatabaseMetaData? (usually only useful when using WebObjects, true/false, defaults to 'false')",
			"2.0.7", MISC_CATEGORY, Integer.MIN_VALUE);

	private StringConnectionProperty characterEncoding = new StringConnectionProperty(
			"characterEncoding",
			null,
			"If 'useUnicode' is set to true, what character encoding should the driver use when dealing with strings? (defaults is to 'autodetect')",
			"1.1g", MISC_CATEGORY, 5);

	private String characterEncodingAsString = null;

	private StringConnectionProperty characterSetResults = new StringConnectionProperty(
			"characterSetResults", null,
			"Character set to tell the server to return results as.", "3.0.13",
			MISC_CATEGORY, 6);
	
	private StringConnectionProperty clientInfoProvider = new StringConnectionProperty(
			"clientInfoProvider", "com.mysql.jdbc.JDBC4CommentClientInfoProvider",
			"The name of a class that implements the com.mysql.jdbc.JDBC4ClientInfoProvider "+
			"interface in order to support JDBC-4.0's Connection.get/setClientInfo() methods",
			"5.1.0",
			DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE);
	
	private BooleanConnectionProperty clobberStreamingResults = new BooleanConnectionProperty(
			"clobberStreamingResults",
			false,
			"This will cause a 'streaming' ResultSet to be automatically closed, "
					+ "and any outstanding data still streaming from the server to be discarded if another query is executed "
					+ "before all the data has been read from the server.",
			"3.0.9", MISC_CATEGORY, Integer.MIN_VALUE);
	
	private StringConnectionProperty clobCharacterEncoding = new StringConnectionProperty(
			"clobCharacterEncoding",
			null,
			"The character encoding to use for sending and retrieving TEXT, MEDIUMTEXT " +
			"and LONGTEXT values instead of the configured connection characterEncoding",
			"5.0.0", MISC_CATEGORY, Integer.MIN_VALUE);

	private StringConnectionProperty connectionCollation = new StringConnectionProperty(
			"connectionCollation",
			null,
			"If set, tells the server to use this collation via 'set collation_connection'",
			"3.0.13", MISC_CATEGORY, 7);

	private IntegerConnectionProperty connectTimeout = new IntegerConnectionProperty(
			"connectTimeout", 0, 0, Integer.MAX_VALUE,
			"Timeout for socket connect (in milliseconds), with 0 being no timeout. "
					+ "Only works on JDK-1.4 or newer. Defaults to '0'.",
			"3.0.1", CONNECTION_AND_AUTH_CATEGORY, 9);

	private BooleanConnectionProperty continueBatchOnError = new BooleanConnectionProperty(
			"continueBatchOnError",
			true,
			"Should the driver continue processing batch commands if "
					+ "one statement fails. The JDBC spec allows either way (defaults to 'true').",
			"3.0.3", MISC_CATEGORY, Integer.MIN_VALUE);

	private BooleanConnectionProperty createDatabaseIfNotExist = new BooleanConnectionProperty(
			"createDatabaseIfNotExist",
			false,
			"Creates the database given in the URL if it doesn't yet exist. Assumes "
					+ " the configured user has permissions to create databases.",
			"3.1.9", MISC_CATEGORY, Integer.MIN_VALUE);

	private IntegerConnectionProperty defaultFetchSize = new IntegerConnectionProperty("defaultFetchSize", 0, "The driver will call setFetchSize(n) with this value on all newly-created Statements", "3.1.9", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);

	private BooleanConnectionProperty detectServerPreparedStmts = new BooleanConnectionProperty(
			"useServerPrepStmts",
			false,
			"Use server-side prepared statements if the server supports them?",
			"3.1.0", MISC_CATEGORY, Integer.MIN_VALUE);
	
	private BooleanConnectionProperty dontTrackOpenResources = new BooleanConnectionProperty(
			"dontTrackOpenResources",
			false,
			"The JDBC specification requires the driver to automatically track and close resources, "
					+ "however if your application doesn't do a good job of "
					+ "explicitly calling close() on statements or result sets, "
					+ "this can cause memory leakage. Setting this property to true "
					+ "relaxes this constraint, and can be more memory efficient for "
					+ "some applications.", "3.1.7", PERFORMANCE_CATEGORY,
			Integer.MIN_VALUE);

	private BooleanConnectionProperty dumpQueriesOnException = new BooleanConnectionProperty(
			"dumpQueriesOnException",
			false,
			"Should the driver dump the contents of the query sent to the server in the message for SQLExceptions?",
			"3.1.3", DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE);

	private BooleanConnectionProperty dynamicCalendars = new BooleanConnectionProperty(
			"dynamicCalendars",
			false,
			"Should the driver retrieve the default"
					+ " calendar when required, or cache it per connection/session?",
			"3.1.5", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);

	private BooleanConnectionProperty elideSetAutoCommits = new BooleanConnectionProperty(
			"elideSetAutoCommits",
			false,
			"If using MySQL-4.1 or newer, should the driver only issue 'set autocommit=n' queries when the server's state doesn't match the requested state by Connection.setAutoCommit(boolean)?",
			"3.1.3", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);

	private BooleanConnectionProperty emptyStringsConvertToZero = new BooleanConnectionProperty(
			"emptyStringsConvertToZero", true,
			"Should the driver allow conversions from empty string "
					+ "fields to numeric values of '0'?", "3.1.8",
			MISC_CATEGORY, Integer.MIN_VALUE);

	private BooleanConnectionProperty emulateLocators = new BooleanConnectionProperty(
			"emulateLocators", false, "N/A", "3.1.0", MISC_CATEGORY,
			Integer.MIN_VALUE);

	private BooleanConnectionProperty emulateUnsupportedPstmts = new BooleanConnectionProperty(
			"emulateUnsupportedPstmts",
			true,
			"Should the driver detect prepared statements that are not supported by the server, and "
					+ "replace them with client-side emulated versions?",
			"3.1.7", MISC_CATEGORY, Integer.MIN_VALUE);

	private BooleanConnectionProperty enablePacketDebug = new BooleanConnectionProperty(
			"enablePacketDebug",
			false,
			"When enabled, a ring-buffer of 'packetDebugBufferSize' packets will be kept, and dumped when exceptions are thrown in key areas in the driver's code",
			"3.1.3", DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE);

	private BooleanConnectionProperty enableQueryTimeouts = new BooleanConnectionProperty(
			"enableQueryTimeouts",
			true,
			"When enabled, query timeouts set via Statement.setQueryTimeout() use a shared "
			+ "java.util.Timer instance for scheduling. Even if the timeout doesn't expire before the query is processed, there will be "
			+ "memory used by the TimerTask for the given timeout which won't be reclaimed until "
			+ "the time the timeout would have expired if it hadn't been cancelled by the driver. High-load environments "
			+ "might want to consider disabling this functionality.",
			"5.0.6",
			PERFORMANCE_CATEGORY, Integer.MIN_VALUE);
			
	private BooleanConnectionProperty explainSlowQueries = new BooleanConnectionProperty(
			"explainSlowQueries",
			false,
			"If 'logSlowQueries' is enabled, should the driver automatically issue an 'EXPLAIN' on the"
					+ " server and send the results to the configured log at a WARN level?",
			"3.1.2", DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE);

	/** When failed-over, set connection to read-only? */
	private BooleanConnectionProperty failOverReadOnly = new BooleanConnectionProperty(
			"failOverReadOnly",
			true,
			"When failing over in autoReconnect mode, should the connection be set to 'read-only'?",
			"3.0.12", HA_CATEGORY, 2);

	private BooleanConnectionProperty gatherPerformanceMetrics = new BooleanConnectionProperty(
			"gatherPerfMetrics",
			false,
			"Should the driver gather performance metrics, and report them via the configured logger every 'reportMetricsIntervalMillis' milliseconds?",
			"3.1.2", DEBUGING_PROFILING_CATEGORY, 1);

	private BooleanConnectionProperty generateSimpleParameterMetadata = new BooleanConnectionProperty(
			"generateSimpleParameterMetadata", false, "Should the driver generate simplified parameter metadata for PreparedStatements when "
			+ "no metadata is available either because the server couldn't support preparing the statement, or server-side prepared statements" +
					" are disabled?"
			, "5.0.5", MISC_CATEGORY, Integer.MIN_VALUE);
	
	private boolean highAvailabilityAsBoolean = false;

	private BooleanConnectionProperty holdResultsOpenOverStatementClose = new BooleanConnectionProperty(
			"holdResultsOpenOverStatementClose",
			false,
			"Should the driver close result sets on Statement.close() as required by the JDBC specification?",
			"3.1.7", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);

	private BooleanConnectionProperty ignoreNonTxTables = new BooleanConnectionProperty(
			"ignoreNonTxTables",
			false,
			"Ignore non-transactional table warning for rollback? (defaults to 'false').",
			"3.0.9", MISC_CATEGORY, Integer.MIN_VALUE);

	private IntegerConnectionProperty initialTimeout = new IntegerConnectionProperty(
			"initialTimeout", 2, 1, Integer.MAX_VALUE,
			"If autoReconnect is enabled, the"
					+ " initial time to wait between"
					+ " re-connect attempts (in seconds, defaults to '2').",
			"1.1", HA_CATEGORY, 5);

	private BooleanConnectionProperty isInteractiveClient = new BooleanConnectionProperty(
			"interactiveClient",
			false,
			"Set the CLIENT_INTERACTIVE flag, which tells MySQL "
					+ "to timeout connections based on INTERACTIVE_TIMEOUT instead of WAIT_TIMEOUT",
			"3.1.0", CONNECTION_AND_AUTH_CATEGORY, Integer.MIN_VALUE);

	private BooleanConnectionProperty jdbcCompliantTruncation = new BooleanConnectionProperty(
			"jdbcCompliantTruncation",
			true,
			"Should the driver throw java.sql.DataTruncation"
					+ " exceptions when data is truncated as is required by the JDBC specification when connected to a server that supports warnings"
					+ "(MySQL 4.1.0 and newer)?", "3.1.2", MISC_CATEGORY,
			Integer.MIN_VALUE);

	private boolean jdbcCompliantTruncationForReads = 
		this.jdbcCompliantTruncation.getValueAsBoolean();
	
	private StringConnectionProperty loadBalanceStrategy = new StringConnectionProperty(
			"loadBalanceStrategy",
			"random",
			new String[] {"random", "bestResponseTime"},
			"If using a load-balanced connection to connect to SQL nodes in a MySQL Cluster/NDB configuration" +
			"(by using the URL prefix \"jdbc:mysql:loadbalance://\"), which load balancin algorithm should the driver " +
			"use: (1) \"random\" - the driver will pick a random host for each request. This tends " +
			"to work better than round-robin, as the randomness will somewhat account for " +
			"spreading loads where requests vary in response time, while round-robin " +
			"can sometimes lead to overloaded nodes if there are variations in response times " +
			"across the workload. (2) \"bestResponseTime\" - the driver will route the request to the host that had " +
			"the best response time for the previous transaction.",
			"5.0.6", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);
	
	private StringConnectionProperty localSocketAddress = new StringConnectionProperty("localSocketAddress",
			null, "Hostname or IP address given to explicitly configure the interface that "
			+ "the driver will bind the client side of the TCP/IP connection to when connecting.",
			"5.0.5", CONNECTION_AND_AUTH_CATEGORY, Integer.MIN_VALUE);
	
	private MemorySizeConnectionProperty locatorFetchBufferSize = new MemorySizeConnectionProperty(
			"locatorFetchBufferSize",
			1024 * 1024,
			0,
			Integer.MAX_VALUE,
			"If 'emulateLocators' is configured to 'true', what size "
					+ " buffer should be used when fetching BLOB data for getBinaryInputStream?",
			"3.2.1", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);

	private StringConnectionProperty loggerClassName = new StringConnectionProperty(
			"logger", STANDARD_LOGGER_NAME,
			"The name of a class that implements '" + Log.class.getName()
					+ "' that will be used to log messages to."
					+ "(default is '" + STANDARD_LOGGER_NAME + "', which "
					+ "logs to STDERR)", "3.1.1", DEBUGING_PROFILING_CATEGORY,
			0);

	private BooleanConnectionProperty logSlowQueries = new BooleanConnectionProperty(
			"logSlowQueries",
			false,
			"Should queries that take longer than 'slowQueryThresholdMillis' be logged?",
			"3.1.2", DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE);

	private BooleanConnectionProperty logXaCommands = new BooleanConnectionProperty(
			"logXaCommands",
			false,
			"Should the driver log XA commands sent by MysqlXaConnection to the server," +
			" at the DEBUG level of logging?",
			"5.0.5", DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE);
	
	private BooleanConnectionProperty maintainTimeStats = new BooleanConnectionProperty(
			"maintainTimeStats",
			true,
			"Should the driver maintain various internal timers to enable "
					+ "idle time calculations as well as more verbose error messages when "
					+ "the connection to the server fails? Setting this property to "
					+ "false removes at least two calls to System.getCurrentTimeMillis() "
					+ "per query.", "3.1.9", PERFORMANCE_CATEGORY,
			Integer.MAX_VALUE);

	private boolean maintainTimeStatsAsBoolean = true;

	private IntegerConnectionProperty maxQuerySizeToLog = new IntegerConnectionProperty(
			"maxQuerySizeToLog",
			2048,
			0,
			Integer.MAX_VALUE,
			"Controls the maximum length/size of a query that will get logged when profiling or tracing",
			"3.1.3", DEBUGING_PROFILING_CATEGORY, 4);

	private IntegerConnectionProperty maxReconnects = new IntegerConnectionProperty(
			"maxReconnects",
			3,
			1,
			Integer.MAX_VALUE,
			"Maximum number of reconnects to attempt if autoReconnect is true, default is '3'.",
			"1.1", HA_CATEGORY, 4);

	private IntegerConnectionProperty maxRows = new IntegerConnectionProperty(
			"maxRows", -1, -1, Integer.MAX_VALUE,
			"The maximum number of rows to return "
					+ " (0, the default means return all rows).",
			"all versions", MISC_CATEGORY, Integer.MIN_VALUE);

	private int maxRowsAsInt = -1;

	private IntegerConnectionProperty metadataCacheSize = new IntegerConnectionProperty(
			"metadataCacheSize",
			50,
			1,
			Integer.MAX_VALUE,
			"The number of queries to cache"
					+ "ResultSetMetadata for if cacheResultSetMetaData is set to 'true' (default 50)",
			"3.1.1", PERFORMANCE_CATEGORY, 5);
	
	private IntegerConnectionProperty netTimeoutForStreamingResults = new IntegerConnectionProperty(
			"netTimeoutForStreamingResults", 600,
			0, Integer.MAX_VALUE,
			"What value should the driver automatically set the server setting 'net_write_timeout' to when" +
			"the streaming result sets feature is in use? (value has unit of seconds, the value '0' means the driver will not try and adjust this value)",
			"5.1.0", MISC_CATEGORY, Integer.MIN_VALUE);
	
	private BooleanConnectionProperty noAccessToProcedureBodies = new BooleanConnectionProperty(
			"noAccessToProcedureBodies",
			false,
			"When determining procedure parameter types for CallableStatements, and the connected user "
			+ " can't access procedure bodies through \"SHOW CREATE PROCEDURE\" or select on mysql.proc "
			+ " should the driver instead create basic metadata (all parameters reported as INOUT VARCHARs) instead "
			+ " of throwing an exception?",
			"5.0.3", MISC_CATEGORY, Integer.MIN_VALUE);
			
	private BooleanConnectionProperty noDatetimeStringSync = new BooleanConnectionProperty(
			"noDatetimeStringSync",
			false,
			"Don't ensure that ResultSet.getDatetimeType().toString().equals(ResultSet.getString())",
			"3.1.7", MISC_CATEGORY, Integer.MIN_VALUE);
	
	private BooleanConnectionProperty noTimezoneConversionForTimeType = new BooleanConnectionProperty(
			"noTimezoneConversionForTimeType",
			false,
			"Don't convert TIME values using the server timezone if 'useTimezone'='true'",
			"5.0.0", MISC_CATEGORY, Integer.MIN_VALUE);

	private BooleanConnectionProperty nullCatalogMeansCurrent = new BooleanConnectionProperty(
			"nullCatalogMeansCurrent",
			true,
			"When DatabaseMetadataMethods ask for a 'catalog' parameter, does the value null mean use the current catalog? "
					+ "(this is not JDBC-compliant, but follows legacy behavior from earlier versions of the driver)",
			"3.1.8", MISC_CATEGORY, Integer.MIN_VALUE);

	private BooleanConnectionProperty nullNamePatternMatchesAll = new BooleanConnectionProperty(
			"nullNamePatternMatchesAll",
			true,
			"Should DatabaseMetaData methods that accept *pattern parameters treat null the same as '%' "
					+ " (this is not JDBC-compliant, however older versions of the driver accepted this departure from the specification)",
			"3.1.8", MISC_CATEGORY, Integer.MIN_VALUE);

	private IntegerConnectionProperty packetDebugBufferSize = new IntegerConnectionProperty(
			"packetDebugBufferSize",
			20,
			0,
			Integer.MAX_VALUE,
			"The maximum number of packets to retain when 'enablePacketDebug' is true",
			"3.1.3", DEBUGING_PROFILING_CATEGORY, 7);
	
	private BooleanConnectionProperty padCharsWithSpace = new BooleanConnectionProperty(
			"padCharsWithSpace",
			false,
			"If a result set column has the CHAR type and the value does not fill the "
			+ "amount of characters specified in the DDL for the column, should the driver "
			+ "pad the remaining characters with space (for ANSI compliance)?",
			"5.0.6",
			MISC_CATEGORY,
			Integer.MIN_VALUE);

	private BooleanConnectionProperty paranoid = new BooleanConnectionProperty(
			"paranoid",
			false,
			"Take measures to prevent exposure sensitive information in error messages and clear "
					+ "data structures holding sensitive data when possible? (defaults to 'false')",
			"3.0.1", SECURITY_CATEGORY, Integer.MIN_VALUE);

	private BooleanConnectionProperty pedantic = new BooleanConnectionProperty(
			"pedantic", false, "Follow the JDBC spec to the letter.", "3.0.0",
			MISC_CATEGORY, Integer.MIN_VALUE);

	private BooleanConnectionProperty pinGlobalTxToPhysicalConnection = new BooleanConnectionProperty(
			"pinGlobalTxToPhysicalConnection", false, "When using XAConnections, should the driver ensure that "
			+ " operations on a given XID are always routed to the same physical connection? This allows the XAConnection"
			+ " to support \"XA START ... JOIN\" after \"XA END\" has been called",
			"5.0.1", MISC_CATEGORY, Integer.MIN_VALUE);
	
	private BooleanConnectionProperty populateInsertRowWithDefaultValues = new BooleanConnectionProperty(
			"populateInsertRowWithDefaultValues", false,
			"When using ResultSets that are CONCUR_UPDATABLE, should the driver pre-poulate " +
			"the \"insert\" row with default values from the DDL for the table used in the query " +
			" so those values are immediately available for ResultSet accessors? This functionality requires a " +
			" call to the database for metadata each time a result set of this type is created. " +
			" If disabled (the default), the default values will be populated by the an internal" +
			" call to refreshRow() which pulls back default values and/or values changed by triggers.",
			"5.0.5", MISC_CATEGORY, Integer.MIN_VALUE);
	
	private IntegerConnectionProperty preparedStatementCacheSize = new IntegerConnectionProperty(
			"prepStmtCacheSize", 25, 0, Integer.MAX_VALUE,
			"If prepared statement caching is enabled, "
					+ "how many prepared statements should be cached?",
			"3.0.10", PERFORMANCE_CATEGORY, 10);

	private IntegerConnectionProperty preparedStatementCacheSqlLimit = new IntegerConnectionProperty(
			"prepStmtCacheSqlLimit",
			256,
			1,
			Integer.MAX_VALUE,
			"If prepared statement caching is enabled, "
					+ "what's the largest SQL the driver will cache the parsing for?",
			"3.0.10", PERFORMANCE_CATEGORY, 11);

	private BooleanConnectionProperty processEscapeCodesForPrepStmts = 
		new BooleanConnectionProperty("processEscapeCodesForPrepStmts",
				true,
				"Should the driver process escape codes in queries that are prepared?",
				"3.1.12",
				MISC_CATEGORY, Integer.MIN_VALUE);
	
	private StringConnectionProperty profileSql = new StringConnectionProperty(
			"profileSql",
			null,
			"Deprecated, use 'profileSQL' instead. Trace queries and their execution/fetch times on STDERR (true/false) defaults to 'false'",
			"2.0.14", DEBUGING_PROFILING_CATEGORY, 3);

	private BooleanConnectionProperty profileSQL = new BooleanConnectionProperty(
			"profileSQL",
			false,
			"Trace queries and their execution/fetch times to the configured logger (true/false) defaults to 'false'",
			"3.1.0", DEBUGING_PROFILING_CATEGORY, 1);

	private boolean profileSQLAsBoolean = false;

	private StringConnectionProperty propertiesTransform = new StringConnectionProperty(
			NonRegisteringDriver.PROPERTIES_TRANSFORM_KEY,
			null,
			"An implementation of com.mysql.jdbc.ConnectionPropertiesTransform that the driver will use to modify URL properties passed to the driver before attempting a connection",
			"3.1.4", CONNECTION_AND_AUTH_CATEGORY, Integer.MIN_VALUE);

	private IntegerConnectionProperty queriesBeforeRetryMaster = new IntegerConnectionProperty(
			"queriesBeforeRetryMaster",
			50,
			1,
			Integer.MAX_VALUE,
			"Number of queries to issue before falling back to master when failed over "
					+ "(when using multi-host failover). Whichever condition is met first, "
					+ "'queriesBeforeRetryMaster' or 'secondsBeforeRetryMaster' will cause an "
					+ "attempt to be made to reconnect to the master. Defaults to 50.",
			"3.0.2", HA_CATEGORY, 7);

	private BooleanConnectionProperty reconnectAtTxEnd = new BooleanConnectionProperty(
			"reconnectAtTxEnd", false,
			"If autoReconnect is set to true, should the driver attempt reconnections"
					+ "at the end of every transaction?", "3.0.10",
			HA_CATEGORY, 4);

	private boolean reconnectTxAtEndAsBoolean = false;

	private BooleanConnectionProperty relaxAutoCommit = new BooleanConnectionProperty(
			"relaxAutoCommit",
			false,
			"If the version of MySQL the driver connects to does not support transactions, still allow calls to commit(), rollback() and setAutoCommit() (true/false, defaults to 'false')?",
			"2.0.13", MISC_CATEGORY, Integer.MIN_VALUE);

	private IntegerConnectionProperty reportMetricsIntervalMillis = new IntegerConnectionProperty(
			"reportMetricsIntervalMillis",
			30000,
			0,
			Integer.MAX_VALUE,
			"If 'gatherPerfMetrics' is enabled, how often should they be logged (in ms)?",
			"3.1.2", DEBUGING_PROFILING_CATEGORY, 3);

	private BooleanConnectionProperty requireSSL = new BooleanConnectionProperty(
			"requireSSL", false,
			"Require SSL connection if useSSL=true? (defaults to 'false').",
			"3.1.0", SECURITY_CATEGORY, 3);

	private StringConnectionProperty resourceId = new StringConnectionProperty(
			"resourceId",
			null, "A globally unique name that identifies the resource that this datasource or connection is " +
			"connected to, used for XAResource.isSameRM() when the driver can't determine this value based on " +
			"hostnames used in the URL",
			"5.0.1",
			HA_CATEGORY,
			Integer.MIN_VALUE);
	
	private IntegerConnectionProperty resultSetSizeThreshold = new IntegerConnectionProperty("resultSetSizeThreshold", 100,
			"If the usage advisor is enabled, how many rows should a result set contain before the driver warns that it "
			+ " is suspiciously large?", "5.0.5", DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE);
			
	private BooleanConnectionProperty retainStatementAfterResultSetClose = new BooleanConnectionProperty(
			"retainStatementAfterResultSetClose",
			false,
			"Should the driver retain the Statement reference in a ResultSet after ResultSet.close()"
					+ " has been called. This is not JDBC-compliant after JDBC-4.0.",
			"3.1.11", MISC_CATEGORY, Integer.MIN_VALUE);
	
	private BooleanConnectionProperty rewriteBatchedStatements = new BooleanConnectionProperty(
			"rewriteBatchedStatements",
			false, 
			"Should the driver use multiqueries (irregardless of the setting of \"allowMultiQueries\") as well as "
			+ "rewriting of prepared statements for INSERT into multi-value inserts when executeBatch() is called? Notice that this has the potential "
			+ "for SQL injection if using plain java.sql.Statements and your code doesn't sanitize input correctly.\n\n"
			+ "Notice that for prepared statements, server-side prepared statements can not currently take advantage of "
			+ "this rewrite option, and that if you don't specify stream lengths when using PreparedStatement.set*Stream()," 
			+ "the driver won't be able to determine the optimium number of parameters per batch and you might receive an" 
			+ "an error from the driver that the resultant packet is too large.\n\n"
			+ "Statement.getGeneratedKeys() for these rewritten statements only works when the entire " 
			+ "batch includes INSERT statements.",
			"3.1.13", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);

	private BooleanConnectionProperty rollbackOnPooledClose = new BooleanConnectionProperty(
			"rollbackOnPooledClose",
			true,
			"Should the driver issue a rollback() when the logical connection in a pool is closed?",
			"3.0.15", MISC_CATEGORY, Integer.MIN_VALUE);

	private BooleanConnectionProperty roundRobinLoadBalance = new BooleanConnectionProperty(
			"roundRobinLoadBalance",
			false,
			"When autoReconnect is enabled, and failoverReadonly is false, should we pick hosts to connect to on a round-robin basis?",
			"3.1.2", HA_CATEGORY, 5);

	private BooleanConnectionProperty runningCTS13 = new BooleanConnectionProperty(
			"runningCTS13",
			false,
			"Enables workarounds for bugs in Sun's JDBC compliance testsuite version 1.3",
			"3.1.7", MISC_CATEGORY, Integer.MIN_VALUE);

	private IntegerConnectionProperty secondsBeforeRetryMaster = new IntegerConnectionProperty(
			"secondsBeforeRetryMaster",
			30,
			1,
			Integer.MAX_VALUE,
			"How long should the driver wait, when failed over, before attempting "
					+ "to reconnect to the master server? Whichever condition is met first, "
					+ "'queriesBeforeRetryMaster' or 'secondsBeforeRetryMaster' will cause an "
					+ "attempt to be made to reconnect to the master. Time in seconds, defaults to 30",
			"3.0.2", HA_CATEGORY, 8);

	private StringConnectionProperty serverTimezone = new StringConnectionProperty(
			"serverTimezone",
			null,
			"Override detection/mapping of timezone. Used when timezone from server doesn't map to Java timezone",
			"3.0.2", MISC_CATEGORY, Integer.MIN_VALUE);

	private StringConnectionProperty sessionVariables = new StringConnectionProperty(
			"sessionVariables", null,
			"A comma-separated list of name/value pairs to be sent as SET SESSION ... to "
					+ " the server when the driver connects.", "3.1.8",
			MISC_CATEGORY, Integer.MAX_VALUE);

	private IntegerConnectionProperty slowQueryThresholdMillis = new IntegerConnectionProperty(
			"slowQueryThresholdMillis",
			2000,
			0,
			Integer.MAX_VALUE,
			"If 'logSlowQueries' is enabled, how long should a query (in ms) before it is logged as 'slow'?",
			"3.1.2", DEBUGING_PROFILING_CATEGORY, 9);
	
	private LongConnectionProperty slowQueryThresholdNanos = new LongConnectionProperty(
			"slowQueryThresholdNanos",
			0,
			"If 'useNanosForElapsedTime' is set to true, and this property is set to a non-zero value," 
			+ " the driver will use this threshold (in nanosecond units) to determine if a query was slow.",
			"5.0.7",
			DEBUGING_PROFILING_CATEGORY,
			10);
	
	private StringConnectionProperty socketFactoryClassName = new StringConnectionProperty(
			"socketFactory",
			StandardSocketFactory.class.getName(),
			"The name of the class that the driver should use for creating socket connections to the server. This class must implement the interface 'com.mysql.jdbc.SocketFactory' and have public no-args constructor.",
			"3.0.3", CONNECTION_AND_AUTH_CATEGORY, 4);

	private IntegerConnectionProperty socketTimeout = new IntegerConnectionProperty(
			"socketTimeout",
			0,
			0,
			Integer.MAX_VALUE,
			"Timeout on network socket operations (0, the default means no timeout).",
			"3.0.1", CONNECTION_AND_AUTH_CATEGORY, 10);

	private BooleanConnectionProperty strictFloatingPoint = new BooleanConnectionProperty(
			"strictFloatingPoint", false,
			"Used only in older versions of compliance test", "3.0.0",
			MISC_CATEGORY, Integer.MIN_VALUE);

	private BooleanConnectionProperty strictUpdates = new BooleanConnectionProperty(
			"strictUpdates",
			true,
			"Should the driver do strict checking (all primary keys selected) of updatable result sets (true, false, defaults to 'true')?",
			"3.0.4", MISC_CATEGORY, Integer.MIN_VALUE);

	private BooleanConnectionProperty overrideSupportsIntegrityEnhancementFacility =
		new BooleanConnectionProperty("overrideSupportsIntegrityEnhancementFacility",
				false,
				"Should the driver return \"true\" for DatabaseMetaData.supportsIntegrityEnhancementFacility() "
				+ "even if the database doesn't support it to workaround applications that require this method to return "
				+ "\"true\" to signal support of foreign keys, even though the SQL specification states that this facility "
				+ "contains much more than just foreign key support (one such application being OpenOffice)?",
				"3.1.12", MISC_CATEGORY, Integer.MIN_VALUE);
	
	private BooleanConnectionProperty tcpNoDelay = new BooleanConnectionProperty(
			StandardSocketFactory.TCP_NO_DELAY_PROPERTY_NAME,
			Boolean.valueOf(StandardSocketFactory.TCP_NO_DELAY_DEFAULT_VALUE).booleanValue(),
			"If connecting using TCP/IP, should the driver set SO_TCP_NODELAY (disabling the Nagle Algorithm)?",
			"5.0.7", NETWORK_CATEGORY, Integer.MIN_VALUE);
	
	private BooleanConnectionProperty tcpKeepAlive = new BooleanConnectionProperty(
			StandardSocketFactory.TCP_KEEP_ALIVE_PROPERTY_NAME,
			Boolean.valueOf(StandardSocketFactory.TCP_KEEP_ALIVE_DEFAULT_VALUE).booleanValue(),
			"If connecting using TCP/IP, should the driver set SO_KEEPALIVE?",
			"5.0.7", NETWORK_CATEGORY, Integer.MIN_VALUE);
	
	private IntegerConnectionProperty tcpRcvBuf = new IntegerConnectionProperty(
			StandardSocketFactory.TCP_RCV_BUF_PROPERTY_NAME,
			Integer.parseInt(StandardSocketFactory.TCP_RCV_BUF_DEFAULT_VALUE),
			0, Integer.MAX_VALUE,
			"If connecting using TCP/IP, should the driver set SO_RCV_BUF to the given value? "
			+ "The default value of '0', means use the platform default value for this property)",
			"5.0.7", NETWORK_CATEGORY, Integer.MIN_VALUE);
	
	private IntegerConnectionProperty tcpSndBuf = new IntegerConnectionProperty(
			StandardSocketFactory.TCP_SND_BUF_PROPERTY_NAME,
			Integer.parseInt(StandardSocketFactory.TCP_SND_BUF_DEFAULT_VALUE),
			0, Integer.MAX_VALUE,
			"If connecting using TCP/IP, shuold the driver set SO_SND_BUF to the given value? "
			+ "The default value of '0', means use the platform default value for this property)",
			"5.0.7", NETWORK_CATEGORY, Integer.MIN_VALUE);
			
	private IntegerConnectionProperty tcpTrafficClass = new IntegerConnectionProperty(
			StandardSocketFactory.TCP_TRAFFIC_CLASS_PROPERTY_NAME,
			Integer.parseInt(StandardSocketFactory.TCP_TRAFFIC_CLASS_DEFAULT_VALUE),
			0, 255,
			"If connecting using TCP/IP, should the driver set traffic class or type-of-service fields ?" + 
			" See the documentation for java.net.Socket.setTrafficClass() for more information.",
			"5.0.7", NETWORK_CATEGORY, Integer.MIN_VALUE);
	
	private BooleanConnectionProperty tinyInt1isBit = new BooleanConnectionProperty(
			"tinyInt1isBit",
			true,
			"Should the driver treat the datatype TINYINT(1) as the BIT type "
					+ "(because the server silently converts BIT -> TINYINT(1) when creating tables)?",
			"3.0.16", MISC_CATEGORY, Integer.MIN_VALUE);

	private BooleanConnectionProperty traceProtocol = new BooleanConnectionProperty(
			"traceProtocol", false,
			"Should trace-level network protocol be logged?", "3.1.2",
			DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE);

	private BooleanConnectionProperty treatUtilDateAsTimestamp = new BooleanConnectionProperty(
			"treatUtilDateAsTimestamp", true,
			"Should the driver treat java.util.Date as a TIMESTAMP for the purposes of PreparedStatement.setObject()?",
			"5.0.5", MISC_CATEGORY, Integer.MIN_VALUE);
	
	private BooleanConnectionProperty transformedBitIsBoolean = new BooleanConnectionProperty(
			"transformedBitIsBoolean",
			false,
			"If the driver converts TINYINT(1) to a different type, should it use BOOLEAN instead of BIT "
					+ " for future compatibility with MySQL-5.0, as MySQL-5.0 has a BIT type?",
			"3.1.9", MISC_CATEGORY, Integer.MIN_VALUE);

	private BooleanConnectionProperty useCompression = new BooleanConnectionProperty(
			"useCompression",
			false,
			"Use zlib compression when communicating with the server (true/false)? Defaults to 'false'.",
			"3.0.17", CONNECTION_AND_AUTH_CATEGORY, Integer.MIN_VALUE);

	private StringConnectionProperty useConfigs = new StringConnectionProperty(
			"useConfigs",
			null,
			"Load the comma-delimited list of configuration properties before parsing the "
					+ "URL or applying user-specified properties. These configurations are explained in the 'Configurations' of the documentation.",
			"3.1.5", CONNECTION_AND_AUTH_CATEGORY, Integer.MAX_VALUE);

	private BooleanConnectionProperty useCursorFetch = new BooleanConnectionProperty(
			"useCursorFetch",
			false,
			"If connected to MySQL > 5.0.2, and setFetchSize() > 0 on a statement, should "
			+ " that statement use cursor-based fetching to retrieve rows?",
			"5.0.0", PERFORMANCE_CATEGORY, Integer.MAX_VALUE);
	
	private BooleanConnectionProperty useDynamicCharsetInfo = new BooleanConnectionProperty(
			"useDynamicCharsetInfo",
			true,
			"Should the driver use a per-connection cache of character set information queried from the "
			+ " server when necessary, or use a built-in static mapping that is more efficient, but isn't "
			+ " aware of custom character sets or character sets implemented after the release of the JDBC driver?"
			, "5.0.6", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);
	
	private BooleanConnectionProperty useFastIntParsing = new BooleanConnectionProperty(
			"useFastIntParsing",
			true,
			"Use internal String->Integer conversion routines to avoid excessive object creation?",
			"3.1.4", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);

	private BooleanConnectionProperty useFastDateParsing = new BooleanConnectionProperty(
			"useFastDateParsing",
			true,
			"Use internal String->Date/Time/Teimstamp conversion routines to avoid excessive object creation?",
			"5.0.5", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);
	
	private BooleanConnectionProperty useHostsInPrivileges = new BooleanConnectionProperty(
			"useHostsInPrivileges",
			true,
			"Add '@hostname' to users in DatabaseMetaData.getColumn/TablePrivileges() (true/false), defaults to 'true'.",
			"3.0.2", MISC_CATEGORY, Integer.MIN_VALUE);
	private BooleanConnectionProperty useInformationSchema = new BooleanConnectionProperty(
			"useInformationSchema",
			false,
			"When connected to MySQL-5.0.7 or newer, should the driver use the INFORMATION_SCHEMA to " 
			+ " derive information used by DatabaseMetaData?",
			"5.0.0", MISC_CATEGORY, Integer.MIN_VALUE);
	private BooleanConnectionProperty useJDBCCompliantTimezoneShift = new BooleanConnectionProperty(
			"useJDBCCompliantTimezoneShift",
			false,
			"Should the driver use JDBC-compliant rules when converting TIME/TIMESTAMP/DATETIME values' timezone information " +
			"for those JDBC arguments which take a java.util.Calendar argument? (Notice that this " +
			"option is exclusive of the \"useTimezone=true\" configuration option.)",
			"5.0.0",
			MISC_CATEGORY, Integer.MIN_VALUE);
	
	private BooleanConnectionProperty useLocalSessionState = new BooleanConnectionProperty(
			"useLocalSessionState",
			false,
			"Should the driver refer to the internal values of autocommit and transaction isolation that are set "
					+ "by Connection.setAutoCommit() and Connection.setTransactionIsolation() and transaction state "
					+ "as maintained by the protocol, rather than querying the database or blindly "
					+ "sending commands to the database for commit() or rollback() method calls?",
			"3.1.7", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);
	
	private BooleanConnectionProperty useNanosForElapsedTime = new BooleanConnectionProperty(
			"useNanosForElapsedTime",
			false,
			"For profiling/debugging functionality that measures elapsed time, should the driver "
			+ "try to use nanoseconds resolution if available (JDK >= 1.5)?",
			"5.0.7",
			DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE);
	
	private BooleanConnectionProperty useOldAliasMetadataBehavior = new BooleanConnectionProperty(
			"useOldAliasMetadataBehavior",
			false,
			"Should the driver use the legacy behavior for \"AS\" clauses on columns and tables, and only "
		    + "return aliases (if any) for ResultSetMetaData.getColumnName() or ResultSetMetaData.getTableName() "
		    + "rather than the original column/table name?",
		    "5.0.4",
		    MISC_CATEGORY,
		    Integer.MIN_VALUE);
	
	private BooleanConnectionProperty useOldUTF8Behavior = new BooleanConnectionProperty(
			"useOldUTF8Behavior",
			false,
			"Use the UTF-8 behavior the driver did when communicating with 4.0 and older servers",
			"3.1.6", MISC_CATEGORY, Integer.MIN_VALUE);

	private boolean useOldUTF8BehaviorAsBoolean = false;

	private BooleanConnectionProperty useOnlyServerErrorMessages = new BooleanConnectionProperty(
			"useOnlyServerErrorMessages",
			true,
			"Don't prepend 'standard' SQLState error messages to error messages returned by the server.",
			"3.0.15", MISC_CATEGORY, Integer.MIN_VALUE);

	private BooleanConnectionProperty useReadAheadInput = new BooleanConnectionProperty(
			"useReadAheadInput",
			true,
			"Use newer, optimized non-blocking, buffered input stream when reading from the server?",
			"3.1.5", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);

	private BooleanConnectionProperty useSqlStateCodes = new BooleanConnectionProperty(
			"useSqlStateCodes",
			true,
			"Use SQL Standard state codes instead of 'legacy' X/Open/SQL state codes (true/false), default is 'true'",
			"3.1.3", MISC_CATEGORY, Integer.MIN_VALUE);

	private BooleanConnectionProperty useSSL = new BooleanConnectionProperty(
			"useSSL",
			false,
			"Use SSL when communicating with the server (true/false), defaults to 'false'",
			"3.0.2", SECURITY_CATEGORY, 2);

	private BooleanConnectionProperty useSSPSCompatibleTimezoneShift = new BooleanConnectionProperty(
			"useSSPSCompatibleTimezoneShift",
			false,
			"If migrating from an environment that was using server-side prepared statements, and the"
			+ " configuration property \"useJDBCCompliantTimeZoneShift\" set to \"true\", use compatible behavior"
			+ " when not using server-side prepared statements when sending TIMESTAMP values to the MySQL server.",
			"5.0.5", MISC_CATEGORY, Integer.MIN_VALUE);
	
	private BooleanConnectionProperty useStreamLengthsInPrepStmts = new BooleanConnectionProperty(
			"useStreamLengthsInPrepStmts",
			true,
			"Honor stream length parameter in "
					+ "PreparedStatement/ResultSet.setXXXStream() method calls (true/false, defaults to 'true')?",
			"3.0.2", MISC_CATEGORY, Integer.MIN_VALUE);

	private BooleanConnectionProperty useTimezone = new BooleanConnectionProperty(
			"useTimezone",
			false,
			"Convert time/date types between client and server timezones (true/false, defaults to 'false')?",
			"3.0.2", MISC_CATEGORY, Integer.MIN_VALUE);

	private BooleanConnectionProperty useUltraDevWorkAround = new BooleanConnectionProperty(
			"ultraDevHack",
			false,
			"Create PreparedStatements for prepareCall() when required, because UltraDev "
					+ " is broken and issues a prepareCall() for _all_ statements? (true/false, defaults to 'false')",
			"2.0.3", MISC_CATEGORY, Integer.MIN_VALUE);

	private BooleanConnectionProperty useUnbufferedInput = new BooleanConnectionProperty(
			"useUnbufferedInput", true,
			"Don't use BufferedInputStream for reading data from the server",
			"3.0.11", MISC_CATEGORY, Integer.MIN_VALUE);

	private BooleanConnectionProperty useUnicode = new BooleanConnectionProperty(
			"useUnicode",
			true,
			"Should the driver use Unicode character encodings when handling strings? Should only be used when the driver can't determine the character set mapping, or you are trying to 'force' the driver to use a character set that MySQL either doesn't natively support (such as UTF-8), true/false, defaults to 'true'",
			"1.1g", MISC_CATEGORY, 0);

	// Cache these values, they are 'hot'
	private boolean useUnicodeAsBoolean = true;

	private BooleanConnectionProperty useUsageAdvisor = new BooleanConnectionProperty(
			"useUsageAdvisor",
			false,
			"Should the driver issue 'usage' warnings advising proper and efficient usage of JDBC and MySQL Connector/J to the log (true/false, defaults to 'false')?",
			"3.1.1", DEBUGING_PROFILING_CATEGORY, 10);

	private boolean useUsageAdvisorAsBoolean = false;

	private BooleanConnectionProperty yearIsDateType = new BooleanConnectionProperty(
			"yearIsDateType",
			true,
			"Should the JDBC driver treat the MySQL type \"YEAR\" as a java.sql.Date, or as a SHORT?",
			"3.1.9", MISC_CATEGORY, Integer.MIN_VALUE);

	private StringConnectionProperty zeroDateTimeBehavior = new StringConnectionProperty(
			"zeroDateTimeBehavior",
			ZERO_DATETIME_BEHAVIOR_EXCEPTION,
			new String[] { ZERO_DATETIME_BEHAVIOR_EXCEPTION,
					ZERO_DATETIME_BEHAVIOR_ROUND,
					ZERO_DATETIME_BEHAVIOR_CONVERT_TO_NULL },
			"What should happen when the driver encounters DATETIME values that are composed "
					+ "entirely of zeroes (used by MySQL to represent invalid dates)? "
					+ "Valid values are '"
					+ ZERO_DATETIME_BEHAVIOR_EXCEPTION
					+ "', '"
					+ ZERO_DATETIME_BEHAVIOR_ROUND
					+ "' and '"
					+ ZERO_DATETIME_BEHAVIOR_CONVERT_TO_NULL + "'.", "3.1.4",
			MISC_CATEGORY, Integer.MIN_VALUE);

	private BooleanConnectionProperty useJvmCharsetConverters = new BooleanConnectionProperty("useJvmCharsetConverters",
			false, "Always use the character encoding routines built into the JVM, rather than using "
			+ "lookup tables for single-byte character sets?", "5.0.1", PERFORMANCE_CATEGORY, Integer.MIN_VALUE);
	
	private BooleanConnectionProperty useGmtMillisForDatetimes = new BooleanConnectionProperty("useGmtMillisForDatetimes", false, "Convert between session timezone and GMT before creating Date and Timestamp instances (value of \"false\" is legacy behavior, \"true\" leads to more JDBC-compliant behavior.", "3.1.12", MISC_CATEGORY, Integer.MIN_VALUE);

	private BooleanConnectionProperty dumpMetadataOnColumnNotFound = new BooleanConnectionProperty("dumpMetadataOnColumnNotFound", false, "Should the driver dump the field-level metadata of a result set into " + "the exception message when ResultSet.findColumn() fails?", "3.1.13", DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE);

	// SSL Options
	
	private StringConnectionProperty clientCertificateKeyStoreUrl = new StringConnectionProperty(
			"clientCertificateKeyStoreUrl", null,
			"URL to the client certificate KeyStore (if not specified, use defaults)", "5.1.0",
			SECURITY_CATEGORY, Integer.MAX_VALUE);
	
	private StringConnectionProperty trustCertificateKeyStoreUrl = new StringConnectionProperty(
			"trustCertificateKeyStoreUrl", null,
			"URL to the trusted root certificate KeyStore (if not specified, use defaults)", "5.1.0",
			SECURITY_CATEGORY, Integer.MAX_VALUE);
	
	private StringConnectionProperty clientCertificateKeyStoreType = new StringConnectionProperty(
			"clientCertificateKeyStoreType", null,
			"KeyStore type for client certificates (NULL or empty means use default, "
			+ " standard keystore types supported by the JVM are \"JKS\" and \"PKCS12\", your "
			+ "environment may have more available depending on what security products are "
			+ "installed and available to the JVM.", "5.1.0",
			SECURITY_CATEGORY, Integer.MAX_VALUE);
	
	private StringConnectionProperty clientCertificateKeyStorePassword = new StringConnectionProperty(
			"clientCertificateKeyStorePassword", null,
			"Password for the client certificates KeyStore", "5.1.0",
			SECURITY_CATEGORY, Integer.MAX_VALUE);
	
	private StringConnectionProperty trustCertificateKeyStoreType = new StringConnectionProperty(
			"trustCertificateKeyStoreType", null,
			"KeyStore type for trusted root certificates (NULL or empty means use default, "
			+ " standard keystore types supported by the JVM are \"JKS\" and \"PKCS12\", your "
			+ "environment may have more available depending on what security products are "
			+ "installed and available to the JVM.", "5.1.0",
			SECURITY_CATEGORY, Integer.MAX_VALUE);
	
	private StringConnectionProperty trustCertificateKeyStorePassword = new StringConnectionProperty(
			"trustCertificateKeyStorePassword", null,
			"Password for the trusted root certificates KeyStore", "5.1.0",
			SECURITY_CATEGORY, Integer.MAX_VALUE);
	
	protected DriverPropertyInfo[] exposeAsDriverPropertyInfoInternal(
			Properties info, int slotsToReserve) throws SQLException {
		initializeProperties(info);

		int numProperties = PROPERTY_LIST.size();

		int listSize = numProperties + slotsToReserve;

		DriverPropertyInfo[] driverProperties = new DriverPropertyInfo[listSize];

		for (int i = slotsToReserve; i < listSize; i++) {
			java.lang.reflect.Field propertyField = (java.lang.reflect.Field) PROPERTY_LIST
					.get(i - slotsToReserve);

			try {
				ConnectionProperty propToExpose = (ConnectionProperty) propertyField
						.get(this);

				if (info != null) {
					propToExpose.initializeFrom(info);
				}

				
				driverProperties[i] = propToExpose.getAsDriverPropertyInfo();
			} catch (IllegalAccessException iae) {
				throw SQLError.createSQLException("Internal properties failure",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		}

		return driverProperties;
	}

	protected Properties exposeAsProperties(Properties info)
			throws SQLException {
		if (info == null) {
			info = new Properties();
		}

		int numPropertiesToSet = PROPERTY_LIST.size();

		for (int i = 0; i < numPropertiesToSet; i++) {
			java.lang.reflect.Field propertyField = (java.lang.reflect.Field) PROPERTY_LIST
					.get(i);

			try {
				ConnectionProperty propToGet = (ConnectionProperty) propertyField
						.get(this);

				Object propValue = propToGet.getValueAsObject();

				if (propValue != null) {
					info.setProperty(propToGet.getPropertyName(), propValue
							.toString());
				}
			} catch (IllegalAccessException iae) {
				throw SQLError.createSQLException("Internal properties failure",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		}

		return info;
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#exposeAsXml()
	 */
	public String exposeAsXml() throws SQLException {
		StringBuffer xmlBuf = new StringBuffer();
		xmlBuf.append("<ConnectionProperties>");

		int numPropertiesToSet = PROPERTY_LIST.size();

		int numCategories = PROPERTY_CATEGORIES.length;

		Map propertyListByCategory = new HashMap();

		for (int i = 0; i < numCategories; i++) {
			propertyListByCategory.put(PROPERTY_CATEGORIES[i], new Map[] {
					new TreeMap(), new TreeMap() });
		}

		//
		// The following properties are not exposed as 'normal' properties, but
		// they are
		// settable nonetheless, so we need to have them documented, make sure
		// that they sort 'first' as #1 and #2 in the category
		//
		StringConnectionProperty userProp = new StringConnectionProperty(
				NonRegisteringDriver.USER_PROPERTY_KEY, null,
				"The user to connect as", "all", CONNECTION_AND_AUTH_CATEGORY,
				Integer.MIN_VALUE + 1);
		StringConnectionProperty passwordProp = new StringConnectionProperty(
				NonRegisteringDriver.PASSWORD_PROPERTY_KEY, null,
				"The password to use when connecting", "all",
				CONNECTION_AND_AUTH_CATEGORY, Integer.MIN_VALUE + 2);

		Map[] connectionSortMaps = (Map[]) propertyListByCategory
				.get(CONNECTION_AND_AUTH_CATEGORY);
		TreeMap userMap = new TreeMap();
		userMap.put(userProp.getPropertyName(), userProp);
		
		connectionSortMaps[0].put(new Integer(userProp.getOrder()), userMap);
		
		TreeMap passwordMap = new TreeMap();
		passwordMap.put(passwordProp.getPropertyName(), passwordProp);
		
		connectionSortMaps[0]
				.put(new Integer(passwordProp.getOrder()), passwordMap);

		try {
			for (int i = 0; i < numPropertiesToSet; i++) {
				java.lang.reflect.Field propertyField = (java.lang.reflect.Field) PROPERTY_LIST
						.get(i);
				ConnectionProperty propToGet = (ConnectionProperty) propertyField
						.get(this);
				Map[] sortMaps = (Map[]) propertyListByCategory.get(propToGet
						.getCategoryName());
				int orderInCategory = propToGet.getOrder();

				if (orderInCategory == Integer.MIN_VALUE) {
					sortMaps[1].put(propToGet.getPropertyName(), propToGet);
				} else {
					Integer order = new Integer(orderInCategory);
					
					Map orderMap = (Map)sortMaps[0].get(order);
					
					if (orderMap == null) {
						orderMap = new TreeMap();
						sortMaps[0].put(order, orderMap);
					}
					
					orderMap.put(propToGet.getPropertyName(), propToGet);
				}
			}

			for (int j = 0; j < numCategories; j++) {
				Map[] sortMaps = (Map[]) propertyListByCategory
						.get(PROPERTY_CATEGORIES[j]);
				Iterator orderedIter = sortMaps[0].values().iterator();
				Iterator alphaIter = sortMaps[1].values().iterator();

				xmlBuf.append("\n <PropertyCategory name=\"");
				xmlBuf.append(PROPERTY_CATEGORIES[j]);
				xmlBuf.append("\">");

				while (orderedIter.hasNext()) {
					Iterator orderedAlphaIter = ((Map)orderedIter.next()).values().iterator();
					
					while (orderedAlphaIter.hasNext()) {
						ConnectionProperty propToGet = (ConnectionProperty) orderedAlphaIter
								.next();
						
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
						xmlBuf.append(propToGet.description);
						xmlBuf.append("\n  </Property>");
					}
				}

				while (alphaIter.hasNext()) {
					ConnectionProperty propToGet = (ConnectionProperty) alphaIter
							.next();
					
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
			throw SQLError.createSQLException("Internal properties failure",
					SQLError.SQL_STATE_GENERAL_ERROR);
		}

		xmlBuf.append("\n</ConnectionProperties>");

		return xmlBuf.toString();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getAllowLoadLocalInfile()
	 */
	public boolean getAllowLoadLocalInfile() {
		return this.allowLoadLocalInfile.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getAllowMultiQueries()
	 */
	public boolean getAllowMultiQueries() {
		return this.allowMultiQueries.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getAllowNanAndInf()
	 */
	public boolean getAllowNanAndInf() {
		return allowNanAndInf.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getAllowUrlInLocalInfile()
	 */
	public boolean getAllowUrlInLocalInfile() {
		return this.allowUrlInLocalInfile.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getAlwaysSendSetIsolation()
	 */
	public boolean getAlwaysSendSetIsolation() {
		return this.alwaysSendSetIsolation.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getAutoDeserialize()
	 */
	public boolean getAutoDeserialize() {
		return autoDeserialize.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getAutoGenerateTestcaseScript()
	 */
	public boolean getAutoGenerateTestcaseScript() {
		return this.autoGenerateTestcaseScriptAsBoolean;
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getAutoReconnectForPools()
	 */
	public boolean getAutoReconnectForPools() {
		return this.autoReconnectForPoolsAsBoolean;
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getBlobSendChunkSize()
	 */
	public int getBlobSendChunkSize() {
		return blobSendChunkSize.getValueAsInt();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getCacheCallableStatements()
	 */
	public boolean getCacheCallableStatements() {
		return this.cacheCallableStatements.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getCachePreparedStatements()
	 */
	public boolean getCachePreparedStatements() {
		return ((Boolean) this.cachePreparedStatements.getValueAsObject())
				.booleanValue();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getCacheResultSetMetadata()
	 */
	public boolean getCacheResultSetMetadata() {
		return this.cacheResultSetMetaDataAsBoolean;
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getCacheServerConfiguration()
	 */
	public boolean getCacheServerConfiguration() {
		return cacheServerConfiguration.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getCallableStatementCacheSize()
	 */
	public int getCallableStatementCacheSize() {
		return this.callableStatementCacheSize.getValueAsInt();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getCapitalizeTypeNames()
	 */
	public boolean getCapitalizeTypeNames() {
		return this.capitalizeTypeNames.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getCharacterSetResults()
	 */
	public String getCharacterSetResults() {
		return this.characterSetResults.getValueAsString();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getClobberStreamingResults()
	 */
	public boolean getClobberStreamingResults() {
		return this.clobberStreamingResults.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getClobCharacterEncoding()
	 */
	public String getClobCharacterEncoding() {
		return this.clobCharacterEncoding.getValueAsString();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getConnectionCollation()
	 */
	public String getConnectionCollation() {
		return this.connectionCollation.getValueAsString();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getConnectTimeout()
	 */
	public int getConnectTimeout() {
		return this.connectTimeout.getValueAsInt();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getContinueBatchOnError()
	 */
	public boolean getContinueBatchOnError() {
		return this.continueBatchOnError.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getCreateDatabaseIfNotExist()
	 */
	public boolean getCreateDatabaseIfNotExist() {
		return this.createDatabaseIfNotExist.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getDefaultFetchSize()
	 */
	public int getDefaultFetchSize() {
		return this.defaultFetchSize.getValueAsInt();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getDontTrackOpenResources()
	 */
	public boolean getDontTrackOpenResources() {
		return this.dontTrackOpenResources.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getDumpQueriesOnException()
	 */
	public boolean getDumpQueriesOnException() {
		return this.dumpQueriesOnException.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getDynamicCalendars()
	 */
	public boolean getDynamicCalendars() {
		return this.dynamicCalendars.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getElideSetAutoCommits()
	 */
	public boolean getElideSetAutoCommits() {
		return this.elideSetAutoCommits.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getEmptyStringsConvertToZero()
	 */
	public boolean getEmptyStringsConvertToZero() {
		return this.emptyStringsConvertToZero.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getEmulateLocators()
	 */
	public boolean getEmulateLocators() {
		return this.emulateLocators.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getEmulateUnsupportedPstmts()
	 */
	public boolean getEmulateUnsupportedPstmts() {
		return this.emulateUnsupportedPstmts.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getEnablePacketDebug()
	 */
	public boolean getEnablePacketDebug() {
		return this.enablePacketDebug.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getEncoding()
	 */
	public String getEncoding() {
		return this.characterEncodingAsString;
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getExplainSlowQueries()
	 */
	public boolean getExplainSlowQueries() {
		return this.explainSlowQueries.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getFailOverReadOnly()
	 */
	public boolean getFailOverReadOnly() {
		return this.failOverReadOnly.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getGatherPerformanceMetrics()
	 */
	public boolean getGatherPerformanceMetrics() {
		return this.gatherPerformanceMetrics.getValueAsBoolean();
	}

	/**
	 * DOCUMENT ME!
	 * 
	 * @return
	 */
	protected boolean getHighAvailability() {
		return this.highAvailabilityAsBoolean;
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getHoldResultsOpenOverStatementClose()
	 */
	public boolean getHoldResultsOpenOverStatementClose() {
		return holdResultsOpenOverStatementClose.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getIgnoreNonTxTables()
	 */
	public boolean getIgnoreNonTxTables() {
		return this.ignoreNonTxTables.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getInitialTimeout()
	 */
	public int getInitialTimeout() {
		return this.initialTimeout.getValueAsInt();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getInteractiveClient()
	 */
	public boolean getInteractiveClient() {
		return this.isInteractiveClient.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getIsInteractiveClient()
	 */
	public boolean getIsInteractiveClient() {
		return this.isInteractiveClient.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getJdbcCompliantTruncation()
	 */
	public boolean getJdbcCompliantTruncation() {
		return this.jdbcCompliantTruncation.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getLocatorFetchBufferSize()
	 */
	public int getLocatorFetchBufferSize() {
		return this.locatorFetchBufferSize.getValueAsInt();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getLogger()
	 */
	public String getLogger() {
		return this.loggerClassName.getValueAsString();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getLoggerClassName()
	 */
	public String getLoggerClassName() {
		return this.loggerClassName.getValueAsString();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getLogSlowQueries()
	 */
	public boolean getLogSlowQueries() {
		return this.logSlowQueries.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getMaintainTimeStats()
	 */
	public boolean getMaintainTimeStats() {
		return maintainTimeStatsAsBoolean;
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getMaxQuerySizeToLog()
	 */
	public int getMaxQuerySizeToLog() {
		return this.maxQuerySizeToLog.getValueAsInt();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getMaxReconnects()
	 */
	public int getMaxReconnects() {
		return this.maxReconnects.getValueAsInt();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getMaxRows()
	 */
	public int getMaxRows() {
		return this.maxRowsAsInt;
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getMetadataCacheSize()
	 */
	public int getMetadataCacheSize() {
		return this.metadataCacheSize.getValueAsInt();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getNoDatetimeStringSync()
	 */
	public boolean getNoDatetimeStringSync() {
		return this.noDatetimeStringSync.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getNullCatalogMeansCurrent()
	 */
	public boolean getNullCatalogMeansCurrent() {
		return this.nullCatalogMeansCurrent.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getNullNamePatternMatchesAll()
	 */
	public boolean getNullNamePatternMatchesAll() {
		return this.nullNamePatternMatchesAll.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getPacketDebugBufferSize()
	 */
	public int getPacketDebugBufferSize() {
		return this.packetDebugBufferSize.getValueAsInt();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getParanoid()
	 */
	public boolean getParanoid() {
		return this.paranoid.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getPedantic()
	 */
	public boolean getPedantic() {
		return this.pedantic.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getPreparedStatementCacheSize()
	 */
	public int getPreparedStatementCacheSize() {
		return ((Integer) this.preparedStatementCacheSize.getValueAsObject())
				.intValue();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getPreparedStatementCacheSqlLimit()
	 */
	public int getPreparedStatementCacheSqlLimit() {
		return ((Integer) this.preparedStatementCacheSqlLimit
				.getValueAsObject()).intValue();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getProfileSql()
	 */
	public boolean getProfileSql() {
		return this.profileSQLAsBoolean;
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getProfileSQL()
	 */
	public boolean getProfileSQL() {
		return this.profileSQL.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getPropertiesTransform()
	 */
	public String getPropertiesTransform() {
		return this.propertiesTransform.getValueAsString();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getQueriesBeforeRetryMaster()
	 */
	public int getQueriesBeforeRetryMaster() {
		return this.queriesBeforeRetryMaster.getValueAsInt();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getReconnectAtTxEnd()
	 */
	public boolean getReconnectAtTxEnd() {
		return this.reconnectTxAtEndAsBoolean;
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getRelaxAutoCommit()
	 */
	public boolean getRelaxAutoCommit() {
		return this.relaxAutoCommit.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getReportMetricsIntervalMillis()
	 */
	public int getReportMetricsIntervalMillis() {
		return this.reportMetricsIntervalMillis.getValueAsInt();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getRequireSSL()
	 */
	public boolean getRequireSSL() {
		return this.requireSSL.getValueAsBoolean();
	}

	protected boolean getRetainStatementAfterResultSetClose() {
		return this.retainStatementAfterResultSetClose.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getRollbackOnPooledClose()
	 */
	public boolean getRollbackOnPooledClose() {
		return this.rollbackOnPooledClose.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getRoundRobinLoadBalance()
	 */
	public boolean getRoundRobinLoadBalance() {
		return this.roundRobinLoadBalance.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getRunningCTS13()
	 */
	public boolean getRunningCTS13() {
		return this.runningCTS13.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getSecondsBeforeRetryMaster()
	 */
	public int getSecondsBeforeRetryMaster() {
		return this.secondsBeforeRetryMaster.getValueAsInt();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getServerTimezone()
	 */
	public String getServerTimezone() {
		return this.serverTimezone.getValueAsString();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getSessionVariables()
	 */
	public String getSessionVariables() {
		return sessionVariables.getValueAsString();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getSlowQueryThresholdMillis()
	 */
	public int getSlowQueryThresholdMillis() {
		return this.slowQueryThresholdMillis.getValueAsInt();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getSocketFactoryClassName()
	 */
	public String getSocketFactoryClassName() {
		return this.socketFactoryClassName.getValueAsString();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getSocketTimeout()
	 */
	public int getSocketTimeout() {
		return this.socketTimeout.getValueAsInt();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getStrictFloatingPoint()
	 */
	public boolean getStrictFloatingPoint() {
		return this.strictFloatingPoint.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getStrictUpdates()
	 */
	public boolean getStrictUpdates() {
		return this.strictUpdates.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getTinyInt1isBit()
	 */
	public boolean getTinyInt1isBit() {
		return this.tinyInt1isBit.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getTraceProtocol()
	 */
	public boolean getTraceProtocol() {
		return this.traceProtocol.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getTransformedBitIsBoolean()
	 */
	public boolean getTransformedBitIsBoolean() {
		return this.transformedBitIsBoolean.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getUseCompression()
	 */
	public boolean getUseCompression() {
		return this.useCompression.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getUseFastIntParsing()
	 */
	public boolean getUseFastIntParsing() {
		return this.useFastIntParsing.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getUseHostsInPrivileges()
	 */
	public boolean getUseHostsInPrivileges() {
		return this.useHostsInPrivileges.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getUseInformationSchema()
	 */
	public boolean getUseInformationSchema() {
		return this.useInformationSchema.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getUseLocalSessionState()
	 */
	public boolean getUseLocalSessionState() {
		return this.useLocalSessionState.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getUseOldUTF8Behavior()
	 */
	public boolean getUseOldUTF8Behavior() {
		return this.useOldUTF8BehaviorAsBoolean;
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getUseOnlyServerErrorMessages()
	 */
	public boolean getUseOnlyServerErrorMessages() {
		return this.useOnlyServerErrorMessages.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getUseReadAheadInput()
	 */
	public boolean getUseReadAheadInput() {
		return this.useReadAheadInput.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getUseServerPreparedStmts()
	 */
	public boolean getUseServerPreparedStmts() {
		return this.detectServerPreparedStmts.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getUseSqlStateCodes()
	 */
	public boolean getUseSqlStateCodes() {
		return this.useSqlStateCodes.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getUseSSL()
	 */
	public boolean getUseSSL() {
		return this.useSSL.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getUseStreamLengthsInPrepStmts()
	 */
	public boolean getUseStreamLengthsInPrepStmts() {
		return this.useStreamLengthsInPrepStmts.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getUseTimezone()
	 */
	public boolean getUseTimezone() {
		return this.useTimezone.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getUseUltraDevWorkAround()
	 */
	public boolean getUseUltraDevWorkAround() {
		return this.useUltraDevWorkAround.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getUseUnbufferedInput()
	 */
	public boolean getUseUnbufferedInput() {
		return this.useUnbufferedInput.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getUseUnicode()
	 */
	public boolean getUseUnicode() {
		return this.useUnicodeAsBoolean;
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getUseUsageAdvisor()
	 */
	public boolean getUseUsageAdvisor() {
		return this.useUsageAdvisorAsBoolean;
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getYearIsDateType()
	 */
	public boolean getYearIsDateType() {
		return this.yearIsDateType.getValueAsBoolean();
	}

	/* (non-Javadoc)
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
	 *             DOCUMENT ME!
	 */
	protected void initializeFromRef(Reference ref) throws SQLException {
		int numPropertiesToSet = PROPERTY_LIST.size();

		for (int i = 0; i < numPropertiesToSet; i++) {
			java.lang.reflect.Field propertyField = (java.lang.reflect.Field) PROPERTY_LIST
					.get(i);

			try {
				ConnectionProperty propToSet = (ConnectionProperty) propertyField
						.get(this);

				if (ref != null) {
					propToSet.initializeFrom(ref);
				}
			} catch (IllegalAccessException iae) {
				throw SQLError.createSQLException("Internal properties failure",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		}

		postInitialization();
	}

	/**
	 * Initializes driver properties that come from URL or properties passed to
	 * the driver manager.
	 * 
	 * @param info
	 *            DOCUMENT ME!
	 * @throws SQLException
	 *             DOCUMENT ME!
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
				java.lang.reflect.Field propertyField = (java.lang.reflect.Field) PROPERTY_LIST
						.get(i);

				try {
					ConnectionProperty propToSet = (ConnectionProperty) propertyField
							.get(this);

					propToSet.initializeFrom(infoCopy);
				} catch (IllegalAccessException iae) {
					throw SQLError.createSQLException(
							"Unable to initialize driver properties due to "
									+ iae.toString(),
							SQLError.SQL_STATE_GENERAL_ERROR);
				}
			}

			// TODO -- Not yet
			/*
			 * int numUnknownProperties = infoCopy.size(); if
			 * (numUnknownProperties > 0) { StringBuffer errorMessageBuf = new
			 * StringBuffer( "Unknown connection ");
			 * errorMessageBuf.append((numUnknownProperties == 1) ? "property " :
			 * "properties "); Iterator propNamesItor =
			 * infoCopy.keySet().iterator(); errorMessageBuf.append("'");
			 * errorMessageBuf.append(propNamesItor.next().toString());
			 * errorMessageBuf.append("'"); while (propNamesItor.hasNext()) {
			 * errorMessageBuf.append(", '");
			 * errorMessageBuf.append(propNamesItor.next().toString());
			 * errorMessageBuf.append("'"); } throw new
			 * SQLException(errorMessageBuf.toString(),
			 * SQLError.SQL_STATE_INVALID_CONNECTION_ATTRIBUTE); }
			 */
			postInitialization();
		}
	}

	protected void postInitialization() throws SQLException {
	
		// Support 'old' profileSql capitalization
		if (this.profileSql.getValueAsObject() != null) {
			this.profileSQL.initializeFrom(this.profileSql.getValueAsObject()
					.toString());
		}

		this.reconnectTxAtEndAsBoolean = ((Boolean) this.reconnectAtTxEnd
				.getValueAsObject()).booleanValue();

		// Adjust max rows
		if (this.getMaxRows() == 0) {
			// adjust so that it will become MysqlDefs.MAX_ROWS
			// in execSQL()
			this.maxRows.setValueAsObject(Constants.integerValueOf(-1));
		}

		//
		// Check character encoding
		//
		String testEncoding = this.getEncoding();

		if (testEncoding != null) {
			// Attempt to use the encoding, and bail out if it
			// can't be used
			try {
				String testString = "abc";
				testString.getBytes(testEncoding);
			} catch (UnsupportedEncodingException UE) {
				throw SQLError.createSQLException("Unsupported character " + "encoding '"
						+ testEncoding + "'.", "0S100");
			}
		}

		// Metadata caching is only supported on JDK-1.4 and newer
		// because it relies on LinkedHashMap being present.
		// Check (and disable) if not supported
		if (((Boolean) this.cacheResultSetMetadata.getValueAsObject())
				.booleanValue()) {
			try {
				Class.forName("java.util.LinkedHashMap");
			} catch (ClassNotFoundException cnfe) {
				this.cacheResultSetMetadata.setValue(false);
			}
		}

		this.cacheResultSetMetaDataAsBoolean = this.cacheResultSetMetadata
				.getValueAsBoolean();
		this.useUnicodeAsBoolean = this.useUnicode.getValueAsBoolean();
		this.characterEncodingAsString = ((String) this.characterEncoding
				.getValueAsObject());
		this.highAvailabilityAsBoolean = this.autoReconnect.getValueAsBoolean();
		this.autoReconnectForPoolsAsBoolean = this.autoReconnectForPools
				.getValueAsBoolean();
		this.maxRowsAsInt = ((Integer) this.maxRows.getValueAsObject())
				.intValue();
		this.profileSQLAsBoolean = this.profileSQL.getValueAsBoolean();
		this.useUsageAdvisorAsBoolean = this.useUsageAdvisor
				.getValueAsBoolean();
		this.useOldUTF8BehaviorAsBoolean = this.useOldUTF8Behavior
				.getValueAsBoolean();
		this.autoGenerateTestcaseScriptAsBoolean = this.autoGenerateTestcaseScript
				.getValueAsBoolean();
		this.maintainTimeStatsAsBoolean = this.maintainTimeStats
				.getValueAsBoolean();
		this.jdbcCompliantTruncationForReads = getJdbcCompliantTruncation();
		
		if (getUseCursorFetch()) {
			// assume they want to use server-side prepared statements
			// because they're required for this functionality
			setDetectServerPreparedStmts(true);
		}
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setAllowLoadLocalInfile(boolean)
	 */
	public void setAllowLoadLocalInfile(boolean property) {
		this.allowLoadLocalInfile.setValue(property);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setAllowMultiQueries(boolean)
	 */
	public void setAllowMultiQueries(boolean property) {
		this.allowMultiQueries.setValue(property);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setAllowNanAndInf(boolean)
	 */
	public void setAllowNanAndInf(boolean flag) {
		this.allowNanAndInf.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setAllowUrlInLocalInfile(boolean)
	 */
	public void setAllowUrlInLocalInfile(boolean flag) {
		this.allowUrlInLocalInfile.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setAlwaysSendSetIsolation(boolean)
	 */
	public void setAlwaysSendSetIsolation(boolean flag) {
		this.alwaysSendSetIsolation.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setAutoDeserialize(boolean)
	 */
	public void setAutoDeserialize(boolean flag) {
		this.autoDeserialize.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setAutoGenerateTestcaseScript(boolean)
	 */
	public void setAutoGenerateTestcaseScript(boolean flag) {
		this.autoGenerateTestcaseScript.setValue(flag);
		this.autoGenerateTestcaseScriptAsBoolean = this.autoGenerateTestcaseScript
				.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setAutoReconnect(boolean)
	 */
	public void setAutoReconnect(boolean flag) {
		this.autoReconnect.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setAutoReconnectForConnectionPools(boolean)
	 */
	public void setAutoReconnectForConnectionPools(boolean property) {
		this.autoReconnectForPools.setValue(property);
		this.autoReconnectForPoolsAsBoolean = this.autoReconnectForPools
				.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setAutoReconnectForPools(boolean)
	 */
	public void setAutoReconnectForPools(boolean flag) {
		this.autoReconnectForPools.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setBlobSendChunkSize(java.lang.String)
	 */
	public void setBlobSendChunkSize(String value) throws SQLException {
		this.blobSendChunkSize.setValue(value);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setCacheCallableStatements(boolean)
	 */
	public void setCacheCallableStatements(boolean flag) {
		this.cacheCallableStatements.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setCachePreparedStatements(boolean)
	 */
	public void setCachePreparedStatements(boolean flag) {
		this.cachePreparedStatements.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setCacheResultSetMetadata(boolean)
	 */
	public void setCacheResultSetMetadata(boolean property) {
		this.cacheResultSetMetadata.setValue(property);
		this.cacheResultSetMetaDataAsBoolean = this.cacheResultSetMetadata
				.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setCacheServerConfiguration(boolean)
	 */
	public void setCacheServerConfiguration(boolean flag) {
		this.cacheServerConfiguration.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setCallableStatementCacheSize(int)
	 */
	public void setCallableStatementCacheSize(int size) {
		this.callableStatementCacheSize.setValue(size);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setCapitalizeDBMDTypes(boolean)
	 */
	public void setCapitalizeDBMDTypes(boolean property) {
		this.capitalizeTypeNames.setValue(property);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setCapitalizeTypeNames(boolean)
	 */
	public void setCapitalizeTypeNames(boolean flag) {
		this.capitalizeTypeNames.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setCharacterEncoding(java.lang.String)
	 */
	public void setCharacterEncoding(String encoding) {
		this.characterEncoding.setValue(encoding);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setCharacterSetResults(java.lang.String)
	 */
	public void setCharacterSetResults(String characterSet) {
		this.characterSetResults.setValue(characterSet);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setClobberStreamingResults(boolean)
	 */
	public void setClobberStreamingResults(boolean flag) {
		this.clobberStreamingResults.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setClobCharacterEncoding(java.lang.String)
	 */
	public void setClobCharacterEncoding(String encoding) {
		this.clobCharacterEncoding.setValue(encoding);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setConnectionCollation(java.lang.String)
	 */
	public void setConnectionCollation(String collation) {
		this.connectionCollation.setValue(collation);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setConnectTimeout(int)
	 */
	public void setConnectTimeout(int timeoutMs) {
		this.connectTimeout.setValue(timeoutMs);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setContinueBatchOnError(boolean)
	 */
	public void setContinueBatchOnError(boolean property) {
		this.continueBatchOnError.setValue(property);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setCreateDatabaseIfNotExist(boolean)
	 */
	public void setCreateDatabaseIfNotExist(boolean flag) {
		this.createDatabaseIfNotExist.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setDefaultFetchSize(int)
	 */
	public void setDefaultFetchSize(int n) {
		this.defaultFetchSize.setValue(n);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setDetectServerPreparedStmts(boolean)
	 */
	public void setDetectServerPreparedStmts(boolean property) {
		this.detectServerPreparedStmts.setValue(property);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setDontTrackOpenResources(boolean)
	 */
	public void setDontTrackOpenResources(boolean flag) {
		this.dontTrackOpenResources.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setDumpQueriesOnException(boolean)
	 */
	public void setDumpQueriesOnException(boolean flag) {
		this.dumpQueriesOnException.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setDynamicCalendars(boolean)
	 */
	public void setDynamicCalendars(boolean flag) {
		this.dynamicCalendars.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setElideSetAutoCommits(boolean)
	 */
	public void setElideSetAutoCommits(boolean flag) {
		this.elideSetAutoCommits.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setEmptyStringsConvertToZero(boolean)
	 */
	public void setEmptyStringsConvertToZero(boolean flag) {
		this.emptyStringsConvertToZero.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setEmulateLocators(boolean)
	 */
	public void setEmulateLocators(boolean property) {
		this.emulateLocators.setValue(property);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setEmulateUnsupportedPstmts(boolean)
	 */
	public void setEmulateUnsupportedPstmts(boolean flag) {
		this.emulateUnsupportedPstmts.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setEnablePacketDebug(boolean)
	 */
	public void setEnablePacketDebug(boolean flag) {
		this.enablePacketDebug.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setEncoding(java.lang.String)
	 */
	public void setEncoding(String property) {
		this.characterEncoding.setValue(property);
		this.characterEncodingAsString = this.characterEncoding
				.getValueAsString();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setExplainSlowQueries(boolean)
	 */
	public void setExplainSlowQueries(boolean flag) {
		this.explainSlowQueries.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setFailOverReadOnly(boolean)
	 */
	public void setFailOverReadOnly(boolean flag) {
		this.failOverReadOnly.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setGatherPerformanceMetrics(boolean)
	 */
	public void setGatherPerformanceMetrics(boolean flag) {
		this.gatherPerformanceMetrics.setValue(flag);
	}

	/**
	 * DOCUMENT ME!
	 * 
	 * @param property
	 */
	protected void setHighAvailability(boolean property) {
		this.autoReconnect.setValue(property);
		this.highAvailabilityAsBoolean = this.autoReconnect.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setHoldResultsOpenOverStatementClose(boolean)
	 */
	public void setHoldResultsOpenOverStatementClose(boolean flag) {
		this.holdResultsOpenOverStatementClose.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setIgnoreNonTxTables(boolean)
	 */
	public void setIgnoreNonTxTables(boolean property) {
		this.ignoreNonTxTables.setValue(property);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setInitialTimeout(int)
	 */
	public void setInitialTimeout(int property) {
		this.initialTimeout.setValue(property);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setIsInteractiveClient(boolean)
	 */
	public void setIsInteractiveClient(boolean property) {
		this.isInteractiveClient.setValue(property);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setJdbcCompliantTruncation(boolean)
	 */
	public void setJdbcCompliantTruncation(boolean flag) {
		this.jdbcCompliantTruncation.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setLocatorFetchBufferSize(java.lang.String)
	 */
	public void setLocatorFetchBufferSize(String value) throws SQLException {
		this.locatorFetchBufferSize.setValue(value);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setLogger(java.lang.String)
	 */
	public void setLogger(String property) {
		this.loggerClassName.setValueAsObject(property);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setLoggerClassName(java.lang.String)
	 */
	public void setLoggerClassName(String className) {
		this.loggerClassName.setValue(className);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setLogSlowQueries(boolean)
	 */
	public void setLogSlowQueries(boolean flag) {
		this.logSlowQueries.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setMaintainTimeStats(boolean)
	 */
	public void setMaintainTimeStats(boolean flag) {
		this.maintainTimeStats.setValue(flag);
		this.maintainTimeStatsAsBoolean = this.maintainTimeStats
				.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setMaxQuerySizeToLog(int)
	 */
	public void setMaxQuerySizeToLog(int sizeInBytes) {
		this.maxQuerySizeToLog.setValue(sizeInBytes);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setMaxReconnects(int)
	 */
	public void setMaxReconnects(int property) {
		this.maxReconnects.setValue(property);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setMaxRows(int)
	 */
	public void setMaxRows(int property) {
		this.maxRows.setValue(property);
		this.maxRowsAsInt = this.maxRows.getValueAsInt();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setMetadataCacheSize(int)
	 */
	public void setMetadataCacheSize(int value) {
		this.metadataCacheSize.setValue(value);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setNoDatetimeStringSync(boolean)
	 */
	public void setNoDatetimeStringSync(boolean flag) {
		this.noDatetimeStringSync.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setNullCatalogMeansCurrent(boolean)
	 */
	public void setNullCatalogMeansCurrent(boolean value) {
		this.nullCatalogMeansCurrent.setValue(value);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setNullNamePatternMatchesAll(boolean)
	 */
	public void setNullNamePatternMatchesAll(boolean value) {
		this.nullNamePatternMatchesAll.setValue(value);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setPacketDebugBufferSize(int)
	 */
	public void setPacketDebugBufferSize(int size) {
		this.packetDebugBufferSize.setValue(size);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setParanoid(boolean)
	 */
	public void setParanoid(boolean property) {
		this.paranoid.setValue(property);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setPedantic(boolean)
	 */
	public void setPedantic(boolean property) {
		this.pedantic.setValue(property);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setPreparedStatementCacheSize(int)
	 */
	public void setPreparedStatementCacheSize(int cacheSize) {
		this.preparedStatementCacheSize.setValue(cacheSize);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setPreparedStatementCacheSqlLimit(int)
	 */
	public void setPreparedStatementCacheSqlLimit(int cacheSqlLimit) {
		this.preparedStatementCacheSqlLimit.setValue(cacheSqlLimit);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setProfileSql(boolean)
	 */
	public void setProfileSql(boolean property) {
		this.profileSQL.setValue(property);
		this.profileSQLAsBoolean = this.profileSQL.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setProfileSQL(boolean)
	 */
	public void setProfileSQL(boolean flag) {
		this.profileSQL.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setPropertiesTransform(java.lang.String)
	 */
	public void setPropertiesTransform(String value) {
		this.propertiesTransform.setValue(value);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setQueriesBeforeRetryMaster(int)
	 */
	public void setQueriesBeforeRetryMaster(int property) {
		this.queriesBeforeRetryMaster.setValue(property);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setReconnectAtTxEnd(boolean)
	 */
	public void setReconnectAtTxEnd(boolean property) {
		this.reconnectAtTxEnd.setValue(property);
		this.reconnectTxAtEndAsBoolean = this.reconnectAtTxEnd
				.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setRelaxAutoCommit(boolean)
	 */
	public void setRelaxAutoCommit(boolean property) {
		this.relaxAutoCommit.setValue(property);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setReportMetricsIntervalMillis(int)
	 */
	public void setReportMetricsIntervalMillis(int millis) {
		this.reportMetricsIntervalMillis.setValue(millis);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setRequireSSL(boolean)
	 */
	public void setRequireSSL(boolean property) {
		this.requireSSL.setValue(property);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setRetainStatementAfterResultSetClose(boolean)
	 */
	public void setRetainStatementAfterResultSetClose(boolean flag) {
		this.retainStatementAfterResultSetClose.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setRollbackOnPooledClose(boolean)
	 */
	public void setRollbackOnPooledClose(boolean flag) {
		this.rollbackOnPooledClose.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setRoundRobinLoadBalance(boolean)
	 */
	public void setRoundRobinLoadBalance(boolean flag) {
		this.roundRobinLoadBalance.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setRunningCTS13(boolean)
	 */
	public void setRunningCTS13(boolean flag) {
		this.runningCTS13.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setSecondsBeforeRetryMaster(int)
	 */
	public void setSecondsBeforeRetryMaster(int property) {
		this.secondsBeforeRetryMaster.setValue(property);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setServerTimezone(java.lang.String)
	 */
	public void setServerTimezone(String property) {
		this.serverTimezone.setValue(property);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setSessionVariables(java.lang.String)
	 */
	public void setSessionVariables(String variables) {
		this.sessionVariables.setValue(variables);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setSlowQueryThresholdMillis(int)
	 */
	public void setSlowQueryThresholdMillis(int millis) {
		this.slowQueryThresholdMillis.setValue(millis);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setSocketFactoryClassName(java.lang.String)
	 */
	public void setSocketFactoryClassName(String property) {
		this.socketFactoryClassName.setValue(property);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setSocketTimeout(int)
	 */
	public void setSocketTimeout(int property) {
		this.socketTimeout.setValue(property);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setStrictFloatingPoint(boolean)
	 */
	public void setStrictFloatingPoint(boolean property) {
		this.strictFloatingPoint.setValue(property);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setStrictUpdates(boolean)
	 */
	public void setStrictUpdates(boolean property) {
		this.strictUpdates.setValue(property);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setTinyInt1isBit(boolean)
	 */
	public void setTinyInt1isBit(boolean flag) {
		this.tinyInt1isBit.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setTraceProtocol(boolean)
	 */
	public void setTraceProtocol(boolean flag) {
		this.traceProtocol.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setTransformedBitIsBoolean(boolean)
	 */
	public void setTransformedBitIsBoolean(boolean flag) {
		this.transformedBitIsBoolean.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setUseCompression(boolean)
	 */
	public void setUseCompression(boolean property) {
		this.useCompression.setValue(property);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setUseFastIntParsing(boolean)
	 */
	public void setUseFastIntParsing(boolean flag) {
		this.useFastIntParsing.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setUseHostsInPrivileges(boolean)
	 */
	public void setUseHostsInPrivileges(boolean property) {
		this.useHostsInPrivileges.setValue(property);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setUseInformationSchema(boolean)
	 */
	public void setUseInformationSchema(boolean flag) {
		this.useInformationSchema.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setUseLocalSessionState(boolean)
	 */
	public void setUseLocalSessionState(boolean flag) {
		this.useLocalSessionState.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setUseOldUTF8Behavior(boolean)
	 */
	public void setUseOldUTF8Behavior(boolean flag) {
		this.useOldUTF8Behavior.setValue(flag);
		this.useOldUTF8BehaviorAsBoolean = this.useOldUTF8Behavior
				.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setUseOnlyServerErrorMessages(boolean)
	 */
	public void setUseOnlyServerErrorMessages(boolean flag) {
		this.useOnlyServerErrorMessages.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setUseReadAheadInput(boolean)
	 */
	public void setUseReadAheadInput(boolean flag) {
		this.useReadAheadInput.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setUseServerPreparedStmts(boolean)
	 */
	public void setUseServerPreparedStmts(boolean flag) {
		this.detectServerPreparedStmts.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setUseSqlStateCodes(boolean)
	 */
	public void setUseSqlStateCodes(boolean flag) {
		this.useSqlStateCodes.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setUseSSL(boolean)
	 */
	public void setUseSSL(boolean property) {
		this.useSSL.setValue(property);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setUseStreamLengthsInPrepStmts(boolean)
	 */
	public void setUseStreamLengthsInPrepStmts(boolean property) {
		this.useStreamLengthsInPrepStmts.setValue(property);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setUseTimezone(boolean)
	 */
	public void setUseTimezone(boolean property) {
		this.useTimezone.setValue(property);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setUseUltraDevWorkAround(boolean)
	 */
	public void setUseUltraDevWorkAround(boolean property) {
		this.useUltraDevWorkAround.setValue(property);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setUseUnbufferedInput(boolean)
	 */
	public void setUseUnbufferedInput(boolean flag) {
		this.useUnbufferedInput.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setUseUnicode(boolean)
	 */
	public void setUseUnicode(boolean flag) {
		this.useUnicode.setValue(flag);
		this.useUnicodeAsBoolean = this.useUnicode.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setUseUsageAdvisor(boolean)
	 */
	public void setUseUsageAdvisor(boolean useUsageAdvisorFlag) {
		this.useUsageAdvisor.setValue(useUsageAdvisorFlag);
		this.useUsageAdvisorAsBoolean = this.useUsageAdvisor
				.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setYearIsDateType(boolean)
	 */
	public void setYearIsDateType(boolean flag) {
		this.yearIsDateType.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setZeroDateTimeBehavior(java.lang.String)
	 */
	public void setZeroDateTimeBehavior(String behavior) {
		this.zeroDateTimeBehavior.setValue(behavior);
	}

	protected void storeToRef(Reference ref) throws SQLException {
		int numPropertiesToSet = PROPERTY_LIST.size();

		for (int i = 0; i < numPropertiesToSet; i++) {
			java.lang.reflect.Field propertyField = (java.lang.reflect.Field) PROPERTY_LIST
					.get(i);

			try {
				ConnectionProperty propToStore = (ConnectionProperty) propertyField
						.get(this);

				if (ref != null) {
					propToStore.storeTo(ref);
				}
			} catch (IllegalAccessException iae) {
				throw SQLError.createSQLException("Huh?");
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#useUnbufferedInput()
	 */
	public boolean useUnbufferedInput() {
		return this.useUnbufferedInput.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getUseCursorFetch()
	 */
	public boolean getUseCursorFetch() {
		return this.useCursorFetch.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setUseCursorFetch(boolean)
	 */
	public void setUseCursorFetch(boolean flag) {
		this.useCursorFetch.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getOverrideSupportsIntegrityEnhancementFacility()
	 */
	public boolean getOverrideSupportsIntegrityEnhancementFacility() {
		return this.overrideSupportsIntegrityEnhancementFacility.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setOverrideSupportsIntegrityEnhancementFacility(boolean)
	 */
	public void setOverrideSupportsIntegrityEnhancementFacility(boolean flag) {
		this.overrideSupportsIntegrityEnhancementFacility.setValue(flag);	
	}
	
	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getNoTimezoneConversionForTimeType()
	 */
	public boolean getNoTimezoneConversionForTimeType() {
		return this.noTimezoneConversionForTimeType.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setNoTimezoneConversionForTimeType(boolean)
	 */
	public void setNoTimezoneConversionForTimeType(boolean flag) {
		this.noTimezoneConversionForTimeType.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getUseJDBCCompliantTimezoneShift()
	 */
	public boolean getUseJDBCCompliantTimezoneShift() {
		return this.useJDBCCompliantTimezoneShift.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setUseJDBCCompliantTimezoneShift(boolean)
	 */
	public void setUseJDBCCompliantTimezoneShift(boolean flag) {
		this.useJDBCCompliantTimezoneShift.setValue(flag);
	}
	
	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getAutoClosePStmtStreams()
	 */
	public boolean getAutoClosePStmtStreams() {
		return this.autoClosePStmtStreams.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setAutoClosePStmtStreams(boolean)
	 */
	public void setAutoClosePStmtStreams(boolean flag) {
		this.autoClosePStmtStreams.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getProcessEscapeCodesForPrepStmts()
	 */
	public boolean getProcessEscapeCodesForPrepStmts() {
		return this.processEscapeCodesForPrepStmts.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setProcessEscapeCodesForPrepStmts(boolean)
	 */
	public void setProcessEscapeCodesForPrepStmts(boolean flag) {
		this.processEscapeCodesForPrepStmts.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getUseGmtMillisForDatetimes()
	 */
	public boolean getUseGmtMillisForDatetimes() {
		return this.useGmtMillisForDatetimes.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setUseGmtMillisForDatetimes(boolean)
	 */
	public void setUseGmtMillisForDatetimes(boolean flag) {
		this.useGmtMillisForDatetimes.setValue(flag);
	}
	
	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getDumpMetadataOnColumnNotFound()
	 */
	public boolean getDumpMetadataOnColumnNotFound() {
		return this.dumpMetadataOnColumnNotFound.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setDumpMetadataOnColumnNotFound(boolean)
	 */
	public void setDumpMetadataOnColumnNotFound(boolean flag) {
		this.dumpMetadataOnColumnNotFound.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getResourceId()
	 */
	public String getResourceId() {
		return this.resourceId.getValueAsString();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setResourceId(java.lang.String)
	 */
	public void setResourceId(String resourceId) {
		this.resourceId.setValue(resourceId);
	}
	
	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getRewriteBatchedStatements()
	 */
	public boolean getRewriteBatchedStatements() {
		return this.rewriteBatchedStatements.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setRewriteBatchedStatements(boolean)
	 */
	public void setRewriteBatchedStatements(boolean flag) {
		this.rewriteBatchedStatements.setValue(flag);
	}
	
	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getJdbcCompliantTruncationForReads()
	 */
	public boolean getJdbcCompliantTruncationForReads() {
		return this.jdbcCompliantTruncationForReads;
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setJdbcCompliantTruncationForReads(boolean)
	 */
	public void setJdbcCompliantTruncationForReads(
			boolean jdbcCompliantTruncationForReads) {
		this.jdbcCompliantTruncationForReads = jdbcCompliantTruncationForReads;
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getUseJvmCharsetConverters()
	 */
	public boolean getUseJvmCharsetConverters() {
		return this.useJvmCharsetConverters.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setUseJvmCharsetConverters(boolean)
	 */
	public void setUseJvmCharsetConverters(boolean flag) {
		this.useJvmCharsetConverters.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getPinGlobalTxToPhysicalConnection()
	 */
	public boolean getPinGlobalTxToPhysicalConnection() {
		return this.pinGlobalTxToPhysicalConnection.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setPinGlobalTxToPhysicalConnection(boolean)
	 */
	public void setPinGlobalTxToPhysicalConnection(boolean flag) {
		this.pinGlobalTxToPhysicalConnection.setValue(flag);
	}
	
	/*
	 * "Aliases" which match the property names to make using 
	 * from datasources easier.
	 */
	
	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setGatherPerfMetrics(boolean)
	 */
	public void setGatherPerfMetrics(boolean flag) {
		setGatherPerformanceMetrics(flag);
	}
	
	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getGatherPerfMetrics()
	 */
	public boolean getGatherPerfMetrics() {
		return getGatherPerformanceMetrics();
	}
	
	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setUltraDevHack(boolean)
	 */
	public void setUltraDevHack(boolean flag) {
		setUseUltraDevWorkAround(flag);
	}
	
	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getUltraDevHack()
	 */
	public boolean getUltraDevHack() {
		return getUseUltraDevWorkAround();
	}
	
	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setInteractiveClient(boolean)
	 */
	public void setInteractiveClient(boolean property) {
		setIsInteractiveClient(property);
	}
	
	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setSocketFactory(java.lang.String)
	 */
	public void setSocketFactory(String name) {
		setSocketFactoryClassName(name);
	}
	
	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getSocketFactory()
	 */
	public String getSocketFactory() {
		return getSocketFactoryClassName();
	}
	
	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setUseServerPrepStmts(boolean)
	 */
	public void setUseServerPrepStmts(boolean flag) {
		setUseServerPreparedStmts(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getUseServerPrepStmts()
	 */
	public boolean getUseServerPrepStmts() {
		return getUseServerPreparedStmts();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setCacheCallableStmts(boolean)
	 */
	public void setCacheCallableStmts(boolean flag) {
		setCacheCallableStatements(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getCacheCallableStmts()
	 */
	public boolean getCacheCallableStmts() {
		return getCacheCallableStatements();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setCachePrepStmts(boolean)
	 */
	public void setCachePrepStmts(boolean flag) {
		setCachePreparedStatements(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getCachePrepStmts()
	 */
	public boolean getCachePrepStmts() {
		return getCachePreparedStatements();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setCallableStmtCacheSize(int)
	 */
	public void setCallableStmtCacheSize(int cacheSize) {
		setCallableStatementCacheSize(cacheSize);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getCallableStmtCacheSize()
	 */
	public int getCallableStmtCacheSize() {
		return getCallableStatementCacheSize();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setPrepStmtCacheSize(int)
	 */
	public void setPrepStmtCacheSize(int cacheSize) {
		setPreparedStatementCacheSize(cacheSize);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getPrepStmtCacheSize()
	 */
	public int getPrepStmtCacheSize() {
		return getPreparedStatementCacheSize();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setPrepStmtCacheSqlLimit(int)
	 */
	public void setPrepStmtCacheSqlLimit(int sqlLimit) {
		setPreparedStatementCacheSqlLimit(sqlLimit);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getPrepStmtCacheSqlLimit()
	 */
	public int getPrepStmtCacheSqlLimit() {
		return getPreparedStatementCacheSqlLimit();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getNoAccessToProcedureBodies()
	 */
	public boolean getNoAccessToProcedureBodies() {
		return this.noAccessToProcedureBodies.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setNoAccessToProcedureBodies(boolean)
	 */
	public void setNoAccessToProcedureBodies(boolean flag) {
		this.noAccessToProcedureBodies.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getUseOldAliasMetadataBehavior()
	 */
	public boolean getUseOldAliasMetadataBehavior() {
		return this.useOldAliasMetadataBehavior.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setUseOldAliasMetadataBehavior(boolean)
	 */
	public void setUseOldAliasMetadataBehavior(boolean flag) {
		this.useOldAliasMetadataBehavior.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getClientCertificateKeyStorePassword()
	 */
	public String getClientCertificateKeyStorePassword() {
		return clientCertificateKeyStorePassword.getValueAsString();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setClientCertificateKeyStorePassword(java.lang.String)
	 */
	public void setClientCertificateKeyStorePassword(
			String value) {
		this.clientCertificateKeyStorePassword.setValue(value);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getClientCertificateKeyStoreType()
	 */
	public String getClientCertificateKeyStoreType() {
		return clientCertificateKeyStoreType.getValueAsString();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setClientCertificateKeyStoreType(java.lang.String)
	 */
	public void setClientCertificateKeyStoreType(
			String value) {
		this.clientCertificateKeyStoreType.setValue(value);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getClientCertificateKeyStoreUrl()
	 */
	public String getClientCertificateKeyStoreUrl() {
		return clientCertificateKeyStoreUrl.getValueAsString();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setClientCertificateKeyStoreUrl(java.lang.String)
	 */
	public void setClientCertificateKeyStoreUrl(
			String value) {
		this.clientCertificateKeyStoreUrl.setValue(value);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getTrustCertificateKeyStorePassword()
	 */
	public String getTrustCertificateKeyStorePassword() {
		return trustCertificateKeyStorePassword.getValueAsString();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setTrustCertificateKeyStorePassword(java.lang.String)
	 */
	public void setTrustCertificateKeyStorePassword(
			String value) {
		this.trustCertificateKeyStorePassword.setValue(value);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getTrustCertificateKeyStoreType()
	 */
	public String getTrustCertificateKeyStoreType() {
		return trustCertificateKeyStoreType.getValueAsString();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setTrustCertificateKeyStoreType(java.lang.String)
	 */
	public void setTrustCertificateKeyStoreType(
			String value) {
		this.trustCertificateKeyStoreType.setValue(value);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getTrustCertificateKeyStoreUrl()
	 */
	public String getTrustCertificateKeyStoreUrl() {
		return trustCertificateKeyStoreUrl.getValueAsString();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setTrustCertificateKeyStoreUrl(java.lang.String)
	 */
	public void setTrustCertificateKeyStoreUrl(
			String value) {
		this.trustCertificateKeyStoreUrl.setValue(value);
	}
	
	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getUseSSPSCompatibleTimezoneShift()
	 */
	public boolean getUseSSPSCompatibleTimezoneShift() {
		return this.useSSPSCompatibleTimezoneShift.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setUseSSPSCompatibleTimezoneShift(boolean)
	 */
	public void setUseSSPSCompatibleTimezoneShift(boolean flag) {
		this.useSSPSCompatibleTimezoneShift.setValue(flag);
	}
	
	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getTreatUtilDateAsTimestamp()
	 */
	public boolean getTreatUtilDateAsTimestamp() {
		return this.treatUtilDateAsTimestamp.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setTreatUtilDateAsTimestamp(boolean)
	 */
	public void setTreatUtilDateAsTimestamp(boolean flag) {
		this.treatUtilDateAsTimestamp.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getUseFastDateParsing()
	 */
	public boolean getUseFastDateParsing() {
		return this.useFastDateParsing.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setUseFastDateParsing(boolean)
	 */
	public void setUseFastDateParsing(boolean flag) {
		this.useFastDateParsing.setValue(flag);
	}
	
	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getLocalSocketAddress()
	 */
	public String getLocalSocketAddress() {
		return this.localSocketAddress.getValueAsString();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setLocalSocketAddress(java.lang.String)
	 */
	public void setLocalSocketAddress(String address) {
		this.localSocketAddress.setValue(address);
	}
	
	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setUseConfigs(java.lang.String)
	 */
	public void setUseConfigs(String configs) {
		this.useConfigs.setValue(configs);
	}
	
	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getUseConfigs()
	 */
	public String getUseConfigs() {
		return this.useConfigs.getValueAsString();
	}
	
	
	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getGenerateSimpleParameterMetadata()
	 */
	public boolean getGenerateSimpleParameterMetadata() {
		return this.generateSimpleParameterMetadata.getValueAsBoolean();
	}
	
	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setGenerateSimpleParameterMetadata(boolean)
	 */
	public void setGenerateSimpleParameterMetadata(boolean flag) {
		this.generateSimpleParameterMetadata.setValue(flag);
	}	

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getLogXaCommands()
	 */
	public boolean getLogXaCommands() {
		return this.logXaCommands.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setLogXaCommands(boolean)
	 */
	public void setLogXaCommands(boolean flag) {
		this.logXaCommands.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getResultSetSizeThreshold()
	 */
	public int getResultSetSizeThreshold() {
		return this.resultSetSizeThreshold.getValueAsInt();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setResultSetSizeThreshold(int)
	 */
	public void setResultSetSizeThreshold(int threshold) {
		this.resultSetSizeThreshold.setValue(threshold);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getNetTimeoutForStreamingResults()
	 */
	public int getNetTimeoutForStreamingResults() {
		return this.netTimeoutForStreamingResults.getValueAsInt();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setNetTimeoutForStreamingResults(int)
	 */
	public void setNetTimeoutForStreamingResults(int value) {
		this.netTimeoutForStreamingResults.setValue(value);
	}
	
	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getEnableQueryTimeouts()
	 */
	public boolean getEnableQueryTimeouts() {
		return this.enableQueryTimeouts.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setEnableQueryTimeouts(boolean)
	 */
	public void setEnableQueryTimeouts(boolean flag) {
		this.enableQueryTimeouts.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getPadCharsWithSpace()
	 */
	public boolean getPadCharsWithSpace() {
		return this.padCharsWithSpace.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setPadCharsWithSpace(boolean)
	 */
	public void setPadCharsWithSpace(boolean flag) {
		this.padCharsWithSpace.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getUseDynamicCharsetInfo()
	 */
	public boolean getUseDynamicCharsetInfo() {
		return this.useDynamicCharsetInfo.getValueAsBoolean();
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#setUseDynamicCharsetInfo(boolean)
	 */
	public void setUseDynamicCharsetInfo(boolean flag) {
		this.useDynamicCharsetInfo.setValue(flag);
	}

	/* (non-Javadoc)
	 * @see com.mysql.jdbc.IConnectionProperties#getClientInfoProvider()
	 */
	public String getClientInfoProvider() {
		return this.clientInfoProvider.getValueAsString();
	}

	/* (non-Javadoc)
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

	public void setTcpRcvBuf(int bufSize) {
		this.tcpRcvBuf.setValue(bufSize);
	}

	public int getTcpSndBuf() {
		return this.tcpSndBuf.getValueAsInt();
	}

	public void setTcpSndBuf(int bufSize) {
		this.tcpSndBuf.setValue(bufSize);
	}

	public int getTcpTrafficClass() {
		return this.tcpTrafficClass.getValueAsInt();
	}

	public void setTcpTrafficClass(int classFlags) {
		this.tcpTrafficClass.setValue(classFlags);
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

	public void setSlowQueryThresholdNanos(long nanos) {
		this.slowQueryThresholdNanos.setValue(nanos);
	}
}
