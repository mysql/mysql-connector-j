/*
 Copyright (C) 2007 MySQL AB

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

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * Classes that implement this interface represent one row of data from the
 * MySQL server that might be stored in different ways depending on whether the
 * result set was streaming (so they wrap a reuseable packet), or whether the
 * result set was cached or via a server-side cursor (so they represent a
 * byte[][]).
 * 
 * Notice that <strong>no</strong> bounds checking is expected for implementors
 * of this interface, it happens in ResultSetImpl.
 * 
 * @version $Id: $
 */
public abstract class RowHolder {
	/**
	 * The metadata of the fields of this result set.
	 */
	protected Field[] metadata;

	/**
	 * Returns the value at the given column (index starts at 0) "raw" (i.e.
	 * as-returned by the server).
	 * 
	 * @param index
	 *            of the column value (starting at 0) to return.
	 * @return the value for the given column (including NULL if it is)
	 * @throws SQLException
	 *             if an error occurs while retrieving the value.
	 */
	public abstract byte[] getColumnValue(int index) throws SQLException;

	/**
	 * Returns the value at the given column (index starts at 0) as an int. *
	 * 
	 * @param index
	 *            of the column value (starting at 0) to return.
	 * @return the value for the given column (returns 0 if NULL, use isNull()
	 *         to determine if the value was actually NULL)
	 * @throws SQLException
	 *             if an error occurs while retrieving the value.
	 */
	public abstract int getInt(int columnIndex) throws SQLException;

	/**
	 * Returns the value at the given column (index starts at 0) as a long. *
	 * 
	 * @param index
	 *            of the column value (starting at 0) to return.
	 * @return the value for the given column (returns 0 if NULL, use isNull()
	 *         to determine if the value was actually NULL)
	 * @throws SQLException
	 *             if an error occurs while retrieving the value.
	 */
	public abstract long getLong(int columnIndex) throws SQLException;

	protected double getNativeDouble(byte[] bits, int offset) {
		long valueAsLong = (bits[offset + 0] & 0xff)
				| ((long) (bits[offset + 1] & 0xff) << 8)
				| ((long) (bits[offset + 2] & 0xff) << 16)
				| ((long) (bits[offset + 3] & 0xff) << 24)
				| ((long) (bits[offset + 4] & 0xff) << 32)
				| ((long) (bits[offset + 5] & 0xff) << 40)
				| ((long) (bits[offset + 6] & 0xff) << 48)
				| ((long) (bits[offset + 7] & 0xff) << 56);

		return Double.longBitsToDouble(valueAsLong);
	}

	public abstract double getNativeDouble(int columnIndex) throws SQLException;

	protected float getNativeFloat(byte[] bits, int offset) {
		int asInt = (bits[offset + 0] & 0xff)
				| ((bits[offset + 1] & 0xff) << 8)
				| ((bits[offset + 2] & 0xff) << 16)
				| ((bits[offset + 3] & 0xff) << 24);

		return Float.intBitsToFloat(asInt);
	}

	public abstract float getNativeFloat(int columnIndex) throws SQLException;

	protected int getNativeInt(byte[] bits, int offset) {

		int valueAsInt = (bits[offset + 0] & 0xff)
				| ((bits[offset + 1] & 0xff) << 8)
				| ((bits[offset + 2] & 0xff) << 16)
				| ((bits[offset + 3] & 0xff) << 24);

		return valueAsInt;
	}

	public abstract int getNativeInt(int columnIndex) throws SQLException;

	protected long getNativeLong(byte[] bits, int offset) {
		long valueAsLong = (bits[offset + 0] & 0xff)
				| ((long) (bits[offset + 1] & 0xff) << 8)
				| ((long) (bits[offset + 2] & 0xff) << 16)
				| ((long) (bits[offset + 3] & 0xff) << 24)
				| ((long) (bits[offset + 4] & 0xff) << 32)
				| ((long) (bits[offset + 5] & 0xff) << 40)
				| ((long) (bits[offset + 6] & 0xff) << 48)
				| ((long) (bits[offset + 7] & 0xff) << 56);

		return valueAsLong;
	}

	public abstract long getNativeLong(int columnIndex) throws SQLException;

	protected short getNativeShort(byte[] bits, int offset) {
		short asShort = (short) ((bits[offset + 0] & 0xff) | ((bits[offset + 1] & 0xff) << 8));

		return asShort;
	}

	public abstract short getNativeShort(int columnIndex) throws SQLException;

	/**
	 * Returns the value at the given column (index starts at 0) as a
	 * java.lang.String with the requested encoding, using the given
	 * ConnectionImpl to find character converters.
	 * 
	 * @param index
	 *            of the column value (starting at 0) to return.
	 * @param encoding
	 *            the Java name for the character encoding
	 * @param conn
	 *            the connection that created this result set row
	 * 
	 * @return the value for the given column (including NULL if it is) as a
	 *         String
	 * 
	 * @throws SQLException
	 *             if an error occurs while retrieving the value.
	 */
	public abstract String getString(int index, String encoding,
			ConnectionImpl conn) throws SQLException;

	/**
	 * Convenience method for turning a byte[] into a string with the given
	 * encoding.
	 * 
	 * @param encoding
	 *            the Java encoding name for the byte[] -> char conversion
	 * @param conn
	 *            the ConnectionImpl that created the result set
	 * @param value
	 *            the String value as a series of bytes, encoded using
	 *            "encoding"
	 * @param offset
	 *            where to start the decoding
	 * @param length
	 *            how many bytes to decode
	 * 
	 * @return the String as decoded from bytes with the given encoding
	 * 
	 * @throws SQLException
	 *             if an error occurs
	 */
	protected String getString(String encoding, ConnectionImpl conn,
			byte[] value, int offset, int length) throws SQLException {
		String stringVal = null;

		if ((conn != null) && conn.getUseUnicode()) {
			try {
				if (encoding == null) {
					stringVal = new String(value);
				} else {
					SingleByteCharsetConverter converter = conn
							.getCharsetConverter(encoding);

					if (converter != null) {
						stringVal = converter.toString(value, offset, length);
					} else {
						stringVal = new String(value, offset, length, encoding);
					}
				}
			} catch (java.io.UnsupportedEncodingException E) {
				throw SQLError
						.createSQLException(
								Messages
										.getString("ResultSet.Unsupported_character_encoding____101") //$NON-NLS-1$
										+ encoding + "'.", "0S100");
			}
		} else {
			stringVal = StringUtils.toAsciiString(value, offset, length);
		}

		return stringVal;
	}

	protected Timestamp getTimestampFast(byte[] timestampAsBytes,
			int columnIndex, ConnectionImpl conn, ResultSetImpl rs,
			Calendar targetCalendar, TimeZone tz, boolean rollForward,
			int offset, int length) throws SQLException {

		try {
			Calendar sessionCalendar = conn.getUseJDBCCompliantTimezoneShift() ? conn
					.getUtcCalendar()
					: rs.getCalendarInstanceForSessionOrNew();

			synchronized (sessionCalendar) {
				boolean allZeroTimestamp = true;

				boolean onlyTimePresent = false;

				for (int i = offset; i < length; i++) {
					if (timestampAsBytes[i] == ':') {
						onlyTimePresent = true;
						break;
					}
				}

				for (int i = 0; i < length; i++) {
					byte b = timestampAsBytes[i + offset];

					if (b == ' ' || b == '-' || b == '/') {
						onlyTimePresent = false;
					}

					if (b != '0' && b != ' ' && b != ':' && b != '-'
							&& b != '/' && b != '.') {
						allZeroTimestamp = false;

						break;
					}
				}

				if (!onlyTimePresent && allZeroTimestamp) {

					if (ConnectionPropertiesImpl.ZERO_DATETIME_BEHAVIOR_CONVERT_TO_NULL
							.equals(conn.getZeroDateTimeBehavior())) {

						return null;
					} else if (ConnectionPropertiesImpl.ZERO_DATETIME_BEHAVIOR_EXCEPTION
							.equals(conn.getZeroDateTimeBehavior())) {
						throw SQLError
								.createSQLException(
										"Value '"
												+ timestampAsBytes
												+ "' can not be represented as java.sql.Timestamp",
										SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
					}

					// We're left with the case of 'round' to a date Java _can_
					// represent, which is '0001-01-01'.
					return rs.fastTimestampCreate(null, 1, 1, 1, 0, 0, 0, 0);

				} else if (this.metadata[columnIndex].getMysqlType() == MysqlDefs.FIELD_TYPE_YEAR) {

					return TimeUtil.changeTimezone(conn, sessionCalendar,
							targetCalendar, rs.fastTimestampCreate(
									sessionCalendar, StringUtils.getInt(
											timestampAsBytes, offset, 4), 1, 1,
									0, 0, 0, 0), conn.getServerTimezoneTZ(),
							tz, rollForward);

				} else {
					if (timestampAsBytes[length - 1] == '.') {
						length--;
					}

					// Convert from TIMESTAMP or DATE
					switch (length) {
					case 26:
					case 25:
					case 24:
					case 23:
					case 22:
					case 21:
					case 20:
					case 19: {
						int year = StringUtils.getInt(timestampAsBytes,
								offset + 0, 4);
						int month = StringUtils.getInt(timestampAsBytes,
								offset + 5, 7);
						int day = StringUtils.getInt(timestampAsBytes,
								offset + 8, 10);
						int hour = StringUtils.getInt(timestampAsBytes,
								offset + 11, 13);
						int minutes = StringUtils.getInt(timestampAsBytes,
								offset + 14, 16);
						int seconds = StringUtils.getInt(timestampAsBytes,
								offset + 17, 19);

						int nanos = 0;

						if (length > 19) {
							int decimalIndex = StringUtils.lastIndexOf(
									timestampAsBytes, '.');

							if (decimalIndex != -1) {
								if ((decimalIndex + 2) <= length) {
									nanos = StringUtils.getInt(
											timestampAsBytes, decimalIndex + 1,
											length);
								} else {
									throw new IllegalArgumentException(); // re-thrown
									// further
									// down
									// with
									// a
									// much better error message
								}
							}
						}

						return TimeUtil
								.changeTimezone(conn, sessionCalendar,
										targetCalendar, rs.fastTimestampCreate(
												sessionCalendar, year, month,
												day, hour, minutes, seconds,
												nanos), conn
												.getServerTimezoneTZ(), tz,
										rollForward);
					}

					case 14: {
						int year = StringUtils.getInt(timestampAsBytes,
								offset + 0, offset + 4);
						int month = StringUtils.getInt(timestampAsBytes,
								offset + 4, offset + 6);
						int day = StringUtils.getInt(timestampAsBytes,
								offset + 6, offset + 8);
						int hour = StringUtils.getInt(timestampAsBytes,
								offset + 8, offset + 10);
						int minutes = StringUtils.getInt(timestampAsBytes,
								offset + 10, offset + 12);
						int seconds = StringUtils.getInt(timestampAsBytes,
								offset + 12, offset + 14);

						return TimeUtil
								.changeTimezone(conn, sessionCalendar,
										targetCalendar,
										rs.fastTimestampCreate(sessionCalendar,
												year, month, day, hour,
												minutes, seconds, 0), conn
												.getServerTimezoneTZ(), tz,
										rollForward);
					}

					case 12: {
						int year = StringUtils.getInt(timestampAsBytes,
								offset + 0, offset + 2);

						if (year <= 69) {
							year = (year + 100);
						}

						int month = StringUtils.getInt(timestampAsBytes,
								offset + 2, offset + 4);
						int day = StringUtils.getInt(timestampAsBytes,
								offset + 4, offset + 6);
						int hour = StringUtils.getInt(timestampAsBytes,
								offset + 6, offset + 8);
						int minutes = StringUtils.getInt(timestampAsBytes,
								offset + 8, offset + 10);
						int seconds = StringUtils.getInt(timestampAsBytes,
								offset + 10, offset + 12);

						return TimeUtil
								.changeTimezone(conn, sessionCalendar,
										targetCalendar, rs.fastTimestampCreate(
												sessionCalendar, year + 1900,
												month, day, hour, minutes,
												seconds, 0), conn
												.getServerTimezoneTZ(), tz,
										rollForward);
					}

					case 10: {
						int year;
						int month;
						int day;
						int hour;
						int minutes;

						boolean hasDash = false;

						for (int i = offset; i < length; i++) {
							if (timestampAsBytes[i] == '-') {
								hasDash = true;
								break;
							}
						}

						if ((this.metadata[columnIndex].getMysqlType() == MysqlDefs.FIELD_TYPE_DATE)
								|| hasDash) {
							year = StringUtils.getInt(timestampAsBytes,
									offset + 0, offset + 4);
							month = StringUtils.getInt(timestampAsBytes,
									offset + 5, offset + 7);
							day = StringUtils.getInt(timestampAsBytes,
									offset + 8, offset + 10);
							hour = 0;
							minutes = 0;
						} else {
							year = StringUtils.getInt(timestampAsBytes,
									offset + 0, offset + 2);

							if (year <= 69) {
								year = (year + 100);
							}

							month = StringUtils.getInt(timestampAsBytes,
									offset + 2, offset + 4);
							day = StringUtils.getInt(timestampAsBytes,
									offset + 4, offset + 6);
							hour = StringUtils.getInt(timestampAsBytes,
									offset + 6, offset + 8);
							minutes = StringUtils.getInt(timestampAsBytes,
									offset + 8, offset + 10);

							year += 1900; // two-digit year
						}

						return TimeUtil
								.changeTimezone(conn, sessionCalendar,
										targetCalendar, rs.fastTimestampCreate(
												sessionCalendar, year, month,
												day, hour, minutes, 0, 0), conn
												.getServerTimezoneTZ(), tz,
										rollForward);
					}

					case 8: {
						boolean hasColon = false;

						for (int i = offset; i < length; i++) {
							if (timestampAsBytes[i] == ':') {
								hasColon = true;
								break;
							}
						}

						if (StringUtils.indexOf(timestampAsBytes, ':') != -1) {
							int hour = StringUtils.getInt(timestampAsBytes,
									offset + 0, offset + 2);
							int minutes = StringUtils.getInt(timestampAsBytes,
									offset + 3, offset + 5);
							int seconds = StringUtils.getInt(timestampAsBytes,
									offset + 6, offset + 8);

							return TimeUtil.changeTimezone(conn,
									sessionCalendar, targetCalendar, rs
											.fastTimestampCreate(
													sessionCalendar, 1970, 1,
													1, hour, minutes, seconds,
													0), conn
											.getServerTimezoneTZ(), tz,
									rollForward);

						}

						int year = StringUtils.getInt(timestampAsBytes,
								offset + 0, offset + 4);
						int month = StringUtils.getInt(timestampAsBytes,
								offset + 4, offset + 6);
						int day = StringUtils.getInt(timestampAsBytes,
								offset + 6, offset + 8);

						return TimeUtil
								.changeTimezone(conn, sessionCalendar,
										targetCalendar, rs.fastTimestampCreate(
												sessionCalendar, year - 1900,
												month - 1, day, 0, 0, 0, 0),
										conn.getServerTimezoneTZ(), tz,
										rollForward);
					}

					case 6: {
						int year = StringUtils.getInt(timestampAsBytes,
								offset + 0, offset + 2);

						if (year <= 69) {
							year = (year + 100);
						}

						int month = StringUtils.getInt(timestampAsBytes,
								offset + 2, offset + 4);
						int day = StringUtils.getInt(timestampAsBytes,
								offset + 4, offset + 6);

						return TimeUtil
								.changeTimezone(conn, sessionCalendar,
										targetCalendar, rs.fastTimestampCreate(
												sessionCalendar, year + 1900,
												month, day, 0, 0, 0, 0), conn
												.getServerTimezoneTZ(), tz,
										rollForward);
					}

					case 4: {
						int year = StringUtils.getInt(timestampAsBytes,
								offset + 0, 2);

						if (year <= 69) {
							year = (year + 100);
						}

						int month = StringUtils.getInt(timestampAsBytes,
								offset + 2, offset + 4);

						return TimeUtil.changeTimezone(conn, sessionCalendar,
								targetCalendar, rs.fastTimestampCreate(
										sessionCalendar, year + 1900, month, 1,
										0, 0, 0, 0),
								conn.getServerTimezoneTZ(), tz, rollForward);
					}

					case 2: {
						int year = StringUtils.getInt(timestampAsBytes,
								offset + 0, offset + 2);

						if (year <= 69) {
							year = (year + 100);
						}

						return TimeUtil
								.changeTimezone(conn, sessionCalendar,
										targetCalendar, rs.fastTimestampCreate(
												null, year + 1900, 1, 1, 0, 0,
												0, 0), conn
												.getServerTimezoneTZ(), tz,
										rollForward);
					}

					default:
						throw new java.sql.SQLException(
								"Bad format for Timestamp '"
										+ new String(timestampAsBytes)
										+ "' in column " + columnIndex + ".",
								SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
					}
				}
			}
		} catch (Exception e) {
			throw new java.sql.SQLException("Cannot convert value '"
					+ getString(columnIndex, "ISO8859_1", conn)
					+ "' from column " + (columnIndex + 1) + " to TIMESTAMP.",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
		}
	}

	public abstract Timestamp getTimestampFast(int columnIndex,
			ConnectionImpl conn, ResultSetImpl rs, Calendar targetCalendar,
			TimeZone tz, boolean rollForward) throws SQLException;

	/**
	 * Could the column value at the given index (which starts at 0) be
	 * interpreted as a floating-point number (has +/-/E/e in it)?
	 * 
	 * @param index
	 *            of the column value (starting at 0) to check.
	 * 
	 * @return true if the column value at the given index looks like it might
	 *         be a floating-point number, false if not.
	 * 
	 * @throws SQLException
	 *             if an error occurs
	 */
	public abstract boolean isFloatingPointNumber(int index)
			throws SQLException;

	/**
	 * Is the column value at the given index (which starts at 0) NULL?
	 * 
	 * @param index
	 *            of the column value (starting at 0) to check.
	 * 
	 * @return true if the column value is NULL, false if not.
	 * 
	 * @throws SQLException
	 *             if an error occurs
	 */
	public abstract boolean isNull(int index) throws SQLException;

	/**
	 * Returns the length of the column at the given index (which starts at 0).
	 * 
	 * @param index
	 *            of the column value (starting at 0) for which to return the
	 *            length.
	 * @return the length of the requested column, 0 if null (clients of this
	 *         interface should use isNull() beforehand to determine status of
	 *         NULL values in the column).
	 * 
	 * @throws SQLException
	 */
	public abstract long length(int index) throws SQLException;

	/**
	 * Sets the given column value (only works currently with
	 * ByteArrayRowHolder).
	 * 
	 * @param index
	 *            index of the column value (starting at 0) to set.
	 * @param value
	 *            the (raw) value to set
	 * 
	 * @throws SQLException
	 *             if an error occurs, or the concrete RowHolder doesn't support
	 *             this operation.
	 */
	public abstract void setColumnValue(int index, byte[] value)
			throws SQLException;

	public void setMetadata(Field[] f) throws SQLException {
		this.metadata = f;
	}

	public abstract Timestamp getNativeTimestamp(int columnIndex,
			Calendar targetCalendar, TimeZone tz, boolean rollForward,
			ConnectionImpl conn, ResultSetImpl rs) throws SQLException;
	
	protected Timestamp getNativeTimestamp(byte[] bits, int offset, int length,
			Calendar targetCalendar, TimeZone tz, boolean rollForward,
			ConnectionImpl conn, ResultSetImpl rs) throws SQLException {
		int year = 0;
		int month = 0;
		int day = 0;

		int hour = 0;
		int minute = 0;
		int seconds = 0;

		int nanos = 0;

		if (length != 0) {
			year = (bits[offset + 0] & 0xff) | ((bits[offset + 1] & 0xff) << 8);
			month = bits[2];
			day = bits[3];

			if (length > 4) {
				hour = bits[offset + 4];
				minute = bits[offset + 5];
				seconds = bits[offset + 6];
			}

			if (length > 7) {
				nanos = (bits[offset + 7] & 0xff)
						| ((bits[offset + 8] & 0xff) << 8)
						| ((bits[offset + 9] & 0xff) << 16)
						| ((bits[offset + 10] & 0xff) << 24);
			}
		}

		if ((year == 0) && (month == 0) && (day == 0)) {
			if (ConnectionPropertiesImpl.ZERO_DATETIME_BEHAVIOR_CONVERT_TO_NULL
					.equals(conn.getZeroDateTimeBehavior())) {

				return null;
			} else if (ConnectionPropertiesImpl.ZERO_DATETIME_BEHAVIOR_EXCEPTION
					.equals(conn.getZeroDateTimeBehavior())) {
				throw SQLError
						.createSQLException(
								"Value '0000-00-00' can not be represented as java.sql.Timestamp",
								SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
			}

			year = 1;
			month = 1;
			day = 1;
		}

		Calendar sessionCalendar = conn.getUseJDBCCompliantTimezoneShift() ? conn
				.getUtcCalendar()
				: rs.getCalendarInstanceForSessionOrNew();

		synchronized (sessionCalendar) {
			Timestamp ts = rs.fastTimestampCreate(sessionCalendar, year, month,
					day, hour, minute, seconds, nanos);

			Timestamp adjustedTs = TimeUtil.changeTimezone(conn,
					sessionCalendar, targetCalendar, ts, conn
							.getServerTimezoneTZ(), tz, rollForward);

			return adjustedTs;
		}
	}
}
