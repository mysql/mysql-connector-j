package com.mysql.jdbc;

/**
 * @author Administrator
 *
 * To change this generated comment edit the template variable "typecomment":
 * Window>Preferences>Java>Templates.
 * To enable and disable the creation of type comments go to
 * Window>Preferences>Java>Code Generation.
 */

import java.sql.Time;
import java.sql.Timestamp;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.TimeZone;


public class TimeUtil {

	static TimeZone GMT_TIMEZONE = TimeZone.getTimeZone("GMT");

	static final HashMap TIMEZONE_MAPPINGS;

	static {
		TIMEZONE_MAPPINGS = new HashMap(175);

		//
		// Windows Mappings
		//

		TIMEZONE_MAPPINGS.put("Romance", "Europe/Paris");
		TIMEZONE_MAPPINGS.put("Romance Standard Time", "Europe/Paris");
		TIMEZONE_MAPPINGS.put("Warsaw", "Europe/Warsaw");
		TIMEZONE_MAPPINGS.put("Central Europe", "Europe/Prague");
		TIMEZONE_MAPPINGS.put("Central Europe Standard Time", "Europe/Prague");
		TIMEZONE_MAPPINGS.put("Prague Bratislava", "Europe/Prague");
		TIMEZONE_MAPPINGS.put(
			"W. Central Africa Standard Time",
			"Africa/Luanda");
		TIMEZONE_MAPPINGS.put("FLE", "Europe/Helsinki");
		TIMEZONE_MAPPINGS.put("FLE Standard Time", "Europe/Helsinki");
		TIMEZONE_MAPPINGS.put("GFT", "Europe/Athens");
		TIMEZONE_MAPPINGS.put("GFT Standard Time", "Europe/Athens");
		TIMEZONE_MAPPINGS.put("GTB", "Europe/Athens");
		TIMEZONE_MAPPINGS.put("GTB Standard Time", "Europe/Athens");
		TIMEZONE_MAPPINGS.put("Israel", "Asia/Jerusalem");
		TIMEZONE_MAPPINGS.put("Israel Standard Time", "Asia/Jerusalem");
		TIMEZONE_MAPPINGS.put("Arab", "Asia/Riyadh");
		TIMEZONE_MAPPINGS.put("Arab Standard Time", "Asia/Riyadh");
		TIMEZONE_MAPPINGS.put("Arabic Standard Time", "Asia/Baghdad");
		TIMEZONE_MAPPINGS.put("E. Africa", "Africa/Nairobi");
		TIMEZONE_MAPPINGS.put("E. Africa Standard Time", "Africa/Nairobi");
		TIMEZONE_MAPPINGS.put("Saudi Arabia", "Asia/Riyadh");
		TIMEZONE_MAPPINGS.put("Saudi Arabia Standard Time", "Asia/Riyadh");
		TIMEZONE_MAPPINGS.put("Iran", "Asia/Tehran");
		TIMEZONE_MAPPINGS.put("Iran Standard Time", "Asia/Tehran");
		TIMEZONE_MAPPINGS.put("Afghanistan", "Asia/Kabul");
		TIMEZONE_MAPPINGS.put("Afghanistan Standard Time", "Asia/Kabul");
		TIMEZONE_MAPPINGS.put("India", "Asia/Calcutta");
		TIMEZONE_MAPPINGS.put("India Standard Time", "Asia/Calcutta");
		TIMEZONE_MAPPINGS.put("Myanmar Standard Time", "Asia/Rangoon");
		TIMEZONE_MAPPINGS.put("Nepal Standard Time", "Asia/Katmandu");
		TIMEZONE_MAPPINGS.put("Sri Lanka", "Asia/Colombo");
		TIMEZONE_MAPPINGS.put("Sri Lanka Standard Time", "Asia/Colombo");
		TIMEZONE_MAPPINGS.put("Beijing", "Asia/Shanghai");
		TIMEZONE_MAPPINGS.put("China", "Asia/Shanghai");
		TIMEZONE_MAPPINGS.put("China Standard Time", "Asia/Shanghai");
		TIMEZONE_MAPPINGS.put("AUS Central", "Australia/Darwin");
		TIMEZONE_MAPPINGS.put("AUS Central Standard Time", "Australia/Darwin");
		TIMEZONE_MAPPINGS.put("Cen. Australia", "Australia/Adelaide");
		TIMEZONE_MAPPINGS.put(
			"Cen. Australia Standard Time",
			"Australia/Adelaide");
		TIMEZONE_MAPPINGS.put("Vladivostok", "Asia/Vladivostok");
		TIMEZONE_MAPPINGS.put("Vladivostok Standard Time", "Asia/Vladivostok");
		TIMEZONE_MAPPINGS.put("West Pacific", "Pacific/Guam");
		TIMEZONE_MAPPINGS.put("West Pacific Standard Time", "Pacific/Guam");
		TIMEZONE_MAPPINGS.put("E. South America", "America/Sao_Paulo");
		TIMEZONE_MAPPINGS.put(
			"E. South America Standard Time",
			"America/Sao_Paulo");
		TIMEZONE_MAPPINGS.put("Greenland Standard Time", "America/Godthab");
		TIMEZONE_MAPPINGS.put("Newfoundland", "America/St_Johns");
		TIMEZONE_MAPPINGS.put("Newfoundland Standard Time", "America/St_Johns");
		TIMEZONE_MAPPINGS.put("Pacific SA", "America/Caracas");
		TIMEZONE_MAPPINGS.put("Pacific SA Standard Time", "America/Caracas");
		TIMEZONE_MAPPINGS.put("SA Western", "America/Caracas");
		TIMEZONE_MAPPINGS.put("SA Western Standard Time", "America/Caracas");
		TIMEZONE_MAPPINGS.put("SA Pacific", "America/Bogota");
		TIMEZONE_MAPPINGS.put("SA Pacific Standard Time", "America/Bogota");
		TIMEZONE_MAPPINGS.put("US Eastern", "America/Indianapolis");
		TIMEZONE_MAPPINGS.put(
			"US Eastern Standard Time",
			"America/Indianapolis");
		TIMEZONE_MAPPINGS.put(
			"Central America Standard Time",
			"America/Regina");
		TIMEZONE_MAPPINGS.put("Mexico", "America/Mexico_City");
		TIMEZONE_MAPPINGS.put("Mexico Standard Time", "America/Mexico_City");
		TIMEZONE_MAPPINGS.put("Canada Central", "America/Regina");
		TIMEZONE_MAPPINGS.put("Canada Central Standard Time", "America/Regina");
		TIMEZONE_MAPPINGS.put("US Mountain", "America/Phoenix");
		TIMEZONE_MAPPINGS.put("US Mountain Standard Time", "America/Phoenix");
		TIMEZONE_MAPPINGS.put("GMT", "Europe/London");
		TIMEZONE_MAPPINGS.put("GMT Standard Time", "Europe/London");
		TIMEZONE_MAPPINGS.put("Ekaterinburg", "Asia/Yekaterinburg");
		TIMEZONE_MAPPINGS.put(
			"Ekaterinburg Standard Time",
			"Asia/Yekaterinburg");
		TIMEZONE_MAPPINGS.put("West Asia", "Asia/Karachi");
		TIMEZONE_MAPPINGS.put("West Asia Standard Time", "Asia/Karachi");
		TIMEZONE_MAPPINGS.put("Central Asia", "Asia/Dhaka");
		TIMEZONE_MAPPINGS.put("Central Asia Standard Time", "Asia/Dhaka");
		TIMEZONE_MAPPINGS.put(
			"N. Central Asia Standard Time",
			"Asia/Novosibirsk");
		TIMEZONE_MAPPINGS.put("Bangkok", "Asia/Bangkok");
		TIMEZONE_MAPPINGS.put("Bangkok Standard Time", "Asia/Bangkok");
		TIMEZONE_MAPPINGS.put("North Asia Standard Time", "Asia/Krasnoyarsk");
		TIMEZONE_MAPPINGS.put("SE Asia", "Asia/Bangkok");
		TIMEZONE_MAPPINGS.put("SE Asia Standard Time", "Asia/Bangkok");
		TIMEZONE_MAPPINGS.put(
			"North Asia East Standard Time",
			"Asia/Ulaanbaatar");
		TIMEZONE_MAPPINGS.put("Singapore", "Asia/Singapore");
		TIMEZONE_MAPPINGS.put("Singapore Standard Time", "Asia/Singapore");
		TIMEZONE_MAPPINGS.put("Taipei", "Asia/Taipei");
		TIMEZONE_MAPPINGS.put("Taipei Standard Time", "Asia/Taipei");
		TIMEZONE_MAPPINGS.put("W. Australia", "Australia/Perth");
		TIMEZONE_MAPPINGS.put("W. Australia Standard Time", "Australia/Perth");
		TIMEZONE_MAPPINGS.put("Korea", "Asia/Seoul");
		TIMEZONE_MAPPINGS.put("Korea Standard Time", "Asia/Seoul");
		TIMEZONE_MAPPINGS.put("Tokyo", "Asia/Tokyo");
		TIMEZONE_MAPPINGS.put("Tokyo Standard Time", "Asia/Tokyo");
		TIMEZONE_MAPPINGS.put("Yakutsk", "Asia/Yakutsk");
		TIMEZONE_MAPPINGS.put("Yakutsk Standard Time", "Asia/Yakutsk");
		TIMEZONE_MAPPINGS.put("Central European", "Europe/Belgrade");
		TIMEZONE_MAPPINGS.put(
			"Central European Standard Time",
			"Europe/Belgrade");
		TIMEZONE_MAPPINGS.put("W. Europe", "Europe/Berlin");
		TIMEZONE_MAPPINGS.put("W. Europe Standard Time", "Europe/Berlin");
		TIMEZONE_MAPPINGS.put("Tasmania", "Australia/Hobart");
		TIMEZONE_MAPPINGS.put("Tasmania Standard Time", "Australia/Hobart");
		TIMEZONE_MAPPINGS.put("AUS Eastern", "Australia/Sydney");
		TIMEZONE_MAPPINGS.put("AUS Eastern Standard Time", "Australia/Sydney");
		TIMEZONE_MAPPINGS.put("E. Australia", "Australia/Brisbane");
		TIMEZONE_MAPPINGS.put(
			"E. Australia Standard Time",
			"Australia/Brisbane");
		TIMEZONE_MAPPINGS.put("Sydney Standard Time", "Australia/Sydney");
		TIMEZONE_MAPPINGS.put("Central Pacific", "Pacific/Guadalcanal");
		TIMEZONE_MAPPINGS.put(
			"Central Pacific Standard Time",
			"Pacific/Guadalcanal");
		TIMEZONE_MAPPINGS.put("Dateline", "Pacific/Majuro");
		TIMEZONE_MAPPINGS.put("Dateline Standard Time", "Pacific/Majuro");
		TIMEZONE_MAPPINGS.put("Fiji", "Pacific/Fiji");
		TIMEZONE_MAPPINGS.put("Fiji Standard Time", "Pacific/Fiji");
		TIMEZONE_MAPPINGS.put("Samoa", "Pacific/Apia");
		TIMEZONE_MAPPINGS.put("Samoa Standard Time", "Pacific/Apia");
		TIMEZONE_MAPPINGS.put("Hawaiian", "Pacific/Honolulu");
		TIMEZONE_MAPPINGS.put("Hawaiian Standard Time", "Pacific/Honolulu");
		TIMEZONE_MAPPINGS.put("Alaskan", "America/Anchorage");
		TIMEZONE_MAPPINGS.put("Alaskan Standard Time", "America/Anchorage");
		TIMEZONE_MAPPINGS.put("Pacific", "America/Los_Angeles");
		TIMEZONE_MAPPINGS.put("Pacific Standard Time", "America/Los_Angeles");
		TIMEZONE_MAPPINGS.put("Mexico Standard Time 2", "America/Chihuahua");
		TIMEZONE_MAPPINGS.put("Mountain", "America/Denver");
		TIMEZONE_MAPPINGS.put("Mountain Standard Time", "America/Denver");
		TIMEZONE_MAPPINGS.put("Central", "America/Chicago");
		TIMEZONE_MAPPINGS.put("Central Standard Time", "America/Chicago");
		TIMEZONE_MAPPINGS.put("Eastern", "America/New_York");
		TIMEZONE_MAPPINGS.put("Eastern Standard Time", "America/New_York");
		TIMEZONE_MAPPINGS.put("E. Europe", "Europe/Bucharest");
		TIMEZONE_MAPPINGS.put("E. Europe Standard Time", "Europe/Bucharest");
		TIMEZONE_MAPPINGS.put("Egypt", "Africa/Cairo");
		TIMEZONE_MAPPINGS.put("Egypt Standard Time", "Africa/Cairo");
		TIMEZONE_MAPPINGS.put("South Africa", "Africa/Harare");
		TIMEZONE_MAPPINGS.put("South Africa Standard Time", "Africa/Harare");
		TIMEZONE_MAPPINGS.put("Atlantic", "America/Halifax");
		TIMEZONE_MAPPINGS.put("Atlantic Standard Time", "America/Halifax");
		TIMEZONE_MAPPINGS.put("SA Eastern", "America/Buenos_Aires");
		TIMEZONE_MAPPINGS.put(
			"SA Eastern Standard Time",
			"America/Buenos_Aires");
		TIMEZONE_MAPPINGS.put("Mid-Atlantic", "Atlantic/South_Georgia");
		TIMEZONE_MAPPINGS.put(
			"Mid-Atlantic Standard Time",
			"Atlantic/South_Georgia");
		TIMEZONE_MAPPINGS.put("Azores", "Atlantic/Azores");
		TIMEZONE_MAPPINGS.put("Azores Standard Time", "Atlantic/Azores");
		TIMEZONE_MAPPINGS.put(
			"Cape Verde Standard Time",
			"Atlantic/Cape_Verde");
		TIMEZONE_MAPPINGS.put("Russian", "Europe/Moscow");
		TIMEZONE_MAPPINGS.put("Russian Standard Time", "Europe/Moscow");
		TIMEZONE_MAPPINGS.put("New Zealand", "Pacific/Auckland");
		TIMEZONE_MAPPINGS.put("New Zealand Standard Time", "Pacific/Auckland");
		TIMEZONE_MAPPINGS.put("Tonga Standard Time", "Pacific/Tongatapu");
		TIMEZONE_MAPPINGS.put("Arabian", "Asia/Muscat");
		TIMEZONE_MAPPINGS.put("Arabian Standard Time", "Asia/Muscat");
		TIMEZONE_MAPPINGS.put("Caucasus", "Asia/Tbilisi");
		TIMEZONE_MAPPINGS.put("Caucasus Standard Time", "Asia/Tbilisi");
		TIMEZONE_MAPPINGS.put("GMT Standard Time", "GMT");
		TIMEZONE_MAPPINGS.put("Greenwich", "GMT");
		TIMEZONE_MAPPINGS.put("Greenwich Standard Time", "GMT");
	}

	public static String getCanoncialTimezone(String timezoneStr) {

		if (timezoneStr == null) {
			return null;
		}

		timezoneStr = timezoneStr.trim();

		// Fix windows Daylight/Standard shift JDK doesn't map these (doh)

		String timezoneStrUC = timezoneStr.toUpperCase();

		int daylightIndex = timezoneStrUC.indexOf("DAYLIGHT");

		if (daylightIndex != -1) {
			StringBuffer timezoneBuf = new StringBuffer();
			timezoneBuf.append(timezoneStr.substring(0, daylightIndex));
			timezoneBuf.append("Standard");
			timezoneBuf.append(
				timezoneStr.substring(
					daylightIndex + "DAYLIGHT".length(),
					timezoneStr.length()));

			timezoneStr = timezoneBuf.toString();
		}

		return (String) TIMEZONE_MAPPINGS.get(timezoneStr);
	}

	public static Timestamp changeTimezone(Timestamp tstamp, TimeZone fromTz,	TimeZone toTz) {

		/*
		// Convert the timestamp to the GMT timezone

		Calendar fromCal = Calendar.getInstance(fromTz);
		fromCal.setTime(tstamp);
		int fromOffset =
			fromCal.get(Calendar.ZONE_OFFSET)
				+ fromCal.get(Calendar.DST_OFFSET);

		Calendar toCal = Calendar.getInstance(toTz);
		toCal.setTime(tstamp);
		int toOffset =
			toCal.get(Calendar.ZONE_OFFSET) + toCal.get(Calendar.DST_OFFSET);

		int offsetDiff = toOffset - fromOffset;
		long toTime = toCal.getTime().getTime();
		toTime += offsetDiff;

		Timestamp changedTimestamp = new Timestamp(toTime);

		return changedTimestamp;
		*/
		
		return tstamp;
	}

	public static Time changeTimezone(Time t, TimeZone fromTz, TimeZone toTz) {
		/*
		// Convert the timestamp to the GMT timezone

		Calendar fromCal = Calendar.getInstance(fromTz);
		fromCal.setTime(t);
		int fromOffset =
			fromCal.get(Calendar.ZONE_OFFSET)
				+ fromCal.get(Calendar.DST_OFFSET);

		Calendar toCal = Calendar.getInstance(toTz);
		toCal.setTime(t);
		int toOffset =
			toCal.get(Calendar.ZONE_OFFSET) + toCal.get(Calendar.DST_OFFSET);

		int offsetDiff = toOffset - fromOffset;
		long toTime = toCal.getTime().getTime();
		toTime += offsetDiff;

		Time changedTime = new Time(toTime);

		return changedTime;
		*/
		
		return t;
	}

}
