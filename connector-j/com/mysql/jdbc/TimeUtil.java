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
;

public class TimeUtil {

	static TimeZone GMT_TIMEZONE = TimeZone.getTimeZone("GMT");

	static HashMap _timezoneMappings;

	static {
		_timezoneMappings = new HashMap(175);

		//
		// Windows Mappings
		//

		_timezoneMappings.put("Romance", "Europe/Paris");
		_timezoneMappings.put("Romance Standard Time", "Europe/Paris");
		_timezoneMappings.put("Warsaw", "Europe/Warsaw");
		_timezoneMappings.put("Central Europe", "Europe/Prague");
		_timezoneMappings.put("Central Europe Standard Time", "Europe/Prague");
		_timezoneMappings.put("Prague Bratislava", "Europe/Prague");
		_timezoneMappings.put(
			"W. Central Africa Standard Time",
			"Africa/Luanda");
		_timezoneMappings.put("FLE", "Europe/Helsinki");
		_timezoneMappings.put("FLE Standard Time", "Europe/Helsinki");
		_timezoneMappings.put("GFT", "Europe/Athens");
		_timezoneMappings.put("GFT Standard Time", "Europe/Athens");
		_timezoneMappings.put("GTB", "Europe/Athens");
		_timezoneMappings.put("GTB Standard Time", "Europe/Athens");
		_timezoneMappings.put("Israel", "Asia/Jerusalem");
		_timezoneMappings.put("Israel Standard Time", "Asia/Jerusalem");
		_timezoneMappings.put("Arab", "Asia/Riyadh");
		_timezoneMappings.put("Arab Standard Time", "Asia/Riyadh");
		_timezoneMappings.put("Arabic Standard Time", "Asia/Baghdad");
		_timezoneMappings.put("E. Africa", "Africa/Nairobi");
		_timezoneMappings.put("E. Africa Standard Time", "Africa/Nairobi");
		_timezoneMappings.put("Saudi Arabia", "Asia/Riyadh");
		_timezoneMappings.put("Saudi Arabia Standard Time", "Asia/Riyadh");
		_timezoneMappings.put("Iran", "Asia/Tehran");
		_timezoneMappings.put("Iran Standard Time", "Asia/Tehran");
		_timezoneMappings.put("Afghanistan", "Asia/Kabul");
		_timezoneMappings.put("Afghanistan Standard Time", "Asia/Kabul");
		_timezoneMappings.put("India", "Asia/Calcutta");
		_timezoneMappings.put("India Standard Time", "Asia/Calcutta");
		_timezoneMappings.put("Myanmar Standard Time", "Asia/Rangoon");
		_timezoneMappings.put("Nepal Standard Time", "Asia/Katmandu");
		_timezoneMappings.put("Sri Lanka", "Asia/Colombo");
		_timezoneMappings.put("Sri Lanka Standard Time", "Asia/Colombo");
		_timezoneMappings.put("Beijing", "Asia/Shanghai");
		_timezoneMappings.put("China", "Asia/Shanghai");
		_timezoneMappings.put("China Standard Time", "Asia/Shanghai");
		_timezoneMappings.put("AUS Central", "Australia/Darwin");
		_timezoneMappings.put("AUS Central Standard Time", "Australia/Darwin");
		_timezoneMappings.put("Cen. Australia", "Australia/Adelaide");
		_timezoneMappings.put(
			"Cen. Australia Standard Time",
			"Australia/Adelaide");
		_timezoneMappings.put("Vladivostok", "Asia/Vladivostok");
		_timezoneMappings.put("Vladivostok Standard Time", "Asia/Vladivostok");
		_timezoneMappings.put("West Pacific", "Pacific/Guam");
		_timezoneMappings.put("West Pacific Standard Time", "Pacific/Guam");
		_timezoneMappings.put("E. South America", "America/Sao_Paulo");
		_timezoneMappings.put(
			"E. South America Standard Time",
			"America/Sao_Paulo");
		_timezoneMappings.put("Greenland Standard Time", "America/Godthab");
		_timezoneMappings.put("Newfoundland", "America/St_Johns");
		_timezoneMappings.put("Newfoundland Standard Time", "America/St_Johns");
		_timezoneMappings.put("Pacific SA", "America/Caracas");
		_timezoneMappings.put("Pacific SA Standard Time", "America/Caracas");
		_timezoneMappings.put("SA Western", "America/Caracas");
		_timezoneMappings.put("SA Western Standard Time", "America/Caracas");
		_timezoneMappings.put("SA Pacific", "America/Bogota");
		_timezoneMappings.put("SA Pacific Standard Time", "America/Bogota");
		_timezoneMappings.put("US Eastern", "America/Indianapolis");
		_timezoneMappings.put(
			"US Eastern Standard Time",
			"America/Indianapolis");
		_timezoneMappings.put(
			"Central America Standard Time",
			"America/Regina");
		_timezoneMappings.put("Mexico", "America/Mexico_City");
		_timezoneMappings.put("Mexico Standard Time", "America/Mexico_City");
		_timezoneMappings.put("Canada Central", "America/Regina");
		_timezoneMappings.put("Canada Central Standard Time", "America/Regina");
		_timezoneMappings.put("US Mountain", "America/Phoenix");
		_timezoneMappings.put("US Mountain Standard Time", "America/Phoenix");
		_timezoneMappings.put("GMT", "Europe/London");
		_timezoneMappings.put("GMT Standard Time", "Europe/London");
		_timezoneMappings.put("Ekaterinburg", "Asia/Yekaterinburg");
		_timezoneMappings.put(
			"Ekaterinburg Standard Time",
			"Asia/Yekaterinburg");
		_timezoneMappings.put("West Asia", "Asia/Karachi");
		_timezoneMappings.put("West Asia Standard Time", "Asia/Karachi");
		_timezoneMappings.put("Central Asia", "Asia/Dhaka");
		_timezoneMappings.put("Central Asia Standard Time", "Asia/Dhaka");
		_timezoneMappings.put(
			"N. Central Asia Standard Time",
			"Asia/Novosibirsk");
		_timezoneMappings.put("Bangkok", "Asia/Bangkok");
		_timezoneMappings.put("Bangkok Standard Time", "Asia/Bangkok");
		_timezoneMappings.put("North Asia Standard Time", "Asia/Krasnoyarsk");
		_timezoneMappings.put("SE Asia", "Asia/Bangkok");
		_timezoneMappings.put("SE Asia Standard Time", "Asia/Bangkok");
		_timezoneMappings.put(
			"North Asia East Standard Time",
			"Asia/Ulaanbaatar");
		_timezoneMappings.put("Singapore", "Asia/Singapore");
		_timezoneMappings.put("Singapore Standard Time", "Asia/Singapore");
		_timezoneMappings.put("Taipei", "Asia/Taipei");
		_timezoneMappings.put("Taipei Standard Time", "Asia/Taipei");
		_timezoneMappings.put("W. Australia", "Australia/Perth");
		_timezoneMappings.put("W. Australia Standard Time", "Australia/Perth");
		_timezoneMappings.put("Korea", "Asia/Seoul");
		_timezoneMappings.put("Korea Standard Time", "Asia/Seoul");
		_timezoneMappings.put("Tokyo", "Asia/Tokyo");
		_timezoneMappings.put("Tokyo Standard Time", "Asia/Tokyo");
		_timezoneMappings.put("Yakutsk", "Asia/Yakutsk");
		_timezoneMappings.put("Yakutsk Standard Time", "Asia/Yakutsk");
		_timezoneMappings.put("Central European", "Europe/Belgrade");
		_timezoneMappings.put(
			"Central European Standard Time",
			"Europe/Belgrade");
		_timezoneMappings.put("W. Europe", "Europe/Berlin");
		_timezoneMappings.put("W. Europe Standard Time", "Europe/Berlin");
		_timezoneMappings.put("Tasmania", "Australia/Hobart");
		_timezoneMappings.put("Tasmania Standard Time", "Australia/Hobart");
		_timezoneMappings.put("AUS Eastern", "Australia/Sydney");
		_timezoneMappings.put("AUS Eastern Standard Time", "Australia/Sydney");
		_timezoneMappings.put("E. Australia", "Australia/Brisbane");
		_timezoneMappings.put(
			"E. Australia Standard Time",
			"Australia/Brisbane");
		_timezoneMappings.put("Sydney Standard Time", "Australia/Sydney");
		_timezoneMappings.put("Central Pacific", "Pacific/Guadalcanal");
		_timezoneMappings.put(
			"Central Pacific Standard Time",
			"Pacific/Guadalcanal");
		_timezoneMappings.put("Dateline", "Pacific/Majuro");
		_timezoneMappings.put("Dateline Standard Time", "Pacific/Majuro");
		_timezoneMappings.put("Fiji", "Pacific/Fiji");
		_timezoneMappings.put("Fiji Standard Time", "Pacific/Fiji");
		_timezoneMappings.put("Samoa", "Pacific/Apia");
		_timezoneMappings.put("Samoa Standard Time", "Pacific/Apia");
		_timezoneMappings.put("Hawaiian", "Pacific/Honolulu");
		_timezoneMappings.put("Hawaiian Standard Time", "Pacific/Honolulu");
		_timezoneMappings.put("Alaskan", "America/Anchorage");
		_timezoneMappings.put("Alaskan Standard Time", "America/Anchorage");
		_timezoneMappings.put("Pacific", "America/Los_Angeles");
		_timezoneMappings.put("Pacific Standard Time", "America/Los_Angeles");
		_timezoneMappings.put("Mexico Standard Time 2", "America/Chihuahua");
		_timezoneMappings.put("Mountain", "America/Denver");
		_timezoneMappings.put("Mountain Standard Time", "America/Denver");
		_timezoneMappings.put("Central", "America/Chicago");
		_timezoneMappings.put("Central Standard Time", "America/Chicago");
		_timezoneMappings.put("Eastern", "America/New_York");
		_timezoneMappings.put("Eastern Standard Time", "America/New_York");
		_timezoneMappings.put("E. Europe", "Europe/Bucharest");
		_timezoneMappings.put("E. Europe Standard Time", "Europe/Bucharest");
		_timezoneMappings.put("Egypt", "Africa/Cairo");
		_timezoneMappings.put("Egypt Standard Time", "Africa/Cairo");
		_timezoneMappings.put("South Africa", "Africa/Harare");
		_timezoneMappings.put("South Africa Standard Time", "Africa/Harare");
		_timezoneMappings.put("Atlantic", "America/Halifax");
		_timezoneMappings.put("Atlantic Standard Time", "America/Halifax");
		_timezoneMappings.put("SA Eastern", "America/Buenos_Aires");
		_timezoneMappings.put(
			"SA Eastern Standard Time",
			"America/Buenos_Aires");
		_timezoneMappings.put("Mid-Atlantic", "Atlantic/South_Georgia");
		_timezoneMappings.put(
			"Mid-Atlantic Standard Time",
			"Atlantic/South_Georgia");
		_timezoneMappings.put("Azores", "Atlantic/Azores");
		_timezoneMappings.put("Azores Standard Time", "Atlantic/Azores");
		_timezoneMappings.put(
			"Cape Verde Standard Time",
			"Atlantic/Cape_Verde");
		_timezoneMappings.put("Russian", "Europe/Moscow");
		_timezoneMappings.put("Russian Standard Time", "Europe/Moscow");
		_timezoneMappings.put("New Zealand", "Pacific/Auckland");
		_timezoneMappings.put("New Zealand Standard Time", "Pacific/Auckland");
		_timezoneMappings.put("Tonga Standard Time", "Pacific/Tongatapu");
		_timezoneMappings.put("Arabian", "Asia/Muscat");
		_timezoneMappings.put("Arabian Standard Time", "Asia/Muscat");
		_timezoneMappings.put("Caucasus", "Asia/Tbilisi");
		_timezoneMappings.put("Caucasus Standard Time", "Asia/Tbilisi");
		_timezoneMappings.put("GMT Standard Time", "GMT");
		_timezoneMappings.put("Greenwich", "GMT");
		_timezoneMappings.put("Greenwich Standard Time", "GMT");
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

		return (String) _timezoneMappings.get(timezoneStr);
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
