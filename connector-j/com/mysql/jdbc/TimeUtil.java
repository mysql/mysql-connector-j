/*
   Copyright (C) 2002 MySQL AB
   
      This program is free software; you can redistribute it and/or modify
      it under the terms of the GNU General Public License as published by
      the Free Software Foundation; either version 2 of the License, or
      (at your option) any later version.
   
      This program is distributed in the hope that it will be useful,
      but WITHOUT ANY WARRANTY; without even the implied warranty of
      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
      GNU General Public License for more details.
   
      You should have received a copy of the GNU General Public License
      along with this program; if not, write to the Free Software
      Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
      
 */
package com.mysql.jdbc;

import java.sql.Time;
import java.sql.Timestamp;

import java.util.Calendar;
import java.util.Collections;

import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;


/**
 * Timezone conversion routines
 * 
 * @author Mark Matthews
 */
public class TimeUtil {

    //~ Instance/static variables .............................................

    static TimeZone GMT_TIMEZONE = TimeZone.getTimeZone("GMT");
    static final Map TIMEZONE_MAPPINGS;

    //~ Initializers ..........................................................

    static {
        HashMap tempMap = new HashMap();
        
        //
        // Windows Mappings
        //
        tempMap.put("Romance", "Europe/Paris");
        tempMap.put("Romance Standard Time", "Europe/Paris");
        tempMap.put("Warsaw", "Europe/Warsaw");
        tempMap.put("Central Europe", "Europe/Prague");
        tempMap.put("Central Europe Standard Time", "Europe/Prague");
        tempMap.put("Prague Bratislava", "Europe/Prague");
        tempMap.put("W. Central Africa Standard Time", 
                              "Africa/Luanda");
        tempMap.put("FLE", "Europe/Helsinki");
        tempMap.put("FLE Standard Time", "Europe/Helsinki");
        tempMap.put("GFT", "Europe/Athens");
        tempMap.put("GFT Standard Time", "Europe/Athens");
        tempMap.put("GTB", "Europe/Athens");
        tempMap.put("GTB Standard Time", "Europe/Athens");
        tempMap.put("Israel", "Asia/Jerusalem");
        tempMap.put("Israel Standard Time", "Asia/Jerusalem");
        tempMap.put("Arab", "Asia/Riyadh");
        tempMap.put("Arab Standard Time", "Asia/Riyadh");
        tempMap.put("Arabic Standard Time", "Asia/Baghdad");
        tempMap.put("E. Africa", "Africa/Nairobi");
        tempMap.put("E. Africa Standard Time", "Africa/Nairobi");
        tempMap.put("Saudi Arabia", "Asia/Riyadh");
        tempMap.put("Saudi Arabia Standard Time", "Asia/Riyadh");
        tempMap.put("Iran", "Asia/Tehran");
        tempMap.put("Iran Standard Time", "Asia/Tehran");
        tempMap.put("Afghanistan", "Asia/Kabul");
        tempMap.put("Afghanistan Standard Time", "Asia/Kabul");
        tempMap.put("India", "Asia/Calcutta");
        tempMap.put("India Standard Time", "Asia/Calcutta");
        tempMap.put("Myanmar Standard Time", "Asia/Rangoon");
        tempMap.put("Nepal Standard Time", "Asia/Katmandu");
        tempMap.put("Sri Lanka", "Asia/Colombo");
        tempMap.put("Sri Lanka Standard Time", "Asia/Colombo");
        tempMap.put("Beijing", "Asia/Shanghai");
        tempMap.put("China", "Asia/Shanghai");
        tempMap.put("China Standard Time", "Asia/Shanghai");
        tempMap.put("AUS Central", "Australia/Darwin");
        tempMap.put("AUS Central Standard Time", "Australia/Darwin");
        tempMap.put("Cen. Australia", "Australia/Adelaide");
        tempMap.put("Cen. Australia Standard Time", 
                              "Australia/Adelaide");
        tempMap.put("Vladivostok", "Asia/Vladivostok");
        tempMap.put("Vladivostok Standard Time", "Asia/Vladivostok");
        tempMap.put("West Pacific", "Pacific/Guam");
        tempMap.put("West Pacific Standard Time", "Pacific/Guam");
        tempMap.put("E. South America", "America/Sao_Paulo");
        tempMap.put("E. South America Standard Time", 
                              "America/Sao_Paulo");
        tempMap.put("Greenland Standard Time", "America/Godthab");
        tempMap.put("Newfoundland", "America/St_Johns");
        tempMap.put("Newfoundland Standard Time", "America/St_Johns");
        tempMap.put("Pacific SA", "America/Caracas");
        tempMap.put("Pacific SA Standard Time", "America/Caracas");
        tempMap.put("SA Western", "America/Caracas");
        tempMap.put("SA Western Standard Time", "America/Caracas");
        tempMap.put("SA Pacific", "America/Bogota");
        tempMap.put("SA Pacific Standard Time", "America/Bogota");
        tempMap.put("US Eastern", "America/Indianapolis");
        tempMap.put("US Eastern Standard Time", 
                              "America/Indianapolis");
        tempMap.put("Central America Standard Time", 
                              "America/Regina");
        tempMap.put("Mexico", "America/Mexico_City");
        tempMap.put("Mexico Standard Time", "America/Mexico_City");
        tempMap.put("Canada Central", "America/Regina");
        tempMap.put("Canada Central Standard Time", "America/Regina");
        tempMap.put("US Mountain", "America/Phoenix");
        tempMap.put("US Mountain Standard Time", "America/Phoenix");
        tempMap.put("GMT", "Europe/London");
        tempMap.put("GMT Standard Time", "Europe/London");
        tempMap.put("Ekaterinburg", "Asia/Yekaterinburg");
        tempMap.put("Ekaterinburg Standard Time", 
                              "Asia/Yekaterinburg");
        tempMap.put("West Asia", "Asia/Karachi");
        tempMap.put("West Asia Standard Time", "Asia/Karachi");
        tempMap.put("Central Asia", "Asia/Dhaka");
        tempMap.put("Central Asia Standard Time", "Asia/Dhaka");
        tempMap.put("N. Central Asia Standard Time", 
                              "Asia/Novosibirsk");
        tempMap.put("Bangkok", "Asia/Bangkok");
        tempMap.put("Bangkok Standard Time", "Asia/Bangkok");
        tempMap.put("North Asia Standard Time", "Asia/Krasnoyarsk");
        tempMap.put("SE Asia", "Asia/Bangkok");
        tempMap.put("SE Asia Standard Time", "Asia/Bangkok");
        tempMap.put("North Asia East Standard Time", 
                              "Asia/Ulaanbaatar");
        tempMap.put("Singapore", "Asia/Singapore");
        tempMap.put("Singapore Standard Time", "Asia/Singapore");
        tempMap.put("Taipei", "Asia/Taipei");
        tempMap.put("Taipei Standard Time", "Asia/Taipei");
        tempMap.put("W. Australia", "Australia/Perth");
        tempMap.put("W. Australia Standard Time", "Australia/Perth");
        tempMap.put("Korea", "Asia/Seoul");
        tempMap.put("Korea Standard Time", "Asia/Seoul");
        tempMap.put("Tokyo", "Asia/Tokyo");
        tempMap.put("Tokyo Standard Time", "Asia/Tokyo");
        tempMap.put("Yakutsk", "Asia/Yakutsk");
        tempMap.put("Yakutsk Standard Time", "Asia/Yakutsk");
        tempMap.put("Central European", "Europe/Belgrade");
        tempMap.put("Central European Standard Time", 
                              "Europe/Belgrade");
        tempMap.put("W. Europe", "Europe/Berlin");
        tempMap.put("W. Europe Standard Time", "Europe/Berlin");
        tempMap.put("Tasmania", "Australia/Hobart");
        tempMap.put("Tasmania Standard Time", "Australia/Hobart");
        tempMap.put("AUS Eastern", "Australia/Sydney");
        tempMap.put("AUS Eastern Standard Time", "Australia/Sydney");
        tempMap.put("E. Australia", "Australia/Brisbane");
        tempMap.put("E. Australia Standard Time", 
                              "Australia/Brisbane");
        tempMap.put("Sydney Standard Time", "Australia/Sydney");
        tempMap.put("Central Pacific", "Pacific/Guadalcanal");
        tempMap.put("Central Pacific Standard Time", 
                              "Pacific/Guadalcanal");
        tempMap.put("Dateline", "Pacific/Majuro");
        tempMap.put("Dateline Standard Time", "Pacific/Majuro");
        tempMap.put("Fiji", "Pacific/Fiji");
        tempMap.put("Fiji Standard Time", "Pacific/Fiji");
        tempMap.put("Samoa", "Pacific/Apia");
        tempMap.put("Samoa Standard Time", "Pacific/Apia");
        tempMap.put("Hawaiian", "Pacific/Honolulu");
        tempMap.put("Hawaiian Standard Time", "Pacific/Honolulu");
        tempMap.put("Alaskan", "America/Anchorage");
        tempMap.put("Alaskan Standard Time", "America/Anchorage");
        tempMap.put("Pacific", "America/Los_Angeles");
        tempMap.put("Pacific Standard Time", "America/Los_Angeles");
        tempMap.put("Mexico Standard Time 2", "America/Chihuahua");
        tempMap.put("Mountain", "America/Denver");
        tempMap.put("Mountain Standard Time", "America/Denver");
        tempMap.put("Central", "America/Chicago");
        tempMap.put("Central Standard Time", "America/Chicago");
        tempMap.put("Eastern", "America/New_York");
        tempMap.put("Eastern Standard Time", "America/New_York");
        tempMap.put("E. Europe", "Europe/Bucharest");
        tempMap.put("E. Europe Standard Time", "Europe/Bucharest");
        tempMap.put("Egypt", "Africa/Cairo");
        tempMap.put("Egypt Standard Time", "Africa/Cairo");
        tempMap.put("South Africa", "Africa/Harare");
        tempMap.put("South Africa Standard Time", "Africa/Harare");
        tempMap.put("Atlantic", "America/Halifax");
        tempMap.put("Atlantic Standard Time", "America/Halifax");
        tempMap.put("SA Eastern", "America/Buenos_Aires");
        tempMap.put("SA Eastern Standard Time", 
                              "America/Buenos_Aires");
        tempMap.put("Mid-Atlantic", "Atlantic/South_Georgia");
        tempMap.put("Mid-Atlantic Standard Time", 
                              "Atlantic/South_Georgia");
        tempMap.put("Azores", "Atlantic/Azores");
        tempMap.put("Azores Standard Time", "Atlantic/Azores");
        tempMap.put("Cape Verde Standard Time", 
                              "Atlantic/Cape_Verde");
        tempMap.put("Russian", "Europe/Moscow");
        tempMap.put("Russian Standard Time", "Europe/Moscow");
        tempMap.put("New Zealand", "Pacific/Auckland");
        tempMap.put("New Zealand Standard Time", "Pacific/Auckland");
        tempMap.put("Tonga Standard Time", "Pacific/Tongatapu");
        tempMap.put("Arabian", "Asia/Muscat");
        tempMap.put("Arabian Standard Time", "Asia/Muscat");
        tempMap.put("Caucasus", "Asia/Tbilisi");
        tempMap.put("Caucasus Standard Time", "Asia/Tbilisi");
        tempMap.put("GMT Standard Time", "GMT");
        tempMap.put("Greenwich", "GMT");
        tempMap.put("Greenwich Standard Time", "GMT");
        
        TIMEZONE_MAPPINGS = Collections.unmodifiableMap(tempMap);
    }

    //~ Methods ...............................................................

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
            timezoneBuf.append(timezoneStr.substring(
                                       daylightIndex + "DAYLIGHT".length(), 
                                       timezoneStr.length()));
            timezoneStr = timezoneBuf.toString();
        }

        return (String) TIMEZONE_MAPPINGS.get(timezoneStr);
    }

    public static Timestamp changeTimezone(Connection conn, Timestamp tstamp, 
                                           TimeZone fromTz, TimeZone toTz) {

        if (conn != null && conn.useTimezone()) {

            // Convert the timestamp from GMT to the server's timezone
            Calendar fromCal = Calendar.getInstance(fromTz);
            fromCal.setTime(tstamp);

            int fromOffset = fromCal.get(Calendar.ZONE_OFFSET)
                             + fromCal.get(Calendar.DST_OFFSET);
            Calendar toCal = Calendar.getInstance(toTz);
            toCal.setTime(tstamp);

            int toOffset = toCal.get(Calendar.ZONE_OFFSET)
                           + toCal.get(Calendar.DST_OFFSET);
            int offsetDiff = toOffset - fromOffset;
            long toTime = toCal.getTime().getTime();
            toTime += offsetDiff;

            Timestamp changedTimestamp = new Timestamp(toTime);

            return changedTimestamp;
        } else {

            return tstamp;
        }
    }

    public static Time changeTimezone(Connection conn, Time t, TimeZone fromTz, 
                                      TimeZone toTz) {

        if (conn != null && conn.useTimezone()) {

            // Convert the timestamp from GMT to the server's timezone
            Calendar fromCal = Calendar.getInstance(fromTz);
            fromCal.setTime(t);

            int fromOffset = fromCal.get(Calendar.ZONE_OFFSET)
                             + fromCal.get(Calendar.DST_OFFSET);
            Calendar toCal = Calendar.getInstance(toTz);
            toCal.setTime(t);

            int toOffset = toCal.get(Calendar.ZONE_OFFSET)
                           + toCal.get(Calendar.DST_OFFSET);
            int offsetDiff = toOffset - fromOffset;
            long toTime = toCal.getTime().getTime();
            toTime += offsetDiff;

            Time changedTime = new Time(toTime);

            return changedTime;
        } else {

            return t;
        }
    }
}