/*
  Copyright (c) 2002, 2014, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.jdbc;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TimeZone;

/**
 * Timezone conversion routines
 * 
 * @author Mark Matthews
 */
public class TimeUtil {
	static final Map<String, String[]> ABBREVIATED_TIMEZONES;

	static final TimeZone GMT_TIMEZONE = TimeZone.getTimeZone("GMT");

	static final Map<String, String> TIMEZONE_MAPPINGS;

	static {
		HashMap<String, String> tempTzMap = new HashMap<String, String>();

		//
		// Windows Mappings
		//
		tempTzMap.put("Romance", "Europe/Paris");
		tempTzMap.put("Romance Standard Time", "Europe/Paris");
		tempTzMap.put("Warsaw", "Europe/Warsaw");
		tempTzMap.put("Central Europe", "Europe/Prague");
		tempTzMap.put("Central Europe Standard Time", "Europe/Prague");
		tempTzMap.put("Prague Bratislava", "Europe/Prague");
		tempTzMap.put("W. Central Africa Standard Time", "Africa/Luanda");
		tempTzMap.put("FLE", "Europe/Helsinki");
		tempTzMap.put("FLE Standard Time", "Europe/Helsinki");
		tempTzMap.put("GFT", "Europe/Athens");
		tempTzMap.put("GFT Standard Time", "Europe/Athens");
		tempTzMap.put("GTB", "Europe/Athens");
		tempTzMap.put("GTB Standard Time", "Europe/Athens");
		tempTzMap.put("Israel", "Asia/Jerusalem");
		tempTzMap.put("Israel Standard Time", "Asia/Jerusalem");
		tempTzMap.put("Arab", "Asia/Riyadh");
		tempTzMap.put("Arab Standard Time", "Asia/Riyadh");
		tempTzMap.put("Arabic Standard Time", "Asia/Baghdad");
		tempTzMap.put("E. Africa", "Africa/Nairobi");
		tempTzMap.put("E. Africa Standard Time", "Africa/Nairobi");
		tempTzMap.put("Saudi Arabia", "Asia/Riyadh");
		tempTzMap.put("Saudi Arabia Standard Time", "Asia/Riyadh");
		tempTzMap.put("Iran", "Asia/Tehran");
		tempTzMap.put("Iran Standard Time", "Asia/Tehran");
		tempTzMap.put("Afghanistan", "Asia/Kabul");
		tempTzMap.put("Afghanistan Standard Time", "Asia/Kabul");
		tempTzMap.put("India", "Asia/Calcutta");
		tempTzMap.put("India Standard Time", "Asia/Calcutta");
		tempTzMap.put("Myanmar Standard Time", "Asia/Rangoon");
		tempTzMap.put("Nepal Standard Time", "Asia/Katmandu");
		tempTzMap.put("Sri Lanka", "Asia/Colombo");
		tempTzMap.put("Sri Lanka Standard Time", "Asia/Colombo");
		tempTzMap.put("Beijing", "Asia/Shanghai");
		tempTzMap.put("China", "Asia/Shanghai");
		tempTzMap.put("China Standard Time", "Asia/Shanghai");
		tempTzMap.put("AUS Central", "Australia/Darwin");
		tempTzMap.put("AUS Central Standard Time", "Australia/Darwin");
		tempTzMap.put("Cen. Australia", "Australia/Adelaide");
		tempTzMap.put("Cen. Australia Standard Time", "Australia/Adelaide");
		tempTzMap.put("Vladivostok", "Asia/Vladivostok");
		tempTzMap.put("Vladivostok Standard Time", "Asia/Vladivostok");
		tempTzMap.put("West Pacific", "Pacific/Guam");
		tempTzMap.put("West Pacific Standard Time", "Pacific/Guam");
		tempTzMap.put("E. South America", "America/Sao_Paulo");
		tempTzMap.put("E. South America Standard Time", "America/Sao_Paulo");
		tempTzMap.put("Greenland Standard Time", "America/Godthab");
		tempTzMap.put("Newfoundland", "America/St_Johns");
		tempTzMap.put("Newfoundland Standard Time", "America/St_Johns");
		tempTzMap.put("Pacific SA", "America/Caracas");
		tempTzMap.put("Pacific SA Standard Time", "America/Caracas");
		tempTzMap.put("SA Western", "America/Caracas");
		tempTzMap.put("SA Western Standard Time", "America/Caracas");
		tempTzMap.put("SA Pacific", "America/Bogota");
		tempTzMap.put("SA Pacific Standard Time", "America/Bogota");
		tempTzMap.put("US Eastern", "America/Indianapolis");
		tempTzMap.put("US Eastern Standard Time", "America/Indianapolis");
		tempTzMap.put("Central America Standard Time", "America/Regina");
		tempTzMap.put("Mexico", "America/Mexico_City");
		tempTzMap.put("Mexico Standard Time", "America/Mexico_City");
		tempTzMap.put("Canada Central", "America/Regina");
		tempTzMap.put("Canada Central Standard Time", "America/Regina");
		tempTzMap.put("US Mountain", "America/Phoenix");
		tempTzMap.put("US Mountain Standard Time", "America/Phoenix");
		tempTzMap.put("GMT", "GMT");
		tempTzMap.put("Ekaterinburg", "Asia/Yekaterinburg");
		tempTzMap.put("Ekaterinburg Standard Time", "Asia/Yekaterinburg");
		tempTzMap.put("West Asia", "Asia/Karachi");
		tempTzMap.put("West Asia Standard Time", "Asia/Karachi");
		tempTzMap.put("Central Asia", "Asia/Dhaka");
		tempTzMap.put("Central Asia Standard Time", "Asia/Dhaka");
		tempTzMap.put("N. Central Asia Standard Time", "Asia/Novosibirsk");
		tempTzMap.put("Bangkok", "Asia/Bangkok");
		tempTzMap.put("Bangkok Standard Time", "Asia/Bangkok");
		tempTzMap.put("North Asia Standard Time", "Asia/Krasnoyarsk");
		tempTzMap.put("SE Asia", "Asia/Bangkok");
		tempTzMap.put("SE Asia Standard Time", "Asia/Bangkok");
		tempTzMap.put("North Asia East Standard Time", "Asia/Ulaanbaatar");
		tempTzMap.put("Singapore", "Asia/Singapore");
		tempTzMap.put("Singapore Standard Time", "Asia/Singapore");
		tempTzMap.put("Taipei", "Asia/Taipei");
		tempTzMap.put("Taipei Standard Time", "Asia/Taipei");
		tempTzMap.put("W. Australia", "Australia/Perth");
		tempTzMap.put("W. Australia Standard Time", "Australia/Perth");
		tempTzMap.put("Korea", "Asia/Seoul");
		tempTzMap.put("Korea Standard Time", "Asia/Seoul");
		tempTzMap.put("Tokyo", "Asia/Tokyo");
		tempTzMap.put("Tokyo Standard Time", "Asia/Tokyo");
		tempTzMap.put("Yakutsk", "Asia/Yakutsk");
		tempTzMap.put("Yakutsk Standard Time", "Asia/Yakutsk");
		tempTzMap.put("Central European", "Europe/Belgrade");
		tempTzMap.put("Central European Standard Time", "Europe/Belgrade");
		tempTzMap.put("W. Europe", "Europe/Berlin");
		tempTzMap.put("W. Europe Standard Time", "Europe/Berlin");
		tempTzMap.put("Tasmania", "Australia/Hobart");
		tempTzMap.put("Tasmania Standard Time", "Australia/Hobart");
		tempTzMap.put("AUS Eastern", "Australia/Sydney");
		tempTzMap.put("AUS Eastern Standard Time", "Australia/Sydney");
		tempTzMap.put("E. Australia", "Australia/Brisbane");
		tempTzMap.put("E. Australia Standard Time", "Australia/Brisbane");
		tempTzMap.put("Sydney Standard Time", "Australia/Sydney");
		tempTzMap.put("Central Pacific", "Pacific/Guadalcanal");
		tempTzMap.put("Central Pacific Standard Time", "Pacific/Guadalcanal");
		tempTzMap.put("Dateline", "Pacific/Majuro");
		tempTzMap.put("Dateline Standard Time", "Pacific/Majuro");
		tempTzMap.put("Fiji", "Pacific/Fiji");
		tempTzMap.put("Fiji Standard Time", "Pacific/Fiji");
		tempTzMap.put("Samoa", "Pacific/Apia");
		tempTzMap.put("Samoa Standard Time", "Pacific/Apia");
		tempTzMap.put("Hawaiian", "Pacific/Honolulu");
		tempTzMap.put("Hawaiian Standard Time", "Pacific/Honolulu");
		tempTzMap.put("Alaskan", "America/Anchorage");
		tempTzMap.put("Alaskan Standard Time", "America/Anchorage");
		tempTzMap.put("Pacific", "America/Los_Angeles");
		tempTzMap.put("Pacific Standard Time", "America/Los_Angeles");
		tempTzMap.put("Mexico Standard Time 2", "America/Chihuahua");
		tempTzMap.put("Mountain", "America/Denver");
		tempTzMap.put("Mountain Standard Time", "America/Denver");
		tempTzMap.put("Central", "America/Chicago");
		tempTzMap.put("Central Standard Time", "America/Chicago");
		tempTzMap.put("Eastern", "America/New_York");
		tempTzMap.put("Eastern Standard Time", "America/New_York");
		tempTzMap.put("E. Europe", "Europe/Bucharest");
		tempTzMap.put("E. Europe Standard Time", "Europe/Bucharest");
		tempTzMap.put("Egypt", "Africa/Cairo");
		tempTzMap.put("Egypt Standard Time", "Africa/Cairo");
		tempTzMap.put("South Africa", "Africa/Harare");
		tempTzMap.put("South Africa Standard Time", "Africa/Harare");
		tempTzMap.put("Atlantic", "America/Halifax");
		tempTzMap.put("Atlantic Standard Time", "America/Halifax");
		tempTzMap.put("SA Eastern", "America/Buenos_Aires");
		tempTzMap.put("SA Eastern Standard Time", "America/Buenos_Aires");
		tempTzMap.put("Mid-Atlantic", "Atlantic/South_Georgia");
		tempTzMap.put("Mid-Atlantic Standard Time", "Atlantic/South_Georgia");
		tempTzMap.put("Azores", "Atlantic/Azores");
		tempTzMap.put("Azores Standard Time", "Atlantic/Azores");
		tempTzMap.put("Cape Verde Standard Time", "Atlantic/Cape_Verde");
		tempTzMap.put("Russian", "Europe/Moscow");
		tempTzMap.put("Russian Standard Time", "Europe/Moscow");
		tempTzMap.put("New Zealand", "Pacific/Auckland");
		tempTzMap.put("New Zealand Standard Time", "Pacific/Auckland");
		tempTzMap.put("Tonga Standard Time", "Pacific/Tongatapu");
		tempTzMap.put("Arabian", "Asia/Muscat");
		tempTzMap.put("Arabian Standard Time", "Asia/Muscat");
		tempTzMap.put("Caucasus", "Asia/Tbilisi");
		tempTzMap.put("Caucasus Standard Time", "Asia/Tbilisi");
		tempTzMap.put("GMT Standard Time", "GMT");
		tempTzMap.put("Greenwich", "GMT");
		tempTzMap.put("Greenwich Standard Time", "GMT");
		tempTzMap.put("UTC", "GMT");

		// MySQL understands the Continent/City/region as well
		Iterator<Map.Entry<String, String>> entries = tempTzMap.entrySet().iterator();
		Map<String, String> entryMap = new HashMap<String, String>(tempTzMap.size()); // to avoid ConcurrentModificationException
		
		while (entries.hasNext()) {
			String name = entries.next().getValue();
			entryMap.put(name, name);
		}
		
		tempTzMap.putAll(entryMap);
		
		TIMEZONE_MAPPINGS = Collections.unmodifiableMap(tempTzMap);

		//
		// Handle abbreviated mappings
		//
		HashMap<String, String[]> tempAbbrMap = new HashMap<String, String[]>();

		tempAbbrMap.put("ACST", new String[] { "America/Porto_Acre" });
		tempAbbrMap.put("ACT", new String[] { "America/Porto_Acre" });
		tempAbbrMap.put("ADDT", new String[] { "America/Pangnirtung" });
		tempAbbrMap.put("ADMT", new String[] { "Africa/Asmera",
				"Africa/Addis_Ababa" });
		tempAbbrMap.put("ADT", new String[] { "Atlantic/Bermuda", "Asia/Baghdad",
				"America/Thule", "America/Goose_Bay", "America/Halifax",
				"America/Glace_Bay", "America/Pangnirtung", "America/Barbados",
				"America/Martinique" });
		tempAbbrMap.put("AFT", new String[] { "Asia/Kabul" });
		tempAbbrMap.put("AHDT", new String[] { "America/Anchorage" });
		tempAbbrMap.put("AHST", new String[] { "America/Anchorage" });
		tempAbbrMap.put("AHWT", new String[] { "America/Anchorage" });
		tempAbbrMap.put("AKDT", new String[] { "America/Juneau", "America/Yakutat",
				"America/Anchorage", "America/Nome" });
		tempAbbrMap.put("AKST", new String[] { "Asia/Aqtobe", "America/Juneau",
				"America/Yakutat", "America/Anchorage", "America/Nome" });
		tempAbbrMap.put("AKT", new String[] { "Asia/Aqtobe" });
		tempAbbrMap.put("AKTST", new String[] { "Asia/Aqtobe" });
		tempAbbrMap.put("AKWT", new String[] { "America/Juneau", "America/Yakutat",
				"America/Anchorage", "America/Nome" });
		tempAbbrMap.put("ALMST", new String[] { "Asia/Almaty" });
		tempAbbrMap.put("ALMT", new String[] { "Asia/Almaty" });
		tempAbbrMap.put("AMST", new String[] { "Asia/Yerevan", "America/Cuiaba",
				"America/Porto_Velho", "America/Boa_Vista", "America/Manaus" });
		tempAbbrMap.put("AMT", new String[] { "Europe/Athens", "Europe/Amsterdam",
				"Asia/Yerevan", "Africa/Asmera", "America/Cuiaba",
				"America/Porto_Velho", "America/Boa_Vista", "America/Manaus",
				"America/Asuncion" });
		tempAbbrMap.put("ANAMT", new String[] { "Asia/Anadyr" });
		tempAbbrMap.put("ANAST", new String[] { "Asia/Anadyr" });
		tempAbbrMap.put("ANAT", new String[] { "Asia/Anadyr" });
		tempAbbrMap.put("ANT", new String[] { "America/Aruba", "America/Curacao" });
		tempAbbrMap.put("AQTST", new String[] { "Asia/Aqtobe", "Asia/Aqtau" });
		tempAbbrMap.put("AQTT", new String[] { "Asia/Aqtobe", "Asia/Aqtau" });
		tempAbbrMap.put("ARST", new String[] { "Antarctica/Palmer",
				"America/Buenos_Aires", "America/Rosario", "America/Cordoba",
				"America/Jujuy", "America/Catamarca", "America/Mendoza" });
		tempAbbrMap.put("ART", new String[] { "Antarctica/Palmer",
				"America/Buenos_Aires", "America/Rosario", "America/Cordoba",
				"America/Jujuy", "America/Catamarca", "America/Mendoza" });
		tempAbbrMap.put("ASHST", new String[] { "Asia/Ashkhabad" });
		tempAbbrMap.put("ASHT", new String[] { "Asia/Ashkhabad" });
		tempAbbrMap.put("AST", new String[] { "Atlantic/Bermuda", "Asia/Bahrain",
				"Asia/Baghdad", "Asia/Kuwait", "Asia/Qatar", "Asia/Riyadh",
				"Asia/Aden", "America/Thule", "America/Goose_Bay",
				"America/Halifax", "America/Glace_Bay", "America/Pangnirtung",
				"America/Anguilla", "America/Antigua", "America/Barbados",
				"America/Dominica", "America/Santo_Domingo", "America/Grenada",
				"America/Guadeloupe", "America/Martinique",
				"America/Montserrat", "America/Puerto_Rico",
				"America/St_Kitts", "America/St_Lucia", "America/Miquelon",
				"America/St_Vincent", "America/Tortola", "America/St_Thomas",
				"America/Aruba", "America/Curacao", "America/Port_of_Spain" });
		tempAbbrMap.put("AWT", new String[] { "America/Puerto_Rico" });
		tempAbbrMap.put("AZOST", new String[] { "Atlantic/Azores" });
		tempAbbrMap.put("AZOT", new String[] { "Atlantic/Azores" });
		tempAbbrMap.put("AZST", new String[] { "Asia/Baku" });
		tempAbbrMap.put("AZT", new String[] { "Asia/Baku" });
		tempAbbrMap.put("BAKST", new String[] { "Asia/Baku" });
		tempAbbrMap.put("BAKT", new String[] { "Asia/Baku" });
		tempAbbrMap.put("BDT", new String[] { "Asia/Dacca", "America/Nome",
				"America/Adak" });
		tempAbbrMap.put("BEAT", new String[] { "Africa/Nairobi",
				"Africa/Mogadishu", "Africa/Kampala" });
		tempAbbrMap.put("BEAUT", new String[] { "Africa/Nairobi",
				"Africa/Dar_es_Salaam", "Africa/Kampala" });
		tempAbbrMap.put("BMT", new String[] { "Europe/Brussels", "Europe/Chisinau",
				"Europe/Tiraspol", "Europe/Bucharest", "Europe/Zurich",
				"Asia/Baghdad", "Asia/Bangkok", "Africa/Banjul",
				"America/Barbados", "America/Bogota" });
		tempAbbrMap.put("BNT", new String[] { "Asia/Brunei" });
		tempAbbrMap.put("BORT",
				new String[] { "Asia/Ujung_Pandang", "Asia/Kuching" });
		tempAbbrMap.put("BOST", new String[] { "America/La_Paz" });
		tempAbbrMap.put("BOT", new String[] { "America/La_Paz" });
		tempAbbrMap.put("BRST", new String[] { "America/Belem",
				"America/Fortaleza", "America/Araguaina", "America/Maceio",
				"America/Sao_Paulo" });
		tempAbbrMap.put("BRT", new String[] { "America/Belem", "America/Fortaleza",
				"America/Araguaina", "America/Maceio", "America/Sao_Paulo" });
		tempAbbrMap.put("BST", new String[] { "Europe/London", "Europe/Belfast",
				"Europe/Dublin", "Europe/Gibraltar", "Pacific/Pago_Pago",
				"Pacific/Midway", "America/Nome", "America/Adak" });
		tempAbbrMap.put("BTT", new String[] { "Asia/Thimbu" });
		tempAbbrMap.put("BURT", new String[] { "Asia/Dacca", "Asia/Rangoon",
				"Asia/Calcutta" });
		tempAbbrMap.put("BWT", new String[] { "America/Nome", "America/Adak" });
		tempAbbrMap.put("CANT", new String[] { "Atlantic/Canary" });
		tempAbbrMap.put("CAST",
				new String[] { "Africa/Gaborone", "Africa/Khartoum" });
		tempAbbrMap.put("CAT", new String[] { "Africa/Gaborone",
				"Africa/Bujumbura", "Africa/Lubumbashi", "Africa/Blantyre",
				"Africa/Maputo", "Africa/Windhoek", "Africa/Kigali",
				"Africa/Khartoum", "Africa/Lusaka", "Africa/Harare",
				"America/Anchorage" });
		tempAbbrMap.put("CCT", new String[] { "Indian/Cocos" });
		tempAbbrMap.put("CDDT", new String[] { "America/Rankin_Inlet" });
		tempAbbrMap.put("CDT", new String[] { "Asia/Harbin", "Asia/Shanghai",
				"Asia/Chungking", "Asia/Urumqi", "Asia/Kashgar", "Asia/Taipei",
				"Asia/Macao", "America/Chicago", "America/Indianapolis",
				"America/Indiana/Marengo", "America/Indiana/Knox",
				"America/Indiana/Vevay", "America/Louisville",
				"America/Menominee", "America/Rainy_River", "America/Winnipeg",
				"America/Pangnirtung", "America/Iqaluit",
				"America/Rankin_Inlet", "America/Cambridge_Bay",
				"America/Cancun", "America/Mexico_City", "America/Chihuahua",
				"America/Belize", "America/Costa_Rica", "America/Havana",
				"America/El_Salvador", "America/Guatemala",
				"America/Tegucigalpa", "America/Managua" });
		tempAbbrMap.put("CEST", new String[] { "Europe/Tirane", "Europe/Andorra",
				"Europe/Vienna", "Europe/Minsk", "Europe/Brussels",
				"Europe/Sofia", "Europe/Prague", "Europe/Copenhagen",
				"Europe/Tallinn", "Europe/Berlin", "Europe/Gibraltar",
				"Europe/Athens", "Europe/Budapest", "Europe/Rome",
				"Europe/Riga", "Europe/Vaduz", "Europe/Vilnius",
				"Europe/Luxembourg", "Europe/Malta", "Europe/Chisinau",
				"Europe/Tiraspol", "Europe/Monaco", "Europe/Amsterdam",
				"Europe/Oslo", "Europe/Warsaw", "Europe/Lisbon",
				"Europe/Kaliningrad", "Europe/Madrid", "Europe/Stockholm",
				"Europe/Zurich", "Europe/Kiev", "Europe/Uzhgorod",
				"Europe/Zaporozhye", "Europe/Simferopol", "Europe/Belgrade",
				"Africa/Algiers", "Africa/Tripoli", "Africa/Tunis",
				"Africa/Ceuta" });
		tempAbbrMap.put("CET", new String[] { "Europe/Tirane", "Europe/Andorra",
				"Europe/Vienna", "Europe/Minsk", "Europe/Brussels",
				"Europe/Sofia", "Europe/Prague", "Europe/Copenhagen",
				"Europe/Tallinn", "Europe/Berlin", "Europe/Gibraltar",
				"Europe/Athens", "Europe/Budapest", "Europe/Rome",
				"Europe/Riga", "Europe/Vaduz", "Europe/Vilnius",
				"Europe/Luxembourg", "Europe/Malta", "Europe/Chisinau",
				"Europe/Tiraspol", "Europe/Monaco", "Europe/Amsterdam",
				"Europe/Oslo", "Europe/Warsaw", "Europe/Lisbon",
				"Europe/Kaliningrad", "Europe/Madrid", "Europe/Stockholm",
				"Europe/Zurich", "Europe/Kiev", "Europe/Uzhgorod",
				"Europe/Zaporozhye", "Europe/Simferopol", "Europe/Belgrade",
				"Africa/Algiers", "Africa/Tripoli", "Africa/Casablanca",
				"Africa/Tunis", "Africa/Ceuta" });
		tempAbbrMap.put("CGST", new String[] { "America/Scoresbysund" });
		tempAbbrMap.put("CGT", new String[] { "America/Scoresbysund" });
		tempAbbrMap.put("CHDT", new String[] { "America/Belize" });
		tempAbbrMap.put("CHUT", new String[] { "Asia/Chungking" });
		tempAbbrMap.put("CJT", new String[] { "Asia/Tokyo" });
		tempAbbrMap.put("CKHST", new String[] { "Pacific/Rarotonga" });
		tempAbbrMap.put("CKT", new String[] { "Pacific/Rarotonga" });
		tempAbbrMap.put("CLST", new String[] { "Antarctica/Palmer",
				"America/Santiago" });
		tempAbbrMap.put("CLT", new String[] { "Antarctica/Palmer",
				"America/Santiago" });
		tempAbbrMap.put("CMT", new String[] { "Europe/Copenhagen",
				"Europe/Chisinau", "Europe/Tiraspol", "America/St_Lucia",
				"America/Buenos_Aires", "America/Rosario", "America/Cordoba",
				"America/Jujuy", "America/Catamarca", "America/Mendoza",
				"America/Caracas" });
		tempAbbrMap.put("COST", new String[] { "America/Bogota" });
		tempAbbrMap.put("COT", new String[] { "America/Bogota" });
		tempAbbrMap
				.put("CST", new String[] { "Asia/Harbin", "Asia/Shanghai",
						"Asia/Chungking", "Asia/Urumqi", "Asia/Kashgar",
						"Asia/Taipei", "Asia/Macao", "Asia/Jayapura",
						"Australia/Darwin", "Australia/Adelaide",
						"Australia/Broken_Hill", "America/Chicago",
						"America/Indianapolis", "America/Indiana/Marengo",
						"America/Indiana/Knox", "America/Indiana/Vevay",
						"America/Louisville", "America/Detroit",
						"America/Menominee", "America/Rainy_River",
						"America/Winnipeg", "America/Regina",
						"America/Swift_Current", "America/Pangnirtung",
						"America/Iqaluit", "America/Rankin_Inlet",
						"America/Cambridge_Bay", "America/Cancun",
						"America/Mexico_City", "America/Chihuahua",
						"America/Hermosillo", "America/Mazatlan",
						"America/Belize", "America/Costa_Rica",
						"America/Havana", "America/El_Salvador",
						"America/Guatemala", "America/Tegucigalpa",
						"America/Managua" });
		tempAbbrMap.put("CUT", new String[] { "Europe/Zaporozhye" });
		tempAbbrMap.put("CVST", new String[] { "Atlantic/Cape_Verde" });
		tempAbbrMap.put("CVT", new String[] { "Atlantic/Cape_Verde" });
		tempAbbrMap.put("CWT", new String[] { "America/Chicago",
				"America/Indianapolis", "America/Indiana/Marengo",
				"America/Indiana/Knox", "America/Indiana/Vevay",
				"America/Louisville", "America/Menominee" });
		tempAbbrMap.put("CXT", new String[] { "Indian/Christmas" });
		tempAbbrMap.put("DACT", new String[] { "Asia/Dacca" });
		tempAbbrMap.put("DAVT", new String[] { "Antarctica/Davis" });
		tempAbbrMap.put("DDUT", new String[] { "Antarctica/DumontDUrville" });
		tempAbbrMap.put("DFT", new String[] { "Europe/Oslo", "Europe/Paris" });
		tempAbbrMap.put("DMT", new String[] { "Europe/Belfast", "Europe/Dublin" });
		tempAbbrMap.put("DUSST", new String[] { "Asia/Dushanbe" });
		tempAbbrMap.put("DUST", new String[] { "Asia/Dushanbe" });
		tempAbbrMap.put("EASST", new String[] { "Pacific/Easter" });
		tempAbbrMap.put("EAST", new String[] { "Indian/Antananarivo",
				"Pacific/Easter" });
		tempAbbrMap.put("EAT", new String[] { "Indian/Comoro",
				"Indian/Antananarivo", "Indian/Mayotte", "Africa/Djibouti",
				"Africa/Asmera", "Africa/Addis_Ababa", "Africa/Nairobi",
				"Africa/Mogadishu", "Africa/Khartoum", "Africa/Dar_es_Salaam",
				"Africa/Kampala" });
		tempAbbrMap.put("ECT", new String[] { "Pacific/Galapagos",
				"America/Guayaquil" });
		tempAbbrMap.put("EDDT", new String[] { "America/Iqaluit" });
		tempAbbrMap.put("EDT", new String[] { "America/New_York",
				"America/Indianapolis", "America/Indiana/Marengo",
				"America/Indiana/Vevay", "America/Louisville",
				"America/Detroit", "America/Montreal", "America/Thunder_Bay",
				"America/Nipigon", "America/Pangnirtung", "America/Iqaluit",
				"America/Cancun", "America/Nassau", "America/Santo_Domingo",
				"America/Port-au-Prince", "America/Jamaica",
				"America/Grand_Turk" });
		tempAbbrMap.put("EEMT", new String[] { "Europe/Minsk", "Europe/Chisinau",
				"Europe/Tiraspol", "Europe/Kaliningrad", "Europe/Moscow" });
		tempAbbrMap.put("EEST", new String[] { "Europe/Minsk", "Europe/Sofia",
				"Europe/Tallinn", "Europe/Helsinki", "Europe/Athens",
				"Europe/Riga", "Europe/Vilnius", "Europe/Chisinau",
				"Europe/Tiraspol", "Europe/Warsaw", "Europe/Bucharest",
				"Europe/Kaliningrad", "Europe/Moscow", "Europe/Istanbul",
				"Europe/Kiev", "Europe/Uzhgorod", "Europe/Zaporozhye",
				"Asia/Nicosia", "Asia/Amman", "Asia/Beirut", "Asia/Gaza",
				"Asia/Damascus", "Africa/Cairo" });
		tempAbbrMap.put("EET", new String[] { "Europe/Minsk", "Europe/Sofia",
				"Europe/Tallinn", "Europe/Helsinki", "Europe/Athens",
				"Europe/Riga", "Europe/Vilnius", "Europe/Chisinau",
				"Europe/Tiraspol", "Europe/Warsaw", "Europe/Bucharest",
				"Europe/Kaliningrad", "Europe/Moscow", "Europe/Istanbul",
				"Europe/Kiev", "Europe/Uzhgorod", "Europe/Zaporozhye",
				"Europe/Simferopol", "Asia/Nicosia", "Asia/Amman",
				"Asia/Beirut", "Asia/Gaza", "Asia/Damascus", "Africa/Cairo",
				"Africa/Tripoli" });
		tempAbbrMap.put("EGST", new String[] { "America/Scoresbysund" });
		tempAbbrMap.put("EGT", new String[] { "Atlantic/Jan_Mayen",
				"America/Scoresbysund" });
		tempAbbrMap.put("EHDT", new String[] { "America/Santo_Domingo" });
		tempAbbrMap.put("EST", new String[] { "Australia/Brisbane",
				"Australia/Lindeman", "Australia/Hobart",
				"Australia/Melbourne", "Australia/Sydney",
				"Australia/Broken_Hill", "Australia/Lord_Howe",
				"America/New_York", "America/Chicago", "America/Indianapolis",
				"America/Indiana/Marengo", "America/Indiana/Knox",
				"America/Indiana/Vevay", "America/Louisville",
				"America/Detroit", "America/Menominee", "America/Montreal",
				"America/Thunder_Bay", "America/Nipigon",
				"America/Pangnirtung", "America/Iqaluit", "America/Cancun",
				"America/Antigua", "America/Nassau", "America/Cayman",
				"America/Santo_Domingo", "America/Port-au-Prince",
				"America/Jamaica", "America/Managua", "America/Panama",
				"America/Grand_Turk" });
		tempAbbrMap.put("EWT", new String[] { "America/New_York",
				"America/Indianapolis", "America/Indiana/Marengo",
				"America/Indiana/Vevay", "America/Louisville",
				"America/Detroit", "America/Jamaica" });
		tempAbbrMap.put("FFMT", new String[] { "America/Martinique" });
		tempAbbrMap.put("FJST", new String[] { "Pacific/Fiji" });
		tempAbbrMap.put("FJT", new String[] { "Pacific/Fiji" });
		tempAbbrMap.put("FKST", new String[] { "Atlantic/Stanley" });
		tempAbbrMap.put("FKT", new String[] { "Atlantic/Stanley" });
		tempAbbrMap.put("FMT",
				new String[] { "Atlantic/Madeira", "Africa/Freetown" });
		tempAbbrMap.put("FNST", new String[] { "America/Noronha" });
		tempAbbrMap.put("FNT", new String[] { "America/Noronha" });
		tempAbbrMap.put("FRUST", new String[] { "Asia/Bishkek" });
		tempAbbrMap.put("FRUT", new String[] { "Asia/Bishkek" });
		tempAbbrMap.put("GALT", new String[] { "Pacific/Galapagos" });
		tempAbbrMap.put("GAMT", new String[] { "Pacific/Gambier" });
		tempAbbrMap.put("GBGT", new String[] { "America/Guyana" });
		tempAbbrMap.put("GEST", new String[] { "Asia/Tbilisi" });
		tempAbbrMap.put("GET", new String[] { "Asia/Tbilisi" });
		tempAbbrMap.put("GFT", new String[] { "America/Cayenne" });
		tempAbbrMap.put("GHST", new String[] { "Africa/Accra" });
		tempAbbrMap.put("GILT", new String[] { "Pacific/Tarawa" });
		tempAbbrMap.put("GMT", new String[] { "Atlantic/St_Helena",
				"Atlantic/Reykjavik", "Europe/London", "Europe/Belfast",
				"Europe/Dublin", "Europe/Gibraltar", "Africa/Porto-Novo",
				"Africa/Ouagadougou", "Africa/Abidjan", "Africa/Malabo",
				"Africa/Banjul", "Africa/Accra", "Africa/Conakry",
				"Africa/Bissau", "Africa/Monrovia", "Africa/Bamako",
				"Africa/Timbuktu", "Africa/Nouakchott", "Africa/Niamey",
				"Africa/Sao_Tome", "Africa/Dakar", "Africa/Freetown",
				"Africa/Lome" });
		tempAbbrMap.put("GST", new String[] { "Atlantic/South_Georgia",
				"Asia/Bahrain", "Asia/Muscat", "Asia/Qatar", "Asia/Dubai",
				"Pacific/Guam" });
		tempAbbrMap.put("GYT", new String[] { "America/Guyana" });
		tempAbbrMap.put("HADT", new String[] { "America/Adak" });
		tempAbbrMap.put("HART", new String[] { "Asia/Harbin" });
		tempAbbrMap.put("HAST", new String[] { "America/Adak" });
		tempAbbrMap.put("HAWT", new String[] { "America/Adak" });
		tempAbbrMap.put("HDT", new String[] { "Pacific/Honolulu" });
		tempAbbrMap.put("HKST", new String[] { "Asia/Hong_Kong" });
		tempAbbrMap.put("HKT", new String[] { "Asia/Hong_Kong" });
		tempAbbrMap.put("HMT", new String[] { "Atlantic/Azores", "Europe/Helsinki",
				"Asia/Dacca", "Asia/Calcutta", "America/Havana" });
		tempAbbrMap.put("HOVST", new String[] { "Asia/Hovd" });
		tempAbbrMap.put("HOVT", new String[] { "Asia/Hovd" });
		tempAbbrMap.put("HST", new String[] { "Pacific/Johnston",
				"Pacific/Honolulu" });
		tempAbbrMap.put("HWT", new String[] { "Pacific/Honolulu" });
		tempAbbrMap.put("ICT", new String[] { "Asia/Phnom_Penh", "Asia/Vientiane",
				"Asia/Bangkok", "Asia/Saigon" });
		tempAbbrMap.put("IDDT", new String[] { "Asia/Jerusalem", "Asia/Gaza" });
		tempAbbrMap.put("IDT", new String[] { "Asia/Jerusalem", "Asia/Gaza" });
		tempAbbrMap.put("IHST", new String[] { "Asia/Colombo" });
		tempAbbrMap.put("IMT", new String[] { "Europe/Sofia", "Europe/Istanbul",
				"Asia/Irkutsk" });
		tempAbbrMap.put("IOT", new String[] { "Indian/Chagos" });
		tempAbbrMap.put("IRKMT", new String[] { "Asia/Irkutsk" });
		tempAbbrMap.put("IRKST", new String[] { "Asia/Irkutsk" });
		tempAbbrMap.put("IRKT", new String[] { "Asia/Irkutsk" });
		tempAbbrMap.put("IRST", new String[] { "Asia/Tehran" });
		tempAbbrMap.put("IRT", new String[] { "Asia/Tehran" });
		tempAbbrMap.put("ISST", new String[] { "Atlantic/Reykjavik" });
		tempAbbrMap.put("IST", new String[] { "Atlantic/Reykjavik",
				"Europe/Belfast", "Europe/Dublin", "Asia/Dacca", "Asia/Thimbu",
				"Asia/Calcutta", "Asia/Jerusalem", "Asia/Katmandu",
				"Asia/Karachi", "Asia/Gaza", "Asia/Colombo" });
		tempAbbrMap.put("JAYT", new String[] { "Asia/Jayapura" });
		tempAbbrMap.put("JMT", new String[] { "Atlantic/St_Helena",
				"Asia/Jerusalem" });
		tempAbbrMap.put("JST", new String[] { "Asia/Rangoon", "Asia/Dili",
				"Asia/Ujung_Pandang", "Asia/Tokyo", "Asia/Kuala_Lumpur",
				"Asia/Kuching", "Asia/Manila", "Asia/Singapore",
				"Pacific/Nauru" });
		tempAbbrMap.put("KART", new String[] { "Asia/Karachi" });
		tempAbbrMap.put("KAST", new String[] { "Asia/Kashgar" });
		tempAbbrMap.put("KDT", new String[] { "Asia/Seoul" });
		tempAbbrMap.put("KGST", new String[] { "Asia/Bishkek" });
		tempAbbrMap.put("KGT", new String[] { "Asia/Bishkek" });
		tempAbbrMap.put("KMT", new String[] { "Europe/Vilnius", "Europe/Kiev",
				"America/Cayman", "America/Jamaica", "America/St_Vincent",
				"America/Grand_Turk" });
		tempAbbrMap.put("KOST", new String[] { "Pacific/Kosrae" });
		tempAbbrMap.put("KRAMT", new String[] { "Asia/Krasnoyarsk" });
		tempAbbrMap.put("KRAST", new String[] { "Asia/Krasnoyarsk" });
		tempAbbrMap.put("KRAT", new String[] { "Asia/Krasnoyarsk" });
		tempAbbrMap.put("KST", new String[] { "Asia/Seoul", "Asia/Pyongyang" });
		tempAbbrMap.put("KUYMT", new String[] { "Europe/Samara" });
		tempAbbrMap.put("KUYST", new String[] { "Europe/Samara" });
		tempAbbrMap.put("KUYT", new String[] { "Europe/Samara" });
		tempAbbrMap.put("KWAT", new String[] { "Pacific/Kwajalein" });
		tempAbbrMap.put("LHST", new String[] { "Australia/Lord_Howe" });
		tempAbbrMap.put("LINT", new String[] { "Pacific/Kiritimati" });
		tempAbbrMap.put("LKT", new String[] { "Asia/Colombo" });
		tempAbbrMap.put("LPMT", new String[] { "America/La_Paz" });
		tempAbbrMap.put("LRT", new String[] { "Africa/Monrovia" });
		tempAbbrMap.put("LST", new String[] { "Europe/Riga" });
		tempAbbrMap.put("M", new String[] { "Europe/Moscow" });
		tempAbbrMap.put("MADST", new String[] { "Atlantic/Madeira" });
		tempAbbrMap.put("MAGMT", new String[] { "Asia/Magadan" });
		tempAbbrMap.put("MAGST", new String[] { "Asia/Magadan" });
		tempAbbrMap.put("MAGT", new String[] { "Asia/Magadan" });
		tempAbbrMap.put("MALT", new String[] { "Asia/Kuala_Lumpur",
				"Asia/Singapore" });
		tempAbbrMap.put("MART", new String[] { "Pacific/Marquesas" });
		tempAbbrMap.put("MAWT", new String[] { "Antarctica/Mawson" });
		tempAbbrMap.put("MDDT", new String[] { "America/Cambridge_Bay",
				"America/Yellowknife", "America/Inuvik" });
		tempAbbrMap.put("MDST", new String[] { "Europe/Moscow" });
		tempAbbrMap.put("MDT", new String[] { "America/Denver", "America/Phoenix",
				"America/Boise", "America/Regina", "America/Swift_Current",
				"America/Edmonton", "America/Cambridge_Bay",
				"America/Yellowknife", "America/Inuvik", "America/Chihuahua",
				"America/Hermosillo", "America/Mazatlan" });
		tempAbbrMap.put("MEST", new String[] { "Europe/Tirane", "Europe/Andorra",
				"Europe/Vienna", "Europe/Minsk", "Europe/Brussels",
				"Europe/Sofia", "Europe/Prague", "Europe/Copenhagen",
				"Europe/Tallinn", "Europe/Berlin", "Europe/Gibraltar",
				"Europe/Athens", "Europe/Budapest", "Europe/Rome",
				"Europe/Riga", "Europe/Vaduz", "Europe/Vilnius",
				"Europe/Luxembourg", "Europe/Malta", "Europe/Chisinau",
				"Europe/Tiraspol", "Europe/Monaco", "Europe/Amsterdam",
				"Europe/Oslo", "Europe/Warsaw", "Europe/Lisbon",
				"Europe/Kaliningrad", "Europe/Madrid", "Europe/Stockholm",
				"Europe/Zurich", "Europe/Kiev", "Europe/Uzhgorod",
				"Europe/Zaporozhye", "Europe/Simferopol", "Europe/Belgrade",
				"Africa/Algiers", "Africa/Tripoli", "Africa/Tunis",
				"Africa/Ceuta" });
		tempAbbrMap.put("MET", new String[] { "Europe/Tirane", "Europe/Andorra",
				"Europe/Vienna", "Europe/Minsk", "Europe/Brussels",
				"Europe/Sofia", "Europe/Prague", "Europe/Copenhagen",
				"Europe/Tallinn", "Europe/Berlin", "Europe/Gibraltar",
				"Europe/Athens", "Europe/Budapest", "Europe/Rome",
				"Europe/Riga", "Europe/Vaduz", "Europe/Vilnius",
				"Europe/Luxembourg", "Europe/Malta", "Europe/Chisinau",
				"Europe/Tiraspol", "Europe/Monaco", "Europe/Amsterdam",
				"Europe/Oslo", "Europe/Warsaw", "Europe/Lisbon",
				"Europe/Kaliningrad", "Europe/Madrid", "Europe/Stockholm",
				"Europe/Zurich", "Europe/Kiev", "Europe/Uzhgorod",
				"Europe/Zaporozhye", "Europe/Simferopol", "Europe/Belgrade",
				"Africa/Algiers", "Africa/Tripoli", "Africa/Casablanca",
				"Africa/Tunis", "Africa/Ceuta" });
		tempAbbrMap.put("MHT",
				new String[] { "Pacific/Majuro", "Pacific/Kwajalein" });
		tempAbbrMap.put("MMT", new String[] { "Indian/Maldives", "Europe/Minsk",
				"Europe/Moscow", "Asia/Rangoon", "Asia/Ujung_Pandang",
				"Asia/Colombo", "Pacific/Easter", "Africa/Monrovia",
				"America/Managua", "America/Montevideo" });
		tempAbbrMap.put("MOST", new String[] { "Asia/Macao" });
		tempAbbrMap.put("MOT", new String[] { "Asia/Macao" });
		tempAbbrMap.put("MPT", new String[] { "Pacific/Saipan" });
		tempAbbrMap.put("MSK", new String[] { "Europe/Minsk", "Europe/Tallinn",
				"Europe/Riga", "Europe/Vilnius", "Europe/Chisinau",
				"Europe/Kiev", "Europe/Uzhgorod", "Europe/Zaporozhye",
				"Europe/Simferopol" });
		tempAbbrMap.put("MST", new String[] { "Europe/Moscow", "America/Denver",
				"America/Phoenix", "America/Boise", "America/Regina",
				"America/Swift_Current", "America/Edmonton",
				"America/Dawson_Creek", "America/Cambridge_Bay",
				"America/Yellowknife", "America/Inuvik", "America/Mexico_City",
				"America/Chihuahua", "America/Hermosillo", "America/Mazatlan",
				"America/Tijuana" });
		tempAbbrMap.put("MUT", new String[] { "Indian/Mauritius" });
		tempAbbrMap.put("MVT", new String[] { "Indian/Maldives" });
		tempAbbrMap.put("MWT", new String[] { "America/Denver", "America/Phoenix",
				"America/Boise" });
		tempAbbrMap
				.put("MYT",
						new String[] { "Asia/Kuala_Lumpur", "Asia/Kuching" });
		tempAbbrMap.put("NCST", new String[] { "Pacific/Noumea" });
		tempAbbrMap.put("NCT", new String[] { "Pacific/Noumea" });
		tempAbbrMap.put("NDT", new String[] { "America/Nome", "America/Adak",
				"America/St_Johns", "America/Goose_Bay" });
		tempAbbrMap.put("NEGT", new String[] { "America/Paramaribo" });
		tempAbbrMap.put("NFT", new String[] { "Europe/Paris", "Europe/Oslo",
				"Pacific/Norfolk" });
		tempAbbrMap.put("NMT", new String[] { "Pacific/Norfolk" });
		tempAbbrMap.put("NOVMT", new String[] { "Asia/Novosibirsk" });
		tempAbbrMap.put("NOVST", new String[] { "Asia/Novosibirsk" });
		tempAbbrMap.put("NOVT", new String[] { "Asia/Novosibirsk" });
		tempAbbrMap.put("NPT", new String[] { "Asia/Katmandu" });
		tempAbbrMap.put("NRT", new String[] { "Pacific/Nauru" });
		tempAbbrMap.put("NST", new String[] { "Europe/Amsterdam",
				"Pacific/Pago_Pago", "Pacific/Midway", "America/Nome",
				"America/Adak", "America/St_Johns", "America/Goose_Bay" });
		tempAbbrMap.put("NUT", new String[] { "Pacific/Niue" });
		tempAbbrMap.put("NWT", new String[] { "America/Nome", "America/Adak" });
		tempAbbrMap.put("NZDT", new String[] { "Antarctica/McMurdo" });
		tempAbbrMap.put("NZHDT", new String[] { "Pacific/Auckland" });
		tempAbbrMap.put("NZST", new String[] { "Antarctica/McMurdo",
				"Pacific/Auckland" });
		tempAbbrMap.put("OMSMT", new String[] { "Asia/Omsk" });
		tempAbbrMap.put("OMSST", new String[] { "Asia/Omsk" });
		tempAbbrMap.put("OMST", new String[] { "Asia/Omsk" });
		tempAbbrMap.put("PDDT", new String[] { "America/Inuvik",
				"America/Whitehorse", "America/Dawson" });
		tempAbbrMap.put("PDT", new String[] { "America/Los_Angeles",
				"America/Juneau", "America/Boise", "America/Vancouver",
				"America/Dawson_Creek", "America/Inuvik", "America/Whitehorse",
				"America/Dawson", "America/Tijuana" });
		tempAbbrMap.put("PEST", new String[] { "America/Lima" });
		tempAbbrMap.put("PET", new String[] { "America/Lima" });
		tempAbbrMap.put("PETMT", new String[] { "Asia/Kamchatka" });
		tempAbbrMap.put("PETST", new String[] { "Asia/Kamchatka" });
		tempAbbrMap.put("PETT", new String[] { "Asia/Kamchatka" });
		tempAbbrMap.put("PGT", new String[] { "Pacific/Port_Moresby" });
		tempAbbrMap.put("PHOT", new String[] { "Pacific/Enderbury" });
		tempAbbrMap.put("PHST", new String[] { "Asia/Manila" });
		tempAbbrMap.put("PHT", new String[] { "Asia/Manila" });
		tempAbbrMap.put("PKT", new String[] { "Asia/Karachi" });
		tempAbbrMap.put("PMDT", new String[] { "America/Miquelon" });
		tempAbbrMap.put("PMMT", new String[] { "Pacific/Port_Moresby" });
		tempAbbrMap.put("PMST", new String[] { "America/Miquelon" });
		tempAbbrMap.put("PMT", new String[] { "Antarctica/DumontDUrville",
				"Europe/Prague", "Europe/Paris", "Europe/Monaco",
				"Africa/Algiers", "Africa/Tunis", "America/Panama",
				"America/Paramaribo" });
		tempAbbrMap.put("PNT", new String[] { "Pacific/Pitcairn" });
		tempAbbrMap.put("PONT", new String[] { "Pacific/Ponape" });
		tempAbbrMap.put("PPMT", new String[] { "America/Port-au-Prince" });
		tempAbbrMap.put("PST", new String[] { "Pacific/Pitcairn",
				"America/Los_Angeles", "America/Juneau", "America/Boise",
				"America/Vancouver", "America/Dawson_Creek", "America/Inuvik",
				"America/Whitehorse", "America/Dawson", "America/Hermosillo",
				"America/Mazatlan", "America/Tijuana" });
		tempAbbrMap.put("PWT", new String[] { "Pacific/Palau",
				"America/Los_Angeles", "America/Juneau", "America/Boise",
				"America/Tijuana" });
		tempAbbrMap.put("PYST", new String[] { "America/Asuncion" });
		tempAbbrMap.put("PYT", new String[] { "America/Asuncion" });
		tempAbbrMap.put("QMT", new String[] { "America/Guayaquil" });
		tempAbbrMap.put("RET", new String[] { "Indian/Reunion" });
		tempAbbrMap.put("RMT", new String[] { "Atlantic/Reykjavik", "Europe/Rome",
				"Europe/Riga", "Asia/Rangoon" });
		tempAbbrMap.put("S", new String[] { "Europe/Moscow" });
		tempAbbrMap.put("SAMMT", new String[] { "Europe/Samara" });
		tempAbbrMap
				.put("SAMST",
						new String[] { "Europe/Samara", "Asia/Samarkand" });
		tempAbbrMap.put("SAMT", new String[] { "Europe/Samara", "Asia/Samarkand",
				"Pacific/Pago_Pago", "Pacific/Apia" });
		tempAbbrMap.put("SAST", new String[] { "Africa/Maseru", "Africa/Windhoek",
				"Africa/Johannesburg", "Africa/Mbabane" });
		tempAbbrMap.put("SBT", new String[] { "Pacific/Guadalcanal" });
		tempAbbrMap.put("SCT", new String[] { "Indian/Mahe" });
		tempAbbrMap.put("SDMT", new String[] { "America/Santo_Domingo" });
		tempAbbrMap.put("SGT", new String[] { "Asia/Singapore" });
		tempAbbrMap.put("SHEST", new String[] { "Asia/Aqtau" });
		tempAbbrMap.put("SHET", new String[] { "Asia/Aqtau" });
		tempAbbrMap.put("SJMT", new String[] { "America/Costa_Rica" });
		tempAbbrMap.put("SLST", new String[] { "Africa/Freetown" });
		tempAbbrMap.put("SMT", new String[] { "Atlantic/Stanley",
				"Europe/Stockholm", "Europe/Simferopol", "Asia/Phnom_Penh",
				"Asia/Vientiane", "Asia/Kuala_Lumpur", "Asia/Singapore",
				"Asia/Saigon", "America/Santiago" });
		tempAbbrMap.put("SRT", new String[] { "America/Paramaribo" });
		tempAbbrMap.put("SST",
				new String[] { "Pacific/Pago_Pago", "Pacific/Midway" });
		tempAbbrMap.put("SVEMT", new String[] { "Asia/Yekaterinburg" });
		tempAbbrMap.put("SVEST", new String[] { "Asia/Yekaterinburg" });
		tempAbbrMap.put("SVET", new String[] { "Asia/Yekaterinburg" });
		tempAbbrMap.put("SWAT", new String[] { "Africa/Windhoek" });
		tempAbbrMap.put("SYOT", new String[] { "Antarctica/Syowa" });
		tempAbbrMap.put("TAHT", new String[] { "Pacific/Tahiti" });
		tempAbbrMap
				.put("TASST",
						new String[] { "Asia/Samarkand", "Asia/Tashkent" });
		tempAbbrMap.put("TAST", new String[] { "Asia/Samarkand", "Asia/Tashkent" });
		tempAbbrMap.put("TBIST", new String[] { "Asia/Tbilisi" });
		tempAbbrMap.put("TBIT", new String[] { "Asia/Tbilisi" });
		tempAbbrMap.put("TBMT", new String[] { "Asia/Tbilisi" });
		tempAbbrMap.put("TFT", new String[] { "Indian/Kerguelen" });
		tempAbbrMap.put("TJT", new String[] { "Asia/Dushanbe" });
		tempAbbrMap.put("TKT", new String[] { "Pacific/Fakaofo" });
		tempAbbrMap.put("TMST", new String[] { "Asia/Ashkhabad" });
		tempAbbrMap.put("TMT", new String[] { "Europe/Tallinn", "Asia/Tehran",
				"Asia/Ashkhabad" });
		tempAbbrMap.put("TOST", new String[] { "Pacific/Tongatapu" });
		tempAbbrMap.put("TOT", new String[] { "Pacific/Tongatapu" });
		tempAbbrMap.put("TPT", new String[] { "Asia/Dili" });
		tempAbbrMap.put("TRST", new String[] { "Europe/Istanbul" });
		tempAbbrMap.put("TRT", new String[] { "Europe/Istanbul" });
		tempAbbrMap.put("TRUT", new String[] { "Pacific/Truk" });
		tempAbbrMap.put("TVT", new String[] { "Pacific/Funafuti" });
		tempAbbrMap.put("ULAST", new String[] { "Asia/Ulaanbaatar" });
		tempAbbrMap.put("ULAT", new String[] { "Asia/Ulaanbaatar" });
		tempAbbrMap.put("URUT", new String[] { "Asia/Urumqi" });
		tempAbbrMap.put("UYHST", new String[] { "America/Montevideo" });
		tempAbbrMap.put("UYT", new String[] { "America/Montevideo" });
		tempAbbrMap.put("UZST", new String[] { "Asia/Samarkand", "Asia/Tashkent" });
		tempAbbrMap.put("UZT", new String[] { "Asia/Samarkand", "Asia/Tashkent" });
		tempAbbrMap.put("VET", new String[] { "America/Caracas" });
		tempAbbrMap.put("VLAMT", new String[] { "Asia/Vladivostok" });
		tempAbbrMap.put("VLAST", new String[] { "Asia/Vladivostok" });
		tempAbbrMap.put("VLAT", new String[] { "Asia/Vladivostok" });
		tempAbbrMap.put("VUST", new String[] { "Pacific/Efate" });
		tempAbbrMap.put("VUT", new String[] { "Pacific/Efate" });
		tempAbbrMap.put("WAKT", new String[] { "Pacific/Wake" });
		tempAbbrMap.put("WARST",
				new String[] { "America/Jujuy", "America/Mendoza" });
		tempAbbrMap
				.put("WART",
						new String[] { "America/Jujuy", "America/Mendoza" });
		tempAbbrMap.put("WAST",
				new String[] { "Africa/Ndjamena", "Africa/Windhoek" });
		tempAbbrMap.put("WAT", new String[] { "Africa/Luanda", "Africa/Porto-Novo",
				"Africa/Douala", "Africa/Bangui", "Africa/Ndjamena",
				"Africa/Kinshasa", "Africa/Brazzaville", "Africa/Malabo",
				"Africa/Libreville", "Africa/Banjul", "Africa/Conakry",
				"Africa/Bissau", "Africa/Bamako", "Africa/Nouakchott",
				"Africa/El_Aaiun", "Africa/Windhoek", "Africa/Niamey",
				"Africa/Lagos", "Africa/Dakar", "Africa/Freetown" });
		tempAbbrMap.put("WEST", new String[] { "Atlantic/Faeroe",
				"Atlantic/Azores", "Atlantic/Madeira", "Atlantic/Canary",
				"Europe/Brussels", "Europe/Luxembourg", "Europe/Monaco",
				"Europe/Lisbon", "Europe/Madrid", "Africa/Algiers",
				"Africa/Casablanca", "Africa/Ceuta" });
		tempAbbrMap.put("WET", new String[] { "Atlantic/Faeroe", "Atlantic/Azores",
				"Atlantic/Madeira", "Atlantic/Canary", "Europe/Andorra",
				"Europe/Brussels", "Europe/Luxembourg", "Europe/Monaco",
				"Europe/Lisbon", "Europe/Madrid", "Africa/Algiers",
				"Africa/Casablanca", "Africa/El_Aaiun", "Africa/Ceuta" });
		tempAbbrMap.put("WFT", new String[] { "Pacific/Wallis" });
		tempAbbrMap.put("WGST", new String[] { "America/Godthab" });
		tempAbbrMap.put("WGT", new String[] { "America/Godthab" });
		tempAbbrMap.put("WMT", new String[] { "Europe/Vilnius", "Europe/Warsaw" });
		tempAbbrMap.put("WST", new String[] { "Antarctica/Casey", "Pacific/Apia",
				"Australia/Perth" });
		tempAbbrMap.put("YAKMT", new String[] { "Asia/Yakutsk" });
		tempAbbrMap.put("YAKST", new String[] { "Asia/Yakutsk" });
		tempAbbrMap.put("YAKT", new String[] { "Asia/Yakutsk" });
		tempAbbrMap.put("YAPT", new String[] { "Pacific/Yap" });
		tempAbbrMap.put("YDDT", new String[] { "America/Whitehorse",
				"America/Dawson" });
		tempAbbrMap.put("YDT", new String[] { "America/Yakutat",
				"America/Whitehorse", "America/Dawson" });
		tempAbbrMap.put("YEKMT", new String[] { "Asia/Yekaterinburg" });
		tempAbbrMap.put("YEKST", new String[] { "Asia/Yekaterinburg" });
		tempAbbrMap.put("YEKT", new String[] { "Asia/Yekaterinburg" });
		tempAbbrMap.put("YERST", new String[] { "Asia/Yerevan" });
		tempAbbrMap.put("YERT", new String[] { "Asia/Yerevan" });
		tempAbbrMap.put("YST", new String[] { "America/Yakutat",
				"America/Whitehorse", "America/Dawson" });
		tempAbbrMap.put("YWT", new String[] { "America/Yakutat" });

		ABBREVIATED_TIMEZONES = Collections.unmodifiableMap(tempAbbrMap);
	}

	/**
	 * Change the given times from one timezone to another
	 * 
	 * @param conn
	 *            the current connection to the MySQL server
	 * @param t
	 *            the times to change
	 * @param fromTz
	 *            the timezone to change from
	 * @param toTz
	 *            the timezone to change to
	 * 
	 * @return the times changed to the timezone 'toTz'
	 */
	public static Time changeTimezone(MySQLConnection conn,
			Calendar sessionCalendar, 
			Calendar targetCalendar, 
			Time t, 
			TimeZone fromTz,
			TimeZone toTz, 
			boolean rollForward) {
		if ((conn != null)) {
			if (conn.getUseTimezone() &&
				!conn.getNoTimezoneConversionForTimeType()) {
				// Convert the timestamp from GMT to the server's timezone
				Calendar fromCal = Calendar.getInstance(fromTz);
				fromCal.setTime(t);
	
				int fromOffset = fromCal.get(Calendar.ZONE_OFFSET)
						+ fromCal.get(Calendar.DST_OFFSET);
				Calendar toCal = Calendar.getInstance(toTz);
				toCal.setTime(t);
	
				int toOffset = toCal.get(Calendar.ZONE_OFFSET)
						+ toCal.get(Calendar.DST_OFFSET);
				int offsetDiff = fromOffset - toOffset;
				long toTime = toCal.getTime().getTime();
	
				if (rollForward || (conn.isServerTzUTC() && !conn.isClientTzUTC())) {
					toTime += offsetDiff;
				} else {
					toTime -= offsetDiff;
				}
	
				Time changedTime = new Time(toTime);
	
				return changedTime;
			}  else if (conn.getUseJDBCCompliantTimezoneShift()) {
				if (targetCalendar != null) {

					Time adjustedTime = new Time( 
							jdbcCompliantZoneShift(sessionCalendar, 
									targetCalendar, t));
					
					return adjustedTime;
				}
			}
		}
		
		return t;
	}

	/**
	 * Change the given timestamp from one timezone to another
	 * 
	 * @param conn
	 *            the current connection to the MySQL server
	 * @param tstamp
	 *            the timestamp to change
	 * @param fromTz
	 *            the timezone to change from
	 * @param toTz
	 *            the timezone to change to
	 * 
	 * @return the timestamp changed to the timezone 'toTz'
	 */
	public static Timestamp changeTimezone(MySQLConnection conn,
			Calendar sessionCalendar, 
			Calendar targetCalendar, 
			Timestamp tstamp,
			TimeZone fromTz, 
			TimeZone toTz, 
			boolean rollForward) {
		if ((conn != null)) {
			if (conn.getUseTimezone()) {
				// Convert the timestamp from GMT to the server's timezone
				Calendar fromCal = Calendar.getInstance(fromTz);
				fromCal.setTime(tstamp);
	
				int fromOffset = fromCal.get(Calendar.ZONE_OFFSET)
						+ fromCal.get(Calendar.DST_OFFSET);
				Calendar toCal = Calendar.getInstance(toTz);
				toCal.setTime(tstamp);
	
				int toOffset = toCal.get(Calendar.ZONE_OFFSET)
						+ toCal.get(Calendar.DST_OFFSET);
				int offsetDiff = fromOffset - toOffset;
				long toTime = toCal.getTime().getTime();
	
				if (rollForward || (conn.isServerTzUTC() && !conn.isClientTzUTC())) {
					toTime += offsetDiff;
				} else {
					toTime -= offsetDiff;
				}
	
				Timestamp changedTimestamp = new Timestamp(toTime);
	
				return changedTimestamp;
			} else if (conn.getUseJDBCCompliantTimezoneShift()) {
				if (targetCalendar != null) {

					Timestamp adjustedTimestamp = new Timestamp( 
							jdbcCompliantZoneShift(sessionCalendar, 
									targetCalendar, tstamp));
					
					adjustedTimestamp.setNanos(tstamp.getNanos());
					
					return adjustedTimestamp;
				}
			}
		}
		
		return tstamp;
	}

	private static long jdbcCompliantZoneShift(Calendar sessionCalendar, 
			Calendar targetCalendar, 
			java.util.Date dt) {
		if (sessionCalendar == null) {
			sessionCalendar = new GregorianCalendar();
		}
		
		// JDBC spec is not clear whether or not this 
		// calendar should be immutable, so let's treat
		// it like it is, for safety
		
		java.util.Date origCalDate = targetCalendar.getTime();
		java.util.Date origSessionDate = sessionCalendar.getTime();
		
		try {
			sessionCalendar.setTime(dt);
			
			targetCalendar.set(Calendar.YEAR, sessionCalendar.get(Calendar.YEAR));
			targetCalendar.set(Calendar.MONTH, sessionCalendar.get(Calendar.MONTH));
			targetCalendar.set(Calendar.DAY_OF_MONTH, sessionCalendar.get(Calendar.DAY_OF_MONTH));

			targetCalendar.set(Calendar.HOUR_OF_DAY, sessionCalendar.get(Calendar.HOUR_OF_DAY));                
			targetCalendar.set(Calendar.MINUTE, sessionCalendar.get(Calendar.MINUTE));
			targetCalendar.set(Calendar.SECOND, sessionCalendar.get(Calendar.SECOND));
			targetCalendar.set(Calendar.MILLISECOND, sessionCalendar.get(Calendar.MILLISECOND));                
			
			return targetCalendar.getTime().getTime();
			
		} finally {
			sessionCalendar.setTime(origSessionDate);
			targetCalendar.setTime(origCalDate);
		}
	}

	//
	// WARN! You must externally synchronize these calendar instances
	// See ResultSet.fastDateCreate() for an example
	//
	final static Date fastDateCreate(boolean useGmtConversion,
			Calendar gmtCalIfNeeded,
			Calendar cal, int year, int month, int day) {
		
		Calendar dateCal = cal;
		
		if (useGmtConversion) {
			
			if (gmtCalIfNeeded == null) {
				gmtCalIfNeeded = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
			}
			gmtCalIfNeeded.clear();
			
			dateCal = gmtCalIfNeeded;
		}
		
		dateCal.clear();
		dateCal.set(Calendar.MILLISECOND, 0);
		
		// why-oh-why is this different than java.util.date,
		// in the year part, but it still keeps the silly '0'
		// for the start month????
		dateCal.set(year, month - 1, day, 0, 0, 0);
		
		long dateAsMillis = 0;

		try {
			dateAsMillis = dateCal.getTimeInMillis();
		} catch (IllegalAccessError iae) {
			// Must be on JDK-1.3.1 or older....
			dateAsMillis = dateCal.getTime().getTime();
		}

		return new Date(dateAsMillis);
	}
	
	final static Date fastDateCreate(int year, int month, int day, Calendar targetCalendar) {
 		
		
		Calendar dateCal = (targetCalendar == null) ? new GregorianCalendar() : targetCalendar;
		
		dateCal.clear();

		
		// why-oh-why is this different than java.util.date,
		// in the year part, but it still keeps the silly '0'
		// for the start month????
		dateCal.set(year, month - 1, day, 0, 0, 0);
		dateCal.set(Calendar.MILLISECOND, 0);
		
		long dateAsMillis = 0;

		try {
			dateAsMillis = dateCal.getTimeInMillis();
		} catch (IllegalAccessError iae) {
			// Must be on JDK-1.3.1 or older....
			dateAsMillis = dateCal.getTime().getTime();
		}

		return new Date(dateAsMillis);
	}

	final static Time fastTimeCreate(Calendar cal, int hour, int minute,
			int second, ExceptionInterceptor exceptionInterceptor) throws SQLException {
		if (hour < 0 || hour > 24) {
			throw SQLError.createSQLException("Illegal hour value '" + hour + "' for java.sql.Time type in value '"
					+ timeFormattedString(hour, minute, second) + ".", 
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor);
		}
		
		if (minute < 0 || minute > 59) {
			throw SQLError.createSQLException("Illegal minute value '" + minute + "'" + "' for java.sql.Time type in value '"
					+ timeFormattedString(hour, minute, second) + ".", 
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor);
		}
		
		if (second < 0 || second > 59) {
			throw SQLError.createSQLException("Illegal minute value '" + second + "'" + "' for java.sql.Time type in value '"
					+ timeFormattedString(hour, minute, second) + ".", 
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor);
		}
		
		cal.clear();

		// Set 'date' to epoch of Jan 1, 1970
		cal.set(1970, 0, 1, hour, minute, second);

		long timeAsMillis = 0;

		try {
			timeAsMillis = cal.getTimeInMillis();
		} catch (IllegalAccessError iae) {
			// Must be on JDK-1.3.1 or older....
			timeAsMillis = cal.getTime().getTime();
		}

		return new Time(timeAsMillis);
	}

	final static Time fastTimeCreate(int hour, int minute,
 			int second, Calendar targetCalendar, ExceptionInterceptor exceptionInterceptor) throws SQLException {
		if (hour < 0 || hour > 23) {
			throw SQLError.createSQLException("Illegal hour value '" + hour + "' for java.sql.Time type in value '"
					+ timeFormattedString(hour, minute, second) + ".", 
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor);
		}
		
		if (minute < 0 || minute > 59) {
			throw SQLError.createSQLException("Illegal minute value '" + minute + "'" + "' for java.sql.Time type in value '"
					+ timeFormattedString(hour, minute, second) + ".", 
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor);
		}
		
		if (second < 0 || second > 59) {
			throw SQLError.createSQLException("Illegal minute value '" + second + "'" + "' for java.sql.Time type in value '"
					+ timeFormattedString(hour, minute, second) + ".", 
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor);
		}
		
		Calendar cal = (targetCalendar == null) ? new GregorianCalendar() : targetCalendar;
		cal.clear();

		// Set 'date' to epoch of Jan 1, 1970
		cal.set(1970, 0, 1, hour, minute, second);

		long timeAsMillis = 0;

		try {
			timeAsMillis = cal.getTimeInMillis();
		} catch (IllegalAccessError iae) {
			// Must be on JDK-1.3.1 or older....
			timeAsMillis = cal.getTime().getTime();
		}

		return new Time(timeAsMillis);
 	}
	
	final static Timestamp fastTimestampCreate(boolean useGmtConversion,
			Calendar gmtCalIfNeeded,
			Calendar cal, int year,
			int month, int day, int hour, int minute, int seconds,
			int secondsPart) {
		cal.clear();

		// why-oh-why is this different than java.util.date,
		// in the year part, but it still keeps the silly '0'
		// for the start month????
		cal.set(year, month - 1, day, hour, minute, seconds);

		int offsetDiff = 0;
		
		if (useGmtConversion) {
			int fromOffset = cal.get(Calendar.ZONE_OFFSET)
			+ cal.get(Calendar.DST_OFFSET);
			
			if (gmtCalIfNeeded == null) {
				gmtCalIfNeeded = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
			}
			gmtCalIfNeeded.clear();
			
			gmtCalIfNeeded.setTimeInMillis(cal.getTimeInMillis());
	
			int toOffset = gmtCalIfNeeded.get(Calendar.ZONE_OFFSET)
				+ gmtCalIfNeeded.get(Calendar.DST_OFFSET);
			offsetDiff = fromOffset - toOffset;
		}

		if (secondsPart != 0) {
			cal.set(Calendar.MILLISECOND, secondsPart / 1000000);
		}
		
		long tsAsMillis = 0;

		
		try {
			tsAsMillis = cal.getTimeInMillis();
		} catch (IllegalAccessError iae) {
			// Must be on JDK-1.3.1 or older....
			tsAsMillis = cal.getTime().getTime();
		}

		Timestamp ts = new Timestamp(tsAsMillis + offsetDiff);
		
		ts.setNanos(secondsPart);

		return ts;
	}
	
	final static Timestamp fastTimestampCreate(TimeZone tz, int year,
 			int month, int day, int hour, int minute, int seconds,
 			int secondsPart) {
		Calendar cal = (tz == null) ? new GregorianCalendar() : new GregorianCalendar(tz);
		cal.clear();
		
		// why-oh-why is this different than java.util.date,
		// in the year part, but it still keeps the silly '0'
		// for the start month????
		cal.set(year, month - 1, day, hour, minute, seconds);

		long tsAsMillis = 0;

		try {
			tsAsMillis = cal.getTimeInMillis();
		} catch (IllegalAccessError iae) {
			// Must be on JDK-1.3.1 or older....
			tsAsMillis = cal.getTime().getTime();
		}

		Timestamp ts = new Timestamp(tsAsMillis);
		ts.setNanos(secondsPart);

		return ts;
	}
	
	/**
	 * Returns the 'official' Java timezone name for the given timezone
	 * 
	 * @param timezoneStr
	 *            the 'common' timezone name
	 * 
	 * @return the Java timezone name for the given timezone
	 * @throws SQLException 
	 * 
	 * @throws IllegalArgumentException
	 *             DOCUMENT ME!
	 */
	public static String getCanoncialTimezone(String timezoneStr, ExceptionInterceptor exceptionInterceptor) throws SQLException {
		if (timezoneStr == null) {
			return null;
		}

		timezoneStr = timezoneStr.trim();

		// handle '+/-hh:mm' form ...
		
		if (timezoneStr.length() > 2) {
			if ((timezoneStr.charAt(0) == '+' || timezoneStr.charAt(0) == '-') &&
					Character.isDigit(timezoneStr.charAt(1))) {
				return "GMT" + timezoneStr;
			}
		}
		// Fix windows Daylight/Standard shift JDK doesn't map these (doh)

		int daylightIndex = StringUtils.indexOfIgnoreCase(timezoneStr,
				"DAYLIGHT");

		if (daylightIndex != -1) {
			StringBuffer timezoneBuf = new StringBuffer();
			timezoneBuf.append(timezoneStr.substring(0, daylightIndex));
			timezoneBuf.append("Standard");
			timezoneBuf.append(timezoneStr.substring(daylightIndex
					+ "DAYLIGHT".length(), timezoneStr.length()));
			timezoneStr = timezoneBuf.toString();
		}

		String canonicalTz = TIMEZONE_MAPPINGS.get(timezoneStr);

		// if we didn't find it, try abbreviated timezones
		if (canonicalTz == null) {
			String[] abbreviatedTimezone = ABBREVIATED_TIMEZONES
					.get(timezoneStr);

			if (abbreviatedTimezone != null) {
				// If there's only one mapping use that
				if (abbreviatedTimezone.length == 1) {
					canonicalTz = abbreviatedTimezone[0];
				} else {
					StringBuffer possibleTimezones = new StringBuffer(128);
					
					possibleTimezones.append(abbreviatedTimezone[0]);
					
					for (int i = 1; i < abbreviatedTimezone.length; i++) {
						possibleTimezones.append(", ");
						possibleTimezones.append(abbreviatedTimezone[i]);
					}

					throw SQLError.createSQLException(Messages.getString("TimeUtil.TooGenericTimezoneId",
							new Object[] {timezoneStr, possibleTimezones}), SQLError.SQL_STATE_INVALID_CONNECTION_ATTRIBUTE, exceptionInterceptor);
				}
			}
		}

		return canonicalTz;
	}
	
	// we could use SimpleDateFormat, but it won't work when the time values
	// are out-of-bounds, and we're using this for error messages for exactly 
	// that case
	//
	
	private static String timeFormattedString(int hours, int minutes, int seconds) {
		StringBuffer buf = new StringBuffer(8);
		if (hours < 10) {
			buf.append("0");
		}
		
		buf.append(hours);
		buf.append(":");
		
		if (minutes < 10) {
			buf.append("0");
		}
		
		buf.append(minutes);
		buf.append(":");
		
		if (seconds < 10) {
			buf.append("0");
		}
		
		buf.append(seconds);
		
		return buf.toString();
	}

	public static String formatNanos(int nanos, boolean serverSupportsFracSecs, boolean usingMicros) {

		// get only last 9 digits
		if (nanos > 999999999) {
			nanos %= 100000000;
		}

		if (usingMicros) {
			nanos /= 1000;
		}

		if (!serverSupportsFracSecs || nanos == 0) {
		    return "0";
		}

		final int digitCount = usingMicros ? 6 : 9;
		
	    String nanosString = Integer.toString(nanos);
	    final String zeroPadding = usingMicros ? "000000" : "000000000";
	    
	    nanosString = zeroPadding.substring(0, (digitCount-nanosString.length())) +
		nanosString; 
	    
	    int pos = digitCount-1; // the end, we're padded to the end by the code above
	    
	    while (nanosString.charAt(pos) == '0') {
	    	pos--;
	    }
	
	    nanosString = nanosString.substring(0, pos + 1);
	    
	    return nanosString;
	}

}
