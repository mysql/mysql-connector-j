/*
  Copyright (c) 2014, 2016, Oracle and/or its affiliates. All rights reserved.

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

package testsuite.regression;

import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import com.mysql.cj.core.exceptions.CJException;
import com.mysql.cj.jdbc.util.TimeUtil;

import testsuite.BaseTestCase;

/**
 * Regression tests for utility classes.
 */
public class UtilsRegressionTest extends BaseTestCase {

    /**
     * Creates a new UtilsRegressionTest.
     * 
     * @param name
     *            the name of the test
     */
    public UtilsRegressionTest(String name) {
        super(name);
    }

    /**
     * Runs all test cases in this test suite
     * 
     * @param args
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(UtilsRegressionTest.class);
    }

    /**
     * Tests all TimeZone mappings supported.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testTimeZones() throws Exception {
        /*
         * Time Zones can be identified by many different ways according to Unicode CLDR database. The following map contain the correspondence between
         * alternative Time Zone designations to Standard Time Zones ID (IANA/Olson database). This data was generated from IANA Time Zone database v. 2015f
         * (http://www.iana.org/time-zones) and Unicode CLDR v.28 (http://cldr.unicode.org/)
         * 
         * Both the file com/mysql/cj/jdbc/TimeZoneMapping.properties and the following data are generated from a MySQL Connector/J internal utility.
         */

        Map<String, String> tzMap = new HashMap<String, String>();

        // GENERATED CODE STARTS HERE

        // Windows Zones:
        tzMap.put("AUS Central Daylight Time", "Australia/Darwin");
        tzMap.put("AUS Central Standard Time", "Australia/Darwin");
        tzMap.put("AUS Eastern Daylight Time", "Australia/Sydney");
        tzMap.put("AUS Eastern Standard Time", "Australia/Sydney");
        tzMap.put("Afghanistan Daylight Time", "Asia/Kabul");
        tzMap.put("Afghanistan Standard Time", "Asia/Kabul");
        tzMap.put("Alaskan Daylight Time", "America/Anchorage");
        tzMap.put("Alaskan Standard Time", "America/Anchorage");
        tzMap.put("Arab Daylight Time", "Asia/Riyadh");
        tzMap.put("Arab Standard Time", "Asia/Riyadh");
        tzMap.put("Arabian Daylight Time", "Asia/Dubai");
        tzMap.put("Arabian Standard Time", "Asia/Dubai");
        tzMap.put("Arabic Daylight Time", "Asia/Baghdad");
        tzMap.put("Arabic Standard Time", "Asia/Baghdad");
        tzMap.put("Argentina Daylight Time", "America/Buenos_Aires");
        tzMap.put("Argentina Standard Time", "America/Buenos_Aires");
        tzMap.put("Atlantic Daylight Time", "America/Halifax");
        tzMap.put("Atlantic Standard Time", "America/Halifax");
        tzMap.put("Azerbaijan Daylight Time", "Asia/Baku");
        tzMap.put("Azerbaijan Standard Time", "Asia/Baku");
        tzMap.put("Azores Daylight Time", "Atlantic/Azores");
        tzMap.put("Azores Standard Time", "Atlantic/Azores");
        tzMap.put("Bahia Daylight Time", "America/Bahia");
        tzMap.put("Bahia Standard Time", "America/Bahia");
        tzMap.put("Bangladesh Daylight Time", "Asia/Dhaka");
        tzMap.put("Bangladesh Standard Time", "Asia/Dhaka");
        tzMap.put("Belarus Daylight Time", "Europe/Minsk");
        tzMap.put("Belarus Standard Time", "Europe/Minsk");
        tzMap.put("Canada Central Daylight Time", "America/Regina");
        tzMap.put("Canada Central Standard Time", "America/Regina");
        tzMap.put("Cape Verde Daylight Time", "Atlantic/Cape_Verde");
        tzMap.put("Cape Verde Standard Time", "Atlantic/Cape_Verde");
        tzMap.put("Caucasus Daylight Time", "Asia/Yerevan");
        tzMap.put("Caucasus Standard Time", "Asia/Yerevan");
        tzMap.put("Cen. Australia Daylight Time", "Australia/Adelaide");
        tzMap.put("Cen. Australia Standard Time", "Australia/Adelaide");
        tzMap.put("Central America Daylight Time", "America/Guatemala");
        tzMap.put("Central America Standard Time", "America/Guatemala");
        tzMap.put("Central Asia Daylight Time", "Asia/Almaty");
        tzMap.put("Central Asia Standard Time", "Asia/Almaty");
        tzMap.put("Central Brazilian Daylight Time", "America/Cuiaba");
        tzMap.put("Central Brazilian Standard Time", "America/Cuiaba");
        tzMap.put("Central Daylight Time", "America/Chicago");
        tzMap.put("Central Daylight Time (Mexico)", "America/Mexico_City");
        tzMap.put("Central Europe Daylight Time", "Europe/Budapest");
        tzMap.put("Central Europe Standard Time", "Europe/Budapest");
        tzMap.put("Central European Daylight Time", "Europe/Warsaw");
        tzMap.put("Central European Standard Time", "Europe/Warsaw");
        tzMap.put("Central Pacific Daylight Time", "Pacific/Guadalcanal");
        tzMap.put("Central Pacific Standard Time", "Pacific/Guadalcanal");
        tzMap.put("Central Standard Time", "America/Chicago");
        tzMap.put("Central Standard Time (Mexico)", "America/Mexico_City");
        tzMap.put("China Daylight Time", "Asia/Shanghai");
        tzMap.put("China Standard Time", "Asia/Shanghai");
        tzMap.put("Dateline Daylight Time", "Etc/GMT+12");
        tzMap.put("Dateline Standard Time", "Etc/GMT+12");
        tzMap.put("E. Africa Daylight Time", "Africa/Nairobi");
        tzMap.put("E. Africa Standard Time", "Africa/Nairobi");
        tzMap.put("E. Australia Daylight Time", "Australia/Brisbane");
        tzMap.put("E. Australia Standard Time", "Australia/Brisbane");
        tzMap.put("E. South America Daylight Time", "America/Sao_Paulo");
        tzMap.put("E. South America Standard Time", "America/Sao_Paulo");
        tzMap.put("Eastern Daylight Time", "America/New_York");
        tzMap.put("Eastern Daylight Time (Mexico)", "America/Cancun");
        tzMap.put("Eastern Standard Time", "America/New_York");
        tzMap.put("Eastern Standard Time (Mexico)", "America/Cancun");
        tzMap.put("Egypt Daylight Time", "Africa/Cairo");
        tzMap.put("Egypt Standard Time", "Africa/Cairo");
        tzMap.put("Ekaterinburg Daylight Time", "Asia/Yekaterinburg");
        tzMap.put("Ekaterinburg Standard Time", "Asia/Yekaterinburg");
        tzMap.put("FLE Daylight Time", "Europe/Kiev");
        tzMap.put("FLE Standard Time", "Europe/Kiev");
        tzMap.put("Fiji Daylight Time", "Pacific/Fiji");
        tzMap.put("Fiji Standard Time", "Pacific/Fiji");
        tzMap.put("GMT Daylight Time", "Europe/London");
        tzMap.put("GMT Standard Time", "Europe/London");
        tzMap.put("GTB Daylight Time", "Europe/Bucharest");
        tzMap.put("GTB Standard Time", "Europe/Bucharest");
        tzMap.put("Georgian Daylight Time", "Asia/Tbilisi");
        tzMap.put("Georgian Standard Time", "Asia/Tbilisi");
        tzMap.put("Greenland Daylight Time", "America/Godthab");
        tzMap.put("Greenland Standard Time", "America/Godthab");
        tzMap.put("Greenwich Daylight Time", "Atlantic/Reykjavik");
        tzMap.put("Greenwich Standard Time", "Atlantic/Reykjavik");
        tzMap.put("Hawaiian Daylight Time", "Pacific/Honolulu");
        tzMap.put("Hawaiian Standard Time", "Pacific/Honolulu");
        tzMap.put("India Daylight Time", "Asia/Calcutta");
        tzMap.put("India Standard Time", "Asia/Calcutta");
        tzMap.put("Iran Daylight Time", "Asia/Tehran");
        tzMap.put("Iran Standard Time", "Asia/Tehran");
        tzMap.put("Israel Daylight Time", "Asia/Jerusalem");
        tzMap.put("Israel Standard Time", "Asia/Jerusalem");
        tzMap.put("Jordan Daylight Time", "Asia/Amman");
        tzMap.put("Jordan Standard Time", "Asia/Amman");
        tzMap.put("Kaliningrad Daylight Time", "Europe/Kaliningrad");
        tzMap.put("Kaliningrad Standard Time", "Europe/Kaliningrad");
        tzMap.put("Korea Daylight Time", "Asia/Seoul");
        tzMap.put("Korea Standard Time", "Asia/Seoul");
        tzMap.put("Libya Daylight Time", "Africa/Tripoli");
        tzMap.put("Libya Standard Time", "Africa/Tripoli");
        tzMap.put("Line Islands Daylight Time", "Pacific/Kiritimati");
        tzMap.put("Line Islands Standard Time", "Pacific/Kiritimati");
        tzMap.put("Magadan Daylight Time", "Asia/Magadan");
        tzMap.put("Magadan Standard Time", "Asia/Magadan");
        tzMap.put("Mauritius Daylight Time", "Indian/Mauritius");
        tzMap.put("Mauritius Standard Time", "Indian/Mauritius");
        tzMap.put("Middle East Daylight Time", "Asia/Beirut");
        tzMap.put("Middle East Standard Time", "Asia/Beirut");
        tzMap.put("Montevideo Daylight Time", "America/Montevideo");
        tzMap.put("Montevideo Standard Time", "America/Montevideo");
        tzMap.put("Morocco Daylight Time", "Africa/Casablanca");
        tzMap.put("Morocco Standard Time", "Africa/Casablanca");
        tzMap.put("Mountain Daylight Time", "America/Denver");
        tzMap.put("Mountain Daylight Time (Mexico)", "America/Chihuahua");
        tzMap.put("Mountain Standard Time", "America/Denver");
        tzMap.put("Mountain Standard Time (Mexico)", "America/Chihuahua");
        tzMap.put("Myanmar Daylight Time", "Asia/Rangoon");
        tzMap.put("Myanmar Standard Time", "Asia/Rangoon");
        tzMap.put("N. Central Asia Daylight Time", "Asia/Novosibirsk");
        tzMap.put("N. Central Asia Standard Time", "Asia/Novosibirsk");
        tzMap.put("Namibia Daylight Time", "Africa/Windhoek");
        tzMap.put("Namibia Standard Time", "Africa/Windhoek");
        tzMap.put("Nepal Daylight Time", "Asia/Katmandu");
        tzMap.put("Nepal Standard Time", "Asia/Katmandu");
        tzMap.put("New Zealand Daylight Time", "Pacific/Auckland");
        tzMap.put("New Zealand Standard Time", "Pacific/Auckland");
        tzMap.put("Newfoundland Daylight Time", "America/St_Johns");
        tzMap.put("Newfoundland Standard Time", "America/St_Johns");
        tzMap.put("North Asia Daylight Time", "Asia/Krasnoyarsk");
        tzMap.put("North Asia East Daylight Time", "Asia/Irkutsk");
        tzMap.put("North Asia East Standard Time", "Asia/Irkutsk");
        tzMap.put("North Asia Standard Time", "Asia/Krasnoyarsk");
        tzMap.put("Pacific Daylight Time", "America/Los_Angeles");
        tzMap.put("Pacific Daylight Time (Mexico)", "America/Santa_Isabel");
        tzMap.put("Pacific SA Daylight Time", "America/Santiago");
        tzMap.put("Pacific SA Standard Time", "America/Santiago");
        tzMap.put("Pacific Standard Time", "America/Los_Angeles");
        tzMap.put("Pacific Standard Time (Mexico)", "America/Santa_Isabel");
        tzMap.put("Pakistan Daylight Time", "Asia/Karachi");
        tzMap.put("Pakistan Standard Time", "Asia/Karachi");
        tzMap.put("Paraguay Daylight Time", "America/Asuncion");
        tzMap.put("Paraguay Standard Time", "America/Asuncion");
        tzMap.put("Romance Daylight Time", "Europe/Paris");
        tzMap.put("Romance Standard Time", "Europe/Paris");
        tzMap.put("Russia Time Zone 10", "Asia/Srednekolymsk");
        tzMap.put("Russia Time Zone 11", "Asia/Kamchatka");
        tzMap.put("Russia Time Zone 3", "Europe/Samara");
        tzMap.put("Russian Daylight Time", "Europe/Moscow");
        tzMap.put("Russian Standard Time", "Europe/Moscow");
        tzMap.put("SA Eastern Daylight Time", "America/Cayenne");
        tzMap.put("SA Eastern Standard Time", "America/Cayenne");
        tzMap.put("SA Pacific Daylight Time", "America/Bogota");
        tzMap.put("SA Pacific Standard Time", "America/Bogota");
        tzMap.put("SA Western Daylight Time", "America/La_Paz");
        tzMap.put("SA Western Standard Time", "America/La_Paz");
        tzMap.put("SE Asia Daylight Time", "Asia/Bangkok");
        tzMap.put("SE Asia Standard Time", "Asia/Bangkok");
        tzMap.put("Samoa Daylight Time", "Pacific/Apia");
        tzMap.put("Samoa Standard Time", "Pacific/Apia");
        tzMap.put("Singapore Daylight Time", "Asia/Singapore");
        tzMap.put("Singapore Standard Time", "Asia/Singapore");
        tzMap.put("South Africa Daylight Time", "Africa/Johannesburg");
        tzMap.put("South Africa Standard Time", "Africa/Johannesburg");
        tzMap.put("Sri Lanka Daylight Time", "Asia/Colombo");
        tzMap.put("Sri Lanka Standard Time", "Asia/Colombo");
        tzMap.put("Syria Daylight Time", "Asia/Damascus");
        tzMap.put("Syria Standard Time", "Asia/Damascus");
        tzMap.put("Taipei Daylight Time", "Asia/Taipei");
        tzMap.put("Taipei Standard Time", "Asia/Taipei");
        tzMap.put("Tasmania Daylight Time", "Australia/Hobart");
        tzMap.put("Tasmania Standard Time", "Australia/Hobart");
        tzMap.put("Tokyo Daylight Time", "Asia/Tokyo");
        tzMap.put("Tokyo Standard Time", "Asia/Tokyo");
        tzMap.put("Tonga Daylight Time", "Pacific/Tongatapu");
        tzMap.put("Tonga Standard Time", "Pacific/Tongatapu");
        tzMap.put("Turkey Daylight Time", "Europe/Istanbul");
        tzMap.put("Turkey Standard Time", "Europe/Istanbul");
        tzMap.put("US Eastern Daylight Time", "America/Indianapolis");
        tzMap.put("US Eastern Standard Time", "America/Indianapolis");
        tzMap.put("US Mountain Daylight Time", "America/Phoenix");
        tzMap.put("US Mountain Standard Time", "America/Phoenix");
        tzMap.put("UTC", "Etc/GMT");
        tzMap.put("UTC+12", "Etc/GMT-12");
        tzMap.put("UTC-02", "Etc/GMT+2");
        tzMap.put("UTC-11", "Etc/GMT+11");
        tzMap.put("Ulaanbaatar Daylight Time", "Asia/Ulaanbaatar");
        tzMap.put("Ulaanbaatar Standard Time", "Asia/Ulaanbaatar");
        tzMap.put("Venezuela Daylight Time", "America/Caracas");
        tzMap.put("Venezuela Standard Time", "America/Caracas");
        tzMap.put("Vladivostok Daylight Time", "Asia/Vladivostok");
        tzMap.put("Vladivostok Standard Time", "Asia/Vladivostok");
        tzMap.put("W. Australia Daylight Time", "Australia/Perth");
        tzMap.put("W. Australia Standard Time", "Australia/Perth");
        tzMap.put("W. Central Africa Daylight Time", "Africa/Lagos");
        tzMap.put("W. Central Africa Standard Time", "Africa/Lagos");
        tzMap.put("W. Europe Daylight Time", "Europe/Berlin");
        tzMap.put("W. Europe Standard Time", "Europe/Berlin");
        tzMap.put("West Asia Daylight Time", "Asia/Tashkent");
        tzMap.put("West Asia Standard Time", "Asia/Tashkent");
        tzMap.put("West Pacific Daylight Time", "Pacific/Port_Moresby");
        tzMap.put("West Pacific Standard Time", "Pacific/Port_Moresby");
        tzMap.put("Yakutsk Daylight Time", "Asia/Yakutsk");
        tzMap.put("Yakutsk Standard Time", "Asia/Yakutsk");

        // Linked Time Zones alias:
        tzMap.put("Africa/Addis_Ababa", "Africa/Nairobi");
        tzMap.put("Africa/Asmara", "Africa/Nairobi");
        tzMap.put("Africa/Asmera", "Africa/Nairobi");
        tzMap.put("Africa/Bamako", "Africa/Abidjan");
        tzMap.put("Africa/Bangui", "Africa/Lagos");
        tzMap.put("Africa/Banjul", "Africa/Abidjan");
        tzMap.put("Africa/Blantyre", "Africa/Maputo");
        tzMap.put("Africa/Brazzaville", "Africa/Lagos");
        tzMap.put("Africa/Bujumbura", "Africa/Maputo");
        tzMap.put("Africa/Conakry", "Africa/Abidjan");
        tzMap.put("Africa/Dakar", "Africa/Abidjan");
        tzMap.put("Africa/Dar_es_Salaam", "Africa/Nairobi");
        tzMap.put("Africa/Djibouti", "Africa/Nairobi");
        tzMap.put("Africa/Douala", "Africa/Lagos");
        tzMap.put("Africa/Freetown", "Africa/Abidjan");
        tzMap.put("Africa/Gaborone", "Africa/Maputo");
        tzMap.put("Africa/Harare", "Africa/Maputo");
        tzMap.put("Africa/Juba", "Africa/Khartoum");
        tzMap.put("Africa/Kampala", "Africa/Nairobi");
        tzMap.put("Africa/Kigali", "Africa/Maputo");
        tzMap.put("Africa/Kinshasa", "Africa/Lagos");
        tzMap.put("Africa/Libreville", "Africa/Lagos");
        tzMap.put("Africa/Lome", "Africa/Abidjan");
        tzMap.put("Africa/Luanda", "Africa/Lagos");
        tzMap.put("Africa/Lubumbashi", "Africa/Maputo");
        tzMap.put("Africa/Lusaka", "Africa/Maputo");
        tzMap.put("Africa/Malabo", "Africa/Lagos");
        tzMap.put("Africa/Maseru", "Africa/Johannesburg");
        tzMap.put("Africa/Mbabane", "Africa/Johannesburg");
        tzMap.put("Africa/Mogadishu", "Africa/Nairobi");
        tzMap.put("Africa/Niamey", "Africa/Lagos");
        tzMap.put("Africa/Nouakchott", "Africa/Abidjan");
        tzMap.put("Africa/Ouagadougou", "Africa/Abidjan");
        tzMap.put("Africa/Porto-Novo", "Africa/Lagos");
        tzMap.put("Africa/Sao_Tome", "Africa/Abidjan");
        tzMap.put("Africa/Timbuktu", "Africa/Abidjan");
        tzMap.put("America/Anguilla", "America/Port_of_Spain");
        tzMap.put("America/Antigua", "America/Port_of_Spain");
        tzMap.put("America/Argentina/ComodRivadavia", "America/Argentina/Catamarca");
        tzMap.put("America/Aruba", "America/Curacao");
        tzMap.put("America/Atka", "America/Adak");
        tzMap.put("America/Buenos_Aires", "America/Argentina/Buenos_Aires");
        tzMap.put("America/Catamarca", "America/Argentina/Catamarca");
        tzMap.put("America/Coral_Harbour", "America/Atikokan");
        tzMap.put("America/Cordoba", "America/Argentina/Cordoba");
        tzMap.put("America/Dominica", "America/Port_of_Spain");
        tzMap.put("America/Ensenada", "America/Tijuana");
        tzMap.put("America/Fort_Wayne", "America/Indiana/Indianapolis");
        tzMap.put("America/Grenada", "America/Port_of_Spain");
        tzMap.put("America/Guadeloupe", "America/Port_of_Spain");
        tzMap.put("America/Indianapolis", "America/Indiana/Indianapolis");
        tzMap.put("America/Jujuy", "America/Argentina/Jujuy");
        tzMap.put("America/Knox_IN", "America/Indiana/Knox");
        tzMap.put("America/Kralendijk", "America/Curacao");
        tzMap.put("America/Louisville", "America/Kentucky/Louisville");
        tzMap.put("America/Lower_Princes", "America/Curacao");
        tzMap.put("America/Marigot", "America/Port_of_Spain");
        tzMap.put("America/Mendoza", "America/Argentina/Mendoza");
        tzMap.put("America/Montreal", "America/Toronto");
        tzMap.put("America/Montserrat", "America/Port_of_Spain");
        tzMap.put("America/Porto_Acre", "America/Rio_Branco");
        tzMap.put("America/Rosario", "America/Argentina/Cordoba");
        tzMap.put("America/Shiprock", "America/Denver");
        tzMap.put("America/St_Barthelemy", "America/Port_of_Spain");
        tzMap.put("America/St_Kitts", "America/Port_of_Spain");
        tzMap.put("America/St_Lucia", "America/Port_of_Spain");
        tzMap.put("America/St_Thomas", "America/Port_of_Spain");
        tzMap.put("America/St_Vincent", "America/Port_of_Spain");
        tzMap.put("America/Tortola", "America/Port_of_Spain");
        tzMap.put("America/Virgin", "America/Port_of_Spain");
        tzMap.put("Antarctica/McMurdo", "Pacific/Auckland");
        tzMap.put("Antarctica/South_Pole", "Pacific/Auckland");
        tzMap.put("Arctic/Longyearbyen", "Europe/Oslo");
        tzMap.put("Asia/Aden", "Asia/Riyadh");
        tzMap.put("Asia/Ashkhabad", "Asia/Ashgabat");
        tzMap.put("Asia/Bahrain", "Asia/Qatar");
        tzMap.put("Asia/Calcutta", "Asia/Kolkata");
        tzMap.put("Asia/Chongqing", "Asia/Shanghai");
        tzMap.put("Asia/Chungking", "Asia/Shanghai");
        tzMap.put("Asia/Dacca", "Asia/Dhaka");
        tzMap.put("Asia/Harbin", "Asia/Shanghai");
        tzMap.put("Asia/Istanbul", "Europe/Istanbul");
        tzMap.put("Asia/Kashgar", "Asia/Urumqi");
        tzMap.put("Asia/Katmandu", "Asia/Kathmandu");
        tzMap.put("Asia/Kuwait", "Asia/Riyadh");
        tzMap.put("Asia/Macao", "Asia/Macau");
        tzMap.put("Asia/Muscat", "Asia/Dubai");
        tzMap.put("Asia/Phnom_Penh", "Asia/Bangkok");
        tzMap.put("Asia/Saigon", "Asia/Ho_Chi_Minh");
        tzMap.put("Asia/Tel_Aviv", "Asia/Jerusalem");
        tzMap.put("Asia/Thimbu", "Asia/Thimphu");
        tzMap.put("Asia/Ujung_Pandang", "Asia/Makassar");
        tzMap.put("Asia/Ulan_Bator", "Asia/Ulaanbaatar");
        tzMap.put("Asia/Vientiane", "Asia/Bangkok");
        tzMap.put("Atlantic/Faeroe", "Atlantic/Faroe");
        tzMap.put("Atlantic/Jan_Mayen", "Europe/Oslo");
        tzMap.put("Atlantic/St_Helena", "Africa/Abidjan");
        tzMap.put("Australia/ACT", "Australia/Sydney");
        tzMap.put("Australia/Canberra", "Australia/Sydney");
        tzMap.put("Australia/LHI", "Australia/Lord_Howe");
        tzMap.put("Australia/NSW", "Australia/Sydney");
        tzMap.put("Australia/North", "Australia/Darwin");
        tzMap.put("Australia/Queensland", "Australia/Brisbane");
        tzMap.put("Australia/South", "Australia/Adelaide");
        tzMap.put("Australia/Tasmania", "Australia/Hobart");
        tzMap.put("Australia/Victoria", "Australia/Melbourne");
        tzMap.put("Australia/West", "Australia/Perth");
        tzMap.put("Australia/Yancowinna", "Australia/Broken_Hill");
        tzMap.put("Brazil/Acre", "America/Rio_Branco");
        tzMap.put("Brazil/DeNoronha", "America/Noronha");
        tzMap.put("Brazil/East", "America/Sao_Paulo");
        tzMap.put("Brazil/West", "America/Manaus");
        tzMap.put("Canada/Atlantic", "America/Halifax");
        tzMap.put("Canada/Central", "America/Winnipeg");
        tzMap.put("Canada/East-Saskatchewan", "America/Regina");
        tzMap.put("Canada/Eastern", "America/Toronto");
        tzMap.put("Canada/Mountain", "America/Edmonton");
        tzMap.put("Canada/Newfoundland", "America/St_Johns");
        tzMap.put("Canada/Pacific", "America/Vancouver");
        tzMap.put("Canada/Saskatchewan", "America/Regina");
        tzMap.put("Canada/Yukon", "America/Whitehorse");
        tzMap.put("Chile/Continental", "America/Santiago");
        tzMap.put("Chile/EasterIsland", "Pacific/Easter");
        tzMap.put("Cuba", "America/Havana");
        tzMap.put("Egypt", "Africa/Cairo");
        tzMap.put("Eire", "Europe/Dublin");
        tzMap.put("Europe/Belfast", "Europe/London");
        tzMap.put("Europe/Bratislava", "Europe/Prague");
        tzMap.put("Europe/Busingen", "Europe/Zurich");
        tzMap.put("Europe/Guernsey", "Europe/London");
        tzMap.put("Europe/Isle_of_Man", "Europe/London");
        tzMap.put("Europe/Jersey", "Europe/London");
        tzMap.put("Europe/Ljubljana", "Europe/Belgrade");
        tzMap.put("Europe/Mariehamn", "Europe/Helsinki");
        tzMap.put("Europe/Nicosia", "Asia/Nicosia");
        tzMap.put("Europe/Podgorica", "Europe/Belgrade");
        tzMap.put("Europe/San_Marino", "Europe/Rome");
        tzMap.put("Europe/Sarajevo", "Europe/Belgrade");
        tzMap.put("Europe/Skopje", "Europe/Belgrade");
        tzMap.put("Europe/Tiraspol", "Europe/Chisinau");
        tzMap.put("Europe/Vaduz", "Europe/Zurich");
        tzMap.put("Europe/Vatican", "Europe/Rome");
        tzMap.put("Europe/Zagreb", "Europe/Belgrade");
        tzMap.put("GB", "Europe/London");
        tzMap.put("GB-Eire", "Europe/London");
        tzMap.put("GMT+0", "Etc/GMT");
        tzMap.put("GMT-0", "Etc/GMT");
        tzMap.put("GMT0", "Etc/GMT");
        tzMap.put("Greenwich", "Etc/GMT");
        tzMap.put("Hongkong", "Asia/Hong_Kong");
        tzMap.put("Iceland", "Atlantic/Reykjavik");
        tzMap.put("Indian/Antananarivo", "Africa/Nairobi");
        tzMap.put("Indian/Comoro", "Africa/Nairobi");
        tzMap.put("Indian/Mayotte", "Africa/Nairobi");
        tzMap.put("Iran", "Asia/Tehran");
        tzMap.put("Israel", "Asia/Jerusalem");
        tzMap.put("Jamaica", "America/Jamaica");
        tzMap.put("Japan", "Asia/Tokyo");
        tzMap.put("Kwajalein", "Pacific/Kwajalein");
        tzMap.put("Libya", "Africa/Tripoli");
        tzMap.put("Mexico/BajaNorte", "America/Tijuana");
        tzMap.put("Mexico/BajaSur", "America/Mazatlan");
        tzMap.put("Mexico/General", "America/Mexico_City");
        tzMap.put("NZ", "Pacific/Auckland");
        tzMap.put("NZ-CHAT", "Pacific/Chatham");
        tzMap.put("Navajo", "America/Denver");
        tzMap.put("PRC", "Asia/Shanghai");
        tzMap.put("Pacific/Johnston", "Pacific/Honolulu");
        tzMap.put("Pacific/Midway", "Pacific/Pago_Pago");
        tzMap.put("Pacific/Ponape", "Pacific/Pohnpei");
        tzMap.put("Pacific/Saipan", "Pacific/Guam");
        tzMap.put("Pacific/Samoa", "Pacific/Pago_Pago");
        tzMap.put("Pacific/Truk", "Pacific/Chuuk");
        tzMap.put("Pacific/Yap", "Pacific/Chuuk");
        tzMap.put("Poland", "Europe/Warsaw");
        tzMap.put("Portugal", "Europe/Lisbon");
        tzMap.put("ROC", "Asia/Taipei");
        tzMap.put("ROK", "Asia/Seoul");
        tzMap.put("Singapore", "Asia/Singapore");
        tzMap.put("Turkey", "Europe/Istanbul");
        tzMap.put("UCT", "Etc/UCT");
        tzMap.put("US/Alaska", "America/Anchorage");
        tzMap.put("US/Aleutian", "America/Adak");
        tzMap.put("US/Arizona", "America/Phoenix");
        tzMap.put("US/Central", "America/Chicago");
        tzMap.put("US/East-Indiana", "America/Indiana/Indianapolis");
        tzMap.put("US/Eastern", "America/New_York");
        tzMap.put("US/Hawaii", "Pacific/Honolulu");
        tzMap.put("US/Indiana-Starke", "America/Indiana/Knox");
        tzMap.put("US/Michigan", "America/Detroit");
        tzMap.put("US/Mountain", "America/Denver");
        tzMap.put("US/Pacific", "America/Los_Angeles");
        tzMap.put("US/Pacific-New", "America/Los_Angeles");
        tzMap.put("US/Samoa", "Pacific/Pago_Pago");
        tzMap.put("Universal", "Etc/UTC");
        tzMap.put("W-SU", "Europe/Moscow");
        tzMap.put("Zulu", "Etc/UTC");

        // Standard (IANA) abbreviations:
        tzMap.put("ACWST", "Australia/Eucla");
        tzMap.put("AFT", "Asia/Kabul");
        tzMap.put("ALMT", "Asia/Almaty");
        tzMap.put("ANAT", "Asia/Anadyr");
        tzMap.put("AZOST", "Atlantic/Azores");
        tzMap.put("AZOT", "Atlantic/Azores");
        tzMap.put("AZST", "Asia/Baku");
        tzMap.put("AZT", "Asia/Baku");
        tzMap.put("BDT", "Asia/Dhaka");
        tzMap.put("BNT", "Asia/Brunei");
        tzMap.put("BOT", "America/La_Paz");
        tzMap.put("BRST", "America/Sao_Paulo");
        tzMap.put("BTT", "Asia/Thimphu");
        tzMap.put("CAT", "Africa/Maputo");
        tzMap.put("CCT", "Indian/Cocos");
        tzMap.put("CHADT", "Pacific/Chatham");
        tzMap.put("CHAST", "Pacific/Chatham");
        tzMap.put("CHOST", "Asia/Choibalsan");
        tzMap.put("CHOT", "Asia/Choibalsan");
        tzMap.put("CHUT", "Pacific/Chuuk");
        tzMap.put("CKT", "Pacific/Rarotonga");
        tzMap.put("COT", "America/Bogota");
        tzMap.put("CVT", "Atlantic/Cape_Verde");
        tzMap.put("CXT", "Indian/Christmas");
        tzMap.put("ChST", "Pacific/Guam");
        tzMap.put("DAVT", "Antarctica/Davis");
        tzMap.put("DDUT", "Antarctica/DumontDUrville");
        tzMap.put("EAST", "Pacific/Easter");
        tzMap.put("ECT", "America/Guayaquil");
        tzMap.put("EGST", "America/Scoresbysund");
        tzMap.put("EGT", "America/Scoresbysund");
        tzMap.put("FJST", "Pacific/Fiji");
        tzMap.put("FJT", "Pacific/Fiji");
        tzMap.put("FKST", "Atlantic/Stanley");
        tzMap.put("FNT", "America/Noronha");
        tzMap.put("GALT", "Pacific/Galapagos");
        tzMap.put("GAMT", "Pacific/Gambier");
        tzMap.put("GET", "Asia/Tbilisi");
        tzMap.put("GFT", "America/Cayenne");
        tzMap.put("GILT", "Pacific/Tarawa");
        tzMap.put("GYT", "America/Guyana");
        tzMap.put("HDT", "America/Adak");
        tzMap.put("HKT", "Asia/Hong_Kong");
        tzMap.put("HOVST", "Asia/Hovd");
        tzMap.put("HOVT", "Asia/Hovd");
        tzMap.put("IDT", "Asia/Jerusalem");
        tzMap.put("IOT", "Indian/Chagos");
        tzMap.put("IRST", "Asia/Tehran");
        tzMap.put("JST", "Asia/Tokyo");
        tzMap.put("KGT", "Asia/Bishkek");
        tzMap.put("KOST", "Pacific/Kosrae");
        tzMap.put("LHDT", "Australia/Lord_Howe");
        tzMap.put("LHST", "Australia/Lord_Howe");
        tzMap.put("LINT", "Pacific/Kiritimati");
        tzMap.put("MAGT", "Asia/Magadan");
        tzMap.put("MART", "Pacific/Marquesas");
        tzMap.put("MAWT", "Antarctica/Mawson");
        tzMap.put("MEST", "MET");
        tzMap.put("MET", "MET");
        tzMap.put("MIST", "Antarctica/Macquarie");
        tzMap.put("MMT", "Asia/Rangoon");
        tzMap.put("MUT", "Indian/Mauritius");
        tzMap.put("MVT", "Indian/Maldives");
        tzMap.put("NCT", "Pacific/Noumea");
        tzMap.put("NDT", "America/St_Johns");
        tzMap.put("NFT", "Pacific/Norfolk");
        tzMap.put("NOVT", "Asia/Novosibirsk");
        tzMap.put("NPT", "Asia/Kathmandu");
        tzMap.put("NRT", "Pacific/Nauru");
        tzMap.put("NST", "America/St_Johns");
        tzMap.put("NUT", "Pacific/Niue");
        tzMap.put("NZDT", "Pacific/Auckland");
        tzMap.put("NZST", "Pacific/Auckland");
        tzMap.put("OMST", "Asia/Omsk");
        tzMap.put("ORAT", "Asia/Oral");
        tzMap.put("PET", "America/Lima");
        tzMap.put("PETT", "Asia/Kamchatka");
        tzMap.put("PGT", "Pacific/Port_Moresby");
        tzMap.put("PHOT", "Pacific/Enderbury");
        tzMap.put("PHT", "Asia/Manila");
        tzMap.put("PKT", "Asia/Karachi");
        tzMap.put("PMDT", "America/Miquelon");
        tzMap.put("PMST", "America/Miquelon");
        tzMap.put("PONT", "Pacific/Pohnpei");
        tzMap.put("PWT", "Pacific/Palau");
        tzMap.put("PYST", "America/Asuncion");
        tzMap.put("PYT", "America/Asuncion");
        tzMap.put("QYZT", "Asia/Qyzylorda");
        tzMap.put("RET", "Indian/Reunion");
        tzMap.put("ROTT", "Antarctica/Rothera");
        tzMap.put("SAKT", "Asia/Sakhalin");
        tzMap.put("SAMT", "Europe/Samara");
        tzMap.put("SAST", "Africa/Johannesburg");
        tzMap.put("SBT", "Pacific/Guadalcanal");
        tzMap.put("SCT", "Indian/Mahe");
        tzMap.put("SGT", "Asia/Singapore");
        tzMap.put("SRET", "Asia/Srednekolymsk");
        tzMap.put("SRT", "America/Paramaribo");
        tzMap.put("SST", "Pacific/Pago_Pago");
        tzMap.put("SYOT", "Antarctica/Syowa");
        tzMap.put("TAHT", "Pacific/Tahiti");
        tzMap.put("TFT", "Indian/Kerguelen");
        tzMap.put("TJT", "Asia/Dushanbe");
        tzMap.put("TKT", "Pacific/Fakaofo");
        tzMap.put("TLT", "Asia/Dili");
        tzMap.put("TMT", "Asia/Ashgabat");
        tzMap.put("TOT", "Pacific/Tongatapu");
        tzMap.put("TVT", "Pacific/Funafuti");
        tzMap.put("ULAST", "Asia/Ulaanbaatar");
        tzMap.put("ULAT", "Asia/Ulaanbaatar");
        tzMap.put("UYT", "America/Montevideo");
        tzMap.put("VET", "America/Caracas");
        tzMap.put("VOST", "Antarctica/Vostok");
        tzMap.put("VUT", "Pacific/Efate");
        tzMap.put("WAKT", "Pacific/Wake");
        tzMap.put("WAST", "Africa/Windhoek");
        tzMap.put("WFT", "Pacific/Wallis");
        tzMap.put("WGST", "America/Godthab");
        tzMap.put("WGT", "America/Godthab");
        tzMap.put("WIT", "Asia/Jayapura");
        tzMap.put("WITA", "Asia/Makassar");
        tzMap.put("WSDT", "Pacific/Apia");
        tzMap.put("WSST", "Pacific/Apia");
        tzMap.put("XJT", "Asia/Urumqi");
        tzMap.put("YEKT", "Asia/Yekaterinburg");

        // GENERATED CODE ENDS HERE

        for (String key : tzMap.keySet()) {
            assertEquals("Custom time Zone '" + key + "' mapping", tzMap.get(key), TimeUtil.getCanonicalTimezone(key, null));
        }

        for (String tz : TimeZone.getAvailableIDs()) {
            String canonicalTZ;
            try {
                canonicalTZ = TimeUtil.getCanonicalTimezone(tz, null);
            } catch (CJException e) {
                canonicalTZ = null;
            }
            assertNotNull("System Time Zone '" + tz + "' mapping missing", canonicalTZ);
        }
    }

    /**
     * Tests fix for BUG#70436 - Incorrect mapping of windows timezone to Olson timezone.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug70436() throws Exception {
        assertEquals("Asia/Yerevan", TimeUtil.getCanonicalTimezone("Caucasus Standard Time", null));
        assertEquals("Asia/Tbilisi", TimeUtil.getCanonicalTimezone("Georgian Standard Time", null));
    }
}
