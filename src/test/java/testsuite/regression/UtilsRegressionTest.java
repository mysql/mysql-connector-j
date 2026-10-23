/*
 * Copyright (c) 2014, 2026, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package testsuite.regression;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.Socket;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

import com.mysql.cj.Messages;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.exceptions.WrongImplementationException;
import com.mysql.cj.jdbc.CommentClientInfoProvider;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.MysqlDataSource;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.jdbc.ha.StandardLoadBalanceExceptionChecker;
import com.mysql.cj.log.Log;
import com.mysql.cj.log.LogFactory;
import com.mysql.cj.log.StandardLogger;
import com.mysql.cj.protocol.AuthenticationPlugin;
import com.mysql.cj.protocol.SocketFactory;
import com.mysql.cj.protocol.StandardSocketFactory;
import com.mysql.cj.protocol.a.NativeAuthenticationProvider;
import com.mysql.cj.protocol.a.NativePacketPayload;
import com.mysql.cj.protocol.a.NativeSocketConnection;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.util.TimeUtil;
import com.mysql.cj.util.Util;

import testsuite.BaseTestCase;

/**
 * Regression tests for utility classes.
 */
public class UtilsRegressionTest extends BaseTestCase {

    /**
     * Tests all TimeZone mappings supported.
     *
     * @throws Exception
     */
    @Test
    public void testTimeZones() throws Exception {
        /*
         * Time Zones can be identified by many different ways according to Unicode CLDR database. The following map contain the correspondence between
         * alternative Time Zone designations to Standard Time Zones ID (IANA/Olson database). This data was generated from IANA Time Zone database v. 2025b
         * (http://www.iana.org/time-zones) and Unicode CLDR v.47 (http://cldr.unicode.org/)
         *
         * Both the file com/mysql/cj/core/TimeZoneMapping.properties and the following data are generated from a MySQL Connector/J internal utility.
         */
        Map<String, String> tzMap = new HashMap<>();

        // GENERATED CODE STARTS HERE

        // Windows time zones [Unicode CLDR]:
        tzMap.put("AUS Central Daylight Time", "Australia/Darwin");
        tzMap.put("AUS Central Standard Time", "Australia/Darwin");
        tzMap.put("AUS Eastern Daylight Time", "Australia/Sydney");
        tzMap.put("AUS Eastern Standard Time", "Australia/Sydney");
        tzMap.put("Afghanistan Daylight Time", "Asia/Kabul");
        tzMap.put("Afghanistan Standard Time", "Asia/Kabul");
        tzMap.put("Alaskan Daylight Time", "America/Anchorage");
        tzMap.put("Alaskan Standard Time", "America/Anchorage");
        tzMap.put("Aleutian Daylight Time", "America/Adak");
        tzMap.put("Aleutian Standard Time", "America/Adak");
        tzMap.put("Altai Daylight Time", "Asia/Barnaul");
        tzMap.put("Altai Standard Time", "Asia/Barnaul");
        tzMap.put("Arab Daylight Time", "Asia/Riyadh");
        tzMap.put("Arab Standard Time", "Asia/Riyadh");
        tzMap.put("Arabian Daylight Time", "Asia/Dubai");
        tzMap.put("Arabian Standard Time", "Asia/Dubai");
        tzMap.put("Arabic Daylight Time", "Asia/Baghdad");
        tzMap.put("Arabic Standard Time", "Asia/Baghdad");
        tzMap.put("Argentina Daylight Time", "America/Buenos_Aires");
        tzMap.put("Argentina Standard Time", "America/Buenos_Aires");
        tzMap.put("Astrakhan Daylight Time", "Europe/Astrakhan");
        tzMap.put("Astrakhan Standard Time", "Europe/Astrakhan");
        tzMap.put("Atlantic Daylight Time", "America/Halifax");
        tzMap.put("Atlantic Standard Time", "America/Halifax");
        tzMap.put("Aus Central W. Daylight Time", "Australia/Eucla");
        tzMap.put("Aus Central W. Standard Time", "Australia/Eucla");
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
        tzMap.put("Bougainville Daylight Time", "Pacific/Bougainville");
        tzMap.put("Bougainville Standard Time", "Pacific/Bougainville");
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
        tzMap.put("Central Asia Daylight Time", "Asia/Bishkek");
        tzMap.put("Central Asia Standard Time", "Asia/Bishkek");
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
        tzMap.put("Chatham Islands Daylight Time", "Pacific/Chatham");
        tzMap.put("Chatham Islands Standard Time", "Pacific/Chatham");
        tzMap.put("China Daylight Time", "Asia/Shanghai");
        tzMap.put("China Standard Time", "Asia/Shanghai");
        tzMap.put("Cuba Daylight Time", "America/Havana");
        tzMap.put("Cuba Standard Time", "America/Havana");
        tzMap.put("Dateline Daylight Time", "Etc/GMT+12");
        tzMap.put("Dateline Standard Time", "Etc/GMT+12");
        tzMap.put("E. Africa Daylight Time", "Africa/Nairobi");
        tzMap.put("E. Africa Standard Time", "Africa/Nairobi");
        tzMap.put("E. Australia Daylight Time", "Australia/Brisbane");
        tzMap.put("E. Australia Standard Time", "Australia/Brisbane");
        tzMap.put("E. Europe Daylight Time", "Europe/Chisinau");
        tzMap.put("E. Europe Standard Time", "Europe/Chisinau");
        tzMap.put("E. South America Daylight Time", "America/Sao_Paulo");
        tzMap.put("E. South America Standard Time", "America/Sao_Paulo");
        tzMap.put("Easter Island Daylight Time", "Pacific/Easter");
        tzMap.put("Easter Island Standard Time", "Pacific/Easter");
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
        tzMap.put("Haiti Daylight Time", "America/Port-au-Prince");
        tzMap.put("Haiti Standard Time", "America/Port-au-Prince");
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
        tzMap.put("Lord Howe Daylight Time", "Australia/Lord_Howe");
        tzMap.put("Lord Howe Standard Time", "Australia/Lord_Howe");
        tzMap.put("Magadan Daylight Time", "Asia/Magadan");
        tzMap.put("Magadan Standard Time", "Asia/Magadan");
        tzMap.put("Magallanes Daylight Time", "America/Punta_Arenas");
        tzMap.put("Magallanes Standard Time", "America/Punta_Arenas");
        tzMap.put("Marquesas Daylight Time", "Pacific/Marquesas");
        tzMap.put("Marquesas Standard Time", "Pacific/Marquesas");
        tzMap.put("Mauritius Daylight Time", "Indian/Mauritius");
        tzMap.put("Mauritius Standard Time", "Indian/Mauritius");
        tzMap.put("Middle East Daylight Time", "Asia/Beirut");
        tzMap.put("Middle East Standard Time", "Asia/Beirut");
        tzMap.put("Montevideo Daylight Time", "America/Montevideo");
        tzMap.put("Montevideo Standard Time", "America/Montevideo");
        tzMap.put("Morocco Daylight Time", "Africa/Casablanca");
        tzMap.put("Morocco Standard Time", "Africa/Casablanca");
        tzMap.put("Mountain Daylight Time", "America/Denver");
        tzMap.put("Mountain Daylight Time (Mexico)", "America/Mazatlan");
        tzMap.put("Mountain Standard Time", "America/Denver");
        tzMap.put("Mountain Standard Time (Mexico)", "America/Mazatlan");
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
        tzMap.put("Norfolk Daylight Time", "Pacific/Norfolk");
        tzMap.put("Norfolk Standard Time", "Pacific/Norfolk");
        tzMap.put("North Asia Daylight Time", "Asia/Krasnoyarsk");
        tzMap.put("North Asia East Daylight Time", "Asia/Irkutsk");
        tzMap.put("North Asia East Standard Time", "Asia/Irkutsk");
        tzMap.put("North Asia Standard Time", "Asia/Krasnoyarsk");
        tzMap.put("North Korea Daylight Time", "Asia/Pyongyang");
        tzMap.put("North Korea Standard Time", "Asia/Pyongyang");
        tzMap.put("Omsk Daylight Time", "Asia/Omsk");
        tzMap.put("Omsk Standard Time", "Asia/Omsk");
        tzMap.put("Pacific Daylight Time", "America/Los_Angeles");
        tzMap.put("Pacific Daylight Time (Mexico)", "America/Tijuana");
        tzMap.put("Pacific SA Daylight Time", "America/Santiago");
        tzMap.put("Pacific SA Standard Time", "America/Santiago");
        tzMap.put("Pacific Standard Time", "America/Los_Angeles");
        tzMap.put("Pacific Standard Time (Mexico)", "America/Tijuana");
        tzMap.put("Pakistan Daylight Time", "Asia/Karachi");
        tzMap.put("Pakistan Standard Time", "Asia/Karachi");
        tzMap.put("Paraguay Daylight Time", "America/Asuncion");
        tzMap.put("Paraguay Standard Time", "America/Asuncion");
        tzMap.put("Qyzylorda Daylight Time", "Asia/Qyzylorda");
        tzMap.put("Qyzylorda Standard Time", "Asia/Qyzylorda");
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
        tzMap.put("Saint Pierre Daylight Time", "America/Miquelon");
        tzMap.put("Saint Pierre Standard Time", "America/Miquelon");
        tzMap.put("Sakhalin Daylight Time", "Asia/Sakhalin");
        tzMap.put("Sakhalin Standard Time", "Asia/Sakhalin");
        tzMap.put("Samoa Daylight Time", "Pacific/Apia");
        tzMap.put("Samoa Standard Time", "Pacific/Apia");
        tzMap.put("Sao Tome Daylight Time", "Africa/Sao_Tome");
        tzMap.put("Sao Tome Standard Time", "Africa/Sao_Tome");
        tzMap.put("Saratov Daylight Time", "Europe/Saratov");
        tzMap.put("Saratov Standard Time", "Europe/Saratov");
        tzMap.put("Singapore Daylight Time", "Asia/Singapore");
        tzMap.put("Singapore Standard Time", "Asia/Singapore");
        tzMap.put("South Africa Daylight Time", "Africa/Johannesburg");
        tzMap.put("South Africa Standard Time", "Africa/Johannesburg");
        tzMap.put("South Sudan Daylight Time", "Africa/Juba");
        tzMap.put("South Sudan Standard Time", "Africa/Juba");
        tzMap.put("Sri Lanka Daylight Time", "Asia/Colombo");
        tzMap.put("Sri Lanka Standard Time", "Asia/Colombo");
        tzMap.put("Sudan Daylight Time", "Africa/Khartoum");
        tzMap.put("Sudan Standard Time", "Africa/Khartoum");
        tzMap.put("Syria Daylight Time", "Asia/Damascus");
        tzMap.put("Syria Standard Time", "Asia/Damascus");
        tzMap.put("Taipei Daylight Time", "Asia/Taipei");
        tzMap.put("Taipei Standard Time", "Asia/Taipei");
        tzMap.put("Tasmania Daylight Time", "Australia/Hobart");
        tzMap.put("Tasmania Standard Time", "Australia/Hobart");
        tzMap.put("Tocantins Daylight Time", "America/Araguaina");
        tzMap.put("Tocantins Standard Time", "America/Araguaina");
        tzMap.put("Tokyo Daylight Time", "Asia/Tokyo");
        tzMap.put("Tokyo Standard Time", "Asia/Tokyo");
        tzMap.put("Tomsk Daylight Time", "Asia/Tomsk");
        tzMap.put("Tomsk Standard Time", "Asia/Tomsk");
        tzMap.put("Tonga Daylight Time", "Pacific/Tongatapu");
        tzMap.put("Tonga Standard Time", "Pacific/Tongatapu");
        tzMap.put("Transbaikal Daylight Time", "Asia/Chita");
        tzMap.put("Transbaikal Standard Time", "Asia/Chita");
        tzMap.put("Turkey Daylight Time", "Europe/Istanbul");
        tzMap.put("Turkey Standard Time", "Europe/Istanbul");
        tzMap.put("Turks And Caicos Daylight Time", "America/Grand_Turk");
        tzMap.put("Turks And Caicos Standard Time", "America/Grand_Turk");
        tzMap.put("US Eastern Daylight Time", "America/Indianapolis");
        tzMap.put("US Eastern Standard Time", "America/Indianapolis");
        tzMap.put("US Mountain Daylight Time", "America/Phoenix");
        tzMap.put("US Mountain Standard Time", "America/Phoenix");
        tzMap.put("UTC", "Etc/UTC");
        tzMap.put("UTC+12", "Etc/GMT-12");
        tzMap.put("UTC+13", "Etc/GMT-13");
        tzMap.put("UTC-02", "Etc/GMT+2");
        tzMap.put("UTC-08", "Etc/GMT+8");
        tzMap.put("UTC-09", "Etc/GMT+9");
        tzMap.put("UTC-11", "Etc/GMT+11");
        tzMap.put("Ulaanbaatar Daylight Time", "Asia/Ulaanbaatar");
        tzMap.put("Ulaanbaatar Standard Time", "Asia/Ulaanbaatar");
        tzMap.put("Venezuela Daylight Time", "America/Caracas");
        tzMap.put("Venezuela Standard Time", "America/Caracas");
        tzMap.put("Vladivostok Daylight Time", "Asia/Vladivostok");
        tzMap.put("Vladivostok Standard Time", "Asia/Vladivostok");
        tzMap.put("Volgograd Daylight Time", "Europe/Volgograd");
        tzMap.put("Volgograd Standard Time", "Europe/Volgograd");
        tzMap.put("W. Australia Daylight Time", "Australia/Perth");
        tzMap.put("W. Australia Standard Time", "Australia/Perth");
        tzMap.put("W. Central Africa Daylight Time", "Africa/Lagos");
        tzMap.put("W. Central Africa Standard Time", "Africa/Lagos");
        tzMap.put("W. Europe Daylight Time", "Europe/Berlin");
        tzMap.put("W. Europe Standard Time", "Europe/Berlin");
        tzMap.put("W. Mongolia Daylight Time", "Asia/Hovd");
        tzMap.put("W. Mongolia Standard Time", "Asia/Hovd");
        tzMap.put("West Asia Daylight Time", "Asia/Tashkent");
        tzMap.put("West Asia Standard Time", "Asia/Tashkent");
        tzMap.put("West Bank Daylight Time", "Asia/Hebron");
        tzMap.put("West Bank Standard Time", "Asia/Hebron");
        tzMap.put("West Pacific Daylight Time", "Pacific/Port_Moresby");
        tzMap.put("West Pacific Standard Time", "Pacific/Port_Moresby");
        tzMap.put("Yakutsk Daylight Time", "Asia/Yakutsk");
        tzMap.put("Yakutsk Standard Time", "Asia/Yakutsk");
        tzMap.put("Yukon Daylight Time", "America/Whitehorse");
        tzMap.put("Yukon Standard Time", "America/Whitehorse");

        // Exemplar cities [Unicode CLDR]:
        tzMap.put("ACT", "Australia/ACT");
        tzMap.put("Abidjan", "Africa/Abidjan");
        tzMap.put("Accra", "Africa/Accra");
        tzMap.put("Acre", "Brazil/Acre");
        tzMap.put("Adak", "America/Adak");
        tzMap.put("Addis Ababa", "Africa/Addis_Ababa");
        tzMap.put("Adelaide", "Australia/Adelaide");
        tzMap.put("Aden", "Asia/Aden");
        tzMap.put("Aktau", "Asia/Aqtau");
        tzMap.put("Alaska", "US/Alaska");
        tzMap.put("Aleutian", "US/Aleutian");
        tzMap.put("Algiers", "Africa/Algiers");
        tzMap.put("Almaty", "Asia/Almaty");
        tzMap.put("Amman", "Asia/Amman");
        tzMap.put("Amsterdam", "Europe/Amsterdam");
        tzMap.put("Anadyr", "Asia/Anadyr");
        tzMap.put("Anchorage", "America/Anchorage");
        tzMap.put("Andorra", "Europe/Andorra");
        tzMap.put("Anguilla", "America/Anguilla");
        tzMap.put("Antananarivo", "Indian/Antananarivo");
        tzMap.put("Antigua", "America/Antigua");
        tzMap.put("Apia", "Pacific/Apia");
        tzMap.put("Aqtobe", "Asia/Aqtobe");
        tzMap.put("Araguaina", "America/Araguaina");
        tzMap.put("Arizona", "US/Arizona");
        tzMap.put("Aruba", "America/Aruba");
        tzMap.put("Ashgabat", "Asia/Ashgabat");
        tzMap.put("Ashkhabad", "Asia/Ashkhabad");
        tzMap.put("Asmara", "Africa/Asmara");
        tzMap.put("Asmera", "Africa/Asmera");
        tzMap.put("Astrakhan", "Europe/Astrakhan");
        tzMap.put("Asuncion", "America/Asuncion");
        tzMap.put("Athens", "Europe/Athens");
        tzMap.put("Atikokan", "America/Atikokan");
        tzMap.put("Atka", "America/Atka");
        tzMap.put("Atlantic", "Canada/Atlantic");
        tzMap.put("Atyrau", "Asia/Atyrau");
        tzMap.put("Auckland", "Pacific/Auckland");
        tzMap.put("Azores", "Atlantic/Azores");
        tzMap.put("Baghdad", "Asia/Baghdad");
        tzMap.put("Bahia", "America/Bahia");
        tzMap.put("Bahia Banderas", "America/Bahia_Banderas");
        tzMap.put("Bahrain", "Asia/Bahrain");
        tzMap.put("BajaNorte", "Mexico/BajaNorte");
        tzMap.put("BajaSur", "Mexico/BajaSur");
        tzMap.put("Baku", "Asia/Baku");
        tzMap.put("Bamako", "Africa/Bamako");
        tzMap.put("Bangkok", "Asia/Bangkok");
        tzMap.put("Bangui", "Africa/Bangui");
        tzMap.put("Banjul", "Africa/Banjul");
        tzMap.put("Barbados", "America/Barbados");
        tzMap.put("Barnaul", "Asia/Barnaul");
        tzMap.put("Beirut", "Asia/Beirut");
        tzMap.put("Belem", "America/Belem");
        tzMap.put("Belfast", "Europe/Belfast");
        tzMap.put("Belgrade", "Europe/Belgrade");
        tzMap.put("Belize", "America/Belize");
        tzMap.put("Berlin", "Europe/Berlin");
        tzMap.put("Bermuda", "Atlantic/Bermuda");
        tzMap.put("Beulah", "America/North_Dakota/Beulah");
        tzMap.put("Bishkek", "Asia/Bishkek");
        tzMap.put("Bissau", "Africa/Bissau");
        tzMap.put("Blanc-Sablon", "America/Blanc-Sablon");
        tzMap.put("Blantyre", "Africa/Blantyre");
        tzMap.put("Boa Vista", "America/Boa_Vista");
        tzMap.put("Bogota", "America/Bogota");
        tzMap.put("Boise", "America/Boise");
        tzMap.put("Bougainville", "Pacific/Bougainville");
        tzMap.put("Bratislava", "Europe/Bratislava");
        tzMap.put("Brazzaville", "Africa/Brazzaville");
        tzMap.put("Brisbane", "Australia/Brisbane");
        tzMap.put("Broken Hill", "Australia/Broken_Hill");
        tzMap.put("Brunei", "Asia/Brunei");
        tzMap.put("Brussels", "Europe/Brussels");
        tzMap.put("Bucharest", "Europe/Bucharest");
        tzMap.put("Budapest", "Europe/Budapest");
        tzMap.put("Buenos Aires", "America/Buenos_Aires");
        tzMap.put("Bujumbura", "Africa/Bujumbura");
        tzMap.put("Busingen", "Europe/Busingen");
        tzMap.put("Cairo", "Africa/Cairo");
        tzMap.put("Calcutta", "Asia/Calcutta");
        tzMap.put("Cambridge Bay", "America/Cambridge_Bay");
        tzMap.put("Campo Grande", "America/Campo_Grande");
        tzMap.put("Canaries", "Atlantic/Canary");
        tzMap.put("Canberra", "Australia/Canberra");
        tzMap.put("Cancun", "America/Cancun");
        tzMap.put("Canton Island", "Pacific/Enderbury");
        tzMap.put("Cape Verde", "Atlantic/Cape_Verde");
        tzMap.put("Caracas", "America/Caracas");
        tzMap.put("Casablanca", "Africa/Casablanca");
        tzMap.put("Casey Station", "Antarctica/Casey");
        tzMap.put("Catamarca", "America/Catamarca");
        tzMap.put("Cayenne", "America/Cayenne");
        tzMap.put("Cayman", "America/Cayman");
        tzMap.put("Center", "America/North_Dakota/Center");
        tzMap.put("Central", "US/Central");
        tzMap.put("Ceuta", "Africa/Ceuta");
        tzMap.put("Chagos Archipelago", "Indian/Chagos");
        tzMap.put("Chatham Islands", "Pacific/Chatham");
        tzMap.put("Chicago", "America/Chicago");
        tzMap.put("Chihuahua", "America/Chihuahua");
        tzMap.put("Chisinau", "Europe/Chisinau");
        tzMap.put("Chita", "Asia/Chita");
        tzMap.put("Choibalsan", "Asia/Choibalsan");
        tzMap.put("Chongqing", "Asia/Chongqing");
        tzMap.put("Christmas Island", "Indian/Christmas");
        tzMap.put("Chungking", "Asia/Chungking");
        tzMap.put("Chuuk", "Pacific/Chuuk");
        tzMap.put("Ciudad Juarez", "America/Ciudad_Juarez");
        tzMap.put("Cocos Islands", "Indian/Cocos");
        tzMap.put("Colombo", "Asia/Colombo");
        tzMap.put("ComodRivadavia", "America/Argentina/ComodRivadavia");
        tzMap.put("Comoros", "Indian/Comoro");
        tzMap.put("Conakry", "Africa/Conakry");
        tzMap.put("Continental", "Chile/Continental");
        tzMap.put("Copenhagen", "Europe/Copenhagen");
        tzMap.put("Coral Harbour", "America/Coral_Harbour");
        tzMap.put("Cordoba", "America/Cordoba");
        tzMap.put("Costa Rica", "America/Costa_Rica");
        tzMap.put("Coyhaique", "America/Coyhaique");
        tzMap.put("Creston", "America/Creston");
        tzMap.put("Cuiaba", "America/Cuiaba");
        tzMap.put("Curacao", "America/Curacao");
        tzMap.put("Currie", "Australia/Currie");
        tzMap.put("Dacca", "Asia/Dacca");
        tzMap.put("Dakar", "Africa/Dakar");
        tzMap.put("Damascus", "Asia/Damascus");
        tzMap.put("Danmarkshavn", "America/Danmarkshavn");
        tzMap.put("Dar es Salaam", "Africa/Dar_es_Salaam");
        tzMap.put("Darwin", "Australia/Darwin");
        tzMap.put("Davis", "Antarctica/Davis");
        tzMap.put("Dawson", "America/Dawson");
        tzMap.put("Dawson Creek", "America/Dawson_Creek");
        tzMap.put("DeNoronha", "Brazil/DeNoronha");
        tzMap.put("Denver", "America/Denver");
        tzMap.put("Detroit", "America/Detroit");
        tzMap.put("Dhaka", "Asia/Dhaka");
        tzMap.put("Dili", "Asia/Dili");
        tzMap.put("Djibouti", "Africa/Djibouti");
        tzMap.put("Dominica", "America/Dominica");
        tzMap.put("Douala", "Africa/Douala");
        tzMap.put("Dubai", "Asia/Dubai");
        tzMap.put("Dublin", "Europe/Dublin");
        tzMap.put("Dumont d’Urville Station", "Antarctica/DumontDUrville");
        tzMap.put("Dushanbe", "Asia/Dushanbe");
        tzMap.put("East", "Brazil/East");
        tzMap.put("East-Indiana", "US/East-Indiana");
        tzMap.put("East-Saskatchewan", "Canada/East-Saskatchewan");
        tzMap.put("Easter Island", "Pacific/Easter");
        tzMap.put("EasterIsland", "Chile/EasterIsland");
        tzMap.put("Eastern", "US/Eastern");
        tzMap.put("Edmonton", "America/Edmonton");
        tzMap.put("Efate", "Pacific/Efate");
        tzMap.put("Eirunepe", "America/Eirunepe");
        tzMap.put("El Aaiun", "Africa/El_Aaiun");
        tzMap.put("El Salvador", "America/El_Salvador");
        tzMap.put("Ensenada", "America/Ensenada");
        tzMap.put("Eucla", "Australia/Eucla");
        tzMap.put("Fakaofo", "Pacific/Fakaofo");
        tzMap.put("Famagusta", "Asia/Famagusta");
        tzMap.put("Faroe", "Atlantic/Faroe");
        tzMap.put("Faroes", "Atlantic/Faeroe");
        tzMap.put("Fiji", "Pacific/Fiji");
        tzMap.put("Fort Nelson", "America/Fort_Nelson");
        tzMap.put("Fort Wayne", "America/Fort_Wayne");
        tzMap.put("Fortaleza", "America/Fortaleza");
        tzMap.put("Freetown", "Africa/Freetown");
        tzMap.put("Funafuti", "Pacific/Funafuti");
        tzMap.put("GMT", "Etc/GMT");
        tzMap.put("GMT+0", "Etc/GMT+0");
        tzMap.put("GMT+1", "Etc/GMT+1");
        tzMap.put("GMT+10", "Etc/GMT+10");
        tzMap.put("GMT+11", "Etc/GMT+11");
        tzMap.put("GMT+12", "Etc/GMT+12");
        tzMap.put("GMT+2", "Etc/GMT+2");
        tzMap.put("GMT+3", "Etc/GMT+3");
        tzMap.put("GMT+4", "Etc/GMT+4");
        tzMap.put("GMT+5", "Etc/GMT+5");
        tzMap.put("GMT+6", "Etc/GMT+6");
        tzMap.put("GMT+7", "Etc/GMT+7");
        tzMap.put("GMT+8", "Etc/GMT+8");
        tzMap.put("GMT+9", "Etc/GMT+9");
        tzMap.put("GMT-0", "Etc/GMT-0");
        tzMap.put("GMT-1", "Etc/GMT-1");
        tzMap.put("GMT-10", "Etc/GMT-10");
        tzMap.put("GMT-11", "Etc/GMT-11");
        tzMap.put("GMT-12", "Etc/GMT-12");
        tzMap.put("GMT-13", "Etc/GMT-13");
        tzMap.put("GMT-14", "Etc/GMT-14");
        tzMap.put("GMT-2", "Etc/GMT-2");
        tzMap.put("GMT-3", "Etc/GMT-3");
        tzMap.put("GMT-4", "Etc/GMT-4");
        tzMap.put("GMT-5", "Etc/GMT-5");
        tzMap.put("GMT-6", "Etc/GMT-6");
        tzMap.put("GMT-7", "Etc/GMT-7");
        tzMap.put("GMT-8", "Etc/GMT-8");
        tzMap.put("GMT-9", "Etc/GMT-9");
        tzMap.put("GMT0", "Etc/GMT0");
        tzMap.put("Gaborone", "Africa/Gaborone");
        tzMap.put("Galápagos Islands", "Pacific/Galapagos");
        tzMap.put("Gambier", "Pacific/Gambier");
        tzMap.put("Gaza", "Asia/Gaza");
        tzMap.put("General", "Mexico/General");
        tzMap.put("Gibraltar", "Europe/Gibraltar");
        tzMap.put("Glace Bay", "America/Glace_Bay");
        tzMap.put("Godthab", "America/Godthab");
        tzMap.put("Goose Bay", "America/Goose_Bay");
        tzMap.put("Grand Turk", "America/Grand_Turk");
        tzMap.put("Greenwich", "Etc/Greenwich");
        tzMap.put("Grenada", "America/Grenada");
        tzMap.put("Guadalcanal", "Pacific/Guadalcanal");
        tzMap.put("Guadeloupe", "America/Guadeloupe");
        tzMap.put("Guam", "Pacific/Guam");
        tzMap.put("Guatemala", "America/Guatemala");
        tzMap.put("Guayaquil", "America/Guayaquil");
        tzMap.put("Guernsey", "Europe/Guernsey");
        tzMap.put("Guyana", "America/Guyana");
        tzMap.put("Halifax", "America/Halifax");
        tzMap.put("Harare", "Africa/Harare");
        tzMap.put("Harbin", "Asia/Harbin");
        tzMap.put("Havana", "America/Havana");
        tzMap.put("Hawaii", "US/Hawaii");
        tzMap.put("Hebron", "Asia/Hebron");
        tzMap.put("Helsinki", "Europe/Helsinki");
        tzMap.put("Hermosillo", "America/Hermosillo");
        tzMap.put("Ho Chi Minh", "Asia/Ho_Chi_Minh");
        tzMap.put("Ho Chi Minh City", "Asia/Saigon");
        tzMap.put("Hobart", "Australia/Hobart");
        tzMap.put("Hong Kong", "Asia/Hong_Kong");
        tzMap.put("Honolulu", "Pacific/Honolulu");
        tzMap.put("Hovd", "Asia/Hovd");
        tzMap.put("Indiana-Starke", "US/Indiana-Starke");
        tzMap.put("Indianapolis", "America/Indianapolis");
        tzMap.put("Inuvik", "America/Inuvik");
        tzMap.put("Iqaluit", "America/Iqaluit");
        tzMap.put("Irkutsk", "Asia/Irkutsk");
        tzMap.put("Isle of Man", "Europe/Isle_of_Man");
        tzMap.put("Istanbul", "Europe/Istanbul");
        tzMap.put("Jakarta", "Asia/Jakarta");
        tzMap.put("Jamaica", "America/Jamaica");
        tzMap.put("Jan Mayen", "Atlantic/Jan_Mayen");
        tzMap.put("Jayapura", "Asia/Jayapura");
        tzMap.put("Jersey", "Europe/Jersey");
        tzMap.put("Jerusalem", "Asia/Jerusalem");
        tzMap.put("Johannesburg", "Africa/Johannesburg");
        tzMap.put("Johnston", "Pacific/Johnston");
        tzMap.put("Juba", "Africa/Juba");
        tzMap.put("Jujuy", "America/Jujuy");
        tzMap.put("Juneau", "America/Juneau");
        tzMap.put("Kabul", "Asia/Kabul");
        tzMap.put("Kaliningrad", "Europe/Kaliningrad");
        tzMap.put("Kamchatka", "Asia/Kamchatka");
        tzMap.put("Kampala", "Africa/Kampala");
        tzMap.put("Kanton", "Pacific/Kanton");
        tzMap.put("Karachi", "Asia/Karachi");
        tzMap.put("Kashgar", "Asia/Kashgar");
        tzMap.put("Kathmandu", "Asia/Kathmandu");
        tzMap.put("Katmandu", "Asia/Katmandu");
        tzMap.put("Kerguelen Islands", "Indian/Kerguelen");
        tzMap.put("Khandyga", "Asia/Khandyga");
        tzMap.put("Khartoum", "Africa/Khartoum");
        tzMap.put("Kiev", "Europe/Kiev");
        tzMap.put("Kigali", "Africa/Kigali");
        tzMap.put("Kinshasa", "Africa/Kinshasa");
        tzMap.put("Kiritimati", "Pacific/Kiritimati");
        tzMap.put("Kirov", "Europe/Kirov");
        tzMap.put("Knox", "America/Indiana/Knox");
        tzMap.put("Knox IN", "America/Knox_IN");
        tzMap.put("Kolkata", "Asia/Kolkata");
        tzMap.put("Kosrae", "Pacific/Kosrae");
        tzMap.put("Kostanay", "Asia/Qostanay");
        tzMap.put("Kralendijk", "America/Kralendijk");
        tzMap.put("Krasnoyarsk", "Asia/Krasnoyarsk");
        tzMap.put("Kuala Lumpur", "Asia/Kuala_Lumpur");
        tzMap.put("Kuching", "Asia/Kuching");
        tzMap.put("Kuwait", "Asia/Kuwait");
        tzMap.put("Kwajalein Atoll", "Pacific/Kwajalein");
        tzMap.put("Kyiv", "Europe/Kyiv");
        tzMap.put("Kyzylorda", "Asia/Qyzylorda");
        tzMap.put("LHI", "Australia/LHI");
        tzMap.put("La Paz", "America/La_Paz");
        tzMap.put("La Rioja", "America/Argentina/La_Rioja");
        tzMap.put("Lagos", "Africa/Lagos");
        tzMap.put("Libreville", "Africa/Libreville");
        tzMap.put("Lima", "America/Lima");
        tzMap.put("Lindeman", "Australia/Lindeman");
        tzMap.put("Lisbon", "Europe/Lisbon");
        tzMap.put("Ljubljana", "Europe/Ljubljana");
        tzMap.put("Lome", "Africa/Lome");
        tzMap.put("London", "Europe/London");
        tzMap.put("Longyearbyen", "Arctic/Longyearbyen");
        tzMap.put("Lord Howe Island", "Australia/Lord_Howe");
        tzMap.put("Los Angeles", "America/Los_Angeles");
        tzMap.put("Louisville", "America/Louisville");
        tzMap.put("Lower Princes", "America/Lower_Princes");
        tzMap.put("Luanda", "Africa/Luanda");
        tzMap.put("Lubumbashi", "Africa/Lubumbashi");
        tzMap.put("Lusaka", "Africa/Lusaka");
        tzMap.put("Luxembourg", "Europe/Luxembourg");
        tzMap.put("Macao", "Asia/Macao");
        tzMap.put("Macau", "Asia/Macau");
        tzMap.put("Maceio", "America/Maceio");
        tzMap.put("Macquarie Island", "Antarctica/Macquarie");
        tzMap.put("Madeira", "Atlantic/Madeira");
        tzMap.put("Madrid", "Europe/Madrid");
        tzMap.put("Magadan", "Asia/Magadan");
        tzMap.put("Mahe", "Indian/Mahe");
        tzMap.put("Majuro", "Pacific/Majuro");
        tzMap.put("Makassar", "Asia/Makassar");
        tzMap.put("Malabo", "Africa/Malabo");
        tzMap.put("Maldives", "Indian/Maldives");
        tzMap.put("Malta", "Europe/Malta");
        tzMap.put("Managua", "America/Managua");
        tzMap.put("Manaus", "America/Manaus");
        tzMap.put("Manila", "Asia/Manila");
        tzMap.put("Maputo", "Africa/Maputo");
        tzMap.put("Marengo", "America/Indiana/Marengo");
        tzMap.put("Mariehamn", "Europe/Mariehamn");
        tzMap.put("Marigot", "America/Marigot");
        tzMap.put("Marquesas Islands", "Pacific/Marquesas");
        tzMap.put("Martinique", "America/Martinique");
        tzMap.put("Maseru", "Africa/Maseru");
        tzMap.put("Matamoros", "America/Matamoros");
        tzMap.put("Mauritius", "Indian/Mauritius");
        tzMap.put("Mawson Station", "Antarctica/Mawson");
        tzMap.put("Mayotte", "Indian/Mayotte");
        tzMap.put("Mazatlan", "America/Mazatlan");
        tzMap.put("Mbabane", "Africa/Mbabane");
        tzMap.put("McMurdo Station", "Antarctica/McMurdo");
        tzMap.put("Melbourne", "Australia/Melbourne");
        tzMap.put("Mendoza", "America/Mendoza");
        tzMap.put("Menominee", "America/Menominee");
        tzMap.put("Merida", "America/Merida");
        tzMap.put("Metlakatla", "America/Metlakatla");
        tzMap.put("Mexico City", "America/Mexico_City");
        tzMap.put("Michigan", "US/Michigan");
        tzMap.put("Midway Atoll", "Pacific/Midway");
        tzMap.put("Minsk", "Europe/Minsk");
        tzMap.put("Miquelon", "America/Miquelon");
        tzMap.put("Mogadishu", "Africa/Mogadishu");
        tzMap.put("Monaco", "Europe/Monaco");
        tzMap.put("Moncton", "America/Moncton");
        tzMap.put("Monrovia", "Africa/Monrovia");
        tzMap.put("Monterrey", "America/Monterrey");
        tzMap.put("Montevideo", "America/Montevideo");
        tzMap.put("Monticello", "America/Kentucky/Monticello");
        tzMap.put("Montreal", "America/Montreal");
        tzMap.put("Montserrat", "America/Montserrat");
        tzMap.put("Moscow", "Europe/Moscow");
        tzMap.put("Mountain", "US/Mountain");
        tzMap.put("Muscat", "Asia/Muscat");
        tzMap.put("NSW", "Australia/NSW");
        tzMap.put("Nairobi", "Africa/Nairobi");
        tzMap.put("Nassau", "America/Nassau");
        tzMap.put("Nauru", "Pacific/Nauru");
        tzMap.put("Ndjamena", "Africa/Ndjamena");
        tzMap.put("New Salem", "America/North_Dakota/New_Salem");
        tzMap.put("New York", "America/New_York");
        tzMap.put("Newfoundland", "Canada/Newfoundland");
        tzMap.put("Niamey", "Africa/Niamey");
        tzMap.put("Nicosia", "Europe/Nicosia");
        tzMap.put("Nipigon", "America/Nipigon");
        tzMap.put("Niue", "Pacific/Niue");
        tzMap.put("Nome", "America/Nome");
        tzMap.put("Norfolk Island", "Pacific/Norfolk");
        tzMap.put("Noronha", "America/Noronha");
        tzMap.put("North", "Australia/North");
        tzMap.put("Nouakchott", "Africa/Nouakchott");
        tzMap.put("Noumea", "Pacific/Noumea");
        tzMap.put("Novokuznetsk", "Asia/Novokuznetsk");
        tzMap.put("Novosibirsk", "Asia/Novosibirsk");
        tzMap.put("Nuuk", "America/Nuuk");
        tzMap.put("Ojinaga", "America/Ojinaga");
        tzMap.put("Omsk", "Asia/Omsk");
        tzMap.put("Oral", "Asia/Oral");
        tzMap.put("Oslo", "Europe/Oslo");
        tzMap.put("Ouagadougou", "Africa/Ouagadougou");
        tzMap.put("Pacific", "US/Pacific");
        tzMap.put("Pacific-New", "US/Pacific-New");
        tzMap.put("Pago Pago", "Pacific/Pago_Pago");
        tzMap.put("Palau", "Pacific/Palau");
        tzMap.put("Palmer Land", "Antarctica/Palmer");
        tzMap.put("Panama", "America/Panama");
        tzMap.put("Pangnirtung", "America/Pangnirtung");
        tzMap.put("Paramaribo", "America/Paramaribo");
        tzMap.put("Paris", "Europe/Paris");
        tzMap.put("Perth", "Australia/Perth");
        tzMap.put("Petersburg", "America/Indiana/Petersburg");
        tzMap.put("Phnom Penh", "Asia/Phnom_Penh");
        tzMap.put("Phoenix", "America/Phoenix");
        tzMap.put("Pitcairn Islands", "Pacific/Pitcairn");
        tzMap.put("Podgorica", "Europe/Podgorica");
        tzMap.put("Pohnpei", "Pacific/Pohnpei");
        tzMap.put("Ponape", "Pacific/Ponape");
        tzMap.put("Pontianak", "Asia/Pontianak");
        tzMap.put("Port Moresby", "Pacific/Port_Moresby");
        tzMap.put("Port of Spain", "America/Port_of_Spain");
        tzMap.put("Port-au-Prince", "America/Port-au-Prince");
        tzMap.put("Porto Acre", "America/Porto_Acre");
        tzMap.put("Porto Velho", "America/Porto_Velho");
        tzMap.put("Porto-Novo", "Africa/Porto-Novo");
        tzMap.put("Prague", "Europe/Prague");
        tzMap.put("Puerto Rico", "America/Puerto_Rico");
        tzMap.put("Punta Arenas", "America/Punta_Arenas");
        tzMap.put("Pyongyang", "Asia/Pyongyang");
        tzMap.put("Qatar", "Asia/Qatar");
        tzMap.put("Queensland", "Australia/Queensland");
        tzMap.put("Rainy River", "America/Rainy_River");
        tzMap.put("Rangoon", "Asia/Rangoon");
        tzMap.put("Rankin Inlet", "America/Rankin_Inlet");
        tzMap.put("Rarotonga", "Pacific/Rarotonga");
        tzMap.put("Recife", "America/Recife");
        tzMap.put("Regina", "America/Regina");
        tzMap.put("Resolute", "America/Resolute");
        tzMap.put("Reunion", "Indian/Reunion");
        tzMap.put("Reykjavik", "Atlantic/Reykjavik");
        tzMap.put("Riga", "Europe/Riga");
        tzMap.put("Rio Branco", "America/Rio_Branco");
        tzMap.put("Rio Gallegos", "America/Argentina/Rio_Gallegos");
        tzMap.put("Riyadh", "Asia/Riyadh");
        tzMap.put("Rome", "Europe/Rome");
        tzMap.put("Rosario", "America/Rosario");
        tzMap.put("Rothera Station", "Antarctica/Rothera");
        tzMap.put("Saipan", "Pacific/Saipan");
        tzMap.put("Sakhalin", "Asia/Sakhalin");
        tzMap.put("Salta", "America/Argentina/Salta");
        tzMap.put("Samara", "Europe/Samara");
        tzMap.put("Samarkand", "Asia/Samarkand");
        tzMap.put("Samoa", "US/Samoa");
        tzMap.put("San Juan", "America/Argentina/San_Juan");
        tzMap.put("San Luis", "America/Argentina/San_Luis");
        tzMap.put("San Marino", "Europe/San_Marino");
        tzMap.put("Santa Isabel", "America/Santa_Isabel");
        tzMap.put("Santarem", "America/Santarem");
        tzMap.put("Santiago", "America/Santiago");
        tzMap.put("Santo Domingo", "America/Santo_Domingo");
        tzMap.put("Sao Paulo", "America/Sao_Paulo");
        tzMap.put("Sao Tome", "Africa/Sao_Tome");
        tzMap.put("Sarajevo", "Europe/Sarajevo");
        tzMap.put("Saratov", "Europe/Saratov");
        tzMap.put("Saskatchewan", "Canada/Saskatchewan");
        tzMap.put("Scoresbysund", "America/Scoresbysund");
        tzMap.put("Seoul", "Asia/Seoul");
        tzMap.put("Shanghai", "Asia/Shanghai");
        tzMap.put("Shiprock", "America/Shiprock");
        tzMap.put("Showa Station", "Antarctica/Syowa");
        tzMap.put("Simferopol", "Europe/Simferopol");
        tzMap.put("Singapore", "Asia/Singapore");
        tzMap.put("Sitka", "America/Sitka");
        tzMap.put("Skopje", "Europe/Skopje");
        tzMap.put("Sofia", "Europe/Sofia");
        tzMap.put("South", "Australia/South");
        tzMap.put("South Georgia", "Atlantic/South_Georgia");
        tzMap.put("South Pole", "Antarctica/South_Pole");
        tzMap.put("Srednekolymsk", "Asia/Srednekolymsk");
        tzMap.put("St Barthelemy", "America/St_Barthelemy");
        tzMap.put("St Helena", "Atlantic/St_Helena");
        tzMap.put("St Johns", "America/St_Johns");
        tzMap.put("St Kitts", "America/St_Kitts");
        tzMap.put("St Lucia", "America/St_Lucia");
        tzMap.put("St Thomas", "America/St_Thomas");
        tzMap.put("St Vincent", "America/St_Vincent");
        tzMap.put("Stanley", "Atlantic/Stanley");
        tzMap.put("Stockholm", "Europe/Stockholm");
        tzMap.put("Swift Current", "America/Swift_Current");
        tzMap.put("Sydney", "Australia/Sydney");
        tzMap.put("Tahiti", "Pacific/Tahiti");
        tzMap.put("Taipei", "Asia/Taipei");
        tzMap.put("Tallinn", "Europe/Tallinn");
        tzMap.put("Tarawa", "Pacific/Tarawa");
        tzMap.put("Tashkent", "Asia/Tashkent");
        tzMap.put("Tasmania", "Australia/Tasmania");
        tzMap.put("Tbilisi", "Asia/Tbilisi");
        tzMap.put("Tegucigalpa", "America/Tegucigalpa");
        tzMap.put("Tehran", "Asia/Tehran");
        tzMap.put("Tel Aviv", "Asia/Tel_Aviv");
        tzMap.put("Tell City", "America/Indiana/Tell_City");
        tzMap.put("Thimbu", "Asia/Thimbu");
        tzMap.put("Thimphu", "Asia/Thimphu");
        tzMap.put("Thule", "America/Thule");
        tzMap.put("Thunder Bay", "America/Thunder_Bay");
        tzMap.put("Tijuana", "America/Tijuana");
        tzMap.put("Timbuktu", "Africa/Timbuktu");
        tzMap.put("Tirane", "Europe/Tirane");
        tzMap.put("Tiraspol", "Europe/Tiraspol");
        tzMap.put("Tokyo", "Asia/Tokyo");
        tzMap.put("Tomsk", "Asia/Tomsk");
        tzMap.put("Tongatapu", "Pacific/Tongatapu");
        tzMap.put("Toronto", "America/Toronto");
        tzMap.put("Tortola", "America/Tortola");
        tzMap.put("Tripoli", "Africa/Tripoli");
        tzMap.put("Troll Station", "Antarctica/Troll");
        tzMap.put("Truk", "Pacific/Truk");
        tzMap.put("Tucuman", "America/Argentina/Tucuman");
        tzMap.put("Tunis", "Africa/Tunis");
        tzMap.put("UCT", "Etc/UCT");
        tzMap.put("Ujung Pandang", "Asia/Ujung_Pandang");
        tzMap.put("Ulaanbaatar", "Asia/Ulaanbaatar");
        tzMap.put("Ulan Bator", "Asia/Ulan_Bator");
        tzMap.put("Ulyanovsk", "Europe/Ulyanovsk");
        tzMap.put("Universal", "Etc/Universal");
        tzMap.put("Unknown Location", "Etc/Unknown");
        tzMap.put("Urumqi", "Asia/Urumqi");
        tzMap.put("Ushuaia", "America/Argentina/Ushuaia");
        tzMap.put("Ust-Nera", "Asia/Ust-Nera");
        tzMap.put("Uzhgorod", "Europe/Uzhgorod");
        tzMap.put("Vaduz", "Europe/Vaduz");
        tzMap.put("Vancouver", "America/Vancouver");
        tzMap.put("Vatican", "Europe/Vatican");
        tzMap.put("Vevay", "America/Indiana/Vevay");
        tzMap.put("Victoria", "Australia/Victoria");
        tzMap.put("Vienna", "Europe/Vienna");
        tzMap.put("Vientiane", "Asia/Vientiane");
        tzMap.put("Vilnius", "Europe/Vilnius");
        tzMap.put("Vincennes", "America/Indiana/Vincennes");
        tzMap.put("Virgin", "America/Virgin");
        tzMap.put("Vladivostok", "Asia/Vladivostok");
        tzMap.put("Volgograd", "Europe/Volgograd");
        tzMap.put("Vostok Station", "Antarctica/Vostok");
        tzMap.put("Wake Island", "Pacific/Wake");
        tzMap.put("Wallis", "Pacific/Wallis");
        tzMap.put("Warsaw", "Europe/Warsaw");
        tzMap.put("West", "Brazil/West");
        tzMap.put("Whitehorse", "America/Whitehorse");
        tzMap.put("Winamac", "America/Indiana/Winamac");
        tzMap.put("Windhoek", "Africa/Windhoek");
        tzMap.put("Winnipeg", "America/Winnipeg");
        tzMap.put("Yakutat", "America/Yakutat");
        tzMap.put("Yakutsk", "Asia/Yakutsk");
        tzMap.put("Yancowinna", "Australia/Yancowinna");
        tzMap.put("Yangon", "Asia/Yangon");
        tzMap.put("Yap", "Pacific/Yap");
        tzMap.put("Yekaterinburg", "Asia/Yekaterinburg");
        tzMap.put("Yellowknife", "America/Yellowknife");
        tzMap.put("Yerevan", "Asia/Yerevan");
        tzMap.put("Yukon", "Canada/Yukon");
        tzMap.put("Zagreb", "Europe/Zagreb");
        tzMap.put("Zaporozhye", "Europe/Zaporozhye");
        tzMap.put("Zulu", "Etc/Zulu");
        tzMap.put("Zurich", "Europe/Zurich");

        // Time zone designations [Unicode CLDR]:
        tzMap.put("1 hour ahead of UTC", "Etc/GMT-1");
        tzMap.put("1 hour behind UTC", "Etc/GMT+1");
        tzMap.put("10 hours ahead of UTC", "Etc/GMT-10");
        tzMap.put("10 hours behind UTC", "Etc/GMT+10");
        tzMap.put("11 hours ahead of UTC", "Etc/GMT-11");
        tzMap.put("11 hours behind UTC", "Etc/GMT+11");
        tzMap.put("12 hours ahead of UTC", "Etc/GMT-12");
        tzMap.put("12 hours behind UTC", "Etc/GMT+12");
        tzMap.put("13 hours ahead of UTC", "Etc/GMT-13");
        tzMap.put("14 hours ahead of UTC", "Etc/GMT-14");
        tzMap.put("2 hours ahead of UTC", "Etc/GMT-2");
        tzMap.put("2 hours behind UTC", "Etc/GMT+2");
        tzMap.put("3 hours ahead of UTC", "Etc/GMT-3");
        tzMap.put("3 hours behind UTC", "Etc/GMT+3");
        tzMap.put("4 hours ahead of UTC", "Etc/GMT-4");
        tzMap.put("4 hours behind UTC", "Etc/GMT+4");
        tzMap.put("5 hours ahead of UTC", "Etc/GMT-5");
        tzMap.put("5 hours behind UTC", "Etc/GMT+5");
        tzMap.put("6 hours ahead of UTC", "Etc/GMT-6");
        tzMap.put("6 hours behind UTC", "Etc/GMT+6");
        tzMap.put("7 hours ahead of UTC", "Etc/GMT-7");
        tzMap.put("7 hours behind UTC", "Etc/GMT+7");
        tzMap.put("8 hours ahead of UTC", "Etc/GMT-8");
        tzMap.put("8 hours behind UTC", "Etc/GMT+8");
        tzMap.put("9 hours ahead of UTC", "Etc/GMT-9");
        tzMap.put("9 hours behind UTC", "Etc/GMT+9");
        tzMap.put("Abidjan, Côte d'Ivoire", "Africa/Abidjan");
        tzMap.put("Accra, Ghana", "Africa/Accra");
        tzMap.put("Adak (Alaska), United States", "America/Adak");
        tzMap.put("Addis Ababa, Ethiopia", "Africa/Addis_Ababa");
        tzMap.put("Adelaide, Australia", "Australia/Adelaide");
        tzMap.put("Aden, Yemen", "Asia/Aden");
        tzMap.put("Algiers, Algeria", "Africa/Algiers");
        tzMap.put("Almaty, Kazakhstan", "Asia/Almaty");
        tzMap.put("Amman, Jordan", "Asia/Amman");
        tzMap.put("Amsterdam, Netherlands", "Europe/Amsterdam");
        tzMap.put("Anadyr, Russia", "Asia/Anadyr");
        tzMap.put("Anchorage, United States", "America/Anchorage");
        tzMap.put("Antananarivo, Madagascar", "Indian/Antananarivo");
        tzMap.put("Apia, Samoa", "Pacific/Apia");
        tzMap.put("Aqtau, Kazakhstan", "Asia/Aqtau");
        tzMap.put("Aqtobe, Kazakhstan", "Asia/Aqtobe");
        tzMap.put("Araguaína, Brazil", "America/Araguaina");
        tzMap.put("Ashgabat, Turkmenistan", "Asia/Ashgabat");
        tzMap.put("Asmara, Eritrea", "Africa/Asmera");
        tzMap.put("Astrakhan, Russia", "Europe/Astrakhan");
        tzMap.put("Asunción, Paraguay", "America/Asuncion");
        tzMap.put("Athens, Greece", "Europe/Athens");
        tzMap.put("Atikokan, Canada", "America/Coral_Harbour");
        tzMap.put("Atyrau (Guryev), Kazakhstan", "Asia/Atyrau");
        tzMap.put("Auckland, New Zealand", "Pacific/Auckland");
        tzMap.put("Aysén Region, Chile", "America/Coyhaique");
        tzMap.put("Azores, Portugal", "Atlantic/Azores");
        tzMap.put("Baghdad, Iraq", "Asia/Baghdad");
        tzMap.put("Bahia, Brazil", "America/Bahia");
        tzMap.put("Bahía de Banderas, Mexico", "America/Bahia_Banderas");
        tzMap.put("Baku, Azerbaijan", "Asia/Baku");
        tzMap.put("Bamako, Mali", "Africa/Bamako");
        tzMap.put("Bangkok, Thailand", "Asia/Bangkok");
        tzMap.put("Bangui, Central African Republic", "Africa/Bangui");
        tzMap.put("Banjul, Gambia", "Africa/Banjul");
        tzMap.put("Barnaul, Russia", "Asia/Barnaul");
        tzMap.put("Beirut, Lebanon", "Asia/Beirut");
        tzMap.put("Belgrade, Serbia", "Europe/Belgrade");
        tzMap.put("Belém, Brazil", "America/Belem");
        tzMap.put("Berlin, Germany", "Europe/Berlin");
        tzMap.put("Beulah (North Dakota), United States", "America/North_Dakota/Beulah");
        tzMap.put("Bishkek, Kyrgyzstan", "Asia/Bishkek");
        tzMap.put("Bissau, Guinea-Bissau", "Africa/Bissau");
        tzMap.put("Blanc-Sablon, Canada", "America/Blanc-Sablon");
        tzMap.put("Blantyre, Malawi", "Africa/Blantyre");
        tzMap.put("Boa Vista, Brazil", "America/Boa_Vista");
        tzMap.put("Bogotá, Colombia", "America/Bogota");
        tzMap.put("Boise (Idaho), United States", "America/Boise");
        tzMap.put("Bonaire, Sint Estatius and Saba", "America/Kralendijk");
        tzMap.put("Bougainville, Papua New Guinea", "Pacific/Bougainville");
        tzMap.put("Bratislava, Slovakia", "Europe/Bratislava");
        tzMap.put("Brazzaville, Republic of the Congo", "Africa/Brazzaville");
        tzMap.put("Brisbane, Australia", "Australia/Brisbane");
        tzMap.put("British Summer Time", "Europe/London");
        tzMap.put("Broken Hill, Australia", "Australia/Broken_Hill");
        tzMap.put("Brussels, Belgium", "Europe/Brussels");
        tzMap.put("Bucharest, Romania", "Europe/Bucharest");
        tzMap.put("Budapest, Hungary", "Europe/Budapest");
        tzMap.put("Buenos Aires, Argentina", "America/Buenos_Aires");
        tzMap.put("Bujumbura, Burundi", "Africa/Bujumbura");
        tzMap.put("Busingen, Germany", "Europe/Busingen");
        tzMap.put("Cairo, Egypt", "Africa/Cairo");
        tzMap.put("Cambridge Bay, Canada", "America/Cambridge_Bay");
        tzMap.put("Campo Grande, Brazil", "America/Campo_Grande");
        tzMap.put("Canary Islands, Spain", "Atlantic/Canary");
        tzMap.put("Cancún, Mexico", "America/Cancun");
        tzMap.put("Caracas, Venezuela", "America/Caracas");
        tzMap.put("Casablanca, Morocco", "Africa/Casablanca");
        tzMap.put("Casey Station, Bailey Peninsula", "Antarctica/Casey");
        tzMap.put("Catamarca, Argentina", "America/Catamarca");
        tzMap.put("Cayenne, French Guiana", "America/Cayenne");
        tzMap.put("Cayman Islands", "America/Cayman");
        tzMap.put("Center (North Dakota), United States", "America/North_Dakota/Center");
        tzMap.put("Ceuta, Spain", "Africa/Ceuta");
        tzMap.put("Chatham Islands, New Zealand", "Pacific/Chatham");
        tzMap.put("Chicago, United States", "America/Chicago");
        tzMap.put("Chihuahua, Mexico", "America/Chihuahua");
        tzMap.put("Chita Zabaykalsky, Russia", "Asia/Chita");
        tzMap.put("Chişinău, Moldova", "Europe/Chisinau");
        tzMap.put("Chuuk, Micronesia", "Pacific/Truk");
        tzMap.put("Ciudad Juárez, Mexico", "America/Ciudad_Juarez");
        tzMap.put("Cocos (Keeling) Islands", "Indian/Cocos");
        tzMap.put("Colombo, Sri Lanka", "Asia/Colombo");
        tzMap.put("Conakry, Guinea", "Africa/Conakry");
        tzMap.put("Coordinated Universal Time", "Etc/UTC");
        tzMap.put("Copenhagen, Denmark", "Europe/Copenhagen");
        tzMap.put("Creston, Canada", "America/Creston");
        tzMap.put("Cuiabá, Brazil", "America/Cuiaba");
        tzMap.put("Curaçao", "America/Curacao");
        tzMap.put("Córdoba, Argentina", "America/Cordoba");
        tzMap.put("Dakar, Senegal", "Africa/Dakar");
        tzMap.put("Damascus, Syria", "Asia/Damascus");
        tzMap.put("Danmarkshavn, Greenland", "America/Danmarkshavn");
        tzMap.put("Dar es Salaam, Tanzania", "Africa/Dar_es_Salaam");
        tzMap.put("Darwin, Australia", "Australia/Darwin");
        tzMap.put("Davis Station, Vestfold Hills", "Antarctica/Davis");
        tzMap.put("Dawson Creek, Canada", "America/Dawson_Creek");
        tzMap.put("Dawson, Canada", "America/Dawson");
        tzMap.put("Denver, United States", "America/Denver");
        tzMap.put("Detroit, United States", "America/Detroit");
        tzMap.put("Dhaka, Bangladesh", "Asia/Dhaka");
        tzMap.put("Dili, East Timor", "Asia/Dili");
        tzMap.put("Douala, Cameroon", "Africa/Douala");
        tzMap.put("Dubai, United Arab Emirates", "Asia/Dubai");
        tzMap.put("Dublin, Ireland", "Europe/Dublin");
        tzMap.put("Dumont d'Urville Station, Terre Adélie", "Antarctica/DumontDUrville");
        tzMap.put("Dushanbe, Tajikistan", "Asia/Dushanbe");
        tzMap.put("Easter Island, Chile", "Pacific/Easter");
        tzMap.put("Edmonton, Canada", "America/Edmonton");
        tzMap.put("Efate, Vanuatu", "Pacific/Efate");
        tzMap.put("Eirunepé, Brazil", "America/Eirunepe");
        tzMap.put("El Aaiún, Western Sahara", "Africa/El_Aaiun");
        tzMap.put("Enderbury Island, Kiribati", "Pacific/Enderbury");
        tzMap.put("Eucla, Australia", "Australia/Eucla");
        tzMap.put("Fakaofo, Tokelau", "Pacific/Fakaofo");
        tzMap.put("Famagusta, Cyprus", "Asia/Famagusta");
        tzMap.put("Faroe Islands", "Atlantic/Faeroe");
        tzMap.put("Fernando de Noronha, Brazil", "America/Noronha");
        tzMap.put("Fort Nelson, Canada", "America/Fort_Nelson");
        tzMap.put("Fortaleza, Brazil", "America/Fortaleza");
        tzMap.put("Freetown, Sierra Leone", "Africa/Freetown");
        tzMap.put("Funafuti, Tuvalu", "Pacific/Funafuti");
        tzMap.put("Gaborone, Botswana", "Africa/Gaborone");
        tzMap.put("Galápagos Islands, Ecuador", "Pacific/Galapagos");
        tzMap.put("Gambiera Islands, French Polynesia", "Pacific/Gambier");
        tzMap.put("Gaza Strip, Palestinian Territories", "Asia/Gaza");
        tzMap.put("Glace Bay, Canada", "America/Glace_Bay");
        tzMap.put("Goose Bay, Canada", "America/Goose_Bay");
        tzMap.put("Grand Turk, Turks and Caicos Islands", "America/Grand_Turk");
        tzMap.put("Greenwich Mean Time", "Etc/GMT");
        tzMap.put("Guadalcanal, Solomon Islands", "Pacific/Guadalcanal");
        tzMap.put("Guayaquil, Ecuador", "America/Guayaquil");
        tzMap.put("Halifax, Canada", "America/Halifax");
        tzMap.put("Harare, Zimbabwe", "Africa/Harare");
        tzMap.put("Havana, Cuba", "America/Havana");
        tzMap.put("Helsinki, Finland", "Europe/Helsinki");
        tzMap.put("Hermosillo, Mexico", "America/Hermosillo");
        tzMap.put("Ho Chi Minh City, Vietnam", "Asia/Saigon");
        tzMap.put("Hobart, Australia", "Australia/Hobart");
        tzMap.put("Hong Kong SAR China", "Asia/Hong_Kong");
        tzMap.put("Honolulu, United States", "Pacific/Honolulu");
        tzMap.put("Indianapolis, United States", "America/Indianapolis");
        tzMap.put("Inuvik, Canada", "America/Inuvik");
        tzMap.put("Iqaluit, Canada", "America/Iqaluit");
        tzMap.put("Irish Standard Time", "Europe/Dublin");
        tzMap.put("Irkutsk, Russia", "Asia/Irkutsk");
        tzMap.put("Istanbul, Türkiye", "Europe/Istanbul");
        tzMap.put("Ittoqqortoormiit (Scoresbysund), Greenland", "America/Scoresbysund");
        tzMap.put("Jakarta, Indonesia", "Asia/Jakarta");
        tzMap.put("Jayapura, Indonesia", "Asia/Jayapura");
        tzMap.put("Johannesburg, South Africa", "Africa/Johannesburg");
        tzMap.put("Juba, South Sudan", "Africa/Juba");
        tzMap.put("Jujuy, Argentina", "America/Jujuy");
        tzMap.put("Juneau (Alaska), United States", "America/Juneau");
        tzMap.put("Kabul, Afghanistan", "Asia/Kabul");
        tzMap.put("Kaliningrad, Russia", "Europe/Kaliningrad");
        tzMap.put("Kamchatka Peninsula, Russia", "Asia/Kamchatka");
        tzMap.put("Kampala, Uganda", "Africa/Kampala");
        tzMap.put("Karachi, Pakistan", "Asia/Karachi");
        tzMap.put("Kathmandu, Nepal", "Asia/Katmandu");
        tzMap.put("Kerguelen Islands, French Southern Territories", "Indian/Kerguelen");
        tzMap.put("Khandyga Tomponsky, Russia", "Asia/Khandyga");
        tzMap.put("Khartoum, Sudan", "Africa/Khartoum");
        tzMap.put("Khovd (Hovd), Mongolia", "Asia/Hovd");
        tzMap.put("Kigali, Rwanda", "Africa/Kigali");
        tzMap.put("Kinshasa, Democratic Republic of the Congo", "Africa/Kinshasa");
        tzMap.put("Kiritimati, Kiribati", "Pacific/Kiritimati");
        tzMap.put("Kirov, Russia", "Europe/Kirov");
        tzMap.put("Knox (Indiana), United States", "America/Indiana/Knox");
        tzMap.put("Kolkata, India", "Asia/Calcutta");
        tzMap.put("Kosrae, Micronesia", "Pacific/Kosrae");
        tzMap.put("Krasnoyarsk, Russia", "Asia/Krasnoyarsk");
        tzMap.put("Kuala Lumpur, Malaysia", "Asia/Kuala_Lumpur");
        tzMap.put("Kuching, Malaysia", "Asia/Kuching");
        tzMap.put("Kwajalein, Marshall Islands", "Pacific/Kwajalein");
        tzMap.put("Kyiv, Ukraine", "Europe/Kiev");
        tzMap.put("Kyzylorda, Kazakhstan", "Asia/Qyzylorda");
        tzMap.put("La Paz, Bolivia", "America/La_Paz");
        tzMap.put("La Rioja, Argentina", "America/Argentina/La_Rioja");
        tzMap.put("Lagos, Nigeria", "Africa/Lagos");
        tzMap.put("Libreville, Gabon", "Africa/Libreville");
        tzMap.put("Lima, Peru", "America/Lima");
        tzMap.put("Lindeman Island, Australia", "Australia/Lindeman");
        tzMap.put("Lisbon, Portugal", "Europe/Lisbon");
        tzMap.put("Ljubljana, Slovenia", "Europe/Ljubljana");
        tzMap.put("Lomé, Togo", "Africa/Lome");
        tzMap.put("London, United Kingdom", "Europe/London");
        tzMap.put("Longyearbyen, Svalbard", "Arctic/Longyearbyen");
        tzMap.put("Lord Howe Island, Australia", "Australia/Lord_Howe");
        tzMap.put("Los Angeles, United States", "America/Los_Angeles");
        tzMap.put("Louisville (Kentucky), United States", "America/Louisville");
        tzMap.put("Luanda, Angola", "Africa/Luanda");
        tzMap.put("Lubumbashi, Democratic Republic of the Congo", "Africa/Lubumbashi");
        tzMap.put("Lusaka, Zambia", "Africa/Lusaka");
        tzMap.put("Macau SAR China", "Asia/Macau");
        tzMap.put("Maceió, Brazil", "America/Maceio");
        tzMap.put("Macquarie Island Station, Macquarie Island", "Antarctica/Macquarie");
        tzMap.put("Madeira, Portugal", "Atlantic/Madeira");
        tzMap.put("Madrid, Spain", "Europe/Madrid");
        tzMap.put("Magadan, Russia", "Asia/Magadan");
        tzMap.put("Mahé, Seychelles", "Indian/Mahe");
        tzMap.put("Majuro, Marshall Islands", "Pacific/Majuro");
        tzMap.put("Makassar, Indonesia", "Asia/Makassar");
        tzMap.put("Malabo, Equatorial Guinea", "Africa/Malabo");
        tzMap.put("Managua, Nicaragua", "America/Managua");
        tzMap.put("Manaus, Brazil", "America/Manaus");
        tzMap.put("Manila, Philippines", "Asia/Manila");
        tzMap.put("Maputo, Mozambique", "Africa/Maputo");
        tzMap.put("Marengo (Indiana), United States", "America/Indiana/Marengo");
        tzMap.put("Mariehamn, Åland, Finland", "Europe/Mariehamn");
        tzMap.put("Marigot, Saint Martin", "America/Marigot");
        tzMap.put("Marquesas Islands, French Polynesia", "Pacific/Marquesas");
        tzMap.put("Maseru, Lesotho", "Africa/Maseru");
        tzMap.put("Matamoros, Mexico", "America/Matamoros");
        tzMap.put("Mawson Station, Holme Bay", "Antarctica/Mawson");
        tzMap.put("Mazatlán, Mexico", "America/Mazatlan");
        tzMap.put("Mbabane, Swaziland", "Africa/Mbabane");
        tzMap.put("McMurdo Station, Ross Island", "Antarctica/McMurdo");
        tzMap.put("Melbourne, Australia", "Australia/Melbourne");
        tzMap.put("Mendoza, Argentina", "America/Mendoza");
        tzMap.put("Menominee (Michigan), United States", "America/Menominee");
        tzMap.put("Metlakatla (Alaska), United States", "America/Metlakatla");
        tzMap.put("Mexico City, Mexico", "America/Mexico_City");
        tzMap.put("Midway Islands, U.S. Minor Outlying Islands", "Pacific/Midway");
        tzMap.put("Minsk, Belarus", "Europe/Minsk");
        tzMap.put("Mogadishu, Somalia", "Africa/Mogadishu");
        tzMap.put("Moncton, Canada", "America/Moncton");
        tzMap.put("Monrovia, Liberia", "Africa/Monrovia");
        tzMap.put("Monterrey, Mexico", "America/Monterrey");
        tzMap.put("Montevideo, Uruguay", "America/Montevideo");
        tzMap.put("Monticello (Kentucky), United States", "America/Kentucky/Monticello");
        tzMap.put("Moscow, Russia", "Europe/Moscow");
        tzMap.put("Muscat, Oman", "Asia/Muscat");
        tzMap.put("Mérida, Mexico", "America/Merida");
        tzMap.put("N'Djamena, Chad", "Africa/Ndjamena");
        tzMap.put("Nairobi, Kenya", "Africa/Nairobi");
        tzMap.put("Nassau, Bahamas", "America/Nassau");
        tzMap.put("New Salem (North Dakota), United States", "America/North_Dakota/New_Salem");
        tzMap.put("New York, United States", "America/New_York");
        tzMap.put("Niamey, Niger", "Africa/Niamey");
        tzMap.put("Nicosia, Cyprus", "Asia/Nicosia");
        tzMap.put("Nome (Alaska), United States", "America/Nome");
        tzMap.put("Nouakchott, Mauritania", "Africa/Nouakchott");
        tzMap.put("Noumea, New Caledonia", "Pacific/Noumea");
        tzMap.put("Novokuznetsk, Russia", "Asia/Novokuznetsk");
        tzMap.put("Novosibirsk, Russia", "Asia/Novosibirsk");
        tzMap.put("Nuuk (Godthåb), Greenland", "America/Godthab");
        tzMap.put("Ojinaga, Mexico", "America/Ojinaga");
        tzMap.put("Omsk, Russia", "Asia/Omsk");
        tzMap.put("Oral, Kazakhstan", "Asia/Oral");
        tzMap.put("Oslo, Norway", "Europe/Oslo");
        tzMap.put("Ouagadougou, Burkina Faso", "Africa/Ouagadougou");
        tzMap.put("Pago Pago, American Samoa", "Pacific/Pago_Pago");
        tzMap.put("Palmer Station, Anvers Island", "Antarctica/Palmer");
        tzMap.put("Paramaribo, Suriname", "America/Paramaribo");
        tzMap.put("Paris, France", "Europe/Paris");
        tzMap.put("Perth, Australia", "Australia/Perth");
        tzMap.put("Petersburg (Indiana), United States", "America/Indiana/Petersburg");
        tzMap.put("Phnom Penh, Cambodia", "Asia/Phnom_Penh");
        tzMap.put("Phoenix, United States", "America/Phoenix");
        tzMap.put("Podgorica, Montenegro", "Europe/Podgorica");
        tzMap.put("Pohnpei, Micronesia", "Pacific/Ponape");
        tzMap.put("Pontianak, Indonesia", "Asia/Pontianak");
        tzMap.put("Port Moresby, Papua New Guinea", "Pacific/Port_Moresby");
        tzMap.put("Port of Spain, Trinidad and Tobago", "America/Port_of_Spain");
        tzMap.put("Port-au-Prince, Haiti", "America/Port-au-Prince");
        tzMap.put("Porto Velho, Brazil", "America/Porto_Velho");
        tzMap.put("Porto-Novo, Benin", "Africa/Porto-Novo");
        tzMap.put("Prague, Czech Republic", "Europe/Prague");
        tzMap.put("Punta Arenas, Chile", "America/Punta_Arenas");
        tzMap.put("Pyongyang, North Korea", "Asia/Pyongyang");
        tzMap.put("Qaanaaq (Thule), Greenland", "America/Thule");
        tzMap.put("Qostanay (Kostanay), Kazakhstan", "Asia/Qostanay");
        tzMap.put("Rankin Inlet, Canada", "America/Rankin_Inlet");
        tzMap.put("Rarotonga, Cook Islands", "Pacific/Rarotonga");
        tzMap.put("Recife, Brazil", "America/Recife");
        tzMap.put("Regina, Canada", "America/Regina");
        tzMap.put("Resolute, Canada", "America/Resolute");
        tzMap.put("Reykjavik, Iceland", "Atlantic/Reykjavik");
        tzMap.put("Riga, Latvia", "Europe/Riga");
        tzMap.put("Rio Branco, Brazil", "America/Rio_Branco");
        tzMap.put("Riyadh, Saudi Arabia", "Asia/Riyadh");
        tzMap.put("Rome, Italy", "Europe/Rome");
        tzMap.put("Rothera Station, Adelaide Island", "Antarctica/Rothera");
        tzMap.put("Réunion", "Indian/Reunion");
        tzMap.put("Río Gallegos, Argentina", "America/Argentina/Rio_Gallegos");
        tzMap.put("Saint Barthélemy", "America/St_Barthelemy");
        tzMap.put("Saint Helena", "Atlantic/St_Helena");
        tzMap.put("Saint Kitts", "America/St_Kitts");
        tzMap.put("Saint Lucia", "America/St_Lucia");
        tzMap.put("Saint Pierre and Miquelon", "America/Miquelon");
        tzMap.put("Saint Thomas, U.S. Virgin Islands", "America/St_Thomas");
        tzMap.put("Saint Vincent, Saint Vincent and the Grenadines", "America/St_Vincent");
        tzMap.put("Saipan, Northern Mariana Islands", "Pacific/Saipan");
        tzMap.put("Sakhalin, Russia", "Asia/Sakhalin");
        tzMap.put("Salta, Argentina", "America/Argentina/Salta");
        tzMap.put("Samara, Russia", "Europe/Samara");
        tzMap.put("Samarkand, Uzbekistan", "Asia/Samarkand");
        tzMap.put("San Juan, Argentina", "America/Argentina/San_Juan");
        tzMap.put("San Luis, Argentina", "America/Argentina/San_Luis");
        tzMap.put("Santarém, Brazil", "America/Santarem");
        tzMap.put("Santiago, Chile", "America/Santiago");
        tzMap.put("Santo Domingo, Dominican Republic", "America/Santo_Domingo");
        tzMap.put("Sarajevo, Bosnia and Herzegovina", "Europe/Sarajevo");
        tzMap.put("Saratov, Russia", "Europe/Saratov");
        tzMap.put("Seoul, South Korea", "Asia/Seoul");
        tzMap.put("Shanghai, China", "Asia/Shanghai");
        tzMap.put("Simferopol, Ukraine", "Europe/Simferopol");
        tzMap.put("Sint Maarten", "America/Lower_Princes");
        tzMap.put("Sitka (Alaska), United States", "America/Sitka");
        tzMap.put("Skopje, Macedonia", "Europe/Skopje");
        tzMap.put("Sofia, Bulgaria", "Europe/Sofia");
        tzMap.put("South Georgia and the South Sandwich Islands", "Atlantic/South_Georgia");
        tzMap.put("Srednekolymsk, Russia", "Asia/Srednekolymsk");
        tzMap.put("St. John's, Canada", "America/St_Johns");
        tzMap.put("Stanley, Falkland Islands", "Atlantic/Stanley");
        tzMap.put("Stockholm, Sweden", "Europe/Stockholm");
        tzMap.put("Swift Current, Canada", "America/Swift_Current");
        tzMap.put("Sydney, Australia", "Australia/Sydney");
        tzMap.put("Syowa Station, East Ongul Island", "Antarctica/Syowa");
        tzMap.put("São Paulo, Brazil", "America/Sao_Paulo");
        tzMap.put("São Tomé, São Tomé and Príncipe", "Africa/Sao_Tome");
        tzMap.put("Tahiti, French Polynesia", "Pacific/Tahiti");
        tzMap.put("Taipei, Taiwan", "Asia/Taipei");
        tzMap.put("Tallinn, Estonia", "Europe/Tallinn");
        tzMap.put("Tarawa, Kiribati", "Pacific/Tarawa");
        tzMap.put("Tashkent, Uzbekistan", "Asia/Tashkent");
        tzMap.put("Tbilisi, Georgia", "Asia/Tbilisi");
        tzMap.put("Tegucigalpa, Honduras", "America/Tegucigalpa");
        tzMap.put("Tehran, Iran", "Asia/Tehran");
        tzMap.put("Tell City (Indiana), United States", "America/Indiana/Tell_City");
        tzMap.put("Thimphu, Bhutan", "Asia/Thimphu");
        tzMap.put("Tijuana, Mexico", "America/Tijuana");
        tzMap.put("Tirane, Albania", "Europe/Tirane");
        tzMap.put("Tokyo, Japan", "Asia/Tokyo");
        tzMap.put("Tomsk, Russia", "Asia/Tomsk");
        tzMap.put("Tongatapu, Tonga", "Pacific/Tongatapu");
        tzMap.put("Toronto, Canada", "America/Toronto");
        tzMap.put("Tortola, British Virgin Islands", "America/Tortola");
        tzMap.put("Tripoli, Libya", "Africa/Tripoli");
        tzMap.put("Troll Station, Queen Maud Land", "Antarctica/Troll");
        tzMap.put("Tucumán, Argentina", "America/Argentina/Tucuman");
        tzMap.put("Tunis, Tunisia", "Africa/Tunis");
        tzMap.put("UTC (Coordinated Universal Time)", "Etc/UTC");
        tzMap.put("Ulaanbaatar (Ulan Bator), Mongolia", "Asia/Ulaanbaatar");
        tzMap.put("Ulyanovsk, Russia", "Europe/Ulyanovsk");
        tzMap.put("Unknown time zone", "Etc/Unknown");
        tzMap.put("Ushuaia, Argentina", "America/Argentina/Ushuaia");
        tzMap.put("Ust-Nera Oymyakonsky, Russia", "Asia/Ust-Nera");
        tzMap.put("Vaduz, Liechtenstein", "Europe/Vaduz");
        tzMap.put("Vancouver, Canada", "America/Vancouver");
        tzMap.put("Vatican City", "Europe/Vatican");
        tzMap.put("Vevay (Indiana), United States", "America/Indiana/Vevay");
        tzMap.put("Vienna, Austria", "Europe/Vienna");
        tzMap.put("Vientiane, Laos", "Asia/Vientiane");
        tzMap.put("Vilnius, Lithuania", "Europe/Vilnius");
        tzMap.put("Vincennes (Indiana), United States", "America/Indiana/Vincennes");
        tzMap.put("Vladivostok, Russia", "Asia/Vladivostok");
        tzMap.put("Volgograd, Russia", "Europe/Volgograd");
        tzMap.put("Vostok Station, Lake Vostok", "Antarctica/Vostok");
        tzMap.put("Wake Island, U.S. Minor Outlying Islands", "Pacific/Wake");
        tzMap.put("Wallis Islands, Wallis and Futuna", "Pacific/Wallis");
        tzMap.put("Warsaw, Poland", "Europe/Warsaw");
        tzMap.put("West Bank, Palestinian Territories", "Asia/Hebron");
        tzMap.put("Whitehorse, Canada", "America/Whitehorse");
        tzMap.put("Winamac (Indiana), United States", "America/Indiana/Winamac");
        tzMap.put("Windhoek, Namibia", "Africa/Windhoek");
        tzMap.put("Winnipeg, Canada", "America/Winnipeg");
        tzMap.put("Yakutat (Alaska), United States", "America/Yakutat");
        tzMap.put("Yakutsk, Russia", "Asia/Yakutsk");
        tzMap.put("Yangon (Rangoon), Burma", "Asia/Rangoon");
        tzMap.put("Yekaterinburg, Russia", "Asia/Yekaterinburg");
        tzMap.put("Yerevan, Armenia", "Asia/Yerevan");
        tzMap.put("Zagreb, Croatia", "Europe/Zagreb");
        tzMap.put("Zurich, Switzerland", "Europe/Zurich");
        tzMap.put("Ürümqi, China", "Asia/Urumqi");

        // Meta zones [Unicode CLDR]:
        tzMap.put("ADT", "America/Halifax");
        tzMap.put("AKDT", "America/Juneau");
        tzMap.put("AKST", "America/Juneau");
        tzMap.put("AKT", "America/Juneau");
        tzMap.put("AST", "America/Halifax");
        tzMap.put("AT", "America/Halifax");
        tzMap.put("Acre Standard Time", "America/Rio_Branco");
        tzMap.put("Acre Summer Time", "America/Rio_Branco");
        tzMap.put("Acre Time", "America/Rio_Branco");
        tzMap.put("Afghanistan Time", "Asia/Kabul");
        tzMap.put("Alaska Daylight Time", "America/Juneau");
        tzMap.put("Alaska Standard Time", "America/Juneau");
        tzMap.put("Alaska Time", "America/Juneau");
        tzMap.put("Amazon Standard Time", "America/Manaus");
        tzMap.put("Amazon Summer Time", "America/Manaus");
        tzMap.put("Amazon Time", "America/Manaus");
        tzMap.put("American Samoa Daylight Time", "Pacific/Pago_Pago");
        tzMap.put("American Samoa Standard Time", "Pacific/Pago_Pago");
        tzMap.put("American Samoa Time", "Pacific/Pago_Pago");
        tzMap.put("Arabian Time", "Asia/Riyadh");
        tzMap.put("Argentina Summer Time", "America/Buenos_Aires");
        tzMap.put("Argentina Time", "America/Buenos_Aires");
        tzMap.put("Armenia Standard Time", "Asia/Yerevan");
        tzMap.put("Armenia Summer Time", "Asia/Yerevan");
        tzMap.put("Armenia Time", "Asia/Yerevan");
        tzMap.put("Atlantic Time", "America/Halifax");
        tzMap.put("Australian Central Daylight Time", "Australia/Adelaide");
        tzMap.put("Australian Central Standard Time", "Australia/Adelaide");
        tzMap.put("Australian Central Time", "Australia/Adelaide");
        tzMap.put("Australian Central Western Daylight Time", "Australia/Eucla");
        tzMap.put("Australian Central Western Standard Time", "Australia/Eucla");
        tzMap.put("Australian Central Western Time", "Australia/Eucla");
        tzMap.put("Australian Eastern Daylight Time", "Australia/Sydney");
        tzMap.put("Australian Eastern Standard Time", "Australia/Sydney");
        tzMap.put("Australian Eastern Time", "Australia/Sydney");
        tzMap.put("Australian Western Daylight Time", "Australia/Perth");
        tzMap.put("Australian Western Standard Time", "Australia/Perth");
        tzMap.put("Australian Western Time", "Australia/Perth");
        tzMap.put("Azerbaijan Summer Time", "Asia/Baku");
        tzMap.put("Azerbaijan Time", "Asia/Baku");
        tzMap.put("Azores Summer Time", "Atlantic/Azores");
        tzMap.put("Azores Time", "Atlantic/Azores");
        tzMap.put("Bangladesh Summer Time", "Asia/Dhaka");
        tzMap.put("Bangladesh Time", "Asia/Dhaka");
        tzMap.put("Bhutan Time", "Asia/Thimphu");
        tzMap.put("Bolivia Time", "America/La_Paz");
        tzMap.put("Brasilia Standard Time", "America/Sao_Paulo");
        tzMap.put("Brasilia Summer Time", "America/Sao_Paulo");
        tzMap.put("Brasilia Time", "America/Sao_Paulo");
        tzMap.put("Brunei Time", "Asia/Brunei");
        tzMap.put("CDT", "America/Chicago");
        tzMap.put("CST", "America/Chicago");
        tzMap.put("CT", "America/Chicago");
        tzMap.put("Cape Verde Summer Time", "Atlantic/Cape_Verde");
        tzMap.put("Cape Verde Time", "Atlantic/Cape_Verde");
        tzMap.put("Central Africa Time", "Africa/Maputo");
        tzMap.put("Central European Summer Time", "Europe/Paris");
        tzMap.put("Central European Time", "Europe/Paris");
        tzMap.put("Central Indonesia Time", "Asia/Makassar");
        tzMap.put("Central Time", "America/Chicago");
        tzMap.put("Chamorro Standard Time", "Pacific/Saipan");
        tzMap.put("Chatham Daylight Time", "Pacific/Chatham");
        tzMap.put("Chatham Standard Time", "Pacific/Chatham");
        tzMap.put("Chatham Time", "Pacific/Chatham");
        tzMap.put("Chile Standard Time", "America/Santiago");
        tzMap.put("Chile Summer Time", "America/Santiago");
        tzMap.put("Chile Time", "America/Santiago");
        tzMap.put("China Time", "Asia/Shanghai");
        tzMap.put("Christmas Island Time", "Indian/Christmas");
        tzMap.put("Chuuk Time", "Pacific/Truk");
        tzMap.put("Cocos Islands Time", "Indian/Cocos");
        tzMap.put("Colombia Standard Time", "America/Bogota");
        tzMap.put("Colombia Summer Time", "America/Bogota");
        tzMap.put("Colombia Time", "America/Bogota");
        tzMap.put("Cook Islands Standard Time", "Pacific/Rarotonga");
        tzMap.put("Cook Islands Summer Time", "Pacific/Rarotonga");
        tzMap.put("Cook Islands Time", "Pacific/Rarotonga");
        tzMap.put("Cuba Time", "America/Havana");
        tzMap.put("Davis Time", "Antarctica/Davis");
        tzMap.put("Dumont d’Urville Time", "Antarctica/DumontDUrville");
        tzMap.put("EDT", "America/New_York");
        tzMap.put("EST", "America/New_York");
        tzMap.put("ET", "America/New_York");
        tzMap.put("East Africa Time", "Africa/Nairobi");
        tzMap.put("Easter Island Summer Time", "Pacific/Easter");
        tzMap.put("Easter Island Time", "Pacific/Easter");
        tzMap.put("Eastern European Standard Time", "Europe/Bucharest");
        tzMap.put("Eastern European Summer Time", "Europe/Bucharest");
        tzMap.put("Eastern European Time", "Europe/Bucharest");
        tzMap.put("Eastern Indonesia Time", "Asia/Jayapura");
        tzMap.put("Eastern Time", "America/New_York");
        tzMap.put("Ecuador Time", "America/Guayaquil");
        tzMap.put("Falkland Islands Standard Time", "Atlantic/Stanley");
        tzMap.put("Falkland Islands Summer Time", "Atlantic/Stanley");
        tzMap.put("Falkland Islands Time", "Atlantic/Stanley");
        tzMap.put("Fernando de Noronha Standard Time", "America/Noronha");
        tzMap.put("Fernando de Noronha Summer Time", "America/Noronha");
        tzMap.put("Fernando de Noronha Time", "America/Noronha");
        tzMap.put("Fiji Summer Time", "Pacific/Fiji");
        tzMap.put("Fiji Time", "Pacific/Fiji");
        tzMap.put("French Guiana Time", "America/Cayenne");
        tzMap.put("French Southern & Antarctic Time", "Indian/Kerguelen");
        tzMap.put("Galapagos Time", "Pacific/Galapagos");
        tzMap.put("Gambier Time", "Pacific/Gambier");
        tzMap.put("Georgia Standard Time", "Asia/Tbilisi");
        tzMap.put("Georgia Summer Time", "Asia/Tbilisi");
        tzMap.put("Georgia Time", "Asia/Tbilisi");
        tzMap.put("Gilbert Islands Time", "Pacific/Tarawa");
        tzMap.put("Greenland Summer Time", "America/Godthab");
        tzMap.put("Greenland Time", "America/Godthab");
        tzMap.put("Gulf Standard Time", "Asia/Dubai");
        tzMap.put("Guyana Time", "America/Guyana");
        tzMap.put("HADT", "America/Adak");
        tzMap.put("HAST", "America/Adak");
        tzMap.put("HAT", "America/Adak");
        tzMap.put("HST", "Pacific/Honolulu");
        tzMap.put("Hawaii-Aleutian Daylight Time", "America/Adak");
        tzMap.put("Hawaii-Aleutian Standard Time", "Pacific/Honolulu");
        tzMap.put("Hawaii-Aleutian Time", "America/Adak");
        tzMap.put("Hong Kong Standard Time", "Asia/Hong_Kong");
        tzMap.put("Hong Kong Summer Time", "Asia/Hong_Kong");
        tzMap.put("Hong Kong Time", "Asia/Hong_Kong");
        tzMap.put("Indian Ocean Time", "Indian/Chagos");
        tzMap.put("Indochina Time", "Asia/Bangkok");
        tzMap.put("Iran Time", "Asia/Tehran");
        tzMap.put("Irkutsk Standard Time", "Asia/Irkutsk");
        tzMap.put("Irkutsk Summer Time", "Asia/Irkutsk");
        tzMap.put("Irkutsk Time", "Asia/Irkutsk");
        tzMap.put("Israel Time", "Asia/Jerusalem");
        tzMap.put("Japan Daylight Time", "Asia/Tokyo");
        tzMap.put("Japan Standard Time", "Asia/Tokyo");
        tzMap.put("Japan Time", "Asia/Tokyo");
        tzMap.put("Kamchatka Standard Time", "Asia/Kamchatka");
        tzMap.put("Kamchatka Summer Time", "Asia/Kamchatka");
        tzMap.put("Kamchatka Time", "Asia/Kamchatka");
        tzMap.put("Kazakhstan Time", "Asia/Almaty");
        tzMap.put("Khovd Standard Time", "Asia/Hovd");
        tzMap.put("Khovd Summer Time", "Asia/Hovd");
        tzMap.put("Khovd Time", "Asia/Hovd");
        tzMap.put("Korean Daylight Time", "Asia/Seoul");
        tzMap.put("Korean Standard Time", "Asia/Seoul");
        tzMap.put("Korean Time", "Asia/Seoul");
        tzMap.put("Kosrae Time", "Pacific/Kosrae");
        tzMap.put("Krasnoyarsk Standard Time", "Asia/Krasnoyarsk");
        tzMap.put("Krasnoyarsk Summer Time", "Asia/Krasnoyarsk");
        tzMap.put("Krasnoyarsk Time", "Asia/Krasnoyarsk");
        tzMap.put("Kyrgyzstan Time", "Asia/Bishkek");
        tzMap.put("Line Islands Time", "Pacific/Kiritimati");
        tzMap.put("Lord Howe Time", "Australia/Lord_Howe");
        tzMap.put("MDT", "America/Denver");
        tzMap.put("MST", "America/Denver");
        tzMap.put("MT", "America/Denver");
        tzMap.put("Magadan Summer Time", "Asia/Magadan");
        tzMap.put("Magadan Time", "Asia/Magadan");
        tzMap.put("Malaysia Time", "Asia/Kuching");
        tzMap.put("Maldives Time", "Indian/Maldives");
        tzMap.put("Marquesas Time", "Pacific/Marquesas");
        tzMap.put("Marshall Islands Time", "Pacific/Majuro");
        tzMap.put("Mauritius Summer Time", "Indian/Mauritius");
        tzMap.put("Mauritius Time", "Indian/Mauritius");
        tzMap.put("Mawson Time", "Antarctica/Mawson");
        tzMap.put("Mexican Pacific Daylight Time", "America/Mazatlan");
        tzMap.put("Mexican Pacific Standard Time", "America/Mazatlan");
        tzMap.put("Mexican Pacific Time", "America/Mazatlan");
        tzMap.put("Moscow Standard Time", "Europe/Moscow");
        tzMap.put("Moscow Summer Time", "Europe/Moscow");
        tzMap.put("Moscow Time", "Europe/Moscow");
        tzMap.put("Mountain Time", "America/Denver");
        tzMap.put("Myanmar Time", "Asia/Rangoon");
        tzMap.put("Nauru Time", "Pacific/Nauru");
        tzMap.put("Nepal Time", "Asia/Katmandu");
        tzMap.put("New Caledonia Standard Time", "Pacific/Noumea");
        tzMap.put("New Caledonia Summer Time", "Pacific/Noumea");
        tzMap.put("New Caledonia Time", "Pacific/Noumea");
        tzMap.put("New Zealand Time", "Pacific/Auckland");
        tzMap.put("Newfoundland Time", "America/St_Johns");
        tzMap.put("Niue Time", "Pacific/Niue");
        tzMap.put("Norfolk Island Daylight Time", "Pacific/Norfolk");
        tzMap.put("Norfolk Island Standard Time", "Pacific/Norfolk");
        tzMap.put("Norfolk Island Time", "Pacific/Norfolk");
        tzMap.put("Omsk Summer Time", "Asia/Omsk");
        tzMap.put("Omsk Time", "Asia/Omsk");
        tzMap.put("PDT", "America/Los_Angeles");
        tzMap.put("PST", "America/Los_Angeles");
        tzMap.put("PT", "America/Los_Angeles");
        tzMap.put("Pacific Time", "America/Los_Angeles");
        tzMap.put("Pakistan Summer Time", "Asia/Karachi");
        tzMap.put("Pakistan Time", "Asia/Karachi");
        tzMap.put("Palau Time", "Pacific/Palau");
        tzMap.put("Papua New Guinea Time", "Pacific/Port_Moresby");
        tzMap.put("Paraguay Summer Time", "America/Asuncion");
        tzMap.put("Paraguay Time", "America/Asuncion");
        tzMap.put("Peru Standard Time", "America/Lima");
        tzMap.put("Peru Summer Time", "America/Lima");
        tzMap.put("Peru Time", "America/Lima");
        tzMap.put("Philippine Standard Time", "Asia/Manila");
        tzMap.put("Philippine Summer Time", "Asia/Manila");
        tzMap.put("Philippine Time", "Asia/Manila");
        tzMap.put("Phoenix Islands Time", "Pacific/Enderbury");
        tzMap.put("Pitcairn Time", "Pacific/Pitcairn");
        tzMap.put("Pohnpei Time", "Pacific/Ponape");
        tzMap.put("Rothera Time", "Antarctica/Rothera");
        tzMap.put("Réunion Time", "Indian/Reunion");
        tzMap.put("Samara Standard Time", "Europe/Samara");
        tzMap.put("Samara Summer Time", "Europe/Samara");
        tzMap.put("Samara Time", "Europe/Samara");
        tzMap.put("Samoa Time", "Pacific/Apia");
        tzMap.put("Seychelles Time", "Indian/Mahe");
        tzMap.put("Solomon Islands Time", "Pacific/Guadalcanal");
        tzMap.put("South Georgia Time", "Atlantic/South_Georgia");
        tzMap.put("St. Pierre & Miquelon Daylight Time", "America/Miquelon");
        tzMap.put("St. Pierre & Miquelon Standard Time", "America/Miquelon");
        tzMap.put("St. Pierre & Miquelon Time", "America/Miquelon");
        tzMap.put("Suriname Time", "America/Paramaribo");
        tzMap.put("Syowa Time", "Antarctica/Syowa");
        tzMap.put("Tahiti Time", "Pacific/Tahiti");
        tzMap.put("Taiwan Daylight Time", "Asia/Taipei");
        tzMap.put("Taiwan Standard Time", "Asia/Taipei");
        tzMap.put("Taiwan Time", "Asia/Taipei");
        tzMap.put("Tajikistan Time", "Asia/Dushanbe");
        tzMap.put("Timor-Leste Time", "Asia/Dili");
        tzMap.put("Tokelau Time", "Pacific/Fakaofo");
        tzMap.put("Tonga Summer Time", "Pacific/Tongatapu");
        tzMap.put("Tonga Time", "Pacific/Tongatapu");
        tzMap.put("Turkmenistan Standard Time", "Asia/Ashgabat");
        tzMap.put("Turkmenistan Summer Time", "Asia/Ashgabat");
        tzMap.put("Turkmenistan Time", "Asia/Ashgabat");
        tzMap.put("Tuvalu Time", "Pacific/Funafuti");
        tzMap.put("Türkiye Standard Time", "Europe/Istanbul");
        tzMap.put("Türkiye Summer Time", "Europe/Istanbul");
        tzMap.put("Türkiye Time", "Europe/Istanbul");
        tzMap.put("Ulaanbaatar Summer Time", "Asia/Ulaanbaatar");
        tzMap.put("Ulaanbaatar Time", "Asia/Ulaanbaatar");
        tzMap.put("Uruguay Standard Time", "America/Montevideo");
        tzMap.put("Uruguay Summer Time", "America/Montevideo");
        tzMap.put("Uruguay Time", "America/Montevideo");
        tzMap.put("Uzbekistan Standard Time", "Asia/Tashkent");
        tzMap.put("Uzbekistan Summer Time", "Asia/Tashkent");
        tzMap.put("Uzbekistan Time", "Asia/Tashkent");
        tzMap.put("Vanuatu Standard Time", "Pacific/Efate");
        tzMap.put("Vanuatu Summer Time", "Pacific/Efate");
        tzMap.put("Vanuatu Time", "Pacific/Efate");
        tzMap.put("Venezuela Time", "America/Caracas");
        tzMap.put("Vladivostok Summer Time", "Asia/Vladivostok");
        tzMap.put("Vladivostok Time", "Asia/Vladivostok");
        tzMap.put("Vostok Time", "Antarctica/Vostok");
        tzMap.put("Wake Island Time", "Pacific/Wake");
        tzMap.put("Wallis & Futuna Time", "Pacific/Wallis");
        tzMap.put("West Africa Time", "Africa/Lagos");
        tzMap.put("Western European Standard Time", "Atlantic/Canary");
        tzMap.put("Western European Summer Time", "Atlantic/Canary");
        tzMap.put("Western European Time", "Atlantic/Canary");
        tzMap.put("Western Indonesia Time", "Asia/Jakarta");
        tzMap.put("Yakutsk Summer Time", "Asia/Yakutsk");
        tzMap.put("Yakutsk Time", "Asia/Yakutsk");
        tzMap.put("Yekaterinburg Standard Time", "Asia/Yekaterinburg");
        tzMap.put("Yekaterinburg Summer Time", "Asia/Yekaterinburg");
        tzMap.put("Yekaterinburg Time", "Asia/Yekaterinburg");
        tzMap.put("Yukon Time", "America/Whitehorse");

        // Time zone aliases [Unicode CLDR]:
        tzMap.put("Africa/Asmara", "Africa/Asmera");
        tzMap.put("Africa/Timbuktu", "Africa/Bamako");
        tzMap.put("America/Argentina/Buenos_Aires", "America/Buenos_Aires");
        tzMap.put("America/Argentina/Catamarca", "America/Catamarca");
        tzMap.put("America/Argentina/ComodRivadavia", "America/Catamarca");
        tzMap.put("America/Argentina/Cordoba", "America/Cordoba");
        tzMap.put("America/Argentina/Jujuy", "America/Jujuy");
        tzMap.put("America/Argentina/Mendoza", "America/Mendoza");
        tzMap.put("America/Atikokan", "America/Coral_Harbour");
        tzMap.put("America/Atka", "America/Adak");
        tzMap.put("America/Ensenada", "America/Tijuana");
        tzMap.put("America/Fort_Wayne", "America/Indianapolis");
        tzMap.put("America/Indiana/Indianapolis", "America/Indianapolis");
        tzMap.put("America/Kentucky/Louisville", "America/Louisville");
        tzMap.put("America/Knox_IN", "America/Indiana/Knox");
        tzMap.put("America/Montreal", "America/Toronto");
        tzMap.put("America/Nipigon", "America/Toronto");
        tzMap.put("America/Nuuk", "America/Godthab");
        tzMap.put("America/Pangnirtung", "America/Iqaluit");
        tzMap.put("America/Porto_Acre", "America/Rio_Branco");
        tzMap.put("America/Rainy_River", "America/Winnipeg");
        tzMap.put("America/Rosario", "America/Cordoba");
        tzMap.put("America/Santa_Isabel", "America/Tijuana");
        tzMap.put("America/Shiprock", "America/Denver");
        tzMap.put("America/Thunder_Bay", "America/Toronto");
        tzMap.put("America/Virgin", "America/St_Thomas");
        tzMap.put("America/Yellowknife", "America/Edmonton");
        tzMap.put("Antarctica/South_Pole", "Antarctica/McMurdo");
        tzMap.put("Asia/Ashkhabad", "Asia/Ashgabat");
        tzMap.put("Asia/Choibalsan", "Asia/Ulaanbaatar");
        tzMap.put("Asia/Chongqing", "Asia/Shanghai");
        tzMap.put("Asia/Chungking", "Asia/Shanghai");
        tzMap.put("Asia/Dacca", "Asia/Dhaka");
        tzMap.put("Asia/Harbin", "Asia/Shanghai");
        tzMap.put("Asia/Ho_Chi_Minh", "Asia/Saigon");
        tzMap.put("Asia/Istanbul", "Europe/Istanbul");
        tzMap.put("Asia/Kashgar", "Asia/Urumqi");
        tzMap.put("Asia/Kathmandu", "Asia/Katmandu");
        tzMap.put("Asia/Kolkata", "Asia/Calcutta");
        tzMap.put("Asia/Macao", "Asia/Macau");
        tzMap.put("Asia/Tel_Aviv", "Asia/Jerusalem");
        tzMap.put("Asia/Thimbu", "Asia/Thimphu");
        tzMap.put("Asia/Ujung_Pandang", "Asia/Makassar");
        tzMap.put("Asia/Ulan_Bator", "Asia/Ulaanbaatar");
        tzMap.put("Asia/Yangon", "Asia/Rangoon");
        tzMap.put("Atlantic/Faroe", "Atlantic/Faeroe");
        tzMap.put("Atlantic/Jan_Mayen", "Arctic/Longyearbyen");
        tzMap.put("Australia/ACT", "Australia/Sydney");
        tzMap.put("Australia/Canberra", "Australia/Sydney");
        tzMap.put("Australia/Currie", "Australia/Hobart");
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
        tzMap.put("CET", "Europe/Brussels");
        tzMap.put("CST6CDT", "America/Chicago");
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
        tzMap.put("EET", "Europe/Athens");
        tzMap.put("EST5EDT", "America/New_York");
        tzMap.put("Egypt", "Africa/Cairo");
        tzMap.put("Eire", "Europe/Dublin");
        tzMap.put("Etc/GMT+0", "Etc/GMT");
        tzMap.put("Etc/GMT-0", "Etc/GMT");
        tzMap.put("Etc/GMT0", "Etc/GMT");
        tzMap.put("Etc/Greenwich", "Etc/GMT");
        tzMap.put("Etc/UCT", "Etc/UTC");
        tzMap.put("Etc/Universal", "Etc/UTC");
        tzMap.put("Etc/Zulu", "Etc/UTC");
        tzMap.put("Europe/Belfast", "Europe/London");
        tzMap.put("Europe/Kyiv", "Europe/Kiev");
        tzMap.put("Europe/Nicosia", "Asia/Nicosia");
        tzMap.put("Europe/Tiraspol", "Europe/Chisinau");
        tzMap.put("Europe/Uzhgorod", "Europe/Kiev");
        tzMap.put("Europe/Zaporozhye", "Europe/Kiev");
        tzMap.put("Factory", "Etc/Unknown");
        tzMap.put("GB", "Europe/London");
        tzMap.put("GB-Eire", "Europe/London");
        tzMap.put("Hongkong", "Asia/Hong_Kong");
        tzMap.put("Iceland", "Atlantic/Reykjavik");
        tzMap.put("Iran", "Asia/Tehran");
        tzMap.put("Israel", "Asia/Jerusalem");
        tzMap.put("Japan", "Asia/Tokyo");
        tzMap.put("Kwajalein", "Pacific/Kwajalein");
        tzMap.put("Libya", "Africa/Tripoli");
        tzMap.put("MET", "Europe/Brussels");
        tzMap.put("MST7MDT", "America/Denver");
        tzMap.put("Mexico/BajaNorte", "America/Tijuana");
        tzMap.put("Mexico/BajaSur", "America/Mazatlan");
        tzMap.put("Mexico/General", "America/Mexico_City");
        tzMap.put("NZ", "Pacific/Auckland");
        tzMap.put("NZ-CHAT", "Pacific/Chatham");
        tzMap.put("Navajo", "America/Denver");
        tzMap.put("PRC", "Asia/Shanghai");
        tzMap.put("PST8PDT", "America/Los_Angeles");
        tzMap.put("Pacific/Chuuk", "Pacific/Truk");
        tzMap.put("Pacific/Johnston", "Pacific/Honolulu");
        tzMap.put("Pacific/Kanton", "Pacific/Enderbury");
        tzMap.put("Pacific/Pohnpei", "Pacific/Ponape");
        tzMap.put("Pacific/Samoa", "Pacific/Pago_Pago");
        tzMap.put("Pacific/Yap", "Pacific/Truk");
        tzMap.put("Poland", "Europe/Warsaw");
        tzMap.put("Portugal", "Europe/Lisbon");
        tzMap.put("ROC", "Asia/Taipei");
        tzMap.put("ROK", "Asia/Seoul");
        tzMap.put("Turkey", "Europe/Istanbul");
        tzMap.put("US/Alaska", "America/Anchorage");
        tzMap.put("US/Aleutian", "America/Adak");
        tzMap.put("US/Arizona", "America/Phoenix");
        tzMap.put("US/Central", "America/Chicago");
        tzMap.put("US/East-Indiana", "America/Indianapolis");
        tzMap.put("US/Eastern", "America/New_York");
        tzMap.put("US/Hawaii", "Pacific/Honolulu");
        tzMap.put("US/Indiana-Starke", "America/Indiana/Knox");
        tzMap.put("US/Michigan", "America/Detroit");
        tzMap.put("US/Mountain", "America/Denver");
        tzMap.put("US/Pacific", "America/Los_Angeles");
        tzMap.put("US/Pacific-New", "America/Los_Angeles");
        tzMap.put("US/Samoa", "Pacific/Pago_Pago");
        tzMap.put("W-SU", "Europe/Moscow");
        tzMap.put("WET", "Europe/Lisbon");

        // Territories [Unicode CLDR]:
        tzMap.put("Albania", "Europe/Tirane");
        tzMap.put("Angola", "Africa/Luanda");
        tzMap.put("Antarctica", "Antarctica/McMurdo");
        tzMap.put("Antigua & Barbuda", "America/Antigua");
        tzMap.put("Austria", "Europe/Vienna");
        tzMap.put("Bahamas", "America/Nassau");
        tzMap.put("Belgium", "Europe/Brussels");
        tzMap.put("Benin", "Africa/Porto-Novo");
        tzMap.put("Bosnia", "Europe/Sarajevo");
        tzMap.put("Bosnia & Herzegovina", "Europe/Sarajevo");
        tzMap.put("Botswana", "Africa/Gaborone");
        tzMap.put("British Virgin Islands", "America/Tortola");
        tzMap.put("Bulgaria", "Europe/Sofia");
        tzMap.put("Burkina Faso", "Africa/Ouagadougou");
        tzMap.put("Burundi", "Africa/Bujumbura");
        tzMap.put("Cambodia", "Asia/Phnom_Penh");
        tzMap.put("Cameroon", "Africa/Douala");
        tzMap.put("Caribbean Netherlands", "America/Kralendijk");
        tzMap.put("Central African Republic", "Africa/Bangui");
        tzMap.put("Chad", "Africa/Ndjamena");
        tzMap.put("Congo (Republic)", "Africa/Brazzaville");
        tzMap.put("Congo - Brazzaville", "Africa/Brazzaville");
        tzMap.put("Croatia", "Europe/Zagreb");
        tzMap.put("Cyprus", "Asia/Nicosia");
        tzMap.put("Czech Republic", "Europe/Prague");
        tzMap.put("Czechia", "Europe/Prague");
        tzMap.put("Côte d’Ivoire", "Africa/Abidjan");
        tzMap.put("Denmark", "Europe/Copenhagen");
        tzMap.put("Equatorial Guinea", "Africa/Malabo");
        tzMap.put("Eritrea", "Africa/Asmera");
        tzMap.put("Eswatini", "Africa/Mbabane");
        tzMap.put("Ethiopia", "Africa/Addis_Ababa");
        tzMap.put("Finland", "Europe/Helsinki");
        tzMap.put("Gabon", "Africa/Libreville");
        tzMap.put("Gambia", "Africa/Banjul");
        tzMap.put("Germany", "Europe/Berlin");
        tzMap.put("Ghana", "Africa/Accra");
        tzMap.put("Greece", "Europe/Athens");
        tzMap.put("Greenland", "America/Thule");
        tzMap.put("Guinea", "Africa/Conakry");
        tzMap.put("Haiti", "America/Port-au-Prince");
        tzMap.put("Honduras", "America/Tegucigalpa");
        tzMap.put("Hungary", "Europe/Budapest");
        tzMap.put("Iraq", "Asia/Baghdad");
        tzMap.put("Ireland", "Europe/Dublin");
        tzMap.put("Italy", "Europe/Rome");
        tzMap.put("Ivory Coast", "Africa/Abidjan");
        tzMap.put("Kosovo", "Europe/Belgrade");
        tzMap.put("Laos", "Asia/Vientiane");
        tzMap.put("Lebanon", "Asia/Beirut");
        tzMap.put("Lesotho", "Africa/Maseru");
        tzMap.put("Liechtenstein", "Europe/Vaduz");
        tzMap.put("Madagascar", "Indian/Antananarivo");
        tzMap.put("Malawi", "Africa/Blantyre");
        tzMap.put("Mali", "Africa/Bamako");
        tzMap.put("Mauritania", "Africa/Nouakchott");
        tzMap.put("Montenegro", "Europe/Podgorica");
        tzMap.put("Netherlands", "Europe/Amsterdam");
        tzMap.put("Niger", "Africa/Niamey");
        tzMap.put("North Macedonia", "Europe/Skopje");
        tzMap.put("Norway", "Europe/Oslo");
        tzMap.put("Oman", "Asia/Muscat");
        tzMap.put("Rwanda", "Africa/Kigali");
        tzMap.put("Senegal", "Africa/Dakar");
        tzMap.put("Sierra Leone", "Africa/Freetown");
        tzMap.put("Slovakia", "Europe/Bratislava");
        tzMap.put("Slovenia", "Europe/Ljubljana");
        tzMap.put("Somalia", "Africa/Mogadishu");
        tzMap.put("Spain", "Europe/Madrid");
        tzMap.put("Sri Lanka", "Asia/Colombo");
        tzMap.put("St. Helena", "Atlantic/St_Helena");
        tzMap.put("St. Kitts & Nevis", "America/St_Kitts");
        tzMap.put("St. Lucia", "America/St_Lucia");
        tzMap.put("St. Martin", "America/Marigot");
        tzMap.put("St. Vincent & Grenadines", "America/St_Vincent");
        tzMap.put("Svalbard & Jan Mayen", "Arctic/Longyearbyen");
        tzMap.put("Swaziland", "Africa/Mbabane");
        tzMap.put("Sweden", "Europe/Stockholm");
        tzMap.put("Switzerland", "Europe/Zurich");
        tzMap.put("Tanzania", "Africa/Dar_es_Salaam");
        tzMap.put("Togo", "Africa/Lome");
        tzMap.put("Trinidad & Tobago", "America/Port_of_Spain");
        tzMap.put("Tunisia", "Africa/Tunis");
        tzMap.put("U.S. Virgin Islands", "America/St_Thomas");
        tzMap.put("UK", "Europe/London");
        tzMap.put("Uganda", "Africa/Kampala");
        tzMap.put("United Kingdom", "Europe/London");
        tzMap.put("Yemen", "Asia/Aden");
        tzMap.put("Zambia", "Africa/Lusaka");
        tzMap.put("Zimbabwe", "Africa/Harare");
        tzMap.put("Åland Islands", "Europe/Mariehamn");

        // Time zone abbreviations [IANA Time Zone Database]:
        tzMap.put("AWST", "Australia/Perth");
        tzMap.put("BST", "Europe/London");
        tzMap.put("ChST", "Pacific/Guam");
        tzMap.put("EAT", "Africa/Nairobi");
        tzMap.put("HDT", "America/Adak");
        tzMap.put("HKT", "Asia/Hong_Kong");
        tzMap.put("IDT", "Asia/Jerusalem");
        tzMap.put("JST", "Asia/Tokyo");
        tzMap.put("NDT", "America/St_Johns");
        tzMap.put("NST", "America/St_Johns");
        tzMap.put("NZDT", "Pacific/Auckland");
        tzMap.put("NZST", "Pacific/Auckland");
        tzMap.put("PKT", "Asia/Karachi");
        tzMap.put("SAST", "Africa/Johannesburg");
        tzMap.put("SST", "Pacific/Pago_Pago");
        tzMap.put("WIT", "Asia/Jayapura");
        tzMap.put("WITA", "Asia/Makassar");

        // Legacy time zones [IANA Time Zone Database]:
        tzMap.put("Africa/Accra", "Africa/Abidjan");
        tzMap.put("Africa/Addis_Ababa", "Africa/Nairobi");
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
        tzMap.put("America/Anguilla", "America/Puerto_Rico");
        tzMap.put("America/Antigua", "America/Puerto_Rico");
        tzMap.put("America/Aruba", "America/Puerto_Rico");
        tzMap.put("America/Blanc-Sablon", "America/Puerto_Rico");
        tzMap.put("America/Buenos_Aires", "America/Argentina/Buenos_Aires");
        tzMap.put("America/Catamarca", "America/Argentina/Catamarca");
        tzMap.put("America/Cayman", "America/Panama");
        tzMap.put("America/Coral_Harbour", "America/Panama");
        tzMap.put("America/Cordoba", "America/Argentina/Cordoba");
        tzMap.put("America/Creston", "America/Phoenix");
        tzMap.put("America/Curacao", "America/Puerto_Rico");
        tzMap.put("America/Dominica", "America/Puerto_Rico");
        tzMap.put("America/Godthab", "America/Nuuk");
        tzMap.put("America/Grenada", "America/Puerto_Rico");
        tzMap.put("America/Guadeloupe", "America/Puerto_Rico");
        tzMap.put("America/Indianapolis", "America/Indiana/Indianapolis");
        tzMap.put("America/Jujuy", "America/Argentina/Jujuy");
        tzMap.put("America/Kralendijk", "America/Puerto_Rico");
        tzMap.put("America/Louisville", "America/Kentucky/Louisville");
        tzMap.put("America/Lower_Princes", "America/Puerto_Rico");
        tzMap.put("America/Marigot", "America/Puerto_Rico");
        tzMap.put("America/Mendoza", "America/Argentina/Mendoza");
        tzMap.put("America/Montserrat", "America/Puerto_Rico");
        tzMap.put("America/Nassau", "America/Toronto");
        tzMap.put("America/Port_of_Spain", "America/Puerto_Rico");
        tzMap.put("America/St_Barthelemy", "America/Puerto_Rico");
        tzMap.put("America/St_Kitts", "America/Puerto_Rico");
        tzMap.put("America/St_Lucia", "America/Puerto_Rico");
        tzMap.put("America/St_Thomas", "America/Puerto_Rico");
        tzMap.put("America/St_Vincent", "America/Puerto_Rico");
        tzMap.put("America/Tortola", "America/Puerto_Rico");
        tzMap.put("Antarctica/DumontDUrville", "Pacific/Port_Moresby");
        tzMap.put("Antarctica/McMurdo", "Pacific/Auckland");
        tzMap.put("Antarctica/Syowa", "Asia/Riyadh");
        tzMap.put("Arctic/Longyearbyen", "Europe/Berlin");
        tzMap.put("Asia/Aden", "Asia/Riyadh");
        tzMap.put("Asia/Bahrain", "Asia/Qatar");
        tzMap.put("Asia/Brunei", "Asia/Kuching");
        tzMap.put("Asia/Calcutta", "Asia/Kolkata");
        tzMap.put("Asia/Katmandu", "Asia/Kathmandu");
        tzMap.put("Asia/Kuala_Lumpur", "Asia/Singapore");
        tzMap.put("Asia/Kuwait", "Asia/Riyadh");
        tzMap.put("Asia/Muscat", "Asia/Dubai");
        tzMap.put("Asia/Phnom_Penh", "Asia/Bangkok");
        tzMap.put("Asia/Rangoon", "Asia/Yangon");
        tzMap.put("Asia/Saigon", "Asia/Ho_Chi_Minh");
        tzMap.put("Asia/Vientiane", "Asia/Bangkok");
        tzMap.put("Atlantic/Faeroe", "Atlantic/Faroe");
        tzMap.put("Atlantic/Reykjavik", "Africa/Abidjan");
        tzMap.put("Atlantic/St_Helena", "Africa/Abidjan");
        tzMap.put("Europe/Amsterdam", "Europe/Brussels");
        tzMap.put("Europe/Bratislava", "Europe/Prague");
        tzMap.put("Europe/Busingen", "Europe/Zurich");
        tzMap.put("Europe/Copenhagen", "Europe/Berlin");
        tzMap.put("Europe/Guernsey", "Europe/London");
        tzMap.put("Europe/Isle_of_Man", "Europe/London");
        tzMap.put("Europe/Jersey", "Europe/London");
        tzMap.put("Europe/Kiev", "Europe/Kyiv");
        tzMap.put("Europe/Ljubljana", "Europe/Belgrade");
        tzMap.put("Europe/Luxembourg", "Europe/Brussels");
        tzMap.put("Europe/Mariehamn", "Europe/Helsinki");
        tzMap.put("Europe/Monaco", "Europe/Paris");
        tzMap.put("Europe/Oslo", "Europe/Berlin");
        tzMap.put("Europe/Podgorica", "Europe/Belgrade");
        tzMap.put("Europe/San_Marino", "Europe/Rome");
        tzMap.put("Europe/Sarajevo", "Europe/Belgrade");
        tzMap.put("Europe/Skopje", "Europe/Belgrade");
        tzMap.put("Europe/Stockholm", "Europe/Berlin");
        tzMap.put("Europe/Vaduz", "Europe/Zurich");
        tzMap.put("Europe/Vatican", "Europe/Rome");
        tzMap.put("Europe/Zagreb", "Europe/Belgrade");
        tzMap.put("Indian/Antananarivo", "Africa/Nairobi");
        tzMap.put("Indian/Christmas", "Asia/Bangkok");
        tzMap.put("Indian/Cocos", "Asia/Yangon");
        tzMap.put("Indian/Comoro", "Africa/Nairobi");
        tzMap.put("Indian/Kerguelen", "Indian/Maldives");
        tzMap.put("Indian/Mahe", "Asia/Dubai");
        tzMap.put("Indian/Mayotte", "Africa/Nairobi");
        tzMap.put("Indian/Reunion", "Asia/Dubai");
        tzMap.put("Pacific/Enderbury", "Pacific/Kanton");
        tzMap.put("Pacific/Funafuti", "Pacific/Tarawa");
        tzMap.put("Pacific/Majuro", "Pacific/Tarawa");
        tzMap.put("Pacific/Midway", "Pacific/Pago_Pago");
        tzMap.put("Pacific/Ponape", "Pacific/Guadalcanal");
        tzMap.put("Pacific/Saipan", "Pacific/Guam");
        tzMap.put("Pacific/Truk", "Pacific/Port_Moresby");
        tzMap.put("Pacific/Wake", "Pacific/Tarawa");
        tzMap.put("Pacific/Wallis", "Pacific/Tarawa");

        // GENERATED CODE ENDS HERE

        // Bridge all Time Zones known by Java.
        for (String tz : TimeZone.getAvailableIDs()) {
            tzMap.put(tz, tz);
        }

        for (String key : tzMap.keySet()) {
            assertEquals(tzMap.get(key), TimeUtil.getCanonicalTimeZone(key, null), "Different time Zone '" + key + "' mapping");
        }

        for (String tz : TimeZone.getAvailableIDs()) {
            try {
                TimeUtil.getCanonicalTimeZone(tz, null);
            } catch (CJException e) {
                fail("System Time Zone '" + tz + "' mapping missing");
            }
        }
    }

    /**
     * Tests fix for BUG#70436 - Incorrect mapping of windows timezone to Olson timezone.
     *
     * @throws Exception
     */
    @Test
    public void testBug70436() throws Exception {
        assertEquals("Asia/Yerevan", TimeUtil.getCanonicalTimeZone("Caucasus Standard Time", null));
        assertEquals("Asia/Tbilisi", TimeUtil.getCanonicalTimeZone("Georgian Standard Time", null));
    }

    /**
     * Tests fix for Bug#82115 - Some exceptions are intercepted twice or fail to set the init cause.
     *
     * @throws Exception
     */
    @Test
    public void testBug82115() throws Exception {
        Exception ex = SQLError.createSQLException("ORIGINAL_EXCEPTION", "0", new Exception("ORIGINAL_CAUSE"), null);
        assertEquals("ORIGINAL_EXCEPTION", ex.getMessage());
        assertEquals("ORIGINAL_CAUSE", ex.getCause().getMessage());

        ex = SQLError.createSQLException("ORIGINAL_EXCEPTION", "0", new Exception("ORIGINAL_CAUSE"), new ExceptionInterceptor() {

            boolean alreadyIntercepted = false;

            @Override
            public ExceptionInterceptor init(Properties props, Log log) {
                this.alreadyIntercepted = false;
                return this;
            }

            @Override
            public void destroy() {
            }

            @Override
            public Exception interceptException(Exception sqlEx) {
                assertFalse(this.alreadyIntercepted);
                this.alreadyIntercepted = true;

                assertEquals("ORIGINAL_EXCEPTION", sqlEx.getMessage());
                assertEquals("ORIGINAL_CAUSE", sqlEx.getCause().getMessage());

                SQLException newSqlEx = new SQLException("INTERCEPT_EXCEPTION");
                return newSqlEx;
            }

        });
        assertEquals("INTERCEPT_EXCEPTION", ex.getMessage());
        assertNull(ex.getCause());

        ex = SQLError.createSQLException("ORIGINAL_EXCEPTION", "0", new Exception("ORIGINAL_CAUSE"), new ExceptionInterceptor() {

            boolean alreadyIntercepted = false;

            @Override
            public ExceptionInterceptor init(Properties props, Log log) {
                this.alreadyIntercepted = false;
                return this;
            }

            @Override
            public void destroy() {
            }

            @Override
            public Exception interceptException(Exception sqlEx) {
                assertFalse(this.alreadyIntercepted);
                this.alreadyIntercepted = true;

                assertEquals("ORIGINAL_EXCEPTION", sqlEx.getMessage());
                assertEquals("ORIGINAL_CAUSE", sqlEx.getCause().getMessage());

                SQLException newSqlEx = new SQLException("INTERCEPT_EXCEPTION");
                newSqlEx.initCause(new Exception("INTERCEPT_CAUSE"));
                return newSqlEx;
            }

        });
        assertEquals("INTERCEPT_EXCEPTION", ex.getMessage());
        assertEquals("INTERCEPT_CAUSE", ex.getCause().getMessage());
    }

    /**
     * Test fix for Bug#20913114, STRINGUTILS.WILDCOMPARE() FAILS WITH STACKOVERFLOWERROR ERROR.
     *
     * @throws Exception
     */
    @Test
    public void testBug20913114() throws Exception {
        String s1 = "String Consist of some small Sample Data";

        assertFalse(StringUtils.wildCompareIgnoreCase(s1, "St%no"));
        assertFalse(StringUtils.wildCompareIgnoreCase(s1, "Srin"));
        assertFalse(StringUtils.wildCompareIgnoreCase(s1, "Dae"));

        assertTrue(StringUtils.wildCompareIgnoreCase(s1, "St%so%Sa%Da%"));
        assertFalse(StringUtils.wildCompareIgnoreCase(s1, "St%so%Sp%Da%"));
        assertTrue(StringUtils.wildCompareIgnoreCase(s1, "S_%s_%S_%Da%"));
        assertTrue(StringUtils.wildCompareIgnoreCase(s1, "S_r_n_ C_n_i_t%"));
        assertFalse(StringUtils.wildCompareIgnoreCase(s1, "St%so%Sp%Da%"));

        assertFalse(StringUtils.wildCompareIgnoreCase(s1, "Dae"));
        assertFalse(StringUtils.wildCompareIgnoreCase(s1, "Srin"));
        assertFalse(StringUtils.wildCompareIgnoreCase(s1, "St%no"));
    }

    /**
     * Tests fix for Bug#104170 (33064455), CONTRIBUTION: CLIENTPREPAREDSTMT: LEAVE CALENDAR UNTOUCHED.
     *
     * @throws Exception
     */
    @Test
    public void testBug104170() throws Exception {
        Calendar cal = Calendar.getInstance();
        long orig = cal.getTimeInMillis();

        TimeUtil.getSimpleDateFormat("''yyyy-MM-dd''", cal).format(java.util.Date.from(LocalDateTime.of(1980, 1, 1, 0, 0).toInstant(ZoneOffset.UTC)));

        assertEquals(orig, cal.getTimeInMillis());
    }

    /**
     * Tests fix for Bug#99292 (Bug#31195955), Contribution: Support Windows time zone \'Coordinated Universal Time\'.
     *
     * @throws Exception
     */
    @Test
    void testBug99292() throws Exception {
        assertEquals("Etc/UTC", TimeUtil.getCanonicalTimeZone("Coordinated Universal Time", null));
    }

    /**
     * Tests fix for Bug#39806615, Validate extension class names supplied through connection properties.
     *
     * @throws Exception
     */
    @Test
    public void testBug39806615() throws Exception {
        List<String> requestedClasses = new ArrayList<>();
        ClassLoader recordingLoader = new ClassLoader(null) {

            @Override
            public Class<?> loadClass(String name) throws ClassNotFoundException {
                requestedClasses.add(name);
                throw new ClassNotFoundException(name);
            }

        };

        // Reject locator syntax and malformed names before invoking the class loader, even with initialization disabled.
        String[] invalidNames = { null, "", ".Plugin", "example.", "example..Plugin", "example.1Plugin", "jar:file:.proc.self.fd.16!.Plugin",
                "jar:file:/proc/self/fd/16!/Plugin", "http://localhost/Plugin", "file:/tmp/Plugin", "example/Plugin", "example\\Plugin", "example:Plugin",
                "example!Plugin", "example%3APlugin", "[Ljava.lang.String;", "[I", " example.Plugin", "example.Plugin ", "example.Plu gin", "example.Plugin\n",
                "example.Plugin\t", "example.Foo" + (char) 0 + "Bar", "example.Foo" + (char) 1 + "Bar", "example.Foo\u200bBar", "example.Foo\ud800",
                "example.Foo\udc00" };
        for (String className : invalidNames) {
            String message = Messages.getString("Util.InvalidClassName", new Object[] { className });
            WrongArgumentException ex = assertThrows(WrongArgumentException.class, () -> Util.loadClass(className, recordingLoader));
            assertEquals(message, ex.getMessage());
            assertNull(ex.getCause());
            assertTrue(requestedClasses.isEmpty(), "Invalid names must not reach the class loader.");

            ex = assertThrows(WrongArgumentException.class, () -> Util.getInstance(Object.class, className, null, null, null));
            assertEquals(message, ex.getMessage());
            assertNull(ex.getCause());
        }

        // JLS identifier characters and binary names: '$', Unicode letters, combining marks and supplementary code points are supported.
        for (String className : new String[] { "Plugin", "_Plugin", "$Plugin", "example.Plugin2", "example.Outer$Plugin", "example.Outer$1",
                "example.Outer$1Local", "example.\u00c9Plugin", "example.Plugi\u0301n", "example.\ud801\udc00Plugin" }) {
            assertThrows(ClassNotFoundException.class, () -> Util.loadClass(className, recordingLoader));
            assertEquals(Collections.singletonList(className), requestedClasses);
            requestedClasses.clear();
        }
        assertEquals(String.class, Util.loadClass("java.lang.String", null));
        assertEquals(TestBug39806615\u00c9Plugin.class,
                Util.getInstance(SocketFactory.class, TestBug39806615\u00c9Plugin.class.getName(), null, null, null).getClass());

        // Type checks must still precede initialization, and missing classes and constructor failures must retain their causes.
        WrongArgumentException ex = assertThrows(WrongImplementationException.class,
                Pattern.quote("The class 'testsuite.regression.UtilsRegressionTest$TestBug39806615WrongType' "
                        + "does not implement the interface 'com.mysql.cj.protocol.SocketFactory'."),
                () -> Util.getInstance(SocketFactory.class, TestBug39806615WrongType.class.getName(), null, null, null));
        assertNull(ex.getCause());
        ex = assertThrows(WrongArgumentException.class, () -> Util.getInstance(Object.class, "missing.Plugin", null, null, null));
        assertTrue(ex.getCause() instanceof ClassNotFoundException);
        ex = assertThrows(WrongArgumentException.class,
                () -> Util.getInstance(SocketFactory.class, TestBug39806615BrokenPlugin.class.getName(), null, null, null));
        assertTrue(ex.getCause() instanceof IllegalStateException);
        assertEquals("Plugin constructor failed", ex.getMessage());

        class TestSocketConnection extends NativeSocketConnection {

            @Override
            public SocketFactory createSocketFactory(String className) {
                return super.createSocketFactory(className);
            }

        }
        TestSocketConnection socketConnection = new TestSocketConnection();
        assertEquals(StandardSocketFactory.class, socketConnection.createSocketFactory(StandardSocketFactory.class.getName()).getClass());
        assertEquals(com.mysql.jdbc.SocketFactoryWrapper.class, socketConnection.createSocketFactory(TestBug39806615LegacyPlugin.class.getName()).getClass());
        String invalidName = "jar:file:.proc.self.fd.16!.Plugin";
        String invalidMessage = Messages.getString("Util.InvalidClassName", new Object[] { invalidName });
        ex = assertThrows(WrongArgumentException.class, () -> socketConnection.createSocketFactory(invalidName));
        assertEquals(invalidMessage, ex.getMessage());
        assertEquals(WrongArgumentException.class, ex.getClass(), "Invalid names must not be reported as type mismatches.");
        assertNull(ex.getCause());
        assertEquals(StandardLogger.class, LogFactory.getLogger("StandardLogger", "testBug39806615").getClass());

        // Failover matching continues to support classes/interfaces and to ignore missing classes, but rejects invalid names.
        StandardLoadBalanceExceptionChecker checker = new StandardLoadBalanceExceptionChecker();
        Properties props = new Properties();
        props.setProperty(PropertyKey.loadBalanceSQLExceptionSubclassFailover.getKeyName(), "missing.Exception, java.sql.SQLException, java.lang.Runnable");
        assertDoesNotThrow(() -> checker.init(props));
        assertTrue(checker.shouldExceptionTriggerFailover(new SQLException()));
        assertFalse(checker.shouldExceptionTriggerFailover(new IllegalStateException()));
        props.setProperty(PropertyKey.loadBalanceSQLExceptionSubclassFailover.getKeyName(), invalidName);
        ex = assertThrows(WrongArgumentException.class, () -> {
            new StandardLoadBalanceExceptionChecker().init(props);
            return null;
        });
        assertEquals(invalidMessage, ex.getMessage());
        assertNull(ex.getCause());

        // Non-reusable authentication plugins must be recreated through the common instantiation path.
        NativeAuthenticationProvider provider = new NativeAuthenticationProvider();
        TestBug39806615AuthenticationPlugin plugin = new TestBug39806615AuthenticationPlugin();
        Field pluginsField = NativeAuthenticationProvider.class.getDeclaredField("authenticationPlugins");
        pluginsField.setAccessible(true);
        pluginsField.set(provider, Collections.singletonMap(plugin.getProtocolPluginName(), plugin));
        Method getPlugin = NativeAuthenticationProvider.class.getDeclaredMethod("getAuthenticationPlugin", String.class);
        getPlugin.setAccessible(true);
        Object newPlugin = getPlugin.invoke(provider, plugin.getProtocolPluginName());
        assertEquals(plugin.getClass(), newPlugin.getClass());
        assertNotSame(plugin, newPlugin);

        // Check decoded URL values and separately supplied properties at the JDBC boundary, before any server connection is needed.
        String url = "jdbc:mysql://127.0.0.1:1/test";
        for (PropertyKey key : new PropertyKey[] { PropertyKey.socketFactory, PropertyKey.propertiesTransform, PropertyKey.logger,
                PropertyKey.exceptionInterceptors, PropertyKey.queryInterceptors }) {
            boolean useUrl = false;
            do {
                Properties connectionProps = new Properties();
                String connectionUrl = url;
                if (useUrl) {
                    connectionUrl += "?" + key.getKeyName() + "=" + URLEncoder.encode(invalidName, "UTF-8");
                } else {
                    connectionProps.setProperty(key.getKeyName(), invalidName);
                }
                final String testUrl = connectionUrl;
                SQLException sqlEx = assertThrows(key.getKeyName(), SQLException.class, () -> DriverManager.getConnection(testUrl, connectionProps));
                Throwable cause = sqlEx;
                while (cause != null && !invalidMessage.equals(cause.getMessage())) {
                    cause = cause.getCause();
                }
                assertNotNull(cause, key.getKeyName() + " must report invalid class-name syntax.");
            } while (useUrl = !useUrl);
        }
        MysqlDataSource ds = new MysqlDataSource();
        ds.setUrl(url);
        ds.getStringProperty(PropertyKey.socketFactory).setValue(invalidName);
        assertThrows(SQLException.class, Pattern.quote(invalidMessage), ds::getConnection);

        // Verify a real connection with a Unicode extension name and the existing short client-info provider fallback.
        props.clear();
        props.setProperty(PropertyKey.socketFactory.getKeyName(), TestBug39806615\u00c9Plugin.class.getName());
        props.setProperty(PropertyKey.clientInfoProvider.getKeyName(), "CommentClientInfoProvider");
        try (Connection testConn = getConnectionWithProps(props)) {
            assertEquals(CommentClientInfoProvider.class, ((JdbcConnection) testConn).getClientInfoProviderImpl().getClass());
        }
    }

    public static class TestBug39806615\u00c9Plugin extends StandardSocketFactory {
    }

    public static class TestBug39806615WrongType {

        static {
            if (true) {
                throw new AssertionError("A class of the wrong type must not be initialized.");
            }
        }

    }

    public static class TestBug39806615BrokenPlugin extends StandardSocketFactory {

        public TestBug39806615BrokenPlugin() {
            throw new IllegalStateException("Plugin constructor failed");
        }

    }

    @SuppressWarnings("deprecation")
    public static class TestBug39806615LegacyPlugin implements com.mysql.jdbc.SocketFactory {

        @Override
        public Socket connect(String host, int portNumber, Properties props) {
            return null;
        }

        @Override
        public Socket beforeHandshake() {
            return null;
        }

        @Override
        public Socket afterHandshake() {
            return null;
        }

    }

    public static class TestBug39806615AuthenticationPlugin implements AuthenticationPlugin<NativePacketPayload> {

        @Override
        public String getProtocolPluginName() {
            return "auth_test_plugin";
        }

        @Override
        public boolean requiresConfidentiality() {
            return false;
        }

        @Override
        public boolean isReusable() {
            return false;
        }

        @Override
        public void setAuthenticationParameters(String user, String password) {
        }

        @Override
        public boolean nextAuthenticationStep(NativePacketPayload fromServer, List<NativePacketPayload> toServer) {
            toServer.clear();
            return true;
        }

    }

}
