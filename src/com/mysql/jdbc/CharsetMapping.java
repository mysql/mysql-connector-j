/*
 Copyright (c) 2002, 2012, Oracle and/or its affiliates. All rights reserved.
 
 

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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

/**
 * Mapping between MySQL charset names and Java charset names. I've investigated
 * placing these in a .properties file, but unfortunately under most appservers
 * this complicates configuration because the security policy needs to be
 * changed by the user to allow the driver to read them :(
 * 
 * @author Mark Matthews
 */
public class CharsetMapping {
	private static final Properties CHARSET_CONFIG = new Properties();

	/**
	 * Map of MySQL-4.1 charset indexes to Java encoding names
	 */
	public static final String[] INDEX_TO_CHARSET;
	
	/**
	 * Map of MySQL-4.1 collation index to collation names
	 */
	public static final String[] INDEX_TO_COLLATION;
	/**
	 * Size of static maps INDEX_TO_JAVA_CHARSET, INDEX_TO_MYSQL_CHARSET, INDEX_TO_COLLATION  
	 */
	public static final int MAP_SIZE = 255;
	/**
	 * Map of MySQL-4.1 collation indexes to MySQL encoding names
	 */
	public static final Map<Integer, String> STATIC_INDEX_TO_MYSQL_CHARSET_MAP;
	/**
	 * Map of MySQL-4.1 charset names to mblen
	 */
	public static final Map<String, Integer> STATIC_CHARSET_TO_NUM_BYTES_MAP;
	/**
	 * Map of MySQL-4.0 charset names to mblen
	 */
	public static final Map<String, Integer> STATIC_4_0_CHARSET_TO_NUM_BYTES_MAP;
	
	/** Mapping of Java charset names to MySQL charset names */
	private static final Map<String, List<VersionedStringProperty>> JAVA_TO_MYSQL_CHARSET_MAP;

	private static final Map<String, List<VersionedStringProperty>> JAVA_UC_TO_MYSQL_CHARSET_MAP;
	
	private static final Map<String, String> ERROR_MESSAGE_FILE_TO_MYSQL_CHARSET_MAP;

	/** Map/List of multibyte character sets (using MySQL names) */
	private static final Map<String, String> MULTIBYTE_CHARSETS;

	public static final Map<String, String> MYSQL_TO_JAVA_CHARSET_MAP;
	
	private static final Map<String, Integer> MYSQL_ENCODING_NAME_TO_CHARSET_INDEX_MAP;

	private static final String MYSQL_CHARSET_NAME_armscii8 =	"armscii8";
	private static final String MYSQL_CHARSET_NAME_ascii =		"ascii";
	private static final String MYSQL_CHARSET_NAME_big5 =		"big5";
	private static final String MYSQL_CHARSET_NAME_binary =		"binary";
	private static final String MYSQL_CHARSET_NAME_cp1250 =		"cp1250";
	private static final String MYSQL_CHARSET_NAME_cp1251 =		"cp1251";
	private static final String MYSQL_CHARSET_NAME_cp1256 =		"cp1256";
	private static final String MYSQL_CHARSET_NAME_cp1257 =		"cp1257";
	private static final String MYSQL_CHARSET_NAME_cp850 =		"cp850";
	private static final String MYSQL_CHARSET_NAME_cp852 =		"cp852";
	private static final String MYSQL_CHARSET_NAME_cp866 =		"cp866";
	private static final String MYSQL_CHARSET_NAME_cp932 =		"cp932";
	private static final String MYSQL_CHARSET_NAME_dec8 =		"dec8";
	private static final String MYSQL_CHARSET_NAME_eucjpms =	"eucjpms";
	private static final String MYSQL_CHARSET_NAME_euckr =		"euckr";
	private static final String MYSQL_CHARSET_NAME_gb2312 =		"gb2312";
	private static final String MYSQL_CHARSET_NAME_gbk =		"gbk";
	private static final String MYSQL_CHARSET_NAME_geostd8 =	"geostd8";
	private static final String MYSQL_CHARSET_NAME_greek =		"greek";
	private static final String MYSQL_CHARSET_NAME_hebrew =		"hebrew";
	private static final String MYSQL_CHARSET_NAME_hp8 =		"hp8";
	private static final String MYSQL_CHARSET_NAME_keybcs2 =	"keybcs2";
	private static final String MYSQL_CHARSET_NAME_koi8r =		"koi8r";
	private static final String MYSQL_CHARSET_NAME_koi8u =		"koi8u";
	private static final String MYSQL_CHARSET_NAME_latin1 =		"latin1";
	private static final String MYSQL_CHARSET_NAME_latin2 =		"latin2";
	private static final String MYSQL_CHARSET_NAME_latin5 =		"latin5";
	private static final String MYSQL_CHARSET_NAME_latin7 =		"latin7";
	private static final String MYSQL_CHARSET_NAME_macce =		"macce";
	private static final String MYSQL_CHARSET_NAME_macroman =	"macroman";
	private static final String MYSQL_CHARSET_NAME_sjis =		"sjis";
	private static final String MYSQL_CHARSET_NAME_swe7 =		"swe7";
	private static final String MYSQL_CHARSET_NAME_tis620 =		"tis620";
	private static final String MYSQL_CHARSET_NAME_ucs2 =		"ucs2";
	private static final String MYSQL_CHARSET_NAME_ujis =		"ujis";
	private static final String MYSQL_CHARSET_NAME_utf16 =		"utf16";
	private static final String MYSQL_CHARSET_NAME_utf16le =	"utf16le";
	private static final String MYSQL_CHARSET_NAME_utf32 =		"utf32";
	private static final String MYSQL_CHARSET_NAME_utf8 =		"utf8";
	private static final String MYSQL_CHARSET_NAME_utf8mb4 =	"utf8mb4";

	private static final String MYSQL_4_0_CHARSET_NAME_croat =		"croat";		// 4.1 =>	27	latin2		latin2_croatian_ci
	private static final String MYSQL_4_0_CHARSET_NAME_czech =		"czech";		// 4.1 =>	2	latin2		latin2_czech_ci
	private static final String MYSQL_4_0_CHARSET_NAME_danish =		"danish";		// 4.1 =>	15	latin1		latin1_danish_ci
	private static final String MYSQL_4_0_CHARSET_NAME_dos =		"dos";			// 4.1 =>	4	cp850		cp850_general_ci
	private static final String MYSQL_4_0_CHARSET_NAME_estonia =	"estonia";		// 4.1 =>	20	latin7		latin7_estonian_ci
	private static final String MYSQL_4_0_CHARSET_NAME_euc_kr =		"euc_kr";		// 4.1 =>	19	euckr		euckr_korean_ci
	private static final String MYSQL_4_0_CHARSET_NAME_german1 =	"german1";		// 4.1 =>	5	latin1		latin1_german1_ci
	private static final String MYSQL_4_0_CHARSET_NAME_hungarian =	"hungarian";	// 4.1 =>	21	latin2		latin2_hungarian_ci
	private static final String MYSQL_4_0_CHARSET_NAME_koi8_ru =	"koi8_ru";		// 4.1 =>	7	koi8r		koi8r_general_ci
	private static final String MYSQL_4_0_CHARSET_NAME_koi8_ukr =	"koi8_ukr";		// 4.1 =>	22	koi8u		koi8u_ukrainian_ci
	private static final String MYSQL_4_0_CHARSET_NAME_latin1_de =	"latin1_de";	// 4.1 =>	31	latin1		latin1_german2_ci
	private static final String MYSQL_4_0_CHARSET_NAME_usa7 =		"usa7";			// 4.1 =>	11	ascii		ascii_general_ci
	private static final String MYSQL_4_0_CHARSET_NAME_win1250 =	"win1250";		// 4.1 =>	26	cp1250		cp1250_general_ci
	private static final String MYSQL_4_0_CHARSET_NAME_win1251 =	"win1251";		// 4.1 =>	17	(removed)
	private static final String MYSQL_4_0_CHARSET_NAME_win1251ukr =	"win1251ukr";	// 4.1 =>	23	cp1251		cp1251_ukrainian_ci

	private static final String NOT_USED = "ISO8859_1"; // punting for not-used character sets

	static {	
		HashMap<String, Integer> tempNumBytesMap = new HashMap<String, Integer>();
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_armscii8,		1);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_ascii,			1);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_big5,			2);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_binary,			1);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_cp1250,			1);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_cp1251,			1);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_cp1256,			1);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_cp1257,			1);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_cp850,			1);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_cp852,			1);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_cp866,			1);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_cp932,			2);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_dec8,			1);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_eucjpms,			3);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_euckr,			2);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_gb2312,			2);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_gbk,				2);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_geostd8,			1);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_greek,			1);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_hebrew,			1);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_hp8,				1);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_keybcs2,			1);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_koi8r,			1);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_koi8u,			1);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_latin1,			1);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_latin2,			1);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_latin5,			1);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_latin7,			1);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_macce,			1);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_macroman,		1);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_sjis,			2);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_swe7,			1);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_tis620,			1);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_ucs2,			2);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_ujis,			3);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_utf16,			4);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_utf16le,			4);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_utf32,			4);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_utf8,			3);
		tempNumBytesMap.put(MYSQL_CHARSET_NAME_utf8mb4,			4);
		STATIC_CHARSET_TO_NUM_BYTES_MAP = Collections.unmodifiableMap(tempNumBytesMap);

		tempNumBytesMap = new HashMap<String, Integer>();
		tempNumBytesMap.put(MYSQL_4_0_CHARSET_NAME_croat,		1);
		tempNumBytesMap.put(MYSQL_4_0_CHARSET_NAME_czech,		1);
		tempNumBytesMap.put(MYSQL_4_0_CHARSET_NAME_danish,		1);
		tempNumBytesMap.put(MYSQL_4_0_CHARSET_NAME_dos,			1);
		tempNumBytesMap.put(MYSQL_4_0_CHARSET_NAME_estonia,		1);
		tempNumBytesMap.put(MYSQL_4_0_CHARSET_NAME_euc_kr,		2);
		tempNumBytesMap.put(MYSQL_4_0_CHARSET_NAME_german1,		1);
		tempNumBytesMap.put(MYSQL_4_0_CHARSET_NAME_hungarian,	1);
		tempNumBytesMap.put(MYSQL_4_0_CHARSET_NAME_koi8_ru,		1);
		tempNumBytesMap.put(MYSQL_4_0_CHARSET_NAME_koi8_ukr,	1);
		tempNumBytesMap.put(MYSQL_4_0_CHARSET_NAME_latin1_de,	1);
		tempNumBytesMap.put(MYSQL_4_0_CHARSET_NAME_usa7,		1);
		tempNumBytesMap.put(MYSQL_4_0_CHARSET_NAME_win1250,		1);
		tempNumBytesMap.put(MYSQL_4_0_CHARSET_NAME_win1251,		1);
		tempNumBytesMap.put(MYSQL_4_0_CHARSET_NAME_win1251ukr,	1);
		STATIC_4_0_CHARSET_TO_NUM_BYTES_MAP = Collections.unmodifiableMap(tempNumBytesMap);

		CHARSET_CONFIG.setProperty("javaToMysqlMappings",
			//
			// Note: This used to be stored in Charsets.properties,
			// but turned out to be problematic when dealing with
			// Tomcat classloaders when the security manager was
			// enabled
			//
			// Java Encoding		MySQL Name (and version, '*' 
			//                           denotes preferred value)      
			//
			"US-ASCII =			usa7,"
	 		+ "US-ASCII =			>4.1.0 ascii,"
	 		+ "Big5 = 				big5,"
	 		+ "GBK = 				gbk,"
	 		+ "SJIS = 				sjis,"
	 		+ "EUC_CN = 			gb2312,"
	 		+ "EUC_JP = 			ujis,"
	 		+ "EUC_JP_Solaris = 	>5.0.3 eucjpms,"
	 		+ "EUC_KR = 			euc_kr,"
	 		+ "EUC_KR = 			>4.1.0 euckr,"
	 		+ "ISO8859_1 =			*latin1,"
	 		+ "ISO8859_1 =			latin1_de,"
	 		+ "ISO8859_1 =			german1,"
	 		+ "ISO8859_1 =			danish,"
	 		+ "ISO8859_2 =			latin2,"
			+ "ISO8859_2 =			czech,"
			+ "ISO8859_2 =			hungarian,"
			+ "ISO8859_2  =		croat,"
			+ "ISO8859_7  =		greek,"
			+ "ISO8859_7  =		latin7,"
			+ "ISO8859_8  = 		hebrew,"
			+ "ISO8859_9  =		latin5,"
	 		+ "ISO8859_13 =		latvian,"
			+ "ISO8859_13 =		latvian1,"
			+ "ISO8859_13 =		estonia,"
			+ "Cp437 =             *>4.1.0 cp850,"
	 		+ "Cp437 =				dos,"
	 		+ "Cp850 =				cp850,"
			+ "Cp852 = 			cp852,"
	 		+ "Cp866 = 			cp866,"
	 		+ "KOI8_R = 			koi8_ru,"
			+ "KOI8_R = 			>4.1.0 koi8r,"
	 		+ "TIS620 = 			tis620,"
			+ "Cp1250 = 			cp1250,"
			+ "Cp1250 = 			win1250,"
			+ "Cp1251 = 			*>4.1.0 cp1251,"
			+ "Cp1251 = 			win1251," 
	 		+ "Cp1251 = 			cp1251cias,"
			+ "Cp1251 = 			cp1251csas,"
			+ "Cp1256 = 			cp1256,"
	 		+ "Cp1251 = 			win1251ukr,"
	 		+ "Cp1252 =             latin1,"
			+ "Cp1257 = 			cp1257,"
			+ "MacRoman = 			macroman,"
			+ "MacCentralEurope = 	macce,"
			+ "UTF-8 = 		utf8,"
			+ "UTF-8 =				*> 5.5.2 utf8mb4,"
			+ "UnicodeBig = 	ucs2,"
			+ "US-ASCII =		binary,"
			+ "Cp943 =        	sjis,"
			+ "MS932 =			sjis,"
			+ "MS932 =        	>4.1.11 cp932,"
			+ "WINDOWS-31J =	sjis,"
			+ "WINDOWS-31J = 	>4.1.11 cp932,"
			+ "CP932 =			sjis,"
			+ "CP932 =			*>4.1.11 cp932,"
			+ "SHIFT_JIS = 	sjis,"
			+ "ASCII =			ascii,"
	        + "LATIN5 =		latin5,"
	        + "LATIN7 =		latin7,"
	        + "HEBREW =		hebrew,"
	        + "GREEK =			greek,"
	        + "EUCKR =			euckr,"
	        + "GB2312 =		gb2312,"
	        + "LATIN2 =		latin2,"
	        + "UTF-16 = 	>5.2.0 utf16,"
	        + "UTF-16LE = 	>5.6.0 utf16le,"
	        + "UTF-32 = 	>5.2.0 utf32");

		HashMap<String, List<VersionedStringProperty>> javaToMysqlMap = new HashMap<String, List<VersionedStringProperty>>();

		populateMapWithKeyValuePairsVersioned("javaToMysqlMappings", javaToMysqlMap, false);
		JAVA_TO_MYSQL_CHARSET_MAP = Collections.unmodifiableMap(javaToMysqlMap);


		HashMap<String, String> mysqlToJavaMap = new HashMap<String, String>();

		Set<String> keySet = JAVA_TO_MYSQL_CHARSET_MAP.keySet();
		Iterator<String> javaCharsets = keySet.iterator();
		while (javaCharsets.hasNext()) {
			String javaEncodingName = javaCharsets.next();
			List<VersionedStringProperty> mysqlEncodingList = JAVA_TO_MYSQL_CHARSET_MAP.get(javaEncodingName);

			Iterator<VersionedStringProperty> mysqlEncodings = mysqlEncodingList.iterator();

			String mysqlEncodingName = null;

			while (mysqlEncodings.hasNext()) {
				VersionedStringProperty mysqlProp = mysqlEncodings.next();
				mysqlEncodingName = mysqlProp.toString();

				mysqlToJavaMap.put(mysqlEncodingName, javaEncodingName);
				mysqlToJavaMap.put(mysqlEncodingName.toUpperCase(Locale.ENGLISH), javaEncodingName);
			}
		}

		// we don't want CP932 to map to CP932
		mysqlToJavaMap.put("cp932", "Windows-31J");
		mysqlToJavaMap.put("CP932", "Windows-31J");

		MYSQL_TO_JAVA_CHARSET_MAP = Collections.unmodifiableMap(mysqlToJavaMap);


		TreeMap<String, List<VersionedStringProperty>> ucMap = new TreeMap<String, List<VersionedStringProperty>>(String.CASE_INSENSITIVE_ORDER);
		Iterator<String> javaNamesKeys = JAVA_TO_MYSQL_CHARSET_MAP.keySet().iterator();
		while (javaNamesKeys.hasNext()) {
			String key = javaNamesKeys.next();
			ucMap.put(key.toUpperCase(Locale.ENGLISH), JAVA_TO_MYSQL_CHARSET_MAP.get(key));
		}
		JAVA_UC_TO_MYSQL_CHARSET_MAP = Collections.unmodifiableMap(ucMap);

		//
		// Character sets that we can't convert
		// ourselves.
		//
		HashMap<String, String> tempMapMulti = new HashMap<String, String>();

		CHARSET_CONFIG.setProperty("multibyteCharsets", 
			//
			// Note: This used to be stored in Charsets.properties,
			// but turned out to be problematic when dealing with
			// Tomcat classloaders when the security manager was
			// enabled
			//
			//   Java Name			MySQL Name (not currently used)
			//
				
	        "Big5 = 			big5,"
	 		+ "GBK = 			gbk,"
	 		+ "SJIS = 			sjis,"
	 		+ "EUC_CN = 		gb2312,"
	 		+ "EUC_JP = 		ujis,"
	 		+ "EUC_JP_Solaris = eucjpms,"
	 		+ "EUC_KR = 		euc_kr,"
	 		+ "EUC_KR = 		>4.1.0 euckr,"
	 		+ "Cp943 =        	sjis,"
	 		+ "Cp943 = 		cp943,"
	 		+ "WINDOWS-31J =	sjis,"
	 		+ "WINDOWS-31J = 	cp932,"
	 		+ "CP932 =			cp932,"
	 		+ "MS932 =			sjis,"
	 		+ "MS932 =        	cp932,"
	 		+ "SHIFT_JIS = 	sjis,"
	 		+ "EUCKR =			euckr,"
	 		+ "GB2312 =		gb2312,"
	 		+ "UTF-8 = 		utf8,"
	 		+ "utf8 =          utf8,"
	 		+ "UnicodeBig = 	ucs2,"
			+ "UTF-16 = 	>5.2.0 utf16,"
			+ "UTF-16LE = 	>5.6.0 utf16le,"
			+ "UTF-32 = 	>5.2.0 utf32");
		
		populateMapWithKeyValuePairsUnversioned("multibyteCharsets", tempMapMulti, true);

		MULTIBYTE_CHARSETS = Collections.unmodifiableMap(tempMapMulti);

		Collation[] collation = new Collation[MAP_SIZE];
		collation[1] = new Collation(1,		"big5_chinese_ci",		MYSQL_CHARSET_NAME_big5);
		collation[2] = new Collation(2,		"latin2_czech_cs",		MYSQL_CHARSET_NAME_latin2);
		collation[3] = new Collation(3,		"dec8_swedish_ci",		MYSQL_CHARSET_NAME_dec8,	"ISO8859_1");	// punting for "dec8"
		collation[4] = new Collation(4,		"cp850_general_ci",		MYSQL_CHARSET_NAME_cp850,	"ISO8859_1");	// punting for "dos"
		collation[5] = new Collation(5,		"latin1_german1_ci",	MYSQL_CHARSET_NAME_latin1);
		collation[6] = new Collation(6,		"hp8_english_ci",		MYSQL_CHARSET_NAME_hp8,		"ISO8859_1");	// punting for "hp8"
		collation[7] = new Collation(7,		"koi8r_general_ci",		MYSQL_CHARSET_NAME_koi8r);
		collation[8] = new Collation(8,		"latin1_swedish_ci",	MYSQL_CHARSET_NAME_latin1 );//"Cp1252"
		collation[9] = new Collation(9,		"latin2_general_ci",	MYSQL_CHARSET_NAME_latin2);
		collation[10] = new Collation(10,	"swe7_swedish_ci",		MYSQL_CHARSET_NAME_swe7,	"ISO8859_1");	// punting for "swe7"
		collation[11] = new Collation(11,	"ascii_general_ci",		MYSQL_CHARSET_NAME_ascii);
		collation[12] = new Collation(12,	"ujis_japanese_ci",		MYSQL_CHARSET_NAME_ujis);
		collation[13] = new Collation(13,	"sjis_japanese_ci",		MYSQL_CHARSET_NAME_sjis);
		collation[14] = new Collation(14,	"cp1251_bulgarian_ci",	MYSQL_CHARSET_NAME_cp1251);
		collation[15] = new Collation(15,	"latin1_danish_ci",		MYSQL_CHARSET_NAME_latin1);
		collation[16] = new Collation(16,	"hebrew_general_ci",	MYSQL_CHARSET_NAME_hebrew);
		collation[17] = new Collation(17,	"latin1_german1_ci",	MYSQL_4_0_CHARSET_NAME_win1251); 			// removed since 4.1
		collation[18] = new Collation(18,	"tis620_thai_ci",		MYSQL_CHARSET_NAME_tis620);
		collation[19] = new Collation(19,	"euckr_korean_ci",		MYSQL_CHARSET_NAME_euckr);
		collation[20] = new Collation(20,	"latin7_estonian_cs",	MYSQL_CHARSET_NAME_latin7,	"ISO8859_13");	// punting for "estonia";
		collation[21] = new Collation(21,	"latin2_hungarian_ci",	MYSQL_CHARSET_NAME_latin2);
		collation[22] = new Collation(22,	"koi8u_general_ci",		MYSQL_CHARSET_NAME_koi8u,	"KOI8_R");		// punting for "koi8_ukr"
		collation[23] = new Collation(23,	"cp1251_ukrainian_ci",	MYSQL_CHARSET_NAME_cp1251);
		collation[24] = new Collation(24,	"gb2312_chinese_ci",	MYSQL_CHARSET_NAME_gb2312);
		collation[25] = new Collation(25,	"greek_general_ci",		MYSQL_CHARSET_NAME_greek);
		collation[26] = new Collation(26,	"cp1250_general_ci",	MYSQL_CHARSET_NAME_cp1250);
		collation[27] = new Collation(27,	"latin2_croatian_ci",	MYSQL_CHARSET_NAME_latin2);
		collation[28] = new Collation(28,	"gbk_chinese_ci",		MYSQL_CHARSET_NAME_gbk);
		collation[29] = new Collation(29,	"cp1257_lithuanian_ci",	MYSQL_CHARSET_NAME_cp1257);
		collation[30] = new Collation(30,	"latin5_turkish_ci",	MYSQL_CHARSET_NAME_latin5);
		collation[31] = new Collation(31,	"latin1_german2_ci",	MYSQL_CHARSET_NAME_latin1);
		collation[32] = new Collation(32,	"armscii8_general_ci",	MYSQL_CHARSET_NAME_armscii8,"ISO8859_1");
		collation[33] = new Collation(33,	"utf8_general_ci",		MYSQL_CHARSET_NAME_utf8);
		collation[34] = new Collation(34,	"cp1250_czech_cs",		MYSQL_CHARSET_NAME_cp1250);
		collation[35] = new Collation(35,	"ucs2_general_ci",		MYSQL_CHARSET_NAME_ucs2);
		collation[36] = new Collation(36,	"cp866_general_ci",		MYSQL_CHARSET_NAME_cp866);
		collation[37] = new Collation(37,	"keybcs2_general_ci",	MYSQL_CHARSET_NAME_keybcs2,	"Cp895");
		collation[38] = new Collation(38,	"macce_general_ci",		MYSQL_CHARSET_NAME_macce);
		collation[39] = new Collation(39,	"macroman_general_ci",	MYSQL_CHARSET_NAME_macroman);
		collation[40] = new Collation(40,	"cp852_general_ci",		MYSQL_CHARSET_NAME_cp852,	"LATIN2");		// punting for "pclatin2"
		collation[41] = new Collation(41,	"latin7_general_ci",	MYSQL_CHARSET_NAME_latin7,	"ISO8859_13");	// punting for "latvian";
		collation[42] = new Collation(42,	"latin7_general_cs",	MYSQL_CHARSET_NAME_latin7,	"ISO8859_13");	// punting for "latvian1";
		collation[43] = new Collation(43,	"macce_bin",			MYSQL_CHARSET_NAME_macce);
		collation[44] = new Collation(44,	"cp1250_croatian_ci",	MYSQL_CHARSET_NAME_cp1250);
		collation[45] = new Collation(45,	"utf8mb4_general_ci",	MYSQL_CHARSET_NAME_utf8mb4);
		collation[46] = new Collation(46,	"utf8mb4_bin",			MYSQL_CHARSET_NAME_utf8mb4);
		collation[47] = new Collation(47,	"latin1_bin",			MYSQL_CHARSET_NAME_latin1);
		collation[48] = new Collation(48,	"latin1_general_ci",	MYSQL_CHARSET_NAME_latin1);
		collation[49] = new Collation(49,	"latin1_general_cs",	MYSQL_CHARSET_NAME_latin1);
		collation[50] = new Collation(50,	"cp1251_bin",			MYSQL_CHARSET_NAME_cp1251);
		collation[51] = new Collation(51,	"cp1251_general_ci",	MYSQL_CHARSET_NAME_cp1251);
		collation[52] = new Collation(52,	"cp1251_general_cs",	MYSQL_CHARSET_NAME_cp1251);
		collation[53] = new Collation(53,	"macroman_bin",			MYSQL_CHARSET_NAME_macroman);
		collation[54] = new Collation(54,	"utf16_general_ci",		MYSQL_CHARSET_NAME_utf16);
		collation[55] = new Collation(55,	"utf16_bin",			MYSQL_CHARSET_NAME_utf16);
		collation[56] = new Collation(56,	"utf16le_general_ci",	MYSQL_CHARSET_NAME_utf16le);
		collation[57] = new Collation(57,	"cp1256_general_ci",	MYSQL_CHARSET_NAME_cp1256);
		collation[58] = new Collation(58,	"cp1257_bin",			MYSQL_CHARSET_NAME_cp1257);
		collation[59] = new Collation(59,	"cp1257_general_ci",	MYSQL_CHARSET_NAME_cp1257);
		collation[60] = new Collation(60,	"utf32_general_ci",		MYSQL_CHARSET_NAME_utf32);
		collation[61] = new Collation(61,	"utf32_bin",			MYSQL_CHARSET_NAME_utf32);
		collation[62] = new Collation(62,	"utf16le_bin",			MYSQL_CHARSET_NAME_utf16le);
		collation[63] = new Collation(63,	"binary",				MYSQL_CHARSET_NAME_binary);
		collation[64] = new Collation(64,	"armscii8_bin",			MYSQL_CHARSET_NAME_armscii8,"ISO8859_2");	// punting "armscii"
		collation[65] = new Collation(65,	"ascii_bin",			MYSQL_CHARSET_NAME_ascii);
		collation[66] = new Collation(66,	"cp1250_bin",			MYSQL_CHARSET_NAME_cp1250);
		collation[67] = new Collation(67,	"cp1256_bin",			MYSQL_CHARSET_NAME_cp1256);
		collation[68] = new Collation(68,	"cp866_bin",			MYSQL_CHARSET_NAME_cp866);
		collation[69] = new Collation(69,	"dec8_bin",				MYSQL_CHARSET_NAME_dec8,	"US-ASCII");	// punting for "dec8"
		collation[70] = new Collation(70,	"greek_bin",			MYSQL_CHARSET_NAME_greek);
		collation[71] = new Collation(71,	"hebrew_bin",			MYSQL_CHARSET_NAME_hebrew);
		collation[72] = new Collation(72,	"hp8_bin",				MYSQL_CHARSET_NAME_hp8,		"US-ASCII");	// punting for "hp8"
		collation[73] = new Collation(73,	"keybcs2_bin",			MYSQL_CHARSET_NAME_keybcs2,	"Cp895");		// punting for "keybcs2"
		collation[74] = new Collation(74,	"koi8r_bin",			MYSQL_CHARSET_NAME_koi8r);
		collation[75] = new Collation(75,	"koi8u_bin",			MYSQL_CHARSET_NAME_koi8u,	"KOI8_R");		// punting for koi8ukr"
		collation[76] = new Collation(76,	"latin1_german1_ci",	MYSQL_CHARSET_NAME_latin1,	NOT_USED);
		collation[77] = new Collation(77,	"latin2_bin",			MYSQL_CHARSET_NAME_latin2);
		collation[78] = new Collation(78,	"latin5_bin",			MYSQL_CHARSET_NAME_latin5);
		collation[79] = new Collation(79,	"latin7_bin",			MYSQL_CHARSET_NAME_latin7);
		collation[80] = new Collation(80,	"cp850_bin",			MYSQL_CHARSET_NAME_cp850);
		collation[81] = new Collation(81,	"cp852_bin",			MYSQL_CHARSET_NAME_cp852);
		collation[82] = new Collation(82,	"swe7_bin",				MYSQL_CHARSET_NAME_swe7,	"ISO8859_1");	//"ISO8859_1"; // punting for "swe7"
		collation[83] = new Collation(83,	"utf8_bin",				MYSQL_CHARSET_NAME_utf8);
		collation[84] = new Collation(84,	"big5_bin",				MYSQL_CHARSET_NAME_big5);
		collation[85] = new Collation(85,	"euckr_bin",			MYSQL_CHARSET_NAME_euckr);
		collation[86] = new Collation(86,	"gb2312_bin",			MYSQL_CHARSET_NAME_gb2312);
		collation[87] = new Collation(87,	"gbk_bin",				MYSQL_CHARSET_NAME_gbk);
		collation[88] = new Collation(88,	"sjis_bin",				MYSQL_CHARSET_NAME_sjis);
		collation[89] = new Collation(89,	"tis620_bin",			MYSQL_CHARSET_NAME_tis620);
		collation[90] = new Collation(90,	"ucs2_bin",				MYSQL_CHARSET_NAME_ucs2);
		collation[91] = new Collation(91,	"ujis_bin",				MYSQL_CHARSET_NAME_ujis);
		collation[92] = new Collation(92,	"geostd8_general_ci",	MYSQL_CHARSET_NAME_geostd8,	"US-ASCII");	//"US-ASCII"; //punting for "geostd8"
		collation[93] = new Collation(93,	"geostd8_bin",			MYSQL_CHARSET_NAME_geostd8,	"US-ASCII");	//"US-ASCII"; // punting for "geostd8"
		collation[94] = new Collation(94,	"latin1_spanish_ci",	MYSQL_CHARSET_NAME_latin1);
		collation[95] = new Collation(95,	"cp932_japanese_ci",	MYSQL_CHARSET_NAME_cp932);
		collation[96] = new Collation(96,	"cp932_bin",			MYSQL_CHARSET_NAME_cp932);
		collation[97] = new Collation(97,	"eucjpms_japanese_ci",	MYSQL_CHARSET_NAME_eucjpms);
		collation[98] = new Collation(98,	"eucjpms_bin",			MYSQL_CHARSET_NAME_eucjpms);
		collation[99] = new Collation(99,	"cp1250_polish_ci",		MYSQL_CHARSET_NAME_cp1250);
		collation[100] = new Collation(100,	"latin1_german1_ci",	MYSQL_CHARSET_NAME_latin1,	NOT_USED);
		collation[101] = new Collation(101,	"utf16_unicode_ci",		MYSQL_CHARSET_NAME_utf16);
		collation[102] = new Collation(102,	"utf16_icelandic_ci",	MYSQL_CHARSET_NAME_utf16);
		collation[103] = new Collation(103,	"utf16_latvian_ci",		MYSQL_CHARSET_NAME_utf16);
		collation[104] = new Collation(104,	"utf16_romanian_ci",	MYSQL_CHARSET_NAME_utf16);
		collation[105] = new Collation(105,	"utf16_slovenian_ci",	MYSQL_CHARSET_NAME_utf16);
		collation[106] = new Collation(106,	"utf16_polish_ci",		MYSQL_CHARSET_NAME_utf16);
		collation[107] = new Collation(107,	"utf16_estonian_ci",	MYSQL_CHARSET_NAME_utf16);
		collation[108] = new Collation(108,	"utf16_spanish_ci",		MYSQL_CHARSET_NAME_utf16);
		collation[109] = new Collation(109,	"utf16_swedish_ci",		MYSQL_CHARSET_NAME_utf16);
		collation[110] = new Collation(110,	"utf16_turkish_ci",		MYSQL_CHARSET_NAME_utf16);
		collation[111] = new Collation(111,	"utf16_czech_ci",		MYSQL_CHARSET_NAME_utf16);
		collation[112] = new Collation(112,	"utf16_danish_ci",		MYSQL_CHARSET_NAME_utf16);
		collation[113] = new Collation(113,	"utf16_lithuanian_ci",	MYSQL_CHARSET_NAME_utf16);
		collation[114] = new Collation(114,	"utf16_slovak_ci",		MYSQL_CHARSET_NAME_utf16);
		collation[115] = new Collation(115,	"utf16_spanish2_ci",	MYSQL_CHARSET_NAME_utf16);
		collation[116] = new Collation(116,	"utf16_roman_ci",		MYSQL_CHARSET_NAME_utf16);
		collation[117] = new Collation(117,	"utf16_persian_ci",		MYSQL_CHARSET_NAME_utf16);
		collation[118] = new Collation(118,	"utf16_esperanto_ci",	MYSQL_CHARSET_NAME_utf16);
		collation[119] = new Collation(119,	"utf16_hungarian_ci",	MYSQL_CHARSET_NAME_utf16);
		collation[120] = new Collation(120,	"utf16_sinhala_ci",		MYSQL_CHARSET_NAME_utf16);
		collation[121] = new Collation(121,	"utf16_german2_ci",		MYSQL_CHARSET_NAME_utf16);
		collation[122] = new Collation(122,	"utf16_croatian_ci",	MYSQL_CHARSET_NAME_utf16);
		collation[123] = new Collation(123,	"utf16_unicode_520_ci",	MYSQL_CHARSET_NAME_utf16);
		collation[124] = new Collation(124,	"utf16_vietnamese_ci",	MYSQL_CHARSET_NAME_utf16);
		collation[125] = new Collation(125,	"latin1_german1_ci",	MYSQL_CHARSET_NAME_latin1,	NOT_USED);
		collation[126] = new Collation(126,	"latin1_german1_ci",	MYSQL_CHARSET_NAME_latin1,	NOT_USED);
		collation[127] = new Collation(127,	"latin1_german1_ci",	MYSQL_CHARSET_NAME_latin1,	NOT_USED);
		collation[128] = new Collation(128,	"ucs2_unicode_ci",		MYSQL_CHARSET_NAME_ucs2);
		collation[129] = new Collation(129,	"ucs2_icelandic_ci",	MYSQL_CHARSET_NAME_ucs2);
		collation[130] = new Collation(130,	"ucs2_latvian_ci",		MYSQL_CHARSET_NAME_ucs2);
		collation[131] = new Collation(131,	"ucs2_romanian_ci",		MYSQL_CHARSET_NAME_ucs2);
		collation[132] = new Collation(132,	"ucs2_slovenian_ci",	MYSQL_CHARSET_NAME_ucs2);
		collation[133] = new Collation(133,	"ucs2_polish_ci",		MYSQL_CHARSET_NAME_ucs2);
		collation[134] = new Collation(134,	"ucs2_estonian_ci",		MYSQL_CHARSET_NAME_ucs2);
		collation[135] = new Collation(135,	"ucs2_spanish_ci",		MYSQL_CHARSET_NAME_ucs2);
		collation[136] = new Collation(136,	"ucs2_swedish_ci",		MYSQL_CHARSET_NAME_ucs2);
		collation[137] = new Collation(137,	"ucs2_turkish_ci",		MYSQL_CHARSET_NAME_ucs2);
		collation[138] = new Collation(138,	"ucs2_czech_ci",		MYSQL_CHARSET_NAME_ucs2);
		collation[139] = new Collation(139,	"ucs2_danish_ci",		MYSQL_CHARSET_NAME_ucs2);
		collation[140] = new Collation(140,	"ucs2_lithuanian_ci",	MYSQL_CHARSET_NAME_ucs2);
		collation[141] = new Collation(141,	"ucs2_slovak_ci",		MYSQL_CHARSET_NAME_ucs2);
		collation[142] = new Collation(142,	"ucs2_spanish2_ci",		MYSQL_CHARSET_NAME_ucs2);
		collation[143] = new Collation(143,	"ucs2_roman_ci",		MYSQL_CHARSET_NAME_ucs2);
		collation[144] = new Collation(144,	"ucs2_persian_ci",		MYSQL_CHARSET_NAME_ucs2);
		collation[145] = new Collation(145,	"ucs2_esperanto_ci",	MYSQL_CHARSET_NAME_ucs2);
		collation[146] = new Collation(146,	"ucs2_hungarian_ci",	MYSQL_CHARSET_NAME_ucs2);
		collation[147] = new Collation(147,	"ucs2_sinhala_ci",		MYSQL_CHARSET_NAME_ucs2);
		collation[148] = new Collation(148,	"ucs2_german2_ci",		MYSQL_CHARSET_NAME_ucs2);
		collation[149] = new Collation(149,	"ucs2_croatian_ci",		MYSQL_CHARSET_NAME_ucs2);
		collation[150] = new Collation(150,	"ucs2_unicode_520_ci",	MYSQL_CHARSET_NAME_ucs2);
		collation[151] = new Collation(151,	"ucs2_vietnamese_ci",	MYSQL_CHARSET_NAME_ucs2);
		collation[152] = new Collation(152,	"latin1_german1_ci",	MYSQL_CHARSET_NAME_latin1,	NOT_USED);
		collation[153] = new Collation(153,	"latin1_german1_ci",	MYSQL_CHARSET_NAME_latin1,	NOT_USED);
		collation[154] = new Collation(154,	"latin1_german1_ci",	MYSQL_CHARSET_NAME_latin1,	NOT_USED);
		collation[155] = new Collation(155,	"latin1_german1_ci",	MYSQL_CHARSET_NAME_latin1,	NOT_USED);
		collation[156] = new Collation(156,	"latin1_german1_ci",	MYSQL_CHARSET_NAME_latin1,	NOT_USED);
		collation[157] = new Collation(157,	"latin1_german1_ci",	MYSQL_CHARSET_NAME_latin1,	NOT_USED);
		collation[158] = new Collation(158,	"latin1_german1_ci",	MYSQL_CHARSET_NAME_latin1,	NOT_USED);
		collation[159] = new Collation(159,	"ucs2_general_mysql500_ci",	MYSQL_CHARSET_NAME_ucs2);
		collation[160] = new Collation(160,	"utf32_unicode_ci",		MYSQL_CHARSET_NAME_utf32);
		collation[161] = new Collation(161,	"utf32_icelandic_ci",	MYSQL_CHARSET_NAME_utf32);
		collation[162] = new Collation(162,	"utf32_latvian_ci",		MYSQL_CHARSET_NAME_utf32);
		collation[163] = new Collation(163,	"utf32_romanian_ci",	MYSQL_CHARSET_NAME_utf32);
		collation[164] = new Collation(164,	"utf32_slovenian_ci",	MYSQL_CHARSET_NAME_utf32);
		collation[165] = new Collation(165,	"utf32_polish_ci",		MYSQL_CHARSET_NAME_utf32);
		collation[166] = new Collation(166,	"utf32_estonian_ci",	MYSQL_CHARSET_NAME_utf32);
		collation[167] = new Collation(167,	"utf32_spanish_ci",		MYSQL_CHARSET_NAME_utf32);
		collation[168] = new Collation(168,	"utf32_swedish_ci",		MYSQL_CHARSET_NAME_utf32);
		collation[169] = new Collation(169,	"utf32_turkish_ci",		MYSQL_CHARSET_NAME_utf32);
		collation[170] = new Collation(170,	"utf32_czech_ci",		MYSQL_CHARSET_NAME_utf32);
		collation[171] = new Collation(171,	"utf32_danish_ci",		MYSQL_CHARSET_NAME_utf32);
		collation[172] = new Collation(172,	"utf32_lithuanian_ci",	MYSQL_CHARSET_NAME_utf32);
		collation[173] = new Collation(173,	"utf32_slovak_ci",		MYSQL_CHARSET_NAME_utf32);
		collation[174] = new Collation(174,	"utf32_spanish2_ci",	MYSQL_CHARSET_NAME_utf32);
		collation[175] = new Collation(175,	"utf32_roman_ci",		MYSQL_CHARSET_NAME_utf32);
		collation[176] = new Collation(176,	"utf32_persian_ci",		MYSQL_CHARSET_NAME_utf32);
		collation[177] = new Collation(177,	"utf32_esperanto_ci",	MYSQL_CHARSET_NAME_utf32);
		collation[178] = new Collation(178,	"utf32_hungarian_ci",	MYSQL_CHARSET_NAME_utf32);
		collation[179] = new Collation(179,	"utf32_sinhala_ci",		MYSQL_CHARSET_NAME_utf32);
		collation[180] = new Collation(180,	"utf32_german2_ci",		MYSQL_CHARSET_NAME_utf32);
		collation[181] = new Collation(181,	"utf32_croatian_ci",	MYSQL_CHARSET_NAME_utf32);
		collation[182] = new Collation(182,	"utf32_unicode_520_ci",	MYSQL_CHARSET_NAME_utf32);
		collation[183] = new Collation(183,	"utf32_vietnamese_ci",	MYSQL_CHARSET_NAME_utf32);
		collation[184] = new Collation(184,	"latin1_german1_ci",	MYSQL_CHARSET_NAME_latin1,	NOT_USED);
		collation[185] = new Collation(185,	"latin1_german1_ci",	MYSQL_CHARSET_NAME_latin1,	NOT_USED);
		collation[186] = new Collation(186,	"latin1_german1_ci",	MYSQL_CHARSET_NAME_latin1,	NOT_USED);
		collation[187] = new Collation(187,	"latin1_german1_ci",	MYSQL_CHARSET_NAME_latin1,	NOT_USED);
		collation[188] = new Collation(188,	"latin1_german1_ci",	MYSQL_CHARSET_NAME_latin1,	NOT_USED);
		collation[189] = new Collation(189,	"latin1_german1_ci",	MYSQL_CHARSET_NAME_latin1,	NOT_USED);
		collation[190] = new Collation(190,	"latin1_german1_ci",	MYSQL_CHARSET_NAME_latin1,	NOT_USED);
		collation[191] = new Collation(191,	"latin1_german1_ci",	MYSQL_CHARSET_NAME_latin1,	NOT_USED);
		collation[192] = new Collation(192,	"utf8_unicode_ci",		MYSQL_CHARSET_NAME_utf8);
		collation[193] = new Collation(193,	"utf8_icelandic_ci",	MYSQL_CHARSET_NAME_utf8);
		collation[194] = new Collation(194,	"utf8_latvian_ci",		MYSQL_CHARSET_NAME_utf8);
		collation[195] = new Collation(195,	"utf8_romanian_ci",		MYSQL_CHARSET_NAME_utf8);
		collation[196] = new Collation(196,	"utf8_slovenian_ci",	MYSQL_CHARSET_NAME_utf8);
		collation[197] = new Collation(197,	"utf8_polish_ci",		MYSQL_CHARSET_NAME_utf8);
		collation[198] = new Collation(198,	"utf8_estonian_ci",		MYSQL_CHARSET_NAME_utf8);
		collation[199] = new Collation(199,	"utf8_spanish_ci",		MYSQL_CHARSET_NAME_utf8);
		collation[200] = new Collation(200,	"utf8_swedish_ci",		MYSQL_CHARSET_NAME_utf8);
		collation[201] = new Collation(201,	"utf8_turkish_ci",		MYSQL_CHARSET_NAME_utf8);
		collation[202] = new Collation(202,	"utf8_czech_ci",		MYSQL_CHARSET_NAME_utf8);
		collation[203] = new Collation(203,	"utf8_danish_ci",		MYSQL_CHARSET_NAME_utf8);
		collation[204] = new Collation(204,	"utf8_lithuanian_ci",	MYSQL_CHARSET_NAME_utf8);
		collation[205] = new Collation(205,	"utf8_slovak_ci",		MYSQL_CHARSET_NAME_utf8);
		collation[206] = new Collation(206,	"utf8_spanish2_ci",		MYSQL_CHARSET_NAME_utf8);
		collation[207] = new Collation(207,	"utf8_roman_ci",		MYSQL_CHARSET_NAME_utf8);
		collation[208] = new Collation(208,	"utf8_persian_ci",		MYSQL_CHARSET_NAME_utf8);
		collation[209] = new Collation(209,	"utf8_esperanto_ci",	MYSQL_CHARSET_NAME_utf8);
		collation[210] = new Collation(210,	"utf8_hungarian_ci",	MYSQL_CHARSET_NAME_utf8);
		collation[211] = new Collation(211,	"utf8_sinhala_ci",		MYSQL_CHARSET_NAME_utf8);
		collation[212] = new Collation(212,	"utf8_german2_ci",		MYSQL_CHARSET_NAME_utf8);
		collation[213] = new Collation(213,	"utf8_croatian_ci",		MYSQL_CHARSET_NAME_utf8);
		collation[214] = new Collation(214,	"utf8_unicode_520_ci",	MYSQL_CHARSET_NAME_utf8);
		collation[215] = new Collation(215,	"utf8_vietnamese_ci",	MYSQL_CHARSET_NAME_utf8);
		collation[216] = new Collation(216,	"latin1_german1_ci",	MYSQL_CHARSET_NAME_latin1,	NOT_USED);
		collation[217] = new Collation(217,	"latin1_german1_ci",	MYSQL_CHARSET_NAME_latin1,	NOT_USED);
		collation[218] = new Collation(218,	"latin1_german1_ci",	MYSQL_CHARSET_NAME_latin1,	NOT_USED);
		collation[219] = new Collation(219,	"latin1_german1_ci",	MYSQL_CHARSET_NAME_latin1,	NOT_USED);
		collation[220] = new Collation(220,	"latin1_german1_ci",	MYSQL_CHARSET_NAME_latin1,	NOT_USED);
		collation[221] = new Collation(221,	"latin1_german1_ci",	MYSQL_CHARSET_NAME_latin1,	NOT_USED);
		collation[222] = new Collation(222,	"latin1_german1_ci",	MYSQL_CHARSET_NAME_latin1,	NOT_USED);
		collation[223] = new Collation(223,	"utf8_general_mysql500_ci",	MYSQL_CHARSET_NAME_utf8);
		collation[224] = new Collation(224,	"utf8mb4_unicode_ci",	MYSQL_CHARSET_NAME_utf8mb4);
		collation[225] = new Collation(225,	"utf8mb4_icelandic_ci",	MYSQL_CHARSET_NAME_utf8mb4);
		collation[226] = new Collation(226,	"utf8mb4_latvian_ci",	MYSQL_CHARSET_NAME_utf8mb4);
		collation[227] = new Collation(227,	"utf8mb4_romanian_ci",	MYSQL_CHARSET_NAME_utf8mb4);
		collation[228] = new Collation(228,	"utf8mb4_slovenian_ci",	MYSQL_CHARSET_NAME_utf8mb4);
		collation[229] = new Collation(229,	"utf8mb4_polish_ci",	MYSQL_CHARSET_NAME_utf8mb4);
		collation[230] = new Collation(230,	"utf8mb4_estonian_ci",	MYSQL_CHARSET_NAME_utf8mb4);
		collation[231] = new Collation(231,	"utf8mb4_spanish_ci",	MYSQL_CHARSET_NAME_utf8mb4);
		collation[232] = new Collation(232,	"utf8mb4_swedish_ci",	MYSQL_CHARSET_NAME_utf8mb4);
		collation[233] = new Collation(233,	"utf8mb4_turkish_ci",	MYSQL_CHARSET_NAME_utf8mb4);
		collation[234] = new Collation(234,	"utf8mb4_czech_ci",		MYSQL_CHARSET_NAME_utf8mb4);
		collation[235] = new Collation(235,	"utf8mb4_danish_ci",	MYSQL_CHARSET_NAME_utf8mb4);
		collation[236] = new Collation(236,	"utf8mb4_lithuanian_ci",MYSQL_CHARSET_NAME_utf8mb4);
		collation[237] = new Collation(237,	"utf8mb4_slovak_ci",	MYSQL_CHARSET_NAME_utf8mb4);
		collation[238] = new Collation(238,	"utf8mb4_spanish2_ci",	MYSQL_CHARSET_NAME_utf8mb4);
		collation[239] = new Collation(239,	"utf8mb4_roman_ci",		MYSQL_CHARSET_NAME_utf8mb4);
		collation[240] = new Collation(240,	"utf8mb4_persian_ci",	MYSQL_CHARSET_NAME_utf8mb4);
		collation[241] = new Collation(241,	"utf8mb4_esperanto_ci",	MYSQL_CHARSET_NAME_utf8mb4);
		collation[242] = new Collation(242,	"utf8mb4_hungarian_ci",	MYSQL_CHARSET_NAME_utf8mb4);
		collation[243] = new Collation(243,	"utf8mb4_sinhala_ci",	MYSQL_CHARSET_NAME_utf8mb4);
		collation[244] = new Collation(244,	"utf8mb4_german2_ci",	MYSQL_CHARSET_NAME_utf8mb4);
		collation[245] = new Collation(245,	"utf8mb4_croatian_ci",	MYSQL_CHARSET_NAME_utf8mb4);
		collation[246] = new Collation(246,	"utf8mb4_unicode_520_ci",	MYSQL_CHARSET_NAME_utf8mb4);
		collation[247] = new Collation(247,	"utf8mb4_vietnamese_ci",	MYSQL_CHARSET_NAME_utf8mb4);
		collation[248] = new Collation(248,	"latin1_german1_ci",	MYSQL_CHARSET_NAME_latin1,	NOT_USED);
		collation[249] = new Collation(249,	"latin1_german1_ci",	MYSQL_CHARSET_NAME_latin1,	NOT_USED);
		collation[250] = new Collation(250,	"latin1_german1_ci",	MYSQL_CHARSET_NAME_latin1,	NOT_USED);
		collation[251] = new Collation(251,	"latin1_german1_ci",	MYSQL_CHARSET_NAME_latin1,	NOT_USED);
		collation[252] = new Collation(252,	"latin1_german1_ci",	MYSQL_CHARSET_NAME_latin1,	NOT_USED);
		collation[253] = new Collation(253,	"latin1_german1_ci",	MYSQL_CHARSET_NAME_latin1,	NOT_USED);
		collation[254] = new Collation(254,	"utf8mb3_general_cs",	MYSQL_CHARSET_NAME_utf8);
		
		INDEX_TO_COLLATION = new String[MAP_SIZE];
		INDEX_TO_CHARSET = new String[MAP_SIZE];
		Map<Integer, String> indexToMysqlCharset = new HashMap<Integer, String>();
		Map<String, Integer> indexMap = new TreeMap<String, Integer>(String.CASE_INSENSITIVE_ORDER);

		for (int i = 1; i < MAP_SIZE; i++) {
			INDEX_TO_COLLATION[i] = collation[i].collationName; 
			indexToMysqlCharset.put(i, collation[i].charsetName);
			INDEX_TO_CHARSET[i] = collation[i].javaCharsetName;  
			
			if (INDEX_TO_CHARSET[i] != null) indexMap.put(INDEX_TO_CHARSET[i], i);
		}

		// Sanity check
		for (int i = 1; i < MAP_SIZE; i++) {
			if (INDEX_TO_COLLATION[i] == null) throw new RuntimeException("Assertion failure: No mapping from charset index " + i + " to a mysql collation");
			if (indexToMysqlCharset.get(i) == null) throw new RuntimeException("Assertion failure: No mapping from charset index " + i + " to a mysql character set");
			if (INDEX_TO_CHARSET[i] == null) throw new RuntimeException("Assertion failure: No mapping from charset index " + i + " to a Java character set");
		}

		MYSQL_ENCODING_NAME_TO_CHARSET_INDEX_MAP = Collections.unmodifiableMap(indexMap);
		STATIC_INDEX_TO_MYSQL_CHARSET_MAP = Collections.unmodifiableMap(indexToMysqlCharset);
		
		Map<String, String> tempMap = new HashMap<String, String>();
		
		tempMap.put("czech", "latin2");
		tempMap.put("danish", "latin1");
		tempMap.put("dutch", "latin1");
		tempMap.put("english", "latin1");
		tempMap.put("estonian", "latin7");
		tempMap.put("french", "latin1");
		tempMap.put("german", "latin1");
		tempMap.put("greek", "greek");
		tempMap.put("hungarian", "latin2");
		tempMap.put("italian", "latin1");
		tempMap.put("japanese", "ujis");
		tempMap.put("japanese-sjis", "sjis");
		tempMap.put("korean", "euckr");
		tempMap.put("norwegian", "latin1");
		tempMap.put("norwegian-ny", "latin1");
		tempMap.put("polish", "latin2");
		tempMap.put("portuguese", "latin1");
		tempMap.put("romanian", "latin2");
		tempMap.put("russian", "koi8r");
		tempMap.put("serbian", "cp1250");
		tempMap.put("slovak", "latin2");
		tempMap.put("spanish", "latin1");
		tempMap.put("swedish", "latin1");
		tempMap.put("ukrainian", "koi8u");
		
		ERROR_MESSAGE_FILE_TO_MYSQL_CHARSET_MAP = 
			Collections.unmodifiableMap(tempMap);
	}

	public final static String getMysqlEncodingForJavaEncoding(String javaEncodingUC,
			Connection conn) throws SQLException {
		
		try {
			List<VersionedStringProperty> mysqlEncodings = CharsetMapping.JAVA_UC_TO_MYSQL_CHARSET_MAP.get(javaEncodingUC);

			if (mysqlEncodings != null) {
				Iterator<VersionedStringProperty> iter = mysqlEncodings.iterator();

				VersionedStringProperty versionedProp = null;

				while (iter.hasNext()) {
					VersionedStringProperty propToCheck = iter.next();

					if (conn == null) {
						// Take the first one we get

						return propToCheck.toString();
					}

					if (versionedProp != null && !versionedProp.preferredValue) {
						if (versionedProp.majorVersion == propToCheck.majorVersion
								&& versionedProp.minorVersion == propToCheck.minorVersion
								&& versionedProp.subminorVersion == propToCheck.subminorVersion) {
							return versionedProp.toString();
						}
					}

					if (propToCheck.isOkayForVersion(conn)) {
						if (propToCheck.preferredValue) {
							return propToCheck.toString();
						}

						versionedProp = propToCheck;
					} else {
						break;
					}
				}

				if (versionedProp != null) {
					return versionedProp.toString();
				}
			}

			return null;
		} catch (SQLException ex) {
			throw ex;
		} catch (RuntimeException ex) {
			SQLException sqlEx = SQLError.createSQLException(ex.toString(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, null);
			sqlEx.initCause(ex);
			throw sqlEx;
		}

	}

	final static int getNumberOfCharsetsConfigured() {
		return MYSQL_TO_JAVA_CHARSET_MAP.size() / 2; // because we UC every
														// key
	}

	/**
	 * Returns the character encoding for error messages returned from the
	 * server. Doesn't return useful values other than Cp1252 until the driver
	 * has gone through initialization phase and determined server configuration,
	 * as not enough information is available to make an intelligent decision
	 * until then.
	 * 
	 * @param conn the connection to the MySQL server
	 * @return the Java encoding name that error messages use
	 * @throws SQLException if determination of the character encoding fails
	 */
	final static String getCharacterEncodingForErrorMessages(ConnectionImpl conn) throws SQLException {

		// As of MySQL 5.5, the server constructs error messages using UTF-8
		// and returns them to clients in the character set specified by the
		// character_set_results system variable. 
		if (conn.versionMeetsMinimum(5, 5, 0)) {
			String errorMessageEncodingMysql = conn.getServerVariable("character_set_results");
			if (errorMessageEncodingMysql != null) {
				String javaEncoding = conn.getJavaEncodingForMysqlEncoding(errorMessageEncodingMysql);
				if (javaEncoding != null) {
					return javaEncoding;
				}
			}
			
			return "UTF-8";
		}

		String errorMessageFile = conn.getServerVariable("language");
		
		if (errorMessageFile == null || errorMessageFile.length() == 0) {
			// punt
			return "Cp1252";
		}
		
		int endWithoutSlash = errorMessageFile.length();
		
		if (errorMessageFile.endsWith("/") || errorMessageFile.endsWith("\\")) {
			endWithoutSlash--;
		}
			
		int lastSlashIndex = errorMessageFile.lastIndexOf('/', endWithoutSlash - 1);
		
		if (lastSlashIndex == -1) {
			lastSlashIndex = errorMessageFile.lastIndexOf('\\', endWithoutSlash - 1);
		}
		
		if (lastSlashIndex == -1) {
			lastSlashIndex = 0;
		}
		
		if (lastSlashIndex == endWithoutSlash || endWithoutSlash < lastSlashIndex) {
			// punt
			return "Cp1252";
		}
		
		errorMessageFile = errorMessageFile.substring(lastSlashIndex + 1, endWithoutSlash);
		
		String errorMessageEncodingMysql = ERROR_MESSAGE_FILE_TO_MYSQL_CHARSET_MAP.get(errorMessageFile);
		
		if (errorMessageEncodingMysql == null) {
			// punt
			return "Cp1252";
		}
		
		String javaEncoding = conn.getJavaEncodingForMysqlEncoding(errorMessageEncodingMysql);
		
		if (javaEncoding == null) {
			// punt
			return "Cp1252";
		}
		
		return javaEncoding;
	}
	
	final static boolean isAliasForSjis(String encoding) {
		return ("SJIS".equalsIgnoreCase(encoding)
				|| "WINDOWS-31J".equalsIgnoreCase(encoding)
				|| "MS932".equalsIgnoreCase(encoding)
				|| "SHIFT_JIS".equalsIgnoreCase(encoding) || "CP943"
				.equalsIgnoreCase(encoding));

	}

	final static boolean isMultibyteCharset(String javaEncodingName) {
		String javaEncodingNameUC = javaEncodingName
				.toUpperCase(Locale.ENGLISH);

		return MULTIBYTE_CHARSETS.containsKey(javaEncodingNameUC);
	}

	private static void populateMapWithKeyValuePairsUnversioned(String configKey, Map<String, String> mapToPopulate, boolean addUppercaseKeys) {
		String javaToMysqlConfig = CHARSET_CONFIG.getProperty(configKey);
		if (javaToMysqlConfig == null) throw new RuntimeException("Could not find configuration value " + "\"" + configKey + "\" in Charsets.properties resource");
			
		List<String> mappings = StringUtils.split(javaToMysqlConfig, ",", true);
		if (mappings == null) throw new RuntimeException("Missing/corrupt entry for \"" + configKey + "\" in Charsets.properties."); 

		Iterator<String> mappingsIter = mappings.iterator();
		while (mappingsIter.hasNext()) {
			String aMapping = mappingsIter.next();
			List<String> parsedPair = StringUtils.split(aMapping, "=", true);
			if (parsedPair.size() != 2) throw new RuntimeException("Syntax error in Charsets.properties " + "resource for token \"" + aMapping + "\"."); 

			String key = parsedPair.get(0).toString();
			String value = parsedPair.get(1).toString();
			mapToPopulate.put(key, value);

			if (addUppercaseKeys) mapToPopulate.put(key.toUpperCase(Locale.ENGLISH), value);
		}
	}

	private static void populateMapWithKeyValuePairsVersioned(String configKey, Map<String, List<VersionedStringProperty>> mapToPopulate, boolean addUppercaseKeys) {
		String javaToMysqlConfig = CHARSET_CONFIG.getProperty(configKey);
		if (javaToMysqlConfig == null) throw new RuntimeException("Could not find configuration value " + "\"" + configKey + "\" in Charsets.properties resource");

		List<String> mappings = StringUtils.split(javaToMysqlConfig, ",", true);
		if (mappings == null) throw new RuntimeException("Missing/corrupt entry for \"" + configKey + "\" in Charsets.properties."); 

		Iterator<String> mappingsIter = mappings.iterator();
		while (mappingsIter.hasNext()) {
			String aMapping = mappingsIter.next();
			List<String> parsedPair = StringUtils.split(aMapping, "=", true);
			if (parsedPair.size() != 2) throw new RuntimeException("Syntax error in Charsets.properties " + "resource for token \"" + aMapping + "\"."); 

			String key = parsedPair.get(0).toString();
			String value = parsedPair.get(1).toString();

			List<VersionedStringProperty> versionedProperties = mapToPopulate.get(key);

			if (versionedProperties == null) {
				versionedProperties = new ArrayList<VersionedStringProperty>();
				mapToPopulate.put(key, versionedProperties);
			}

			VersionedStringProperty verProp = new VersionedStringProperty(value);
			versionedProperties.add(verProp);

			if (addUppercaseKeys) {
				String keyUc = key.toUpperCase(Locale.ENGLISH);
				versionedProperties = mapToPopulate.get(keyUc);

				if (versionedProperties == null) {
					versionedProperties = new ArrayList<VersionedStringProperty>();
					mapToPopulate.put(keyUc, versionedProperties);
				}

				versionedProperties.add(verProp);
			}
		}
	}
	
	public static int getCharsetIndexForMysqlEncodingName(String name) {
		if (name == null) {
			return 0;
		}
		
		Integer asInt = MYSQL_ENCODING_NAME_TO_CHARSET_INDEX_MAP.get(name);
		
		if (asInt == null) {
			return 0;
		}
		
		return asInt.intValue();
	}
}

class VersionedStringProperty {
	int majorVersion, minorVersion, subminorVersion;

	boolean preferredValue = false;

	String propertyInfo;

	VersionedStringProperty(String property) {
		property = property.trim();

		if (property.startsWith("*")) {
			property = property.substring(1);
			preferredValue = true;
		}

		if (property.startsWith(">")) {
			property = property.substring(1);

			int charPos = 0;

			for (charPos = 0; charPos < property.length(); charPos++) {
				char c = property.charAt(charPos);

				if (!Character.isWhitespace(c) && !Character.isDigit(c)
						&& c != '.') {
					break;
				}
			}

			String versionInfo = property.substring(0, charPos);
			List<String> versionParts = StringUtils.split(versionInfo, ".", true);

			majorVersion = Integer.parseInt(versionParts.get(0).toString());

			if (versionParts.size() > 1) {
				minorVersion = Integer.parseInt(versionParts.get(1).toString());
			} else {
				minorVersion = 0;
			}

			if (versionParts.size() > 2) {
				subminorVersion = Integer.parseInt(versionParts.get(2)
						.toString());
			} else {
				subminorVersion = 0;
			}

			propertyInfo = property.substring(charPos);
		} else {
			majorVersion = minorVersion = subminorVersion = 0;
			propertyInfo = property;
		}
	}

	VersionedStringProperty(String property, int major, int minor, int subminor) {
		propertyInfo = property;
		majorVersion = major;
		minorVersion = minor;
		subminorVersion = subminor;
	}

	boolean isOkayForVersion(Connection conn) throws SQLException {
		return conn.versionMeetsMinimum(majorVersion, minorVersion,
				subminorVersion);
	}

	
	public String toString() {
		return propertyInfo;
	}
}

class Collation {
	public int index;
	public String collationName;
	public String charsetName;
	public String javaCharsetName;

	public Collation(int index, String collationName, String charsetName) {
		this.index = index;
		this.collationName = collationName;
		this.charsetName = charsetName;
		this.javaCharsetName = CharsetMapping.MYSQL_TO_JAVA_CHARSET_MAP.get(charsetName);
	}

	public Collation(int index, String collationName, String charsetName, String javaCharsetName) {
		this.index = index;
		this.collationName = collationName;
		this.charsetName = charsetName;
		this.javaCharsetName = javaCharsetName;
	}

	public String toString() {
		StringBuffer asString = new StringBuffer();
		asString.append("[");
		asString.append("index=");
		asString.append(this.index);
		asString.append(",collationName=");
		asString.append(this.collationName);
		asString.append(",charsetName=");
		asString.append(this.charsetName);
		asString.append(",javaCharsetName=");
		asString.append(this.javaCharsetName);
		asString.append("]");
		return asString.toString();
	}
}
