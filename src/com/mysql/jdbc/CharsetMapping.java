/*
 Copyright  2002-2006 MySQL AB, 2008 Sun Microsystems

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
	
	/** Mapping of Java charset names to MySQL charset names */
	private static final Map JAVA_TO_MYSQL_CHARSET_MAP;

	private static final Map JAVA_UC_TO_MYSQL_CHARSET_MAP;
	
	private static final Map ERROR_MESSAGE_FILE_TO_MYSQL_CHARSET_MAP;

	/** Map/List of multibyte character sets (using MySQL names) */
	private static final Map MULTIBYTE_CHARSETS;

	private static final Map MYSQL_TO_JAVA_CHARSET_MAP;
	
	private static final Map MYSQL_ENCODING_NAME_TO_CHARSET_INDEX_MAP;
	
	private static final String NOT_USED = "ISO8859_1"; // punting for not-used character sets

	public static final Map STATIC_CHARSET_TO_NUM_BYTES_MAP;

	static {	
		HashMap tempNumBytesMap = new HashMap();
		
		tempNumBytesMap.put("big5", Constants.integerValueOf(2));
		tempNumBytesMap.put("dec8" , Constants.integerValueOf(1));
		tempNumBytesMap.put("cp850", Constants.integerValueOf(1));
		tempNumBytesMap.put("hp8", Constants.integerValueOf(1));
		tempNumBytesMap.put("koi8r", Constants.integerValueOf(1));
		tempNumBytesMap.put("latin1", Constants.integerValueOf(1));
		tempNumBytesMap.put("latin2", Constants.integerValueOf(1));
		tempNumBytesMap.put("swe7", Constants.integerValueOf(1));
		tempNumBytesMap.put("ascii", Constants.integerValueOf(1));
		tempNumBytesMap.put("ujis", Constants.integerValueOf(3));
		tempNumBytesMap.put("sjis", Constants.integerValueOf(2));
		tempNumBytesMap.put("hebrew", Constants.integerValueOf(1));
		tempNumBytesMap.put("tis620", Constants.integerValueOf(1));
		tempNumBytesMap.put("euckr", Constants.integerValueOf(2));
		tempNumBytesMap.put("koi8u", Constants.integerValueOf(1));
		tempNumBytesMap.put("gb2312", Constants.integerValueOf(2));
		tempNumBytesMap.put("greek", Constants.integerValueOf(1));
		tempNumBytesMap.put("cp1250", Constants.integerValueOf(1));
		tempNumBytesMap.put("gbk", Constants.integerValueOf(2));
		tempNumBytesMap.put("latin5", Constants.integerValueOf(1));
		tempNumBytesMap.put("armscii8", Constants.integerValueOf(1));
		tempNumBytesMap.put("utf8", Constants.integerValueOf(3));
		tempNumBytesMap.put("ucs2", Constants.integerValueOf(2));
		tempNumBytesMap.put("cp866", Constants.integerValueOf(1));
		tempNumBytesMap.put("keybcs2", Constants.integerValueOf(1));
		tempNumBytesMap.put("macce", Constants.integerValueOf(1));
		tempNumBytesMap.put("macroman", Constants.integerValueOf(1));
		tempNumBytesMap.put("cp852" , Constants.integerValueOf(1));
		tempNumBytesMap.put("latin7", Constants.integerValueOf(1));
		tempNumBytesMap.put("cp1251", Constants.integerValueOf(1));
		tempNumBytesMap.put("cp1256" , Constants.integerValueOf(1));
		tempNumBytesMap.put("cp1257", Constants.integerValueOf(1));
		tempNumBytesMap.put("binary", Constants.integerValueOf(1));
		tempNumBytesMap.put("geostd8", Constants.integerValueOf(1));
		tempNumBytesMap.put("cp932", Constants.integerValueOf(2));
		tempNumBytesMap.put("eucjpms", Constants.integerValueOf(3));
		
		STATIC_CHARSET_TO_NUM_BYTES_MAP = Collections.unmodifiableMap(
				tempNumBytesMap);

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
	        + "UTF-32 = 	>5.2.0 utf32");

		HashMap javaToMysqlMap = new HashMap();

		populateMapWithKeyValuePairs("javaToMysqlMappings", javaToMysqlMap,
				true, false);
		JAVA_TO_MYSQL_CHARSET_MAP = Collections.unmodifiableMap(javaToMysqlMap);

		HashMap mysqlToJavaMap = new HashMap();

		Set keySet = JAVA_TO_MYSQL_CHARSET_MAP.keySet();

		Iterator javaCharsets = keySet.iterator();

		while (javaCharsets.hasNext()) {
			Object javaEncodingName = javaCharsets.next();
			List mysqlEncodingList = (List) JAVA_TO_MYSQL_CHARSET_MAP
					.get(javaEncodingName);

			Iterator mysqlEncodings = mysqlEncodingList.iterator();

			String mysqlEncodingName = null;

			while (mysqlEncodings.hasNext()) {
				VersionedStringProperty mysqlProp = (VersionedStringProperty) mysqlEncodings
						.next();
				mysqlEncodingName = mysqlProp.toString();

				mysqlToJavaMap.put(mysqlEncodingName, javaEncodingName);
				mysqlToJavaMap.put(mysqlEncodingName
						.toUpperCase(Locale.ENGLISH), javaEncodingName);
			}
		}

		// we don't want CP932 to map to CP932
		mysqlToJavaMap.put("cp932", "Windows-31J");
		mysqlToJavaMap.put("CP932", "Windows-31J");

		MYSQL_TO_JAVA_CHARSET_MAP = Collections.unmodifiableMap(mysqlToJavaMap);

		TreeMap ucMap = new TreeMap(String.CASE_INSENSITIVE_ORDER);

		Iterator javaNamesKeys = JAVA_TO_MYSQL_CHARSET_MAP.keySet().iterator();

		while (javaNamesKeys.hasNext()) {
			String key = (String) javaNamesKeys.next();

			ucMap.put(key.toUpperCase(Locale.ENGLISH),
					JAVA_TO_MYSQL_CHARSET_MAP.get(key));
		}

		JAVA_UC_TO_MYSQL_CHARSET_MAP = Collections.unmodifiableMap(ucMap);

		//
		// Character sets that we can't convert
		// ourselves.
		//
		HashMap tempMapMulti = new HashMap();

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
	 		+ "UnicodeBig = 	ucs2");
		
		populateMapWithKeyValuePairs("multibyteCharsets", tempMapMulti, false,
				true);

		MULTIBYTE_CHARSETS = Collections.unmodifiableMap(tempMapMulti);

		INDEX_TO_CHARSET = new String[255];

		try {
			INDEX_TO_CHARSET[1] = getJavaEncodingForMysqlEncoding("big5", null);
			INDEX_TO_CHARSET[2] = getJavaEncodingForMysqlEncoding("czech", null);
			INDEX_TO_CHARSET[3] = "ISO8859_1"; // punting for "dec8"
			INDEX_TO_CHARSET[4] = "ISO8859_1"; // punting for "dos"
			INDEX_TO_CHARSET[5] = getJavaEncodingForMysqlEncoding("german1",
					null);
			INDEX_TO_CHARSET[6] = "ISO8859_1"; // punting for "hp8"
			INDEX_TO_CHARSET[7] = getJavaEncodingForMysqlEncoding("koi8_ru",
					null);
			INDEX_TO_CHARSET[8] = getJavaEncodingForMysqlEncoding("latin1",
					null);
			INDEX_TO_CHARSET[9] = getJavaEncodingForMysqlEncoding("latin2",
					null);
			INDEX_TO_CHARSET[10] = "ISO8859_1"; // punting for "swe7"
			INDEX_TO_CHARSET[11] = getJavaEncodingForMysqlEncoding("usa7", null);
			INDEX_TO_CHARSET[12] = getJavaEncodingForMysqlEncoding("ujis", null);
			INDEX_TO_CHARSET[13] = getJavaEncodingForMysqlEncoding("sjis", null);
			INDEX_TO_CHARSET[14] = getJavaEncodingForMysqlEncoding("cp1251",
					null);
			INDEX_TO_CHARSET[15] = getJavaEncodingForMysqlEncoding("danish",
					null);
			INDEX_TO_CHARSET[16] = getJavaEncodingForMysqlEncoding("hebrew",
					null);
			
			INDEX_TO_CHARSET[17] = NOT_USED; // not used in the server 
			
			INDEX_TO_CHARSET[18] = getJavaEncodingForMysqlEncoding("tis620",
					null);
			INDEX_TO_CHARSET[19] = getJavaEncodingForMysqlEncoding("euc_kr",
					null);
			INDEX_TO_CHARSET[20] = getJavaEncodingForMysqlEncoding("estonia",
					null);
			INDEX_TO_CHARSET[21] = getJavaEncodingForMysqlEncoding("hungarian",
					null);
			INDEX_TO_CHARSET[22] = "KOI8_R"; //punting for "koi8_ukr"
			INDEX_TO_CHARSET[23] = getJavaEncodingForMysqlEncoding(
					"win1251ukr", null);
			INDEX_TO_CHARSET[24] = getJavaEncodingForMysqlEncoding("gb2312",
					null);
			INDEX_TO_CHARSET[25] = getJavaEncodingForMysqlEncoding("greek",
					null);
			INDEX_TO_CHARSET[26] = getJavaEncodingForMysqlEncoding("win1250",
					null);
			INDEX_TO_CHARSET[27] = getJavaEncodingForMysqlEncoding("croat",
					null);
			INDEX_TO_CHARSET[28] = getJavaEncodingForMysqlEncoding("gbk", null);
			INDEX_TO_CHARSET[29] = getJavaEncodingForMysqlEncoding("cp1257",
					null);
			INDEX_TO_CHARSET[30] = getJavaEncodingForMysqlEncoding("latin5",
					null);
			INDEX_TO_CHARSET[31] = getJavaEncodingForMysqlEncoding("latin1_de",
					null);
			INDEX_TO_CHARSET[32] = "ISO8859_1"; // punting "armscii8"
			INDEX_TO_CHARSET[33] = getJavaEncodingForMysqlEncoding("utf8", null);
			INDEX_TO_CHARSET[34] = "Cp1250"; // punting "win1250ch"
			INDEX_TO_CHARSET[35] = getJavaEncodingForMysqlEncoding("ucs2", null);
			INDEX_TO_CHARSET[36] = getJavaEncodingForMysqlEncoding("cp866",
					null);
			INDEX_TO_CHARSET[37] = "Cp895"; // punting "keybcs2"
			INDEX_TO_CHARSET[38] = getJavaEncodingForMysqlEncoding("macce",
					null);
			INDEX_TO_CHARSET[39] = getJavaEncodingForMysqlEncoding("macroman",
					null);
			INDEX_TO_CHARSET[40] = "latin2"; // punting "pclatin2"
			INDEX_TO_CHARSET[41] = getJavaEncodingForMysqlEncoding("latvian",
					null);
			INDEX_TO_CHARSET[42] = getJavaEncodingForMysqlEncoding("latvian1",
					null);
			INDEX_TO_CHARSET[43] = getJavaEncodingForMysqlEncoding("macce",
					null);
			INDEX_TO_CHARSET[44] = getJavaEncodingForMysqlEncoding("macce",
					null);
			INDEX_TO_CHARSET[45] = getJavaEncodingForMysqlEncoding("macce",
					null);
			INDEX_TO_CHARSET[46] = getJavaEncodingForMysqlEncoding("macce",
					null);
			INDEX_TO_CHARSET[47] = getJavaEncodingForMysqlEncoding("latin1",
					null);
			INDEX_TO_CHARSET[48] = getJavaEncodingForMysqlEncoding(
					"latin1", null);
			INDEX_TO_CHARSET[49] = getJavaEncodingForMysqlEncoding(
					"latin1", null);
			INDEX_TO_CHARSET[50] = getJavaEncodingForMysqlEncoding("cp1251",
					null);
			INDEX_TO_CHARSET[51] = getJavaEncodingForMysqlEncoding(
					"cp1251", null);
			INDEX_TO_CHARSET[52] = getJavaEncodingForMysqlEncoding(
					"cp1251", null);
			INDEX_TO_CHARSET[53] = getJavaEncodingForMysqlEncoding(
					"macroman", null);
			INDEX_TO_CHARSET[54] = getJavaEncodingForMysqlEncoding(
					"macroman", null);
			INDEX_TO_CHARSET[55] = getJavaEncodingForMysqlEncoding(
					"macroman", null);
			INDEX_TO_CHARSET[56] = getJavaEncodingForMysqlEncoding(
					"macroman", null);
			INDEX_TO_CHARSET[57] = getJavaEncodingForMysqlEncoding("cp1256",
					null);
			
			INDEX_TO_CHARSET[58] = NOT_USED; // not used
			INDEX_TO_CHARSET[59] = NOT_USED; // not used
			INDEX_TO_CHARSET[60] = NOT_USED; // not used
			INDEX_TO_CHARSET[61] = NOT_USED; // not used
			INDEX_TO_CHARSET[62] = NOT_USED; // not used 
			
			INDEX_TO_CHARSET[63] = getJavaEncodingForMysqlEncoding("binary",
					null);
			INDEX_TO_CHARSET[64] = "ISO8859_2"; // punting "armscii"
			INDEX_TO_CHARSET[65] = getJavaEncodingForMysqlEncoding("ascii",
					null);
			INDEX_TO_CHARSET[66] = getJavaEncodingForMysqlEncoding("cp1250",
					null);
			INDEX_TO_CHARSET[67] = getJavaEncodingForMysqlEncoding("cp1256",
					null);
			INDEX_TO_CHARSET[68] = getJavaEncodingForMysqlEncoding("cp866",
					null);
			INDEX_TO_CHARSET[69] = "US-ASCII"; // punting for "dec8"
			INDEX_TO_CHARSET[70] = getJavaEncodingForMysqlEncoding("greek",
					null);
			INDEX_TO_CHARSET[71] = getJavaEncodingForMysqlEncoding("hebrew",
					null);
			INDEX_TO_CHARSET[72] = "US-ASCII"; // punting for "hp8"
			INDEX_TO_CHARSET[73] = "Cp895"; // punting for "keybcs2"
			INDEX_TO_CHARSET[74] = getJavaEncodingForMysqlEncoding("koi8r",
					null);
			INDEX_TO_CHARSET[75] = "KOI8_r"; // punting for koi8ukr"
			
			INDEX_TO_CHARSET[76] = NOT_USED; // not used
			
			INDEX_TO_CHARSET[77] = getJavaEncodingForMysqlEncoding("latin2",
					null);
			INDEX_TO_CHARSET[78] = getJavaEncodingForMysqlEncoding("latin5",
					null);
			INDEX_TO_CHARSET[79] = getJavaEncodingForMysqlEncoding("latin7",
					null);
			INDEX_TO_CHARSET[80] = getJavaEncodingForMysqlEncoding("cp850",
					null);
			INDEX_TO_CHARSET[81] = getJavaEncodingForMysqlEncoding("cp852",
					null);
			INDEX_TO_CHARSET[82] = "ISO8859_1"; // punting for "swe7"
			INDEX_TO_CHARSET[83] = getJavaEncodingForMysqlEncoding("utf8", null);
			INDEX_TO_CHARSET[84] = getJavaEncodingForMysqlEncoding("big5", null);
			INDEX_TO_CHARSET[85] = getJavaEncodingForMysqlEncoding("euckr",
					null);
			INDEX_TO_CHARSET[86] = getJavaEncodingForMysqlEncoding("gb2312",
					null);
			INDEX_TO_CHARSET[87] = getJavaEncodingForMysqlEncoding("gbk", null);
			INDEX_TO_CHARSET[88] = getJavaEncodingForMysqlEncoding("sjis", null);
			INDEX_TO_CHARSET[89] = getJavaEncodingForMysqlEncoding("tis620",
					null);
			INDEX_TO_CHARSET[90] = getJavaEncodingForMysqlEncoding("ucs2", null);
			INDEX_TO_CHARSET[91] = getJavaEncodingForMysqlEncoding("ujis", null);
			INDEX_TO_CHARSET[92] = "US-ASCII"; //punting for "geostd8"
			INDEX_TO_CHARSET[93] = "US-ASCII"; // punting for "geostd8"
			INDEX_TO_CHARSET[94] = getJavaEncodingForMysqlEncoding("latin1",
					null);
			INDEX_TO_CHARSET[95] = getJavaEncodingForMysqlEncoding("cp932",
					null);
			INDEX_TO_CHARSET[96] = getJavaEncodingForMysqlEncoding("cp932",
					null);
			INDEX_TO_CHARSET[97] = getJavaEncodingForMysqlEncoding("eucjpms",
					null);
			INDEX_TO_CHARSET[98] = getJavaEncodingForMysqlEncoding("eucjpms",
					null);
			
			for (int i = 99; i < 128; i++) {
				INDEX_TO_CHARSET[i] = NOT_USED; // not used
			}
			
			INDEX_TO_CHARSET[128] = getJavaEncodingForMysqlEncoding("ucs2",
					null);
			INDEX_TO_CHARSET[129] = getJavaEncodingForMysqlEncoding("ucs2",
					null);
			INDEX_TO_CHARSET[130] = getJavaEncodingForMysqlEncoding("ucs2",
					null);
			INDEX_TO_CHARSET[131] = getJavaEncodingForMysqlEncoding("ucs2",
					null);
			INDEX_TO_CHARSET[132] = getJavaEncodingForMysqlEncoding("ucs2",
					null);
			INDEX_TO_CHARSET[133] = getJavaEncodingForMysqlEncoding("ucs2",
					null);
			INDEX_TO_CHARSET[134] = getJavaEncodingForMysqlEncoding("ucs2",
					null);
			INDEX_TO_CHARSET[135] = getJavaEncodingForMysqlEncoding("ucs2",
					null);
			INDEX_TO_CHARSET[136] = getJavaEncodingForMysqlEncoding("ucs2",
					null);
			INDEX_TO_CHARSET[137] = getJavaEncodingForMysqlEncoding("ucs2",
					null);
			INDEX_TO_CHARSET[138] = getJavaEncodingForMysqlEncoding("ucs2",
					null);
			INDEX_TO_CHARSET[139] = getJavaEncodingForMysqlEncoding("ucs2",
					null);
			INDEX_TO_CHARSET[140] = getJavaEncodingForMysqlEncoding("ucs2",
					null);
			INDEX_TO_CHARSET[141] = getJavaEncodingForMysqlEncoding("ucs2",
					null);
			INDEX_TO_CHARSET[142] = getJavaEncodingForMysqlEncoding("ucs2",
					null);
			INDEX_TO_CHARSET[143] = getJavaEncodingForMysqlEncoding("ucs2",
					null);
			INDEX_TO_CHARSET[144] = getJavaEncodingForMysqlEncoding("ucs2",
					null);
			INDEX_TO_CHARSET[145] = getJavaEncodingForMysqlEncoding("ucs2",
					null);
			INDEX_TO_CHARSET[146] = getJavaEncodingForMysqlEncoding("ucs2",
					null);

			for (int i = 147; i < 192; i++) {
				INDEX_TO_CHARSET[i] = NOT_USED; // not used
			}
			
			INDEX_TO_CHARSET[192] = getJavaEncodingForMysqlEncoding("utf8",
					null);
			INDEX_TO_CHARSET[193] = getJavaEncodingForMysqlEncoding("utf8",
					null);
			INDEX_TO_CHARSET[194] = getJavaEncodingForMysqlEncoding("utf8",
					null);
			INDEX_TO_CHARSET[195] = getJavaEncodingForMysqlEncoding("utf8",
					null);
			INDEX_TO_CHARSET[196] = getJavaEncodingForMysqlEncoding("utf8",
					null);
			INDEX_TO_CHARSET[197] = getJavaEncodingForMysqlEncoding("utf8",
					null);
			INDEX_TO_CHARSET[198] = getJavaEncodingForMysqlEncoding("utf8",
					null);
			INDEX_TO_CHARSET[199] = getJavaEncodingForMysqlEncoding("utf8",
					null);
			INDEX_TO_CHARSET[200] = getJavaEncodingForMysqlEncoding("utf8",
					null);
			INDEX_TO_CHARSET[201] = getJavaEncodingForMysqlEncoding("utf8",
					null);
			INDEX_TO_CHARSET[202] = getJavaEncodingForMysqlEncoding("utf8",
					null);
			INDEX_TO_CHARSET[203] = getJavaEncodingForMysqlEncoding("utf8",
					null);
			INDEX_TO_CHARSET[204] = getJavaEncodingForMysqlEncoding("utf8",
					null);
			INDEX_TO_CHARSET[205] = getJavaEncodingForMysqlEncoding("utf8",
					null);
			INDEX_TO_CHARSET[206] = getJavaEncodingForMysqlEncoding("utf8",
					null);
			INDEX_TO_CHARSET[207] = getJavaEncodingForMysqlEncoding("utf8",
					null);
			INDEX_TO_CHARSET[208] = getJavaEncodingForMysqlEncoding("utf8",
					null);
			INDEX_TO_CHARSET[209] = getJavaEncodingForMysqlEncoding("utf8",
					null);
			INDEX_TO_CHARSET[210] = getJavaEncodingForMysqlEncoding("utf8",
					null);
			// 5.2+ charsets
			INDEX_TO_CHARSET[211] = getJavaEncodingForMysqlEncoding("utf8",
					null);
			
			for (int i = 212; i < 224; i++) {
				INDEX_TO_CHARSET[i] = NOT_USED;
			}
			
			for (int i = 224; i <= 243; i++) {
				INDEX_TO_CHARSET[i] = getJavaEncodingForMysqlEncoding("utf8",
						null);
			}
			
			for (int i = 101; i<= 120; i++) {
				INDEX_TO_CHARSET[i] = getJavaEncodingForMysqlEncoding("utf16",
						null);
			}
			
			for (int i = 160; i<= 179; i++) {
				INDEX_TO_CHARSET[i] = getJavaEncodingForMysqlEncoding("utf32",
						null);
			}
			
			for (int i = 244; i < 254; i++) {
				INDEX_TO_CHARSET[i] = NOT_USED;
			}
			
			INDEX_TO_CHARSET[254] = getJavaEncodingForMysqlEncoding("utf8",
					null);
			
			// Sanity check
			
			for (int i = 1; i < INDEX_TO_CHARSET.length; i++) {
				if (INDEX_TO_CHARSET[i] == null) {
					throw new RuntimeException("Assertion failure: No mapping from charset index " + i + " to a Java character set");
				}
			}
		} catch (SQLException sqlEx) {
			// ignore, it won't happen in this case
		}
		
		INDEX_TO_COLLATION = new String[255];
		
		INDEX_TO_COLLATION[1] = "big5_chinese_ci";
		INDEX_TO_COLLATION[2] = "latin2_czech_cs";
		INDEX_TO_COLLATION[3] = "dec8_swedish_ci";
		INDEX_TO_COLLATION[4] = "cp850_general_ci";
		INDEX_TO_COLLATION[5] = "latin1_german1_ci";
		INDEX_TO_COLLATION[6] = "hp8_english_ci";
		INDEX_TO_COLLATION[7] = "koi8r_general_ci";
		INDEX_TO_COLLATION[8] = "latin1_swedish_ci";
		INDEX_TO_COLLATION[9] = "latin2_general_ci";
		INDEX_TO_COLLATION[10] = "swe7_swedish_ci";
		INDEX_TO_COLLATION[11] = "ascii_general_ci";
		INDEX_TO_COLLATION[12] = "ujis_japanese_ci";
		INDEX_TO_COLLATION[13] = "sjis_japanese_ci";
		INDEX_TO_COLLATION[14] = "cp1251_bulgarian_ci";
		INDEX_TO_COLLATION[15] = "latin1_danish_ci";
		INDEX_TO_COLLATION[16] = "hebrew_general_ci";
		INDEX_TO_COLLATION[18] = "tis620_thai_ci";
		INDEX_TO_COLLATION[19] = "euckr_korean_ci";
		INDEX_TO_COLLATION[20] = "latin7_estonian_cs";
		INDEX_TO_COLLATION[21] = "latin2_hungarian_ci";
		INDEX_TO_COLLATION[22] = "koi8u_general_ci";
		INDEX_TO_COLLATION[23] = "cp1251_ukrainian_ci";
		INDEX_TO_COLLATION[24] = "gb2312_chinese_ci";
		INDEX_TO_COLLATION[25] = "greek_general_ci";
		INDEX_TO_COLLATION[26] = "cp1250_general_ci";
		INDEX_TO_COLLATION[27] = "latin2_croatian_ci";
		INDEX_TO_COLLATION[28] = "gbk_chinese_ci";
		INDEX_TO_COLLATION[29] = "cp1257_lithuanian_ci";
		INDEX_TO_COLLATION[30] = "latin5_turkish_ci";
		INDEX_TO_COLLATION[31] = "latin1_german2_ci";
		INDEX_TO_COLLATION[32] = "armscii8_general_ci";
		INDEX_TO_COLLATION[33] = "utf8_general_ci";
		INDEX_TO_COLLATION[34] = "cp1250_czech_cs";
		INDEX_TO_COLLATION[35] = "ucs2_general_ci";
		INDEX_TO_COLLATION[36] = "cp866_general_ci";
		INDEX_TO_COLLATION[37] = "keybcs2_general_ci";
		INDEX_TO_COLLATION[38] = "macce_general_ci";
		INDEX_TO_COLLATION[39] = "macroman_general_ci";
		INDEX_TO_COLLATION[40] = "cp852_general_ci";
		INDEX_TO_COLLATION[41] = "latin7_general_ci";
		INDEX_TO_COLLATION[42] = "latin7_general_cs";
		INDEX_TO_COLLATION[43] = "macce_bin";
		INDEX_TO_COLLATION[44] = "cp1250_croatian_ci";
		INDEX_TO_COLLATION[47] = "latin1_bin";
		INDEX_TO_COLLATION[48] = "latin1_general_ci";
		INDEX_TO_COLLATION[49] = "latin1_general_cs";
		INDEX_TO_COLLATION[50] = "cp1251_bin";
		INDEX_TO_COLLATION[51] = "cp1251_general_ci";
		INDEX_TO_COLLATION[52] = "cp1251_general_cs";
		INDEX_TO_COLLATION[53] = "macroman_bin";
		INDEX_TO_COLLATION[57] = "cp1256_general_ci";
		INDEX_TO_COLLATION[58] = "cp1257_bin";
		INDEX_TO_COLLATION[59] = "cp1257_general_ci";
		INDEX_TO_COLLATION[63] = "binary";
		INDEX_TO_COLLATION[64] = "armscii8_bin";
		INDEX_TO_COLLATION[65] = "ascii_bin";
		INDEX_TO_COLLATION[66] = "cp1250_bin";
		INDEX_TO_COLLATION[67] = "cp1256_bin";
		INDEX_TO_COLLATION[68] = "cp866_bin";
		INDEX_TO_COLLATION[69] = "dec8_bin";
		INDEX_TO_COLLATION[70] = "greek_bin";
		INDEX_TO_COLLATION[71] = "hebrew_bin";
		INDEX_TO_COLLATION[72] = "hp8_bin";
		INDEX_TO_COLLATION[73] = "keybcs2_bin";
		INDEX_TO_COLLATION[74] = "koi8r_bin";
		INDEX_TO_COLLATION[75] = "koi8u_bin";
		INDEX_TO_COLLATION[77] = "latin2_bin";
		INDEX_TO_COLLATION[78] = "latin5_bin";
		INDEX_TO_COLLATION[79] = "latin7_bin";
		INDEX_TO_COLLATION[80] = "cp850_bin";
		INDEX_TO_COLLATION[81] = "cp852_bin";
		INDEX_TO_COLLATION[82] = "swe7_bin";
		INDEX_TO_COLLATION[83] = "utf8_bin";
		INDEX_TO_COLLATION[84] = "big5_bin";
		INDEX_TO_COLLATION[85] = "euckr_bin";
		INDEX_TO_COLLATION[86] = "gb2312_bin";
		INDEX_TO_COLLATION[87] = "gbk_bin";
		INDEX_TO_COLLATION[88] = "sjis_bin";
		INDEX_TO_COLLATION[89] = "tis620_bin";
		INDEX_TO_COLLATION[90] = "ucs2_bin";
		INDEX_TO_COLLATION[91] = "ujis_bin";
		INDEX_TO_COLLATION[92] = "geostd8_general_ci";
		INDEX_TO_COLLATION[93] = "geostd8_bin";
		INDEX_TO_COLLATION[94] = "latin1_spanish_ci";
		INDEX_TO_COLLATION[95] = "cp932_japanese_ci";
		INDEX_TO_COLLATION[96] = "cp932_bin";
		INDEX_TO_COLLATION[97] = "eucjpms_japanese_ci";
		INDEX_TO_COLLATION[98] = "eucjpms_bin";
		INDEX_TO_COLLATION[99] = "cp1250_polish_ci";
		INDEX_TO_COLLATION[128] = "ucs2_unicode_ci";
		INDEX_TO_COLLATION[129] = "ucs2_icelandic_ci";
		INDEX_TO_COLLATION[130] = "ucs2_latvian_ci";
		INDEX_TO_COLLATION[131] = "ucs2_romanian_ci";
		INDEX_TO_COLLATION[132] = "ucs2_slovenian_ci";
		INDEX_TO_COLLATION[133] = "ucs2_polish_ci";
		INDEX_TO_COLLATION[134] = "ucs2_estonian_ci";
		INDEX_TO_COLLATION[135] = "ucs2_spanish_ci";
		INDEX_TO_COLLATION[136] = "ucs2_swedish_ci";
		INDEX_TO_COLLATION[137] = "ucs2_turkish_ci";
		INDEX_TO_COLLATION[138] = "ucs2_czech_ci";
		INDEX_TO_COLLATION[139] = "ucs2_danish_ci";
		INDEX_TO_COLLATION[140] = "ucs2_lithuanian_ci ";
		INDEX_TO_COLLATION[141] = "ucs2_slovak_ci";
		INDEX_TO_COLLATION[142] = "ucs2_spanish2_ci";
		INDEX_TO_COLLATION[143] = "ucs2_roman_ci";
		INDEX_TO_COLLATION[144] = "ucs2_persian_ci";
		INDEX_TO_COLLATION[145] = "ucs2_esperanto_ci";
		INDEX_TO_COLLATION[146] = "ucs2_hungarian_ci";
		INDEX_TO_COLLATION[192] = "utf8_unicode_ci";
		INDEX_TO_COLLATION[193] = "utf8_icelandic_ci";
		INDEX_TO_COLLATION[194] = "utf8_latvian_ci";
		INDEX_TO_COLLATION[195] = "utf8_romanian_ci";
		INDEX_TO_COLLATION[196] = "utf8_slovenian_ci";
		INDEX_TO_COLLATION[197] = "utf8_polish_ci";
		INDEX_TO_COLLATION[198] = "utf8_estonian_ci";
		INDEX_TO_COLLATION[199] = "utf8_spanish_ci";
		INDEX_TO_COLLATION[200] = "utf8_swedish_ci";
		INDEX_TO_COLLATION[201] = "utf8_turkish_ci";
		INDEX_TO_COLLATION[202] = "utf8_czech_ci";
		INDEX_TO_COLLATION[203] = "utf8_danish_ci";
		INDEX_TO_COLLATION[204] = "utf8_lithuanian_ci ";
		INDEX_TO_COLLATION[205] = "utf8_slovak_ci";
		INDEX_TO_COLLATION[206] = "utf8_spanish2_ci";
		INDEX_TO_COLLATION[207] = "utf8_roman_ci";
		INDEX_TO_COLLATION[208] = "utf8_persian_ci";
		INDEX_TO_COLLATION[209] = "utf8_esperanto_ci";
		INDEX_TO_COLLATION[210] = "utf8_hungarian_ci";
		
		// TODO: Remove duplicates
		
		INDEX_TO_COLLATION[33] ="utf8mb3_general_ci";
		INDEX_TO_COLLATION[83] ="utf8mb3_bin";
		INDEX_TO_COLLATION[192] ="utf8mb3_unicode_ci";
		INDEX_TO_COLLATION[193] ="utf8mb3_icelandic_ci";
		INDEX_TO_COLLATION[194] ="utf8mb3_latvian_ci";
		INDEX_TO_COLLATION[195] ="utf8mb3_romanian_ci";
		INDEX_TO_COLLATION[196] ="utf8mb3_slovenian_ci";
		INDEX_TO_COLLATION[197] ="utf8mb3_polish_ci";
		INDEX_TO_COLLATION[198] ="utf8mb3_estonian_ci";
		INDEX_TO_COLLATION[199] ="utf8mb3_spanish_ci";
		INDEX_TO_COLLATION[200] ="utf8mb3_swedish_ci";
		INDEX_TO_COLLATION[201] ="utf8mb3_turkish_ci";
		INDEX_TO_COLLATION[202] ="utf8mb3_czech_ci";
		INDEX_TO_COLLATION[203] ="utf8mb3_danish_ci";
		INDEX_TO_COLLATION[204] ="utf8mb3_lithuanian_ci";
		INDEX_TO_COLLATION[205] ="utf8mb3_slovak_ci";
		INDEX_TO_COLLATION[206] ="utf8mb3_spanish2_ci";
		INDEX_TO_COLLATION[207] ="utf8mb3_roman_ci";
		INDEX_TO_COLLATION[208] ="utf8mb3_persian_ci";
		INDEX_TO_COLLATION[209] ="utf8mb3_esperanto_ci";
		INDEX_TO_COLLATION[210] ="utf8mb3_hungarian_ci";
		INDEX_TO_COLLATION[211] ="utf8mb3_sinhala_ci";
		INDEX_TO_COLLATION[254] ="utf8mb3_general_cs";
		
		INDEX_TO_COLLATION[45] ="utf8_general_ci";
		INDEX_TO_COLLATION[46] ="utf8_bin";
		INDEX_TO_COLLATION[224] ="utf8_unicode_ci";
		INDEX_TO_COLLATION[225] ="utf8_icelandic_ci";
		INDEX_TO_COLLATION[226] ="utf8_latvian_ci";
		INDEX_TO_COLLATION[227] ="utf8_romanian_ci";
		INDEX_TO_COLLATION[228] ="utf8_slovenian_ci";
		INDEX_TO_COLLATION[229] ="utf8_polish_ci";
		INDEX_TO_COLLATION[230] ="utf8_estonian_ci";
		INDEX_TO_COLLATION[231] ="utf8_spanish_ci";
		INDEX_TO_COLLATION[232] ="utf8_swedish_ci";
		INDEX_TO_COLLATION[233] ="utf8_turkish_ci";
		INDEX_TO_COLLATION[234] ="utf8_czech_ci";
		INDEX_TO_COLLATION[235] ="utf8_danish_ci";
		INDEX_TO_COLLATION[236] ="utf8_lithuanian_ci";
		INDEX_TO_COLLATION[237] ="utf8_slovak_ci";
		INDEX_TO_COLLATION[238] ="utf8_spanish2_ci";
		INDEX_TO_COLLATION[239] ="utf8_roman_ci";
		INDEX_TO_COLLATION[240] ="utf8_persian_ci";
		INDEX_TO_COLLATION[241] ="utf8_esperanto_ci";
		INDEX_TO_COLLATION[242] ="utf8_hungarian_ci";
		INDEX_TO_COLLATION[243] ="utf8_sinhala_ci";
		
		INDEX_TO_COLLATION[54] = "utf16_general_ci";
		INDEX_TO_COLLATION[55] = "utf16_bin";
		INDEX_TO_COLLATION[101] = "utf16_unicode_ci";
		INDEX_TO_COLLATION[102] = "utf16_icelandic_ci";
		INDEX_TO_COLLATION[103] = "utf16_latvian_ci";
		INDEX_TO_COLLATION[104] = "utf16_romanian_ci";
		INDEX_TO_COLLATION[105] = "utf16_slovenian_ci";
		INDEX_TO_COLLATION[106] = "utf16_polish_ci";
		INDEX_TO_COLLATION[107] = "utf16_estonian_ci";
		INDEX_TO_COLLATION[108] = "utf16_spanish_ci";
		INDEX_TO_COLLATION[109] = "utf16_swedish_ci";
		INDEX_TO_COLLATION[110] = "utf16_turkish_ci";
		INDEX_TO_COLLATION[111] = "utf16_czech_ci";
		INDEX_TO_COLLATION[112] = "utf16_danish_ci";
		INDEX_TO_COLLATION[113] = "utf16_lithuanian_ci";
		INDEX_TO_COLLATION[114] = "utf16_slovak_ci";
		INDEX_TO_COLLATION[115] = "utf16_spanish2_ci";
		INDEX_TO_COLLATION[116] = "utf16_roman_ci";
		INDEX_TO_COLLATION[117] = "utf16_persian_ci";
		INDEX_TO_COLLATION[118] = "utf16_esperanto_ci";
		INDEX_TO_COLLATION[119] = "utf16_hungarian_ci";
		INDEX_TO_COLLATION[120] = "utf16_sinhala_ci";
		
		INDEX_TO_COLLATION[60] = "utf32_general_ci";
		INDEX_TO_COLLATION[61] = "utf32_bin";
		INDEX_TO_COLLATION[160] = "utf32_unicode_ci";
		INDEX_TO_COLLATION[161] = "utf32_icelandic_ci";
		INDEX_TO_COLLATION[162] = "utf32_latvian_ci";
		INDEX_TO_COLLATION[163] = "utf32_romanian_ci";
		INDEX_TO_COLLATION[164] = "utf32_slovenian_ci";
		INDEX_TO_COLLATION[165] = "utf32_polish_ci";
		INDEX_TO_COLLATION[166] = "utf32_estonian_ci";
		INDEX_TO_COLLATION[167] = "utf32_spanish_ci";
		INDEX_TO_COLLATION[168] = "utf32_swedish_ci";
		INDEX_TO_COLLATION[169] = "utf32_turkish_ci";
		INDEX_TO_COLLATION[170] = "utf32_czech_ci";
		INDEX_TO_COLLATION[171] = "utf32_danish_ci";
		INDEX_TO_COLLATION[172] = "utf32_lithuanian_ci";
		INDEX_TO_COLLATION[173] = "utf32_slovak_ci";
		INDEX_TO_COLLATION[174] = "utf32_spanish2_ci";
		INDEX_TO_COLLATION[175] = "utf32_roman_ci";
		INDEX_TO_COLLATION[176] = "utf32_persian_ci";
		INDEX_TO_COLLATION[177] = "utf32_esperanto_ci";
		INDEX_TO_COLLATION[178] = "utf32_hungarian_ci";
		INDEX_TO_COLLATION[179] = "utf32_sinhala_ci";
		
		Map indexMap = new TreeMap(String.CASE_INSENSITIVE_ORDER);
		
		for (int i = 0; i < INDEX_TO_CHARSET.length; i++) {
			String mysqlEncodingName = INDEX_TO_CHARSET[i];
			
			if (mysqlEncodingName != null) {
				indexMap.put(INDEX_TO_CHARSET[i], Constants.integerValueOf(i));
			}
		}
		
		MYSQL_ENCODING_NAME_TO_CHARSET_INDEX_MAP = Collections.unmodifiableMap(indexMap);
		
		Map tempMap = new HashMap();
		
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

	public final static String getJavaEncodingForMysqlEncoding(String mysqlEncoding,
			Connection conn) throws SQLException {
		
		if (conn != null && conn.versionMeetsMinimum(4, 1, 0) && 
				"latin1".equalsIgnoreCase(mysqlEncoding)) {
			return "Cp1252";
		}
		
		return (String) MYSQL_TO_JAVA_CHARSET_MAP.get(mysqlEncoding);
	}

	public final static String getMysqlEncodingForJavaEncoding(String javaEncodingUC,
			Connection conn) throws SQLException {
		List mysqlEncodings = (List) CharsetMapping.JAVA_UC_TO_MYSQL_CHARSET_MAP
				.get(javaEncodingUC);
		;

		if (mysqlEncodings != null) {
			Iterator iter = mysqlEncodings.iterator();

			VersionedStringProperty versionedProp = null;

			while (iter.hasNext()) {
				VersionedStringProperty propToCheck = (VersionedStringProperty) iter
						.next();

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
		
		String errorMessageEncodingMysql = (String)ERROR_MESSAGE_FILE_TO_MYSQL_CHARSET_MAP.get(errorMessageFile);
		
		if (errorMessageEncodingMysql == null) {
			// punt
			return "Cp1252";
		}
		
		String javaEncoding = getJavaEncodingForMysqlEncoding(errorMessageEncodingMysql, conn);
		
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

	private static void populateMapWithKeyValuePairs(String configKey,
			Map mapToPopulate, boolean addVersionedProperties,
			boolean addUppercaseKeys) {
		String javaToMysqlConfig = CHARSET_CONFIG.getProperty(configKey);

		if (javaToMysqlConfig != null) {
			List mappings = StringUtils.split(javaToMysqlConfig, ",", true);

			if (mappings != null) {
				Iterator mappingsIter = mappings.iterator();

				while (mappingsIter.hasNext()) {
					String aMapping = (String) mappingsIter.next();

					List parsedPair = StringUtils.split(aMapping, "=", true);

					if (parsedPair.size() == 2) {
						String key = parsedPair.get(0).toString();
						String value = parsedPair.get(1).toString();

						if (addVersionedProperties) {
							List versionedProperties = (List) mapToPopulate
									.get(key);

							if (versionedProperties == null) {
								versionedProperties = new ArrayList();
								mapToPopulate.put(key, versionedProperties);
							}

							VersionedStringProperty verProp = new VersionedStringProperty(
									value);
							versionedProperties.add(verProp);

							if (addUppercaseKeys) {
								String keyUc = key.toUpperCase(Locale.ENGLISH);

								versionedProperties = (List) mapToPopulate
										.get(keyUc);

								if (versionedProperties == null) {
									versionedProperties = new ArrayList();
									mapToPopulate.put(keyUc,
											versionedProperties);
								}

								versionedProperties.add(verProp);
							}
						} else {
							mapToPopulate.put(key, value);

							if (addUppercaseKeys) {
								mapToPopulate.put(key
										.toUpperCase(Locale.ENGLISH), value);
							}
						}
					} else {
						throw new RuntimeException(
								"Syntax error in Charsets.properties "
										+ "resource for token \"" + aMapping
										+ "\".");
					}
				}
			} else {
				throw new RuntimeException("Missing/corrupt entry for \""
						+ configKey + "\" in Charsets.properties.");
			}
		} else {
			throw new RuntimeException("Could not find configuration value "
					+ "\"" + configKey + "\" in Charsets.properties resource");
		}
	}
	
	public static int getCharsetIndexForMysqlEncodingName(String name) {
		if (name == null) {
			return 0;
		}
		
		Integer asInt = (Integer)MYSQL_ENCODING_NAME_TO_CHARSET_INDEX_MAP.get(name);
		
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
			List versionParts = StringUtils.split(versionInfo, ".", true);

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
