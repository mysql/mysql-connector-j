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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


/**
 * Mapping between MySQL charset names
 * and Java charset names.
 * 
 * @author Mark Matthews
 */
public class CharsetMapping {

    //~ Instance/static variables .............................................

	/**
	 * Mapping of MySQL charset names to Java charset names
	 */
	
    public static final Map CHARSETMAP;
    
    /**
     * Map/List of multibyte character sets (using MySQL names)
     */
    
    public static final Map MULTIBYTE_CHARSETS;

    //~ Initializers ..........................................................

    static {
        HashMap tempMap = new HashMap();
        
        tempMap.put("usa7", "US-ASCII");
        tempMap.put("big5", "Big5");
        tempMap.put("gbk", "GBK");
        tempMap.put("sjis", "SJIS");
        tempMap.put("gb2312", "EUC_CN");
        tempMap.put("ujis", "EUC_JP");
        tempMap.put("euc_kr", "EUC_KR");
        tempMap.put("latin1", "ISO8859_1");
        tempMap.put("latin1_de", "ISO8859_1");
        tempMap.put("german1", "ISO8859_1");
        tempMap.put("danish", "ISO8859_1");
        tempMap.put("latin2", "ISO8859_2");
        tempMap.put("czech", "ISO8859_2");
        tempMap.put("hungarian", "ISO8859_2");
        tempMap.put("croat", "ISO8859_2");
        tempMap.put("greek", "ISO8859_7");
        tempMap.put("hebrew", "ISO8859_8");
        tempMap.put("latin5", "ISO8859_9");
        tempMap.put("latvian", "ISO8859_13");
        tempMap.put("latvian1", "ISO8859_13");
        tempMap.put("estonia", "ISO8859_13");
        tempMap.put("dos", "Cp437");
        tempMap.put("pclatin2", "Cp852");
        tempMap.put("cp866", "Cp866");
        tempMap.put("koi8_ru", "KOI8_R");
        tempMap.put("tis620", "TIS620");
        tempMap.put("win1250", "Cp1250");
        tempMap.put("win1250ch", "Cp1250");
        tempMap.put("win1251", "Cp1251");
        tempMap.put("cp1251", "Cp1251");
        tempMap.put("win1251ukr", "Cp1251");
        tempMap.put("cp1257", "Cp1257");
        tempMap.put("macroman", "MacRoman");
        tempMap.put("macce", "MacCentralEurope");
        tempMap.put("utf8", "UTF-8");
        tempMap.put("ucs2", "UnicodeBig");
        
        CHARSETMAP = Collections.unmodifiableMap(tempMap);

        //
        // Character sets that we can't convert
        // ourselves.
        //
        
        HashMap tempMapMulti = new HashMap();
        
        tempMapMulti.put("big5", "big5");
        tempMapMulti.put("euc_kr", "euc_kr");
        tempMapMulti.put("gb2312", "gb2312");
        tempMapMulti.put("gbk", "gbk");
        tempMapMulti.put("sjis", "sjis");
        tempMapMulti.put("ujis", "ujist");
        tempMapMulti.put("utf8", "utf8");
        tempMapMulti.put("ucs2", "UnicodeBig");
        
        MULTIBYTE_CHARSETS = Collections.unmodifiableMap(tempMapMulti);
    }
}