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

import java.util.HashMap;


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
	
    public static final HashMap CHARSETMAP = new HashMap();
    
    /**
     * Map/List of multibyte character sets (using MySQL names)
     */
    
    public static final HashMap MULTIBYTE_CHARSETS = new HashMap();

    //~ Initializers ..........................................................

    static {
        CHARSETMAP.put("usa7", "US-ASCII");
        CHARSETMAP.put("big5", "Big5");
        CHARSETMAP.put("gbk", "GBK");
        CHARSETMAP.put("sjis", "SJIS");
        CHARSETMAP.put("gb2312", "EUC_CN");
        CHARSETMAP.put("ujis", "EUC_JP");
        CHARSETMAP.put("euc_kr", "EUC_KR");
        CHARSETMAP.put("latin1", "ISO8859_1");
        CHARSETMAP.put("latin1_de", "ISO8859_1");
        CHARSETMAP.put("german1", "ISO8859_1");
        CHARSETMAP.put("danish", "ISO8859_1");
        CHARSETMAP.put("latin2", "ISO8859_2");
        CHARSETMAP.put("czech", "ISO8859_2");
        CHARSETMAP.put("hungarian", "ISO8859_2");
        CHARSETMAP.put("croat", "ISO8859_2");
        CHARSETMAP.put("greek", "ISO8859_7");
        CHARSETMAP.put("hebrew", "ISO8859_8");
        CHARSETMAP.put("latin5", "ISO8859_9");
        CHARSETMAP.put("latvian", "ISO8859_13");
        CHARSETMAP.put("latvian1", "ISO8859_13");
        CHARSETMAP.put("estonia", "ISO8859_13");
        CHARSETMAP.put("dos", "Cp437");
        CHARSETMAP.put("pclatin2", "Cp852");
        CHARSETMAP.put("cp866", "Cp866");
        CHARSETMAP.put("koi8_ru", "KOI8_R");
        CHARSETMAP.put("tis620", "TIS620");
        CHARSETMAP.put("win1250", "Cp1250");
        CHARSETMAP.put("win1250ch", "Cp1250");
        CHARSETMAP.put("win1251", "Cp1251");
        CHARSETMAP.put("cp1251", "Cp1251");
        CHARSETMAP.put("win1251ukr", "Cp1251");
        CHARSETMAP.put("cp1257", "Cp1257");
        CHARSETMAP.put("macroman", "MacRoman");
        CHARSETMAP.put("macce", "MacCentralEurope");
        CHARSETMAP.put("utf8", "UTF-8");
        CHARSETMAP.put("ucs2", "UnicodeBig");

        //
        // Character sets that we can't convert
        // ourselves.
        //
        MULTIBYTE_CHARSETS.put("big5", "big5");
        MULTIBYTE_CHARSETS.put("euc_kr", "euc_kr");
        MULTIBYTE_CHARSETS.put("gb2312", "gb2312");
        MULTIBYTE_CHARSETS.put("gbk", "gbk");
        MULTIBYTE_CHARSETS.put("sjis", "sjis");
        MULTIBYTE_CHARSETS.put("ujis", "ujist");
        MULTIBYTE_CHARSETS.put("utf8", "utf8");
        MULTIBYTE_CHARSETS.put("ucs2", "UnicodeBig");
    }
}