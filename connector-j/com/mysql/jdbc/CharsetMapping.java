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
 */
public class CharsetMapping
{

    //~ Instance/static variables .............................................

    public static HashMap charsetMapping = new HashMap();
    public static HashMap multibyteCharsets = new HashMap();

    //~ Initializers ..........................................................

    static {
        charsetMapping.put("usa7", "US-ASCII");
        charsetMapping.put("big5", "Big5");
        charsetMapping.put("gbk", "GBK");
        charsetMapping.put("sjis", "SJIS");
        charsetMapping.put("gb2312", "EUC_CN");
        charsetMapping.put("ujis", "EUC_JP");
        charsetMapping.put("euc_kr", "EUC_KR");
        charsetMapping.put("latin1", "ISO8859_1");
        charsetMapping.put("latin1_de", "ISO8859_1");
        charsetMapping.put("german1", "ISO8859_1");
        charsetMapping.put("danish", "ISO8859_1");
        charsetMapping.put("latin2", "ISO8859_2");
        charsetMapping.put("czech", "ISO8859_2");
        charsetMapping.put("hungarian", "ISO8859_2");
        charsetMapping.put("croat", "ISO8859_2");
        charsetMapping.put("greek", "ISO8859_7");
        charsetMapping.put("hebrew", "ISO8859_8");
        charsetMapping.put("latin5", "ISO8859_9");
        charsetMapping.put("latvian", "ISO8859_13");
        charsetMapping.put("latvian1", "ISO8859_13");
        charsetMapping.put("estonia", "ISO8859_13");
        charsetMapping.put("dos", "Cp437");
        charsetMapping.put("pclatin2", "Cp852");
        charsetMapping.put("cp866", "Cp866");
        charsetMapping.put("koi8_ru", "KOI8_R");
        charsetMapping.put("tis620", "TIS620");
        charsetMapping.put("win1250", "Cp1250");
        charsetMapping.put("win1250ch", "Cp1250");
        charsetMapping.put("win1251", "Cp1251");
        charsetMapping.put("cp1251", "Cp1251");
        charsetMapping.put("win1251ukr", "Cp1251");
        charsetMapping.put("cp1257", "Cp1257");
        charsetMapping.put("macroman", "MacRoman");
        charsetMapping.put("macce", "MacCentralEurope");
        charsetMapping.put("utf8", "UTF-8");
        charsetMapping.put("ucs2", "UnicodeBig");

        //
        // Character sets that we can't convert
        // ourselves.
        //
        multibyteCharsets.put("big5", "big5");
        multibyteCharsets.put("euc_kr", "euc_kr");
        multibyteCharsets.put("gb2312", "gb2312");
        multibyteCharsets.put("gbk", "gbk");
        multibyteCharsets.put("sjis", "sjis");
        multibyteCharsets.put("ujis", "ujist");
        multibyteCharsets.put("utf8", "utf8");
        multibyteCharsets.put("ucs2", "UnicodeBig");
    }
}