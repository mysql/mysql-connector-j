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

/**
 * EscapeTokenizer breaks up an SQL statement into SQL and
 * escape code parts.
 *
 * @author Mark Matthews
 */
public class EscapeTokenizer {

    //~ Instance/static variables .............................................

    private boolean emittingEscapeCode = false;
    private boolean inBraces = false;
    private boolean inQuotes = false;
    private char lastChar = 0;
    private char lastLastChar = 0;
    private int pos = 0;
    private char quoteChar = 0;
    private String source = null;
    private int sourceLength = 0;
    private int bracesLevel = 0;
    private boolean inComment = false;

    //~ Constructors ..........................................................

    /**
     * Creates a new EscapeTokenizer object.
     * 
     * @param s DOCUMENT ME!
     */
    public EscapeTokenizer(String s) {
        source = s;
        sourceLength = s.length();
        pos = 0;
    }

    //~ Methods ...............................................................

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public synchronized boolean hasMoreTokens() {

        return (pos < sourceLength);
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public synchronized String nextToken() {

        StringBuffer tokenBuf = new StringBuffer();

        if (emittingEscapeCode) {
            tokenBuf.append("{");
            emittingEscapeCode = false;
        }

        for (; pos < sourceLength; pos++) {

            char c = source.charAt(pos);
            
            if (c == '\'') {

                if (lastChar != '\\') {

                    if (inQuotes) {

                        if (quoteChar == c) {
                            inQuotes = false;
                        }
                    } else {
                        inQuotes = true;
                        quoteChar = c;
                    }
                } else if (lastLastChar == '\\') {

                    if (inQuotes) {

                        if (quoteChar == c) {
                            inQuotes = false;
                        }
                    } else {
                        inQuotes = true;
                        quoteChar = c;
                    }
                }

                tokenBuf.append(c);
            } else if (c == '"') {

                if (lastChar != '\\' && lastChar != '"') {

                    if (inQuotes) {

                        if (quoteChar == c) {
                            inQuotes = false;
                        }
                    } else {
                        inQuotes = true;
                        quoteChar = c;
                    }
                } else if (lastLastChar == '\\') {

                    if (inQuotes) {

                        if (quoteChar == c) {
                            inQuotes = false;
                        }
                    } else {
                        inQuotes = true;
                        quoteChar = c;
                    }
                }

                tokenBuf.append(c);
            } else if (c == '-') {
                if (lastChar == '-' && lastLastChar != '\\' & !inQuotes) {
                    inComment = true;
                }
                
                tokenBuf.append(c);
                
            } else if (c == '\n' || c == '\r') {
                
                inComment = false;
                
                tokenBuf.append(c);
                
            } else if (c == '{') {

                if (inQuotes || inComment) {
                    tokenBuf.append(c);
                } else {
                    bracesLevel++;
                                      
                    if (bracesLevel == 1) {
                        pos++;
                        emittingEscapeCode = true;

                        return tokenBuf.toString();
                    } else {
                        tokenBuf.append(c);
                    }
                    
                }
            } else if (c == '}') {
                tokenBuf.append(c);

                if (!inQuotes && !inComment) {
                    lastChar = c;
                    

                    bracesLevel--;
                            
                    if (bracesLevel == 0) {
                        pos++;
                        return tokenBuf.toString();
                    }
                }
            } else {
                tokenBuf.append(c);
            }

            lastLastChar = lastChar;
            lastChar = c;
        }

        return tokenBuf.toString();
    }
}