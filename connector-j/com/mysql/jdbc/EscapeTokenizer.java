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

/**
 * EscapeTokenizer breaks up an SQL statement into SQL and
 * escape code parts.
 *
 * @author Mark Matthews <mmatthew@worldserver.com>
 */
package com.mysql.jdbc;

public class EscapeTokenizer
{

    //~ Instance/static variables .............................................

    protected boolean _emittingEscapeCode = false;
    protected boolean _inBraces     = false;
    protected boolean _inQuotes     = false;
    protected char    _lastChar     = 0;
    protected char    _lastLastChar = 0;
    protected int     _pos          = 0;
    protected char    _quoteChar    = 0;
    protected String  _source       = null;
    protected int     _sourceLength = 0;

    //~ Constructors ..........................................................

    public EscapeTokenizer(String s)
    {
        _source       = s;
        _sourceLength = s.length();
        _pos          = 0;
    }

    //~ Methods ...............................................................

    public synchronized boolean hasMoreTokens()
    {

        return (_pos < _sourceLength);
    }

    public synchronized String nextToken()
    {

        StringBuffer tokenBuf = new StringBuffer();

        if (_emittingEscapeCode) {
            tokenBuf.append("{");
            _emittingEscapeCode = false;
        }

        for (; _pos < _sourceLength; _pos++) {

            char c = _source.charAt(_pos);

            if (c == '\'') {

                if (_lastChar != '\\') {

                    if (_inQuotes) {

                        if (_quoteChar == c) {
                            _inQuotes = false;
                        }
                    } else {
                        _inQuotes  = true;
                        _quoteChar = c;
                    }
                } else if (_lastLastChar == '\\') {

                    if (_inQuotes) {

                        if (_quoteChar == c) {
                            _inQuotes = false;
                        }
                    } else {
                        _inQuotes  = true;
                        _quoteChar = c;
                    }
                }

                tokenBuf.append(c);
            } else if (c == '"') {

                if (_lastChar != '\\' && _lastChar != '"') {

                    if (_inQuotes) {

                        if (_quoteChar == c) {
                            _inQuotes = false;
                        }
                    } else {
                        _inQuotes  = true;
                        _quoteChar = c;
                    }
                } else if (_lastLastChar == '\\') {

                    if (_inQuotes) {

                        if (_quoteChar == c) {
                            _inQuotes = false;
                        }
                    } else {
                        _inQuotes  = true;
                        _quoteChar = c;
                    }
                }

                tokenBuf.append(c);
            } else if (c == '{') {

                if (_inQuotes) {
                    tokenBuf.append(c);
                } else {
                    _pos++;
                    _emittingEscapeCode = true;

                    return tokenBuf.toString();
                }
            } else if (c == '}') {
                tokenBuf.append(c);

                if (!_inQuotes) {
                    _lastChar = c;
                    _pos++;

                    return tokenBuf.toString();
                }
            } else {
                tokenBuf.append(c);
            }

            _lastLastChar = _lastChar;
            _lastChar     = c;
        }

        return tokenBuf.toString();
    }
}