/*
 * MM JDBC Drivers for MySQL
 *
 * $Id$
 *
 * Copyright (C) 1998 Mark Matthews <mmatthew@worldserver.com>
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 * 
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the
 * Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA  02111-1307, USA.
 *
 * See the COPYING file located in the top-level-directory of
 * the archive of this library for complete text of license.
 */

/**
 * EscapeTokenizer breaks up an SQL statement into SQL and
 * escape code parts.
 *
 * @author Mark Matthews <mmatthew@worldserver.com>
 */

package com.mysql.jdbc;

public class EscapeTokenizer {
	protected char _lastChar = 0;
	protected char _lastLastChar = 0;

	protected boolean _inQuotes = false;

	protected boolean _inBraces = false;

	protected char _quoteChar = 0;

	protected boolean _emittingEscapeCode = false;

	protected String _source = null;

	protected int _pos = 0;
	
	protected int _sourceLength = 0;

	public EscapeTokenizer(String s) {
		_source = s;
		_sourceLength = s.length();
		_pos = 0;
	}

	public synchronized boolean hasMoreTokens() {
		return (_pos < _sourceLength);
	}

	public synchronized String nextToken() {
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
					}
					else {
						_inQuotes = true;
						_quoteChar = c;
					}
				}
				else if (_lastLastChar == '\\') {
					if (_inQuotes) {
						if (_quoteChar == c) {
							_inQuotes = false;
						}
					}
					else {
						_inQuotes = true;
						_quoteChar = c;
					}
				}
				
				tokenBuf.append(c);
			}
			else
				if (c == '"') {
					if (_lastChar != '\\' && _lastChar != '"') {
						if (_inQuotes) {
							if (_quoteChar == c) {
								_inQuotes = false;
							}
						}
						else {
							_inQuotes = true;
							_quoteChar = c;
						}
					}
					else if (_lastLastChar == '\\') {
						if (_inQuotes) {
							if (_quoteChar == c) {
								_inQuotes = false;
							}
						}
						else {
							_inQuotes = true;
							_quoteChar = c;
						}
					}
					
					tokenBuf.append(c);
				}
				else
					if (c == '{') {
						if (_inQuotes) {
							tokenBuf.append(c);
						}
						else {
							_pos++;
							_emittingEscapeCode = true;
							return tokenBuf.toString();
						}
					}
					else
						if (c == '}') {
							tokenBuf.append(c);
							if (!_inQuotes) {
								_lastChar = c;
								_pos++;
								return tokenBuf.toString();
							}
						}
						else {
							tokenBuf.append(c);
						}
						
			_lastLastChar = _lastChar;
			_lastChar = c;
		}

		return tokenBuf.toString();
	}
}
