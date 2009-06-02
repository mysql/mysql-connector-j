/*
 Copyright  2002-2004 MySQL AB, 2008 Sun Microsystems

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

/**
 * EscapeTokenizer breaks up an SQL statement into SQL and escape code parts.
 * 
 * @author Mark Matthews
 */
public class EscapeTokenizer {
	// ~ Instance fields
	// --------------------------------------------------------

	private int bracesLevel = 0;

	private boolean emittingEscapeCode = false;

	private boolean inComment = false;

	private boolean inQuotes = false;

	private char lastChar = 0;

	private char lastLastChar = 0;

	private int pos = 0;

	private char quoteChar = 0;

	private boolean sawVariableUse = false;

	private String source = null;

	private int sourceLength = 0;

	// ~ Constructors
	// -----------------------------------------------------------

	/**
	 * Creates a new EscapeTokenizer object.
	 * 
	 * @param s
	 *            the string to tokenize
	 */
	public EscapeTokenizer(String s) {
		this.source = s;
		this.sourceLength = s.length();
		this.pos = 0;
	}

	// ~ Methods
	// ----------------------------------------------------------------

	/**
	 * Does this tokenizer have more tokens available?
	 * 
	 * @return if this tokenizer has more tokens available
	 */
	public synchronized boolean hasMoreTokens() {
		return (this.pos < this.sourceLength);
	}

	/**
	 * Returns the next token
	 * 
	 * @return the next token.
	 */
	public synchronized String nextToken() {
		StringBuffer tokenBuf = new StringBuffer();

		if (this.emittingEscapeCode) {
			tokenBuf.append("{"); //$NON-NLS-1$
			this.emittingEscapeCode = false;
		}

		for (; this.pos < this.sourceLength; this.pos++) {
			char c = this.source.charAt(this.pos);

			// Detect variable usage

			if (!this.inQuotes && c == '@') {
				this.sawVariableUse = true;
			}

			if ((c == '\'' || c == '"') && !inComment) {
				if (this.inQuotes && c == quoteChar) {
					if (this.pos + 1 < this.sourceLength) {
						if (this.source.charAt(this.pos + 1) == quoteChar) {
							// Doubled-up quote escape, if the first quote isn't already escaped
							if (this.lastChar != '\\') {
								tokenBuf.append(quoteChar);
								tokenBuf.append(quoteChar);
								this.pos++;
								continue;
							}
						}
					}
				}
				if (this.lastChar != '\\') {
					if (this.inQuotes) {
						if (this.quoteChar == c) {
							this.inQuotes = false;
						}
					} else {
						this.inQuotes = true;
						this.quoteChar = c;
					}
				} else if (this.lastLastChar == '\\') {
					if (this.inQuotes) {
						if (this.quoteChar == c) {
							this.inQuotes = false;
						}
					} else {
						this.inQuotes = true;
						this.quoteChar = c;
					}
				}

				tokenBuf.append(c);
			} else if (c == '-') {
				if ((this.lastChar == '-')
						&& ((this.lastLastChar != '\\') && !this.inQuotes)) {
					this.inComment = true;
				}

				tokenBuf.append(c);
			} else if ((c == '\n') || (c == '\r')) {
				this.inComment = false;

				tokenBuf.append(c);
			} else if (c == '{') {
				if (this.inQuotes || this.inComment) {
					tokenBuf.append(c);
				} else {
					this.bracesLevel++;

					if (this.bracesLevel == 1) {
						this.pos++;
						this.emittingEscapeCode = true;

						return tokenBuf.toString();
					}

					tokenBuf.append(c);
				}
			} else if (c == '}') {
				tokenBuf.append(c);

				if (!this.inQuotes && !this.inComment) {
					this.lastChar = c;

					this.bracesLevel--;

					if (this.bracesLevel == 0) {
						this.pos++;

						return tokenBuf.toString();
					}
				}
			} else {
				tokenBuf.append(c);
			}

			this.lastLastChar = this.lastChar;
			this.lastChar = c;
		}

		return tokenBuf.toString();
	}

	boolean sawVariableUse() {
		return this.sawVariableUse;
	}
}
