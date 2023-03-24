/*
 * Copyright (c) 2002, 2020, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.mysql.cj.util;

import java.util.concurrent.locks.ReentrantLock;

/**
 * EscapeTokenizer breaks up an SQL statement into SQL and escape code parts.
 */
public class EscapeTokenizer {
    private static final char CHR_ESCAPE = '\\';
    private static final char CHR_SGL_QUOTE = '\'';
    private static final char CHR_DBL_QUOTE = '"';
    private static final char CHR_LF = '\n';
    private static final char CHR_CR = '\r';
    private static final char CHR_COMMENT = '-';
    private static final char CHR_BEGIN_TOKEN = '{';
    private static final char CHR_END_TOKEN = '}';
    private static final char CHR_VARIABLE = '@';

    private String source = null;
    private int sourceLength = 0;
    private int pos = 0;

    private boolean emittingEscapeCode = false;
    private boolean sawVariableUse = false;
    private int bracesLevel = 0;
    private boolean inQuotes = false;
    private char quoteChar = 0;
    private final ReentrantLock objectLock = new ReentrantLock();

    /**
     * Creates a new EscapeTokenizer object.
     * 
     * @param source
     *            the string to tokenize
     */
    public EscapeTokenizer(String source) {
        this.source = source;
        this.sourceLength = source.length();
        this.pos = 0;
    }

    /**
     * Does this tokenizer have more tokens available?
     * 
     * @return if this tokenizer has more tokens available
     */
    public boolean hasMoreTokens() {
        objectLock.lock();
        try {
            return (this.pos < this.sourceLength);
        } finally {
            objectLock.unlock();
        }
    }

    /**
     * Returns the next token
     * 
     * @return the next token.
     */
    public String nextToken() {
        objectLock.lock();
        try {
            StringBuilder tokenBuf = new StringBuilder();
            boolean backslashEscape = false;

            if (this.emittingEscapeCode) {
                // Previous token ended at the beginning of an escape code, so this token must start with '{'
                tokenBuf.append("{");
                this.emittingEscapeCode = false;
            }

            for (; this.pos < this.sourceLength; this.pos++) {
                char c = this.source.charAt(this.pos);

                // process escape char: (\)
                if (c == CHR_ESCAPE) {
                    tokenBuf.append(c);
                    backslashEscape = !backslashEscape;
                    continue;
                }

                // process quotes: ('|")
                if ((c == CHR_SGL_QUOTE || c == CHR_DBL_QUOTE) && !backslashEscape) {
                    tokenBuf.append(c);
                    if (this.inQuotes) {
                        if (c == this.quoteChar) {
                            // look ahead for doubled quote
                            if ((this.pos + 1 < this.sourceLength) && (this.source.charAt(this.pos + 1) == this.quoteChar)) {
                                tokenBuf.append(c);
                                this.pos++; // consume following char '\'' or '"'
                            } else {
                                this.inQuotes = false;
                            }
                        }
                    } else {
                        this.inQuotes = true;
                        this.quoteChar = c;
                    }
                    continue;
                }

                // process new line: (\n|\r)
                if ((c == CHR_LF) || (c == CHR_CR)) {
                    tokenBuf.append(c);
                    backslashEscape = false;
                    continue;
                }

                if (!this.inQuotes && !backslashEscape) {
                    // process comments: (--)
                    if (c == CHR_COMMENT) {
                        tokenBuf.append(c);
                        // look ahead for double hyphen
                        if ((this.pos + 1 < this.sourceLength) && (this.source.charAt(this.pos + 1) == CHR_COMMENT)) {
                            // consume following chars until new line or end of string
                            while (++this.pos < this.sourceLength && c != CHR_LF && c != CHR_CR) {
                                c = this.source.charAt(this.pos);
                                tokenBuf.append(c);
                            }
                            this.pos--;
                        }
                        continue;
                    }

                    // process begin token: ({)
                    if (c == CHR_BEGIN_TOKEN) {
                        this.bracesLevel++;
                        if (this.bracesLevel == 1) {
                            this.emittingEscapeCode = true;
                            this.pos++; // consume char '{' before returning
                            return tokenBuf.toString();
                        }
                        tokenBuf.append(c);
                        continue;
                    }

                    // process end token: (})
                    if (c == CHR_END_TOKEN) {
                        tokenBuf.append(c);
                        this.bracesLevel--;
                        if (this.bracesLevel == 0) {
                            this.pos++; // consume char '}' before returning
                            return tokenBuf.toString();
                        }
                        continue;
                    }

                    // detect variable usage: (@)
                    if (c == CHR_VARIABLE) {
                        this.sawVariableUse = true;
                    }
                }

                tokenBuf.append(c);
                backslashEscape = false;
            }

            return tokenBuf.toString();
        } finally {
            objectLock.unlock();
        }
    }

    /**
     * Returns true if a variable reference was found. Note that this information isn't accurate until finishing to
     * process all tokens from source String. It also can't be used as per token basis.
     * 
     * @return true if a variable reference was found.
     */
    public boolean sawVariableUse() {
        return this.sawVariableUse;
    }
}
