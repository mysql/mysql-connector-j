/*
 * Copyright (c) 2015, 2020, Oracle and/or its affiliates.
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

package com.mysql.cj.xdevapi;

import java.io.IOException;
import java.io.StringReader;

import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.AssertionFailedException;
import com.mysql.cj.result.DefaultValueFactory;
import com.mysql.cj.result.Field;
import com.mysql.cj.result.ValueFactory;
import com.mysql.cj.util.StringUtils;

/**
 * A {@link ValueFactory} implementation to create {@link DbDoc}s.
 */
public class DbDocValueFactory extends DefaultValueFactory<DbDoc> {
    /**
     * Constructor.
     * 
     * @param pset
     *            {@link PropertySet}
     */
    public DbDocValueFactory(PropertySet pset) {
        super(pset);
    }

    /**
     * Interpret the given byte array as a string. This value factory needs to know the encoding to interpret the string. The default (null) will interpret the
     * byte array using the platform encoding.
     */
    @Override
    public DbDoc createFromBytes(byte[] bytes, int offset, int length, Field f) {
        try {
            return JsonParser.parseDoc(new StringReader(StringUtils.toString(bytes, offset, length, f.getEncoding())));
        } catch (IOException ex) {
            throw AssertionFailedException.shouldNotHappen(ex);
        }
    }

    @Override
    public DbDoc createFromNull() {
        return null; // TODO: ? JsonValueLiteral.NULL;
    }

    public String getTargetTypeName() {
        return DbDoc.class.getName();
    }
}
