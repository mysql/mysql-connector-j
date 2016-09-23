/*
  Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FOSS License Exception
  <http://www.mysql.com/about/legal/licensing/foss-exception.html>.

  This program is free software; you can redistribute it and/or modify it under the terms
  of the GNU General Public License as published by the Free Software Foundation; version 2
  of the License.

  This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
  See the GNU General Public License for more details.

  You should have received a copy of the GNU General Public License along with this
  program; if not, write to the Free Software Foundation, Inc., 51 Franklin St, Fifth
  Floor, Boston, MA 02110-1301  USA

 */

package com.mysql.cj.core.io;

import java.io.IOException;
import java.io.StringReader;

import com.mysql.cj.api.io.ValueFactory;
import com.mysql.cj.core.exceptions.AssertionFailedException;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.x.json.DbDoc;
import com.mysql.cj.x.json.JsonParser;

/**
 * A {@link ValueFactory} implementation to create {@link DbDoc}s.
 */
public class DbDocValueFactory extends DefaultValueFactory<DbDoc> {
    private String encoding;

    public DbDocValueFactory() {
    }

    public DbDocValueFactory(String encoding) {
        this.encoding = encoding;
    }

    /**
     * Interpret the given byte array as a string. This value factory needs to know the encoding to interpret the string. The default (null) will interpet the
     * byte array using the platform encoding.
     */
    @Override
    public DbDoc createFromBytes(byte[] bytes, int offset, int length) {
        try {
            return JsonParser.parseDoc(new StringReader(StringUtils.toString(bytes, offset, length, this.encoding)));
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
