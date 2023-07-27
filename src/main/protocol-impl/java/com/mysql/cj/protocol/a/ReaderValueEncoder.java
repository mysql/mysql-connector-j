/*
 * Copyright (c) 2022, 2023, Oracle and/or its affiliates.
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

package com.mysql.cj.protocol.a;

import java.io.IOException;
import java.io.Reader;
import java.io.UnsupportedEncodingException;

import com.mysql.cj.BindValue;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.CJOperationNotSupportedException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.Message;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.util.Util;

public class ReaderValueEncoder extends AbstractValueEncoder {

    @Override
    public byte[] getBytes(BindValue binding) {
        return readBytes((Reader) binding.getValue(), binding);
    }

    @Override
    public String getString(BindValue binding) {
        return "'** STREAM DATA **'";
    }

    @Override
    public void encodeAsBinary(Message msg, BindValue binding) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    protected byte[] readBytes(Reader reader, BindValue binding) {
        try {
            char[] c = null;
            int len = 0;
            byte[] bytes;

            boolean useLength = this.propertySet.getBooleanProperty(PropertyKey.useStreamLengthsInPrepStmts).getValue();
            String clobEncoding = binding.isNational() ? null : this.propertySet.getStringProperty(PropertyKey.clobCharacterEncoding).getStringValue();
            if (clobEncoding == null) {
                clobEncoding = this.charEncoding.getStringValue();
            }

            long scaleOrLength = binding.getScaleOrLength();
            if (useLength && scaleOrLength != -1) {
                c = new char[(int) scaleOrLength];
                int numCharsRead = Util.readFully(reader, c, (int) scaleOrLength); // blocks until all read
                bytes = StringUtils.getBytes(new String(c, 0, numCharsRead), clobEncoding);
            } else {
                c = new char[4096];
                StringBuilder buf = new StringBuilder();
                while ((len = reader.read(c)) != -1) {
                    buf.append(c, 0, len);
                }
                bytes = StringUtils.getBytes(buf.toString(), clobEncoding);
            }
            return escapeBytesIfNeeded(bytes);

        } catch (UnsupportedEncodingException uec) {
            throw ExceptionFactory.createException(WrongArgumentException.class, uec.toString(), uec, this.exceptionInterceptor);
        } catch (IOException ioEx) {
            throw ExceptionFactory.createException(ioEx.toString(), ioEx, this.exceptionInterceptor);
        }
    }

}
