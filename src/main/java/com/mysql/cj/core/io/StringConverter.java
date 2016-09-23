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

import com.mysql.cj.api.ProfilerEvent;
import com.mysql.cj.api.ProfilerEventHandler;
import com.mysql.cj.api.io.ValueFactory;
import com.mysql.cj.core.Constants;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.exceptions.DataConversionException;
import com.mysql.cj.core.profiler.ProfilerEventImpl;
import com.mysql.cj.core.util.LogUtils;
import com.mysql.cj.core.util.StringUtils;

/**
 * A string converter facilitates "indirect" conversions of values from strings to other non-string types. A byte array is interpreted as a string in the given
 * character set and then inspected to guess that the actual type may be. After this, it is decoded as that type and sent to the value factory.
 */
public class StringConverter<T> extends BaseDecoratingValueFactory<T> {
    private String encoding;
    private boolean emptyStringsConvertToZero = false;
    private ProfilerEventHandler eventSink;

    public StringConverter(String encoding, ValueFactory<T> targetVf) {
        super(targetVf);
        this.encoding = encoding;
    }

    /**
     * Should empty strings be treated as "0"?
     */
    public void setEmptyStringsConvertToZero(boolean val) {
        this.emptyStringsConvertToZero = val;
    }

    public void setEventSink(ProfilerEventHandler eventSink) {
        this.eventSink = eventSink;
    }

    /**
     * @todo context information for the profiler event is unavailable here. Context information should be provided at higher levels. this includes catalog,
     *       query, rs metadata, etc
     */
    private void issueConversionViaParsingWarning() {
        if (this.eventSink == null) {
            return;
        }

        String message = Messages.getString("ResultSet.CostlyConversion",
                new Object[] { this.targetVf.getTargetTypeName(), -1, "<unknown>", "<unknown>", "<unknown>", "<unknown>", "<unknown>", "<unknown>" });

        this.eventSink.consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_WARN, "", "<unknown>", -1, -1, -1, System.currentTimeMillis(), 0,
                Constants.MILLIS_I18N, null, LogUtils.findCallingClassAndMethod(new Throwable()), message));
    }

    /**
     * Implement the logic for indirect conversions.
     */
    @Override
    public T createFromBytes(byte[] origBytes, int offset, int length) {
        MysqlTextValueDecoder stringInterpreter = new MysqlTextValueDecoder();

        // TODO: Too expensive to convert from other charset to ASCII here? UTF-8 (e.g.) doesn't need any conversion before being sent to the decoder
        String s = StringUtils.toString(origBytes, offset, length, this.encoding);
        byte[] bytes = s.getBytes();

        ValueFactory<T> vf = this.targetVf;

        issueConversionViaParsingWarning();

        if (s.length() == 0) {
            if (this.emptyStringsConvertToZero) {
                // use long=0 as this is mainly numerical oriented
                return this.targetVf.createFromLong(0);
            }
            // Else throw exception down below
        } else if (s.equalsIgnoreCase("true")) {
            return vf.createFromLong(1);
        } else if (s.equalsIgnoreCase("false")) {
            return vf.createFromLong(0);
        } else if (s.contains("e") || s.contains("E") || s.matches("-?(\\d+)?\\.\\d+")) {
            // floating point
            return stringInterpreter.decodeDouble(bytes, 0, bytes.length, vf);
        } else if (s.matches("-?\\d+")) {
            // integer
            if (s.charAt(0) == '-') {
                return stringInterpreter.decodeInt8(bytes, 0, bytes.length, vf);
            }
            return stringInterpreter.decodeUInt8(bytes, 0, bytes.length, vf);
        } else if (s.length() == MysqlTextValueDecoder.DATE_BUF_LEN && s.charAt(4) == '-' && s.charAt(7) == '-') {
            return stringInterpreter.decodeDate(bytes, 0, bytes.length, vf);
        } else if (s.length() >= MysqlTextValueDecoder.TIME_STR_LEN_MIN && s.length() <= MysqlTextValueDecoder.TIME_STR_LEN_MAX && s.charAt(2) == ':'
                && s.charAt(5) == ':') {
            return stringInterpreter.decodeTime(bytes, 0, bytes.length, vf);
        } else if (s.length() >= MysqlTextValueDecoder.TIMESTAMP_NOFRAC_STR_LEN
                && (s.length() <= MysqlTextValueDecoder.TIMESTAMP_STR_LEN_MAX || s.length() == MysqlTextValueDecoder.TIMESTAMP_STR_LEN_WITH_NANOS)
                && s.charAt(4) == '-' && s.charAt(7) == '-' && s.charAt(10) == ' ' && s.charAt(13) == ':' && s.charAt(16) == ':') {
            return stringInterpreter.decodeTimestamp(bytes, 0, bytes.length, vf);
        }
        throw new DataConversionException(Messages.getString("ResultSet.UnableToInterpretString", new Object[] { s }));
    }

    @Override
    public T createFromBit(byte[] bytes, int offset, int length) {
        MysqlTextValueDecoder stringInterpreter = new MysqlTextValueDecoder();
        ValueFactory<T> vf = this.targetVf;
        return stringInterpreter.decodeBit(bytes, offset, length, vf);
    }
}
