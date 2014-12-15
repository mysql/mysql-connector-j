/*
  Copyright (c) 2013, 2014, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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

package com.mysql.fabric.xmlrpc.base;

import java.util.GregorianCalendar;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

public class Value {

    public static final byte TYPE_i4 = 0;
    public static final byte TYPE_int = 1;
    public static final byte TYPE_boolean = 2;
    public static final byte TYPE_string = 3;
    public static final byte TYPE_double = 4;
    public static final byte TYPE_dateTime_iso8601 = 5;
    public static final byte TYPE_base64 = 6;
    public static final byte TYPE_struct = 7;
    public static final byte TYPE_array = 8;

    protected Object objValue = "";
    protected byte objType = TYPE_string;
    private DatatypeFactory dtf = null;

    public Value() {
    }

    public Value(int value) {
        setInt(value);
    }

    public Value(String value) {
        setString(value);
    }

    public Value(boolean value) {
        setBoolean(value);
    }

    public Value(double value) {
        setDouble(value);
    }

    public Value(GregorianCalendar value) throws DatatypeConfigurationException {
        setDateTime(value);
    }

    public Value(byte[] value) {
        setBase64(value);
    }

    public Value(Struct value) {
        setStruct(value);
    }

    public Value(Array value) {
        setArray(value);
    }

    public Object getValue() {
        return this.objValue;
    }

    public byte getType() {
        return this.objType;
    }

    public void setInt(int value) {
        this.objValue = Integer.valueOf(value);
        this.objType = TYPE_int;
    }

    public void setInt(String value) {
        this.objValue = Integer.valueOf(value);
        this.objType = TYPE_int;
    }

    public void setString(String value) {
        this.objValue = value;
        this.objType = TYPE_string;
    }

    public void appendString(String value) {
        this.objValue = this.objValue != null ? this.objValue + value : value;
        this.objType = TYPE_string;
    }

    public void setBoolean(boolean value) {
        this.objValue = Boolean.valueOf(value);
        this.objType = TYPE_boolean;
    }

    public void setBoolean(String value) {
        if (value.trim().equals("1") || value.trim().equalsIgnoreCase("true")) {
            this.objValue = true;
        } else {
            this.objValue = false;
        }
        this.objType = TYPE_boolean;
    }

    public void setDouble(double value) {
        this.objValue = Double.valueOf(value);
        this.objType = TYPE_double;
    }

    public void setDouble(String value) {
        this.objValue = Double.valueOf(value);
        this.objType = TYPE_double;
    }

    public void setDateTime(GregorianCalendar value) throws DatatypeConfigurationException {
        if (this.dtf == null) {
            this.dtf = DatatypeFactory.newInstance();
        }
        this.objValue = this.dtf.newXMLGregorianCalendar(value);
        this.objType = TYPE_dateTime_iso8601;
    }

    public void setDateTime(String value) throws DatatypeConfigurationException {
        if (this.dtf == null) {
            this.dtf = DatatypeFactory.newInstance();
        }
        this.objValue = this.dtf.newXMLGregorianCalendar(value);
        this.objType = TYPE_dateTime_iso8601;
    }

    public void setBase64(byte[] value) {
        this.objValue = value;
        this.objType = TYPE_base64;
    }

    public void setStruct(Struct value) {
        this.objValue = value;
        this.objType = TYPE_struct;
    }

    public void setArray(Array value) {
        this.objValue = value;
        this.objType = TYPE_array;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("<value>");
        switch (this.objType) {
            case Value.TYPE_i4:
                sb.append("<i4>" + ((Integer) this.objValue).toString() + "</i4>");
                break;
            case Value.TYPE_int:
                sb.append("<int>" + ((Integer) this.objValue).toString() + "</int>");
                break;

            case Value.TYPE_boolean:
                sb.append("<boolean>" + (((Boolean) this.objValue) ? 1 : 0) + "</boolean>");
                break;

            case Value.TYPE_double:
                sb.append("<double>" + ((Double) this.objValue).toString() + "</double>");
                break;

            case Value.TYPE_dateTime_iso8601:
                sb.append("<dateTime.iso8601>" + ((XMLGregorianCalendar) this.objValue).toString() + "</dateTime.iso8601>");
                break;

            case Value.TYPE_base64:
                sb.append("<base64>" + ((byte[]) this.objValue).toString() + "</base64>");
                break;

            case Value.TYPE_struct:
                sb.append(((Struct) this.objValue).toString());
                break;

            case Value.TYPE_array:
                sb.append(((Array) this.objValue).toString());
                break;

            default:
                sb.append("<string>" + escapeXMLChars(this.objValue.toString()) + "</string>");
        }
        sb.append("</value>");
        return sb.toString();
    }

    private String escapeXMLChars(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        char c;
        for (int i = 0; i < s.length(); i++) {
            c = s.charAt(i);
            switch (c) {
                case '&':
                    sb.append("&amp;");
                    break;
                case '<':
                    sb.append("&lt;");
                    break;
                case '>':
                    sb.append("&gt;");
                    break;
                default:
                    sb.append(c);
                    break;
            }
        }
        return sb.toString();
    }
}
