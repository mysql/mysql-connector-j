/*
  Copyright (c) 2013, 2014, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.fabric.xmlrpc.base;

import java.util.Stack;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

public class ResponseParser extends DefaultHandler {

    private MethodResponse resp = null;

    public MethodResponse getMethodResponse() {
        return this.resp;
    }

    Stack<Object> elNames = new Stack<Object>();
    Stack<Object> objects = new Stack<Object>();

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {

        String thisElement = qName;
        if (thisElement != null) {
            this.elNames.push(thisElement);

            if (thisElement.equals("methodResponse")) {
                this.objects.push(new MethodResponse());
            } else if (thisElement.equals("params")) {
                this.objects.push(new Params());
            } else if (thisElement.equals("param")) {
                this.objects.push(new Param());
            } else if (thisElement.equals("value")) {
                this.objects.push(new Value());
            } else if (thisElement.equals("array")) {
                this.objects.push(new Array());
            } else if (thisElement.equals("data")) {
                this.objects.push(new Data());
            } else if (thisElement.equals("struct")) {
                this.objects.push(new Struct());
            } else if (thisElement.equals("member")) {
                this.objects.push(new Member());
            } else if (thisElement.equals("fault")) {
                this.objects.push(new Fault());
            }
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        String thisElement = (String) this.elNames.pop();
        if (thisElement != null) {
            if (thisElement.equals("methodResponse")) {
                this.resp = (MethodResponse) this.objects.pop();
            } else if (thisElement.equals("params")) {
                Params pms = (Params) this.objects.pop();
                MethodResponse parent = (MethodResponse) this.objects.peek();
                parent.setParams(pms);
            } else if (thisElement.equals("param")) {
                Param p = (Param) this.objects.pop();
                Params parent = (Params) this.objects.peek();
                parent.addParam(p);
            } else if (thisElement.equals("value")) {
                Value v = (Value) this.objects.pop();
                Object parent = this.objects.peek();
                if (parent instanceof Data) {
                    ((Data) parent).addValue(v);
                } else if (parent instanceof Param) {
                    ((Param) parent).setValue(v);
                } else if (parent instanceof Member) {
                    ((Member) parent).setValue(v);
                } else if (parent instanceof Fault) {
                    ((Fault) parent).setValue(v);
                }
            } else if (thisElement.equals("array")) {
                Array a = (Array) this.objects.pop();
                Value parent = (Value) this.objects.peek();
                parent.setArray(a);
            } else if (thisElement.equals("data")) {
                Data d = (Data) this.objects.pop();
                Array parent = (Array) this.objects.peek();
                parent.setData(d);
            } else if (thisElement.equals("struct")) {
                Struct s = (Struct) this.objects.pop();
                Value parent = (Value) this.objects.peek();
                parent.setStruct(s);
            } else if (thisElement.equals("member")) {
                Member m = (Member) this.objects.pop();
                Struct parent = (Struct) this.objects.peek();
                parent.addMember(m);
            } else if (thisElement.equals("fault")) {
                Fault f = (Fault) this.objects.pop();
                MethodResponse parent = (MethodResponse) this.objects.peek();
                parent.setFault(f);
            }
        }
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        try {
            String thisElement = (String) this.elNames.peek();
            if (thisElement != null) {
                if (thisElement.equals("name")) {
                    ((Member) this.objects.peek()).setName(new String(ch, start, length));
                } else if (thisElement.equals("value")) {
                    ((Value) this.objects.peek()).appendString(new String(ch, start, length));
                } else if (thisElement.equals("i4") || thisElement.equals("int")) {
                    ((Value) this.objects.peek()).setInt(new String(ch, start, length));
                } else if (thisElement.equals("boolean")) {
                    ((Value) this.objects.peek()).setBoolean(new String(ch, start, length));
                } else if (thisElement.equals("string")) {
                    ((Value) this.objects.peek()).appendString(new String(ch, start, length));
                } else if (thisElement.equals("double")) {
                    ((Value) this.objects.peek()).setDouble(new String(ch, start, length));
                } else if (thisElement.equals("dateTime.iso8601")) {
                    ((Value) this.objects.peek()).setDateTime(new String(ch, start, length));
                } else if (thisElement.equals("base64")) {
                    ((Value) this.objects.peek()).setBase64(new String(ch, start, length).getBytes());
                }
            }
        } catch (Exception e) {
            throw new SAXParseException(e.getMessage(), null, e);
        }
    }

}
