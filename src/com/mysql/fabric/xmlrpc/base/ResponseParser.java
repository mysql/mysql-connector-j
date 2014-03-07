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

import java.util.Stack;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

public class ResponseParser extends DefaultHandler {

	private MethodResponse resp = null;

	public MethodResponse getMethodResponse() {
		return resp;
	}

	Stack<Object> elNames = new Stack<Object>();
	Stack<Object> objects = new Stack<Object>();

	@Override
	public void startElement(String uri, String localName, String qName,
			Attributes attributes) throws SAXException {

		String thisElement = qName;
		if (thisElement != null) {
			elNames.push(thisElement);

			if (thisElement.equals("methodResponse")) {
				objects.push(new MethodResponse());
			} else if (thisElement.equals("params")) {
				objects.push(new Params());
			} else if (thisElement.equals("param")) {
				objects.push(new Param());
			} else if (thisElement.equals("value")) {
				objects.push(new Value());
			} else if (thisElement.equals("array")) {
				objects.push(new Array());
			} else if (thisElement.equals("data")) {
				objects.push(new Data());
			} else if (thisElement.equals("struct")) {
				objects.push(new Struct());
			} else if (thisElement.equals("member")) {
				objects.push(new Member());
			} else if (thisElement.equals("fault")) {
				objects.push(new Fault());
			}
		}
	}

	@Override
	public void endElement(String uri, String localName, String qName)
			throws SAXException {
		String thisElement = (String) elNames.pop();
		if (thisElement != null) {
			if (thisElement.equals("methodResponse")) {
				this.resp = (MethodResponse) objects.pop();
			} else if (thisElement.equals("params")) {
				Params pms = (Params) objects.pop();
				MethodResponse parent = (MethodResponse) objects.peek();
				parent.setParams(pms);
			} else if (thisElement.equals("param")) {
				Param p = (Param) objects.pop();
				Params parent = (Params) objects.peek();
				parent.addParam(p);
			} else if (thisElement.equals("value")) {
				Value v = (Value) objects.pop();
				Object parent = objects.peek();
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
				Array a = (Array) objects.pop();
				Value parent = (Value) objects.peek();
				parent.setArray(a);
			} else if (thisElement.equals("data")) {
				Data d = (Data) objects.pop();
				Array parent = (Array) objects.peek();
				parent.setData(d);
			} else if (thisElement.equals("struct")) {
				Struct s = (Struct) objects.pop();
				Value parent = (Value) objects.peek();
				parent.setStruct(s);
			} else if (thisElement.equals("member")) {
				Member m = (Member) objects.pop();
				Struct parent = (Struct) objects.peek();
				parent.addMember(m);
			} else if (thisElement.equals("fault")) {
				Fault f = (Fault) objects.pop();
				MethodResponse parent = (MethodResponse) objects.peek();
				parent.setFault(f);
			}
		}
	}

	@Override
	public void characters(char[] ch, int start, int length)
			throws SAXException {
		try {
			String thisElement = (String) elNames.peek();
			if (thisElement != null) {
				if (thisElement.equals("name")) {
					((Member) objects.peek()).setName(new String(ch, start, length));
				} else if (thisElement.equals("value")) {
					((Value) objects.peek()).appendString(new String(ch, start, length));
				} else if (thisElement.equals("i4")
						|| thisElement.equals("int")) {
					((Value) objects.peek()).setInt(new String(ch, start, length));
				} else if (thisElement.equals("boolean")) {
					((Value) objects.peek()).setBoolean(new String(ch, start, length));
				} else if (thisElement.equals("string")) {
					((Value) objects.peek()).appendString(new String(ch, start, length));
				} else if (thisElement.equals("double")) {
					((Value) objects.peek()).setDouble(new String(ch, start, length));
				} else if (thisElement.equals("dateTime.iso8601")) {
					((Value) objects.peek()).setDateTime(new String(ch, start, length));
				} else if (thisElement.equals("base64")) {
					((Value) objects.peek()).setBase64(new String(ch, start, length).getBytes());
				}
			}
		} catch (Exception e) {
			throw new SAXParseException(e.getMessage(), null, e);
		}
	}

}
