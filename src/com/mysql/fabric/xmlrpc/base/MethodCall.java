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

public class MethodCall {

	protected String methodName;
	protected Params params;

	/**
	 * Gets the value of the methodName property.
	 */
	public String getMethodName() {
		return methodName;
	}

	/**
	 * Sets the value of the methodName property.
	 */
	public void setMethodName(String value) {
		this.methodName = value;
	}

	/**
	 * Gets the value of the params property.
	 */
	public Params getParams() {
		return params;
	}

	/**
	 * Sets the value of the params property.
	 */
	public void setParams(Params value) {
		this.params = value;
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
		sb.append("<methodCall>");
		sb.append("	<methodName>" + this.methodName + "</methodName>");
		if (this.params != null) {
			sb.append(this.params.toString());
		}
		sb.append("</methodCall>");
		return sb.toString();
	}

}
