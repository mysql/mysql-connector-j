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

package com.mysql.fabric;

import java.util.List;

/**
 * A response from one of the dump.* commands. These are formatted differently
 * than responses from other commands. Most dump commands return List<List>.
 * `dump.fabric_nodes' is a notable exception returning a list of Fabric host/port pairs
 * as strings.
 */
public class DumpResponse {
	private List<?> returnValue;
	private int ttl;

	public DumpResponse(List<?> responseData) {
		// uuid ignored (why is it int?)
		// version token ignored
		this.ttl = (Integer)responseData.get(2);
		this.returnValue = (List<?>)responseData.get(3);
	}

	public List getReturnValue() {
		return this.returnValue;
	}

	public int getTtl() {
		return this.ttl;
	}

	/**
	 * Debug method to print the response and classes of data values.
	 */
	public String dumpReturnValue() {
		StringBuffer b = new StringBuffer();
		if (this.returnValue.size() > 0 &&
			List.class.isAssignableFrom(this.returnValue.get(0).getClass())) {
			// elements of return value list are lists themselves
			for (List l : (List<List>) this.returnValue) {
				b.append("[\n");
				for (Object o : l) {
					b.append(" " + o.toString());
					b.append(" (" + o.getClass() + "),\n");
				}
				b.append("]\n");
			}
		} else {
			// for dump.fabric_nodes or any other non-list value types
			for (Object o : this.returnValue) {
				b.append(o.toString() + ",");
			}
		}
		return b.toString();
	}
}
