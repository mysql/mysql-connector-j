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

import java.util.ArrayList;
import java.util.List;

public class Struct {

	protected List<Member> member;

	public List<Member> getMember() {
		if (member == null) {
			member = new ArrayList<Member>();
		}
		return this.member;
	}

	public void addMember(Member m) {
		getMember().add(m);
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		if (this.member != null) {
			sb.append("<struct>");
			for (int i = 0; i < this.member.size(); i++) {
				sb.append(this.member.get(i).toString());
			}
			sb.append("</struct>");
		}
		return sb.toString();
	}

}
