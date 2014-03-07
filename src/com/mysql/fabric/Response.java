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
 * Response from Fabric request.
 */
public class Response {
    private boolean successful;
    private Object returnValue;
    private String traceString;

    public Response(List<?> responseData) {
		this.successful = (Boolean)responseData.get(0);
		if (this.successful) {
			this.returnValue = responseData.get(2);
		} else {
			this.traceString = (String)responseData.get(1);
			String trace[] = traceString.split("\n");
			// python uses the second from the end of the array
			this.returnValue = trace[trace.length-1];
		}
		// "details" ignored
    }

    public boolean isSuccessful() {
		return this.successful;
    }

    public Object getReturnValue() {
		return this.returnValue;
    }

	public String getTraceString() {
		return this.traceString;
	}
}
