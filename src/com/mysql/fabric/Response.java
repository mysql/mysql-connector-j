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
import java.util.Map;

import com.mysql.fabric.proto.xmlrpc.ResultSetParser;

/**
 * Response from Fabric request.
 */
public class Response {
    private int protocolVersion;
    private String fabricUuid;
    private int ttl;
    private String errorMessage;
    private List<Map> resultSet;

    public Response(List responseData) throws FabricCommunicationException {
        // parser of protocol version 1 as defined by WL#7760
        this.protocolVersion = (Integer) responseData.get(0);
        if (this.protocolVersion != 1) {
            throw new FabricCommunicationException("Unknown protocol version: " + this.protocolVersion);
        }
        this.fabricUuid = (String) responseData.get(1);
        this.ttl = (Integer) responseData.get(2);
        this.errorMessage = (String) responseData.get(3);
        if ("".equals(this.errorMessage)) {
            this.errorMessage = null;
        }
        List resultSets = (List) responseData.get(4);
        if (resultSets.size() > 0) {
            Map<String, ?> resultData = (Map<String, ?>) resultSets.get(0);
            this.resultSet = new ResultSetParser().parse((Map) resultData.get("info"), (List<List>) resultData.get("rows"));
        }
    }

    public int getProtocolVersion() {
        return this.protocolVersion;
    }

    public String getFabricUuid() {
        return this.fabricUuid;
    }

    public int getTtl() {
        return this.ttl;
    }

    public String getErrorMessage() {
        return this.errorMessage;
    }

    public List<Map> getResultSet() {
        return this.resultSet;
    }
}
