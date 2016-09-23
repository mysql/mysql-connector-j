/*
  Copyright (c) 2014, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.fabric.proto.xmlrpc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Parser for result data returned from Fabric XML-RPC protocol.
 */
public class ResultSetParser {
    public ResultSetParser() {
    }

    /**
     * Transform the Fabric formatted result into a list of
     * hashes/rows.
     */
    public List<Map<String, ?>> parse(Map<String, ?> info, List<List<Object>> rows) {
        @SuppressWarnings("unchecked")
        List<String> fieldNames = (List<String>) info.get("names");
        Map<String, Integer> fieldNameIndexes = new HashMap<String, Integer>();
        for (int i = 0; i < fieldNames.size(); ++i) {
            fieldNameIndexes.put(fieldNames.get(i), i);
        }

        List<Map<String, ?>> result = new ArrayList<Map<String, ?>>(rows.size());
        for (List<Object> r : rows) {
            Map<String, Object> resultRow = new HashMap<String, Object>();
            for (Map.Entry<String, Integer> f : fieldNameIndexes.entrySet()) {
                resultRow.put(f.getKey(), r.get(f.getValue()));
            }
            result.add(resultRow);
        }
        return result;
    }
}
