/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.mysqlx;

import java.util.ArrayList;
import java.util.List;

public class CreateIndexParams {
    private String indexName;
    private boolean unique;
    private List<String> docPaths = new ArrayList<>();
    private List<String> types = new ArrayList<>();
    private List<Boolean> notNulls = new ArrayList<>();

    public CreateIndexParams(String indexName, boolean unique) {
        this.indexName = indexName;
        this.unique = unique;
    }

    public void addField(String docPath, String type, boolean notNull) {
        docPaths.add(docPath);
        types.add(type);
        notNulls.add(notNull);
    }

    public String getIndexName() {
        return this.indexName;
    }

    public boolean isUnique() {
        return this.unique;
    }

    public List<String> getDocPaths() {
        return this.docPaths;
    }

    public List<String> getTypes() {
        return this.types;
    }

    public List<Boolean> getNotNulls() {
        return this.notNulls;
    }
}
