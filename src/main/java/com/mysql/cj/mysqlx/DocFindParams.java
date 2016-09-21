/*
  Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.

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

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

import com.mysql.cj.api.x.Expression;
import com.mysql.cj.mysqlx.protobuf.MysqlxCrud.Projection;

public class DocFindParams extends FindParams {
    public DocFindParams(String schemaName, String collectionName) {
        super(schemaName, collectionName, false);
    }

    public DocFindParams(String schemaName, String collectionName, String criteriaString) {
        super(schemaName, collectionName, criteriaString, false);
    }

    public void setFields(Expression docProjection) {
        this.fields = Collections.singletonList(Projection.newBuilder().setSource(new ExprParser(docProjection.getExpressionString(), false).parse()).build());
    }

    @Override
    public void setFields(String... projection) {
        this.fields = new ExprParser(Arrays.stream(projection).collect(Collectors.joining(", ")), false).parseDocumentProjection();
    }
}
