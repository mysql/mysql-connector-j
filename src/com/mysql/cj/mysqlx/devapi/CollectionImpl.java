/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.mysqlx.devapi;

import java.util.Map;

import com.mysql.cj.api.x.Collection;
import com.mysql.cj.api.x.DbDoc;
import com.mysql.cj.api.x.Schema;
import com.mysql.cj.api.x.Session;
import com.mysql.cj.api.x.CollectionStatement.AddStatement;
import com.mysql.cj.api.x.CollectionStatement.FindStatement;
import com.mysql.cj.api.x.CollectionStatement.ModifyStatement;
import com.mysql.cj.api.x.CollectionStatement.RemoveStatement;

public class CollectionImpl implements Collection {
    private Session session;
    private String name;

    public Session getSession() {
        throw new NullPointerException("TODO:");
    }

    public Schema getSchema() {
        throw new NullPointerException("TODO:");
    }

    public String getName() {
        throw new NullPointerException("TODO:");
    }

    public DbObjectStatus existsInDatabase() {
        throw new NullPointerException("TODO:");
    }

    public AddStatement add(Map<String, ?> doc) {
        throw new NullPointerException("TODO:");
    }

    public AddStatement add(String jsonString) {
        throw new NullPointerException("TODO:");
    }

    public AddStatement add(DbDoc document) {
        throw new NullPointerException("TODO:");
    }

    public FindStatement find(String searchCondition) {
        throw new NullPointerException("TODO:");
    }

    public ModifyStatement modify(String searchCondition) {
        throw new NullPointerException("TODO:");
    }

    public RemoveStatement remove(String searchCondition) {
        throw new NullPointerException("TODO:");
    }

    public void drop() {
        throw new NullPointerException("TODO:");
    }

    public Collection as(String alias) {
        throw new NullPointerException("TODO:");
    }

    public int count() {
        throw new NullPointerException("TODO:");
    }

    public DbDoc newDoc() {
        throw new NullPointerException("TODO:");
    }
}
