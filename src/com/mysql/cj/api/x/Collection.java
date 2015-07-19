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

package com.mysql.cj.api.x;

import java.util.Map;

import com.mysql.cj.api.x.CollectionStatement.AddStatement;
import com.mysql.cj.api.x.CollectionStatement.FindStatement;
import com.mysql.cj.api.x.CollectionStatement.ModifyStatement;
import com.mysql.cj.api.x.CollectionStatement.RemoveStatement;
import com.mysql.cj.x.json.JsonDoc;

public interface Collection extends DatabaseObject {

    /**
     * Add a document in the form of a Map.
     */
    AddStatement add(Map<String, ?> doc);

    /**
     * Add a document in the form of a JSON string.
     */
    AddStatement add(String jsonString);

    /**
     * Add a document in the form of a DbDoc.
     */
    AddStatement add(DbDoc document);

    /**
     * TODO: Temporary avoid compiler issues. JsonDoc implements DbDoc and Map and compiler can't decide which version to dispatch.
     */
    default AddStatement add(JsonDoc document) {
        return add((DbDoc) document);
    }

    FindStatement find();

    FindStatement find(String searchCondition);

    ModifyStatement modify();

    ModifyStatement modify(String searchCondition);

    RemoveStatement remove();

    RemoveStatement remove(String searchCondition);

    /**
     * Collection.drop [53]
     */
    void drop();

    /**
     * Collection.as [41]
     * 
     * @param searchCondition
     * @return
     */
    // TODO not clear, spec refers to CollectionFindFunction, but what it does, return Collection by alias? why is it needed? Or assign alias? For what purpose?
    Collection as(String alias);

    /**
     * Collection.count [43]
     * 
     * @return
     */
    // TODO what's that? we have a requirement but without a specification
    long count();

    /**
     * Method for creating new Document object
     * 
     * @return
     */
    DbDoc newDoc();

    /**
     * Collection Index Creation [59]
     */
    // TODO spec is in progress
    // void createIndex();
    // void dropIndex();
    // void getIndexes();

}
