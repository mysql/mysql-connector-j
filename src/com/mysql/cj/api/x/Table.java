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

import com.mysql.cj.api.x.TableStatement.DeleteStatement;
import com.mysql.cj.api.x.TableStatement.InsertStatement;
import com.mysql.cj.api.x.TableStatement.SelectStatement;
import com.mysql.cj.api.x.TableStatement.UpdateStatement;

public interface Table extends DatabaseObject {

    InsertStatement insert(String fields);

    InsertStatement insert(Object fieldsAndValues);

    SelectStatement select(String searchFields);

    UpdateStatement update();

    DeleteStatement delete();

    /**
     * Collection.as [41]
     * 
     * @param searchCondition
     * @return
     */
    // TODO not clear, spec refers to TableFindFunction, but what it does, return Table by alias? why is it needed? Or assign alias? For what purpose?
    Table as(String alias);

    /**
     * Table.count [43]
     * 
     * @return
     */
    // TODO what's that? we have a requirement but without a specification
    int count();

    /**
     * Table Index Creation [60] - not supported in v1
     */
    // void createIndex();
    // void dropIndex();
    // void getIndexes();

    /**
     * Table.alter [31] - not supported in v1
     */

    /**
     * Table.join (tables) [40] - not supported in v1
     */

    /**
     * Table.drop [53] - not supported in v1
     */

}
