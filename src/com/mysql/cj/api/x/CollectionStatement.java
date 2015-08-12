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

public interface CollectionStatement<STMT_T, RES_T> extends Statement<STMT_T, RES_T> {

    interface AddStatement extends CollectionStatement<AddStatement, Result> {
    }

    interface RemoveStatement extends CollectionStatement<RemoveStatement, Result> {
        RemoveStatement orderBy(String sortFields);

        RemoveStatement limit(long numberOfRows);
    }

    interface FindStatement extends CollectionStatement<FindStatement, FetchedDocs> {
        FindStatement fields(String projection);

        FindStatement groupBy(String groupBy);

        FindStatement having(String having);

        FindStatement orderBy(String sortFields);

        FindStatement skip(long limitOffset);

        FindStatement limit(long numberOfRows);
    }

    interface ModifyStatement extends CollectionStatement<ModifyStatement, Result> {
        ModifyStatement sort(String sortFields);

        ModifyStatement limit(long numberOfRows);

        ModifyStatement set(String docPath, Object value);

        ModifyStatement change(String docPath, Object value);

        ModifyStatement unset(String fields);

        // TODO: should have alternative versions for different document forms? String vs JsonDoc?
        ModifyStatement merge(String document);

        ModifyStatement arrayInsert(String field, Object value);

        ModifyStatement arrayAppend(String field, Object value);

        ModifyStatement arrayDelete(String field, int position);
    }

    interface CreateCollectionIndexStatement extends CollectionStatement<CreateCollectionIndexStatement, Result> {
        CreateCollectionIndexStatement field(String docPath, String type, boolean notNull);
    }

    interface DropCollectionIndexStatement extends CollectionStatement<DropCollectionIndexStatement, Result> {
    }
}
