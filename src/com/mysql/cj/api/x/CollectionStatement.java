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

import java.util.concurrent.Future;

public interface CollectionStatement<STMT_T, RES_T> extends Statement<STMT_T> {

    RES_T execute();

    default Future<RES_T> executeAsync() {
        throw new NullPointerException("TODO: ASYNC NOT SUPPORTED IN THIS VERSION");
    }

    interface AddStatement extends CollectionStatement<AddStatement, Result> {
    }

    interface RemoveStatement extends CollectionStatement<RemoveStatement, Result> {
        RemoveStatement orderBy(String sortFields);

        RemoveStatement limit(long numberOfRows);
    }

    interface FindStatement extends CollectionStatement<FindStatement, FetchedDocs> {
        FindStatement fields(String searchFields);

        FindStatement groupBy(String searchFields);

        FindStatement having(String searchCondition);

        FindStatement orderBy(String sortFields);

        FindStatement skip(long limitOffset);

        FindStatement limit(long numberOfRows);

    }

    interface ModifyStatement extends CollectionStatement<ModifyStatement, Result> {
        ModifyStatement sort(String sortFields);

        ModifyStatement limit(long numberOfRows);

        // TODO: need separate versions for ALL our types?
        ModifyStatement set(String docPath, String value);

        // TODO: need separate versions for ALL our types?
        ModifyStatement change(String docPath, String value);

        ModifyStatement unset(String fields);

        ModifyStatement merge(String document);

        // TODO: should have alternative versions for different document forms?
        ModifyStatement arraySplice(String field, int start, int end, String document);

        // TODO: should have alternative versions for different document forms?
        ModifyStatement arrayInsert(String field, int position, String document);

        // TODO: should have alternative versions for different document forms?
        ModifyStatement arrayAppend(String field, String document);

        ModifyStatement arrayDelete(String field, int position);

        // TODO: should have alternative versions for different document forms?
        ModifyStatement arrayRemove(String field, String document);

        // ArrayModifyStatement array();

        // interface ArrayModifyStatement extends ModifyStatement {

        //     ModifyStatement splice(String field, int number1, int number2, String document);

        //     ModifyStatement insert(String field, int number, String document);

        //     ModifyStatement append(String field, String document);

        //     ModifyStatement delete(String field, int number);

        //     ModifyStatement remove(String field, String document);

        // }
    }

}
