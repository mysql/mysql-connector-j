/*
  Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.api.x;

/**
 * CreateTableFunction
 * ::= '.createTable(' StringLiteral (',' Boolean)? ')'
 * ( '.addColumn(' ColumnDef ')' )*
 * ( '.addPrimaryKey(' StringLiteral (',' StringLiteral)* ')' )?
 * ( '.addIndex(' StringLiteral (',' StringLiteral)+ ')' )*
 * ( '.addUniqueIndex(' StringLiteral (',' StringLiteral)+ ')' )*
 * ( '.addForeignKey(' StringLiteral ',' FkSpec ')' )*
 * ( '.setInitialAutoIncrement(' Number ')' )?
 * ( '.setDefaultCharset(' StringLiteral ')' )?
 * ( '.setDefaultCollation(' StringLiteral ')' )?
 * ( '.setComment(' StringLiteral ')' )?
 * ( '.temporary()' )?
 * ( '.as(' (StringLiteral | SelectStatement) ')' )?
 * '.execute()'
 */
public interface CreateTableStatement {

    Table execute();

    /**
     * This interface is returned by {@link Schema#createTable(String)} to split between
     * two variants of syntax, the full one and "CREATE TABLE LIKE..." one.
     */
    public interface CreateTableSplitStatement extends CreateTableStatement {

        CreateTableFullStatement addColumn(ColumnDefinition<?> colDef);

        CreateTableFullStatement addPrimaryKey(String... pk);

        CreateTableFullStatement addIndex(String name, String... column);

        CreateTableFullStatement addUniqueIndex(String name, String... column);

        CreateTableFullStatement addForeignKey(String fkName, ForeignKeyDefinition fkSpec);

        CreateTableFullStatement setInitialAutoIncrement(Number val);

        CreateTableFullStatement setDefaultCharset(String charsetName);

        CreateTableFullStatement setDefaultCollation(String collationName);

        CreateTableFullStatement setComment(String comment);

        CreateTableFullStatement temporary();

        CreateTableFullStatement as(String select);

        CreateTableFullStatement as(SelectStatement select);

        CreateTableLikeStatement like(String templateTableName);

    }

    public interface CreateTableFullStatement extends CreateTableStatement {

        CreateTableFullStatement addColumn(ColumnDefinition<?> colDef);

        CreateTableFullStatement addPrimaryKey(String... pk);

        CreateTableFullStatement addIndex(String name, String... column);

        CreateTableFullStatement addUniqueIndex(String name, String... column);

        CreateTableFullStatement addForeignKey(String fkName, ForeignKeyDefinition fkSpec);

        CreateTableFullStatement setInitialAutoIncrement(Number val);

        CreateTableFullStatement setDefaultCharset(String charsetName);

        CreateTableFullStatement setDefaultCollation(String collationName);

        CreateTableFullStatement setComment(String comment);

        CreateTableFullStatement temporary();

        CreateTableFullStatement as(String select);

        CreateTableFullStatement as(SelectStatement select);
    }

    /**
     * CreateTableFunction
     * ::= '.createTable(' StringLiteral (',' Boolean)? ')'
     * '.like(' StringLiteral ')'
     * '.execute()'
     */
    public interface CreateTableLikeStatement extends CreateTableStatement {
    }
}
