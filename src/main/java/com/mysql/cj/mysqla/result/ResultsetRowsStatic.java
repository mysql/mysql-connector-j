/*
  Copyright (c) 2002, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.mysqla.result;

import java.util.List;

import com.mysql.cj.api.mysqla.result.ColumnDefinition;
import com.mysql.cj.api.mysqla.result.ResultsetRows;
import com.mysql.cj.api.result.Row;

/**
 * Represents an in-memory result set
 */
public class ResultsetRowsStatic extends AbstractResultsetRows implements ResultsetRows {

    private List<Row> rows;

    /**
     * Creates a new RowDataStatic object.
     * 
     * @param rows
     */
    @SuppressWarnings("unchecked")
    public ResultsetRowsStatic(List<? extends Row> rows, ColumnDefinition columnDefinition) {
        this.currentPositionInFetchedRows = -1;
        this.rows = (List<Row>) rows;
        this.metadata = columnDefinition;
    }

    @Override
    public void addRow(Row row) {
        this.rows.add(row);
    }

    @Override
    public void afterLast() {
        if (this.rows.size() > 0) {
            this.currentPositionInFetchedRows = this.rows.size();
        }
    }

    @Override
    public void beforeFirst() {
        if (this.rows.size() > 0) {
            this.currentPositionInFetchedRows = -1;
        }
    }

    @Override
    public void beforeLast() {
        if (this.rows.size() > 0) {
            this.currentPositionInFetchedRows = this.rows.size() - 2;
        }
    }

    @Override
    public Row get(int atIndex) {
        if ((atIndex < 0) || (atIndex >= this.rows.size())) {
            return null;
        }

        return this.rows.get(atIndex).setMetadata(this.metadata);
    }

    @Override
    public int getPosition() {
        return this.currentPositionInFetchedRows;
    }

    @Override
    public boolean hasNext() {
        boolean hasMore = (this.currentPositionInFetchedRows + 1) < this.rows.size();

        return hasMore;
    }

    @Override
    public boolean isAfterLast() {
        return this.currentPositionInFetchedRows >= this.rows.size() && this.rows.size() != 0;
    }

    @Override
    public boolean isBeforeFirst() {
        return this.currentPositionInFetchedRows == -1 && this.rows.size() != 0;
    }

    @Override
    public boolean isDynamic() {
        return false;
    }

    @Override
    public boolean isEmpty() {
        return this.rows.size() == 0;
    }

    @Override
    public boolean isFirst() {
        return this.currentPositionInFetchedRows == 0;
    }

    @Override
    public boolean isLast() {
        // You can never be on the 'last' row of an empty result set
        if (this.rows.size() == 0) {
            return false;
        }

        return (this.currentPositionInFetchedRows == (this.rows.size() - 1));
    }

    @Override
    public void moveRowRelative(int rowsToMove) {
        if (this.rows.size() > 0) {
            this.currentPositionInFetchedRows += rowsToMove;
            if (this.currentPositionInFetchedRows < -1) {
                beforeFirst();
            } else if (this.currentPositionInFetchedRows > this.rows.size()) {
                afterLast();
            }
        }
    }

    @Override
    public Row next() {
        this.currentPositionInFetchedRows++;

        if (this.currentPositionInFetchedRows > this.rows.size()) {
            afterLast();
        } else if (this.currentPositionInFetchedRows < this.rows.size()) {
            Row row = this.rows.get(this.currentPositionInFetchedRows);

            return row.setMetadata(this.metadata);
        }

        return null;
    }

    @Override
    public void remove() {
        this.rows.remove(getPosition());
    }

    @Override
    public void setCurrentRow(int newIndex) {
        this.currentPositionInFetchedRows = newIndex;
    }

    @Override
    public int size() {
        return this.rows.size();
    }

    @Override
    public boolean wasEmpty() {
        return (this.rows != null && this.rows.size() == 0);
    }
}
