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

package com.mysql.cj.mysqla.result;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import com.mysql.cj.api.mysqla.result.ColumnDefinition;
import com.mysql.cj.core.result.Field;

/**
 * Protocol::ColumnDefinition41 object
 *
 */
public class MysqlaColumnDefinition implements ColumnDefinition {

    protected Field[] fields;

    /** Map column names (and all of their permutations) to column indices */
    private Map<String, Integer> columnLabelToIndex = null;

    /**
     * The above map is a case-insensitive tree-map, it can be slow, this caches lookups into that map, because the other alternative is to create new
     * object instances for every call to findColumn()....
     */
    private Map<String, Integer> columnToIndexCache = new HashMap<String, Integer>();

    /** Map of fully-specified column names to column indices */
    private Map<String, Integer> fullColumnNameToIndex = null;

    /** Map column names (and all of their permutations) to column indices */
    private Map<String, Integer> columnNameToIndex = null;

    private boolean builtIndexMapping = false;

    public MysqlaColumnDefinition() {
    }

    public MysqlaColumnDefinition(Field[] fields) {
        this.fields = fields;
    }

    public Field[] getFields() {
        return this.fields;
    }

    public void setFields(Field[] fields) {
        this.fields = fields;
    }

    @Override

    /**
     * Builds a hash between column names and their indices for fast retrieval.
     */
    public void buildIndexMapping() {
        int numFields = this.fields.length;
        this.columnLabelToIndex = new TreeMap<String, Integer>(String.CASE_INSENSITIVE_ORDER);
        this.fullColumnNameToIndex = new TreeMap<String, Integer>(String.CASE_INSENSITIVE_ORDER);
        this.columnNameToIndex = new TreeMap<String, Integer>(String.CASE_INSENSITIVE_ORDER);

        // We do this in reverse order, so that the 'first' column with a given name ends up as the final mapping in the hashtable...
        //
        // Quoting the JDBC Spec:
        //
        // "Column names used as input to getter methods are case insensitive. When a getter method is called with a column name and several columns have the
        // same name, the value of the first matching column will be returned. "
        //
        for (int i = numFields - 1; i >= 0; i--) {
            Integer index = Integer.valueOf(i);
            String columnName = this.fields[i].getOriginalName();
            String columnLabel = this.fields[i].getName();
            String fullColumnName = this.fields[i].getFullName();

            if (columnLabel != null) {
                this.columnLabelToIndex.put(columnLabel, index);
            }

            if (fullColumnName != null) {
                this.fullColumnNameToIndex.put(fullColumnName, index);
            }

            if (columnName != null) {
                this.columnNameToIndex.put(columnName, index);
            }
        }

        // set the flag to prevent rebuilding...
        this.builtIndexMapping = true;
    }

    public boolean hasBuiltIndexMapping() {
        return this.builtIndexMapping;
    }

    public Map<String, Integer> getColumnLabelToIndex() {
        return this.columnLabelToIndex;
    }

    public void setColumnLabelToIndex(Map<String, Integer> columnLabelToIndex) {
        this.columnLabelToIndex = columnLabelToIndex;
    }

    public Map<String, Integer> getFullColumnNameToIndex() {
        return this.fullColumnNameToIndex;
    }

    public void setFullColumnNameToIndex(Map<String, Integer> fullColNameToIndex) {
        this.fullColumnNameToIndex = fullColNameToIndex;
    }

    public Map<String, Integer> getColumnNameToIndex() {
        return this.columnNameToIndex;
    }

    public void setColumnNameToIndex(Map<String, Integer> colNameToIndex) {
        this.columnNameToIndex = colNameToIndex;
    }

    public Map<String, Integer> getColumnToIndexCache() {
        return this.columnToIndexCache;
    }

    public void setColumnToIndexCache(Map<String, Integer> columnToIndexCache) {
        this.columnToIndexCache = columnToIndexCache;
    }

    public void initializeFrom(ColumnDefinition columnDefinition) {
        this.fields = columnDefinition.getFields();
        this.columnLabelToIndex = columnDefinition.getColumnNameToIndex();
        this.fullColumnNameToIndex = columnDefinition.getFullColumnNameToIndex();
        this.builtIndexMapping = true;
    }

    public void exportTo(ColumnDefinition columnDefinition) {
        columnDefinition.setFields(this.fields);
        columnDefinition.setColumnNameToIndex(this.columnLabelToIndex);
        columnDefinition.setFullColumnNameToIndex(this.fullColumnNameToIndex);
    }

    public int findColumn(String columnName, boolean useColumnNamesInFindColumn) {

        Integer index;

        if (!hasBuiltIndexMapping()) {
            buildIndexMapping();
        }

        index = this.columnToIndexCache.get(columnName);

        if (index != null) {
            return index.intValue() + 1;
        }

        index = this.columnLabelToIndex.get(columnName);

        if (index == null && useColumnNamesInFindColumn) {
            index = this.columnNameToIndex.get(columnName);
        }

        if (index == null) {
            index = this.fullColumnNameToIndex.get(columnName);
        }

        if (index != null) {
            this.columnToIndexCache.put(columnName, index);

            return index.intValue() + 1;
        }

        // Try this inefficient way, now

        for (int i = 0; i < this.fields.length; i++) {
            if (this.fields[i].getName().equalsIgnoreCase(columnName)) {
                return i + 1;
            } else if (this.fields[i].getFullName().equalsIgnoreCase(columnName)) {
                return i + 1;
            }
        }

        return -1;
    }

    /**
     * Check if fields with type BLOB, MEDIUMBLOB, LONGBLOB, TEXT, MEDIUMTEXT or LONGTEXT
     * exist in this ColumnDefinition.
     * This check is used for making a decision about whether we want to force a
     * buffer row (better for rows with large fields).
     */
    public boolean hasLargeFields() {
        if (this.fields != null) {
            for (int i = 0; i < this.fields.length; i++) {
                switch (this.fields[i].getMysqlType()) {
                    case BLOB:
                    case MEDIUMBLOB:
                    case LONGBLOB:
                    case TEXT:
                    case MEDIUMTEXT:
                    case LONGTEXT:
                    case JSON:
                        return true;
                    default:
                        break;
                }
            }
        }
        return false;
    }

}
