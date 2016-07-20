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

package com.mysql.cj.api.mysqla.result;

/**
 * Represents ProtocolText::Resultset or ProtocolBinary::Resultset entity.
 * 
 * See:
 * http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::Resultset
 * http://dev.mysql.com/doc/internals/en/binary-protocol-resultset.html
 *
 */
public interface Resultset extends ProtocolEntity {

    public enum Concurrency {
        /**
         * The constant indicating the concurrency mode for a
         * <code>Resultset</code> object that may NOT be updated.
         */
        READ_ONLY(java.sql.ResultSet.CONCUR_READ_ONLY),

        /**
         * The constant indicating the concurrency mode for a
         * <code>Resultset</code> object that may be updated.
         */
        UPDATABLE(java.sql.ResultSet.CONCUR_UPDATABLE);

        private int value;

        private Concurrency(int jdbcRsConcur) {
            value = jdbcRsConcur;
        }

        public int getIntValue() {
            return this.value;
        }

        public static Concurrency fromValue(int concurMode, Concurrency backupValue) {
            for (Concurrency c : values()) {
                if (c.getIntValue() == concurMode) {
                    return c;
                }
            }
            return backupValue;
        }

    }

    public enum Type {
        /**
         * The constant indicating the type for a <code>Resultset</code> object
         * whose cursor may move only forward.
         */
        FORWARD_ONLY(java.sql.ResultSet.TYPE_FORWARD_ONLY),

        /**
         * The constant indicating the type for a <code>Resultset</code> object
         * that is scrollable but generally not sensitive to changes to the data
         * that underlies the <code>Resultset</code>.
         */
        SCROLL_INSENSITIVE(java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE),

        /**
         * The constant indicating the type for a <code>Resultset</code> object
         * that is scrollable and generally sensitive to changes to the data
         * that underlies the <code>Resultset</code>.
         */
        SCROLL_SENSITIVE(java.sql.ResultSet.TYPE_SCROLL_SENSITIVE);

        private int value;

        private Type(int jdbcRsType) {
            this.value = jdbcRsType;
        }

        public int getIntValue() {
            return this.value;
        }

        public static Type fromValue(int rsType, Type backupValue) {
            for (Type t : values()) {
                if (t.getIntValue() == rsType) {
                    return t;
                }
            }
            return backupValue;
        }
    }

    /**
     * Sometimes the driver doesn't have metadata before consuming the result set rows (because it's cached),
     * or need to coerce the metadata returned by queries into that required by the particular specification
     * (eg metadata returned by metadata queries into that required by the JDBC specification).
     * So it can call this to set it after the fact.
     * 
     * @param metadata
     *            field-level metadata for the result set
     */
    void setColumnDefinition(ColumnDefinition metadata);

    ColumnDefinition getColumnDefinition();

    /**
     * Does the result set contain rows, or is it the result of a DDL or DML statement?
     */
    boolean hasRows();

    /**
     * Set metadata of this Resultset to ResultsetRows it contains.
     */
    void initRowsWithMetadata();

    /**
     * The id (used when profiling) to identify us
     */
    int getResultId();

    /**
     * @param nextResultset
     *            Sets the next result set in the result set chain for multiple result sets.
     */
    void setNextResultset(Resultset nextResultset);

    /**
     * Returns the next ResultSet in a multi-resultset "chain", if any,
     * null if none exists.
     */
    Resultset getNextResultset();

    /**
     * Clears the reference to the next result set in a multi-result set
     * "chain".
     */
    void clearNextResultset();

    /**
     * Returns the update count for this result set (if one exists), otherwise
     * -1.
     * 
     * @ return the update count for this result set (if one exists), otherwise
     * -1.
     */
    long getUpdateCount();

    /**
     * Returns the AUTO_INCREMENT value for the DDL/DML statement which created
     * this result set.
     * 
     * @return the AUTO_INCREMENT value for the DDL/DML statement which created
     *         this result set.
     */
    long getUpdateID();

    /**
     * Returns the server informational message returned from a DDL or DML
     * statement (if any), or null if none.
     */
    String getServerInfo();

}
