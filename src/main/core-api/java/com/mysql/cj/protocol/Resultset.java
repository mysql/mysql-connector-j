/*
 * Copyright (c) 2016, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package com.mysql.cj.protocol;

/**
 * Represents protocol specific result set,
 * eg., for native protocol, a ProtocolText::Resultset or ProtocolBinary::Resultset entity.
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
            this.value = jdbcRsConcur;
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
     *
     * @return true if result set contains rows
     */
    boolean hasRows();

    ResultsetRows getRows();

    /**
     * Set metadata of this Resultset to ResultsetRows it contains.
     */
    void initRowsWithMetadata();

    /**
     * The id (used when profiling) to identify us
     *
     * @return result id
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
     *
     * @return the next Resultset
     */
    Resultset getNextResultset();

    /**
     * Clears the reference to the next result set in a multi-result set "chain".
     */
    void clearNextResultset();

    /**
     * Returns the update count for this result set (if one exists), otherwise
     * -1.
     *
     * @return return the update count for this result set (if one exists), otherwise
     *         -1.
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
     *
     * @return the server informational message
     */
    String getServerInfo();

}
