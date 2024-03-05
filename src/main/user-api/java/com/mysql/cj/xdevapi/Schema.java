/*
 * Copyright (c) 2015, 2024, Oracle and/or its affiliates.
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

package com.mysql.cj.xdevapi;

import java.util.List;

/**
 * A client-side representation of a database schema. Provides access to the schema contents.
 */
public interface Schema extends DatabaseObject {

    /* Browse functions */
    /**
     * Retrieve the set of collections existing in this schema.
     *
     * @return list of {@link Collection} objects
     */
    List<Collection> getCollections();

    /**
     * Retrieve the set of collections existing in this schema and matching the given pattern.
     *
     * @param pattern
     *            match pattern
     * @return list of {@link Collection} objects
     */
    List<Collection> getCollections(String pattern);

    /**
     * Retrieve the set of tables existing in this schema.
     *
     * @return list of {@link Table} objects
     */
    List<Table> getTables();

    /**
     * Retrieve the set of tables existing in this schema and matching the given pattern.
     *
     * @param pattern
     *            match pattern
     * @return list of {@link Table} objects
     */
    List<Table> getTables(String pattern);

    /* Other functions */

    /**
     * Retrieve a reference to the named collection.
     *
     * @param name
     *            collection name
     * @return {@link Collection}
     */
    Collection getCollection(String name);

    /**
     * Retrieve a reference to the named collection hinting that an exception should be thrown if the collection is not known to the server.
     *
     * @param name
     *            collection name
     * @param requireExists
     *            true if required to exist
     * @return {@link Collection}
     */
    Collection getCollection(String name, boolean requireExists);

    /**
     * Retrieve a reference to the named collection using the table API.
     *
     * @param name
     *            collection name
     * @return {@link Table}
     */
    Table getCollectionAsTable(String name);

    /**
     * Retrieve a reference to the named table.
     *
     * @param name
     *            table name
     * @return {@link Table}
     */
    Table getTable(String name);

    /**
     * Retrieve a reference to the named table hinting that an exception should be thrown if the collection is not known to the server.
     *
     * @param tableName
     *            table name
     * @param requireExists
     *            true if required to exist
     * @return {@link Table}
     */
    Table getTable(String tableName, boolean requireExists);

    /* Create functions */

    /**
     * Create a new collection.
     *
     * @param name
     *            collection name
     * @return {@link Collection}
     */
    Collection createCollection(String name);

    /**
     * Create a new collection if it does not already exist on the server.
     *
     * @param name
     *            collection name
     * @param reuseExisting
     *            true if allowed to reuse
     * @return {@link Collection}
     */
    Collection createCollection(String name, boolean reuseExisting);

    /**
     * Create a new collection.
     *
     * @param collectionName
     *            collection name
     * @param options
     *            reuseExisting, validation level and JSON schema options
     * @return {@link Collection}
     */
    Collection createCollection(String collectionName, CreateCollectionOptions options);

    /**
     * Modify the schema validation of a collection.
     *
     * @param collectionName
     *            collection name
     * @param options
     *            validation level and JSON schema options
     */
    void modifyCollection(String collectionName, ModifyCollectionOptions options);

    /**
     * Drop the collection from this schema.
     *
     * @param collectionName
     *            name of collection to drop
     */
    void dropCollection(String collectionName);

    /**
     * Defines options to be passed to {@link Schema#createCollection(String, CreateCollectionOptions)}.
     * <p>
     * Allowed options are:
     * <ul>
     * <li>reuseExisting flag - similar to IF NOT EXISTS for CREATE TABLE
     * <li>{@link Validation} object
     * </ul>
     *
     * Examples:
     *
     * <pre>
     * schema.createCollection(collName,
     *         new CreateCollectionOptions().setReuseExisting(false)
     *                 .setValidation(new Validation().setLevel(ValidationLevel.STRICT)
     *                         .setSchema("{\"id\": \"http://json-schema.org/idx\", \"$schema\": \"http://json-schema.org/draft-06/schema#\","
     *                                 + "\"type\": \"object\", \"properties\": {\"index\": {\"type\": \"number\"}},\"required\": [\"index\"]}")));
     * </pre>
     *
     * <pre>
     * schema.createCollection(collName, new CreateCollectionOptions().setReuseExisting(false).setValidation(new Validation().setLevel(ValidationLevel.OFF)));
     * </pre>
     *
     * <pre>
     * schema.createCollection(collName,
     *         new CreateCollectionOptions().setReuseExisting(true);
     * </pre>
     */
    public class CreateCollectionOptions {

        private Boolean reuseExisting = null;
        private Validation validation = null;

        public CreateCollectionOptions setReuseExisting(boolean reuse) {
            this.reuseExisting = reuse;
            return this;
        }

        public Boolean getReuseExisting() {
            return this.reuseExisting;
        }

        public CreateCollectionOptions setValidation(Validation validation) {
            this.validation = validation;
            return this;
        }

        public Validation getValidation() {
            return this.validation;
        }

    }

    /**
     * Defines options to be passed to {@link Schema#modifyCollection(String, ModifyCollectionOptions)}. Options are defined by {@link Validation} object.
     * <p>
     * Example:
     * </p>
     *
     * <pre>
     * schema.modifyCollection(collName1, new ModifyCollectionOptions().setValidation(new Validation().setLevel(ValidationLevel.OFF)));
     * </pre>
     */
    public class ModifyCollectionOptions {

        private Validation validation = null;

        public ModifyCollectionOptions setValidation(Validation validation) {
            this.validation = validation;
            return this;
        }

        public Validation getValidation() {
            return this.validation;
        }

    }

    /**
     * Validation options to be passed to {@link Schema#createCollection(String, CreateCollectionOptions)} or
     * {@link Schema#modifyCollection(String, ModifyCollectionOptions)}.
     * <p>
     * Allowed options are:
     * <ul>
     * <li>schema - JSON schema as a String
     * <li>level - {@link ValidationLevel}
     * </ul>
     */
    public static class Validation {

        /**
         * Defines how validation options are applied.
         * <ul>
         * <li>STRICT - enable JSON schema validation for documents in the collection.
         * <li>OFF - disable JSON schema validation.
         * </ul>
         */
        public static enum ValidationLevel {
            STRICT, OFF
        }

        private ValidationLevel level = null;
        private String schema = null;

        public Validation setLevel(ValidationLevel level) {
            this.level = level;
            return this;
        }

        public ValidationLevel getLevel() {
            return this.level;
        }

        public Validation setSchema(String schema) {
            this.schema = schema;
            return this;
        }

        public String getSchema() {
            return this.schema;
        }

    }

}
