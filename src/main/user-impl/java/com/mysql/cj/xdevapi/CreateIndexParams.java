/*
 * Copyright (c) 2015, 2020, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.mysql.cj.xdevapi;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import com.mysql.cj.Messages;
import com.mysql.cj.exceptions.AssertionFailedException;

/**
 * Internally-used object passing index creation parameters to XMessageBuilder.
 */
public class CreateIndexParams {
    public static final String INDEX = "INDEX";
    public static final String SPATIAL = "SPATIAL";
    public static final String GEOJSON = "GEOJSON";

    private String indexName;
    /** One of INDEX or SPATIAL. Default is INDEX and may be omitted. **/
    private String indexType = null;
    private List<IndexField> fields = new ArrayList<>();

    /**
     * Constructor.
     * 
     * @param indexName
     *            index name
     * @param indexDefinition
     *            special JSON document containing index definition; see {@link Collection#createIndex(String, DbDoc)} description
     */
    public CreateIndexParams(String indexName, DbDoc indexDefinition) {
        init(indexName, indexDefinition);
    }

    /**
     * Constructor.
     * 
     * @param indexName
     *            index name
     * @param jsonIndexDefinition
     *            special JSON document containing index definition; see {@link Collection#createIndex(String, String)} description
     */
    public CreateIndexParams(String indexName, String jsonIndexDefinition) {
        if (jsonIndexDefinition == null || jsonIndexDefinition.trim().length() == 0) {
            throw new XDevAPIError(Messages.getString("CreateIndexParams.0", new String[] { "jsonIndexDefinition" }));
        }
        try {
            init(indexName, JsonParser.parseDoc(new StringReader(jsonIndexDefinition)));
        } catch (IOException ex) {
            throw AssertionFailedException.shouldNotHappen(ex);
        }
    }

    private void init(String idxName, DbDoc indexDefinition) {
        if (idxName == null || idxName.trim().length() == 0) {
            throw new XDevAPIError(Messages.getString("CreateIndexParams.0", new String[] { "indexName" }));
        }
        if (indexDefinition == null) {
            throw new XDevAPIError(Messages.getString("CreateIndexParams.0", new String[] { "indexDefinition" }));
        }

        this.indexName = idxName;

        for (String key : indexDefinition.keySet()) {
            if (!"type".equals(key) && !"fields".equals(key)) {
                throw new XDevAPIError("The '" + key + "' field is not allowed in indexDefinition.");
            }
        }

        JsonValue val = indexDefinition.get("type");
        if (val != null) {
            if (val instanceof JsonString) {
                String type = ((JsonString) val).getString();
                if (INDEX.equalsIgnoreCase(type) || SPATIAL.equalsIgnoreCase(type)) {
                    this.indexType = type;
                } else {
                    throw new XDevAPIError("Wrong index type '" + type + "'. Must be 'INDEX' or 'SPATIAL'.");
                }
            } else {
                throw new XDevAPIError("Index type must be a string.");
            }
        }

        val = indexDefinition.get("fields");
        if (val != null) {
            if (val instanceof JsonArray) {
                for (JsonValue field : (JsonArray) val) {
                    if (field instanceof DbDoc) {
                        this.fields.add(new IndexField((DbDoc) field));
                    } else {
                        throw new XDevAPIError("Index field definition must be a JSON document.");
                    }
                }

            } else {
                throw new XDevAPIError("Index definition 'fields' member must be an array of index fields.");
            }
        } else {
            throw new XDevAPIError("Index definition does not contain fields.");
        }
    }

    /**
     * Get index name.
     * 
     * @return index name
     */
    public String getIndexName() {
        return this.indexName;
    }

    /**
     * Get index type.
     * 
     * @return index type
     */
    public String getIndexType() {
        return this.indexType;
    }

    /**
     * Get index fields.
     * 
     * @return List of {@link IndexField} objects
     */
    public List<IndexField> getFields() {
        return this.fields;
    }

    /**
     * Internally used object parsed from indexDefinition; see {@link Collection#createIndex(String, DbDoc)} description.
     */
    public static class IndexField {
        private static final String FIELD = "field";
        private static final String TYPE = "type";
        private static final String REQUIRED = "required";
        private static final String OPTIONS = "options";
        private static final String SRID = "srid";
        private static final String ARRAY = "array";

        /** The full document path to the document member or field to be indexed **/
        private String field;

        /**
         * One of the supported SQL column types to map the field into.
         * For numeric types, the optional UNSIGNED keyword may follow. For
         * the TEXT type, the length to consider for indexing may be added.
         **/
        private String type;

        /** (optional) true if the field is required to exist in the document. defaults to false, except for GEOJSON where it defaults to true **/
        private Boolean required = Boolean.FALSE; // Must be sent to server until MySQL 8.0.17.

        /** (optional) special option flags for use when decoding GEOJSON data **/
        private Integer options = null;

        /** (optional) srid value for use when decoding GEOJSON data **/
        private Integer srid = null;

        /** (optional) true if the field is an array **/
        private Boolean array;

        /**
         * Constructor.
         * 
         * @param indexField
         *            a special JSON document, part of indexDefinition document, consisting of the following fields:
         *            <ul>
         *            <li>field: string, the full document path to the document member or field to be indexed</li>
         *            <li>type: string, one of the supported SQL column types to map the field into. For numeric types, the optional UNSIGNED
         *            keyword may follow. For the TEXT type, the length to consider for indexing may be added. Type descriptions are case insensitive.</li>
         *            <li>required: bool, (optional) true if the field is required to exist in the document. Defaults to false, except for GEOJSON where it
         *            defaults to true</li>
         *            <li>options: int, (optional) special option flags for use when decoding GEOJSON data</li>
         *            <li>srid: int, (optional) srid value for use when decoding GEOJSON data</li>
         *            <li>array: bool, (optional) true if the field is an array</li>
         *            </ul>
         */
        public IndexField(DbDoc indexField) {
            for (String key : indexField.keySet()) {
                if (!TYPE.equals(key) && !FIELD.equals(key) && !REQUIRED.equals(key) && !OPTIONS.equals(key) && !SRID.equals(key) && !ARRAY.equals(key)) {
                    throw new XDevAPIError("The '" + key + "' field is not allowed in indexField.");
                }
            }

            JsonValue val = indexField.get(FIELD);
            if (val != null) {
                if (val instanceof JsonString) {
                    this.field = ((JsonString) val).getString();
                } else {
                    throw new XDevAPIError("Index field 'field' member must be a string.");
                }
            } else {
                throw new XDevAPIError("Index field definition has no document path.");
            }

            val = indexField.get(TYPE);
            if (val != null) {
                if (val instanceof JsonString) {
                    this.type = ((JsonString) val).getString();
                    // TODO pure "TEXT" is not allowed as a type, server requires the length specification
                    // we're waiting for clarification about whether we set some default on client side in that case, eg.:
                    //    if ("TEXT".equals(this.type)) {this.type = "TEXT(64)";}
                    // or we do nothing and user has to specify TEXT(n) always
                } else {
                    throw new XDevAPIError("Index type must be a string.");
                }
            } else {
                throw new XDevAPIError("Index field definition has no field type.");
            }

            val = indexField.get(REQUIRED);
            if (val != null) {
                if (val instanceof JsonLiteral && !JsonLiteral.NULL.equals(val)) {
                    this.required = Boolean.valueOf(((JsonLiteral) val).value);
                } else {
                    throw new XDevAPIError("Index field 'required' member must be boolean.");
                }
            } else if (GEOJSON.equalsIgnoreCase(this.type)) {
                this.required = Boolean.TRUE;
            }

            val = indexField.get(OPTIONS);
            if (val != null) {
                if (GEOJSON.equalsIgnoreCase(this.type)) {
                    if (val instanceof JsonNumber) {
                        this.options = ((JsonNumber) val).getInteger();
                    } else {
                        throw new XDevAPIError("Index field 'options' member must be integer.");
                    }
                } else {
                    throw new XDevAPIError("Index field 'options' member should not be used for field types other than GEOJSON.");
                }
            }

            val = indexField.get(SRID);
            if (val != null) {
                if (GEOJSON.equalsIgnoreCase(this.type)) {
                    if (val instanceof JsonNumber) {
                        this.srid = ((JsonNumber) val).getInteger();
                    } else {
                        throw new XDevAPIError("Index field 'srid' member must be integer.");
                    }
                } else {
                    throw new XDevAPIError("Index field 'srid' member should not be used for field types other than GEOJSON.");
                }
            }

            val = indexField.get(ARRAY);
            if (val != null) {
                if (val instanceof JsonLiteral && !JsonLiteral.NULL.equals(val)) {
                    this.array = Boolean.valueOf(((JsonLiteral) val).value);
                } else {
                    throw new XDevAPIError("Index field 'array' member must be boolean.");
                }
            }
        }

        /**
         * Get the full document path to the document member or field to be indexed.
         * 
         * @return field string
         */
        public String getField() {
            return this.field;
        }

        /**
         * Get column type.
         * 
         * @return column type
         */
        public String getType() {
            return this.type;
        }

        /**
         * Is the field required to exist in the document?
         * 
         * @return true if required
         */
        public Boolean isRequired() {
            return this.required;
        }

        /**
         * Get options for decoding GEOJSON data.
         * 
         * @return options
         */
        public Integer getOptions() {
            return this.options;
        }

        /**
         * Get srid for decoding GEOJSON data.
         * 
         * @return srid
         */
        public Integer getSrid() {
            return this.srid;
        }

        /**
         * Is the field an array?
         * 
         * @return true if the field is an array
         */
        public Boolean isArray() {
            return this.array;
        }
    }
}
