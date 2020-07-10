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

import java.util.List;

/**
 * X DevAPI introduces a new, high-level database connection concept that is called Session. When working with X DevAPI it is important to understand this new
 * Session concept which is different from working with traditional low-level MySQL connections.
 * <p>
 * An application using the Session class can be run against a single MySQL server or large number of MySQL servers forming a sharding cluster with no code
 * changes.
 * <p>
 * When using literal/verbatim SQL the common API patterns are mostly the same compared to using DML and CRUD operations on Tables and Collections. Two
 * differences exist: setting the current schema and escaping names.
 * <p>
 * You cannot call {@link Session#getSchema(String)} or {@link Session#getDefaultSchema()} to obtain a {@link Schema} object against which you can
 * issue verbatin SQL statements. The Schema object does not feature a sql() function.
 * <p>
 * The sql() function is a method of the {@link Session} class. Use {@link Session#sql(String)} and the SQL command USE to change the current
 * schema
 * <p>
 * Session session = SessionFactory.getSession("root:s3kr3t@localhost");<br>
 * session.sql("USE test");
 * <p>
 * If a Session has been established using a data source file the name of the default schema can be obtained to change the current database.
 * <p>
 * Properties p = new Properties();<br>
 * p.setProperty("dataSourceFile", "/home/app_instance50/mysqlxconfig.json");<br>
 * Session session = SessionFactory.getSession(p);<br>
 * String defaultSchema = session.getDefaultSchema().getName();<br>
 * session.sql("USE ?").bind(defaultSchema).execute();<br>
 * <p>
 * A quoting function exists to escape SQL names/identifiers. StringUtils.quoteIdentifier(String, boolean) will escape the identifier given in
 * accordance to the settings of the current connection.
 * The escape function must not be used to escape values. Use the value bind syntax of {@link Session#sql(String)} instead.
 * <p>
 * // use bind syntax for values<br>
 * session.sql("DROP TABLE IF EXISTS ?").bind(name).execute();<br>
 * <br>
 * // use escape function to quote names/identifier<br>
 * var create = "CREATE TABLE ";<br>
 * create += StringUtils.quoteIdentifier(name, true);<br>
 * create += "(id INT NOT NULL PRIMARY KEY AUTO_INCREMENT");<br>
 * <br>
 * session.sql(create).execute();
 * <p>
 * Users of the CRUD API do not need to escape identifiers. This is true for working with collections and for working with relational tables.
 */
public interface Session {

    /**
     * Retrieve the list of Schema objects for which the current user has access.
     * 
     * @return list of Schema objects
     */
    List<Schema> getSchemas();

    /**
     * Retrieve the Schema corresponding to name.
     * 
     * @param schemaName
     *            name of schema to retrieve
     * @return {@link Schema}
     */
    Schema getSchema(String schemaName);

    /**
     * Retrieve the default schema name which may be configured at connect time.
     * 
     * @return default schema name
     */
    String getDefaultSchemaName();

    /**
     * Retrieve the default schema which may be configured at connect time.
     * 
     * @return default {@link Schema}
     */
    Schema getDefaultSchema();

    /**
     * Create and return a new schema with the name given by name.
     * 
     * @param schemaName
     *            name of schema to create
     * @return {@link Schema} created
     */
    Schema createSchema(String schemaName);

    /**
     * Create and return a new schema with the name given by name. If the schema already exists, a reference to it is returned.
     * 
     * @param schemaName
     *            name of schema to create
     * @param reuseExistingObject
     *            true to reuse
     * @return {@link Schema} created
     */
    Schema createSchema(String schemaName, boolean reuseExistingObject);

    /**
     * Drop the existing schema with the name given by name.
     * 
     * @param schemaName
     *            name of schema to drop
     */
    void dropSchema(String schemaName);

    /**
     * Get the URL used to create this session.
     * 
     * @return URI
     */
    String getUri();

    /**
     * Is this session open?
     * 
     * @return true if session is open
     */
    boolean isOpen();

    /**
     * Close this session.
     */
    void close();

    /**
     * Start a new transaction.
     */
    void startTransaction();

    /**
     * Commit the transaction.
     */
    void commit();

    /**
     * Rollback the transaction.
     */
    void rollback();

    /**
     * Creates a transaction savepoint with an implementation-defined generated name and returns its name, which can be used in {@link #rollbackTo(String)} or
     * {@link #releaseSavepoint(String)}. Calling this method more than once should always work. The generated name shall be unique per session.
     * 
     * @return savepoint name
     */
    String setSavepoint();

    /**
     * Creates or replaces a transaction savepoint with the given name. Calling this method more than once should always work.
     * 
     * @param name
     *            savepoint name
     * @return savepoint name
     */
    String setSavepoint(String name);

    /**
     * Rolls back the transaction to the named savepoint. This method will succeed as long as the given save point has not been already rolled back or
     * released. Rolling back to a savepoint prior to the one named will release or rollback any that came after.
     * 
     * @param name
     *            savepoint name
     */
    void rollbackTo(String name);

    /**
     * Releases the named savepoint. This method will succeed as long as the given save point has not been already rolled back or
     * released. Rolling back to a savepoint prior to the one named will release or rollback any that came after.
     * 
     * @param name
     *            savepoint name
     */
    void releaseSavepoint(String name);

    /**
     * Create a native SQL command. Placeholders are supported using the native "?" syntax.
     * 
     * @param sql
     *            native SQL statement
     * @return {@link SqlStatement}
     */
    SqlStatement sql(String sql);
}
