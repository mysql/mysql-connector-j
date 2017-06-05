/*
  Copyright (c) 2017, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.xdevapi;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.mysql.cj.api.xdevapi.PersistenceHandler;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.core.util.Util;

/**
 * A {@link PersistenceHandler} implementation that provides persistent storage using two configurations files: a read-only system-wide configuration file
 * ("/etc/mysql/sessions.json" or "%PROGRAMDATA%\mysql\sessions.json" by default) and a user-wide read-write configuration file ("~/.mysql/sessions.json" or
 * "%APPDATA%\mysql\sessions.json" by default).
 * 
 * <p>
 * User-wide configurations take precedence over system-wide ones, overriding any existing duplicates.
 */
public class DefaultPersistenceHandler implements PersistenceHandler {
    private final Path systemWidePath;
    private final Path userWidePath;

    /**
     * Constructs an instance that manages two configuration files named 'sessions.json', each one in the default location.
     */
    public DefaultPersistenceHandler() {
        if (Util.isRunningOnWindows()) { // Paths for Windows.
            this.systemWidePath = Paths.get(System.getenv("PROGRAMDATA"), "MySQL", "sessions.json");
            this.userWidePath = Paths.get(System.getenv("APPDATA"), "MySQL", "sessions.json");
        } else { // Paths for Unix-like Systems.
            this.systemWidePath = Paths.get("/etc/mysql", "sessions.json");
            this.userWidePath = Paths.get(System.getProperty("user.home"), ".mysql", "sessions.json");
        }
    }

    /**
     * Constructs an instance that manages the two given configuration files.
     * 
     * @param systemWidePath
     *            the system-wide, read-only, configuration file.
     * @param userWidePath
     *            the user-wide, read-write, configuration file.
     */
    public DefaultPersistenceHandler(String systemWidePath, String userWidePath) {
        this.systemWidePath = Paths.get(systemWidePath);
        this.userWidePath = Paths.get(userWidePath);
    }

    /**
     * Saves the given JSON object under the attribute with the given name, in the user-wide persistent storage.
     * 
     * @param name
     *            the attribute name.
     * @param data
     *            the data to associate to the given name.
     * @see com.mysql.cj.api.xdevapi.PersistenceHandler#save(java.lang.String, com.mysql.cj.xdevapi.DbDoc)
     */
    @Override
    public void save(String name, DbDoc data) {
        DbDoc persistentData = loadPersistentData(this.userWidePath);
        persistentData.put(name, data);
        saveUserWidePersistentData(persistentData);
    }

    /**
     * Retrieves the JSON object associated with the given name from the persistent storage. When the same name exists in both configuration files only the
     * user-wide data is returned.
     * 
     * @param name
     *            the attribute name.
     * @return
     *         the JSON object associated to the given name.
     * @see com.mysql.cj.api.xdevapi.PersistenceHandler#load(java.lang.String)
     */
    @Override
    public DbDoc load(String name) {
        DbDoc persistentData = loadPersistentData();
        if (!persistentData.containsKey(name)) {
            throw ExceptionFactory.createException("Key '" + name + "' was not found in the persistent data.");
        }
        return (DbDoc) persistentData.get(name);
    }

    /**
     * Deletes the attribute with the given name, from the persistent storage. Only the user-wide data can be deleted. If the named attribute exists only in the
     * system-wide configurations file then this method returns false indicating a failed operation.
     * 
     * @param name
     *            the attribute name.
     * @return
     *         true if the delete succeeds, false otherwise.
     * @see com.mysql.cj.api.xdevapi.PersistenceHandler#delete(java.lang.String)
     */
    @Override
    public boolean delete(String name) {
        DbDoc persistentData = loadPersistentData(this.userWidePath);
        boolean deleted = persistentData.remove(name) != null;
        saveUserWidePersistentData(persistentData);
        return deleted;
    }

    /**
     * Returns a list of all attribute names existing in the persistent storage. The returned list includes the names from both configurations files.
     * 
     * @return
     *         a list of session configuration names.
     * @see com.mysql.cj.api.xdevapi.PersistenceHandler#list()
     */
    @Override
    public List<String> list() {
        DbDoc persistentData = loadPersistentData();
        List<String> keys = Collections.unmodifiableList(persistentData.keySet().stream().collect(Collectors.toList()));
        return keys;
    }

    /**
     * Checks if the given name exists in the persistent storage, either in the user-wide or system-wide configurations files.
     * 
     * @param name
     *            the attribute name.
     * @return
     *         true if the attribute exists, false otherwise.
     * 
     * @see com.mysql.cj.api.xdevapi.PersistenceHandler#exists(java.lang.String)
     */
    @Override
    public boolean exists(String name) {
        DbDoc persistentData = loadPersistentData();
        return persistentData.containsKey(name);
    }

    /**
     * Loads and merges both configurations files. On duplicate attributes user-wide data takes precedence over system-wide data.
     * 
     * @return the JSON object that results from the merging of the two configurations files content.
     */
    private synchronized DbDoc loadPersistentData() {
        DbDoc sysPersistentData = loadPersistentData(this.systemWidePath);
        DbDoc userPersistentData = loadPersistentData(this.userWidePath);

        // Concatenate the two JSon docs and remove duplicates (elements from the second override elements from the first).
        DbDoc persistentData = Stream.concat(sysPersistentData.entrySet().stream(), userPersistentData.entrySet().stream())
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue, (o, n) -> n, DbDoc::new));
        return persistentData;
    }

    /**
     * Loads and returns a single configurations file data.
     * 
     * @param path
     *            the file to load the configurations from.
     * @return the JSON object containing the configurations file contents.
     */
    private synchronized DbDoc loadPersistentData(Path path) {
        String jsonString = readFileToString(path);
        if (StringUtils.isEmptyOrWhitespaceOnly(jsonString)) {
            return new DbDoc();
        }
        try {
            return JsonParser.parseDoc(new StringReader(jsonString));
        } catch (IOException e) {
            throw ExceptionFactory.createException("Failed parsing the JSon data from file '" + path + "'.", e);
        }
    }

    /**
     * Returns the contents of the given text file as a string.
     * 
     * @param path
     *            the text file to load.
     * @return the string with the file contents.
     */
    private synchronized String readFileToString(Path path) {
        if (Files.exists(path)) {
            try {
                byte[] bytes = Files.readAllBytes(path);
                return new String(bytes);
            } catch (IOException e) {
                throw ExceptionFactory.createException("Error reading from file '" + path + "'.", e);
            }
        }
        return "";
    }

    /**
     * Saves the given JSON structure into the user-wide configurations file.
     * 
     * @param data
     *            the JSON structure to write.
     */
    private synchronized void saveUserWidePersistentData(DbDoc data) {
        if (Files.exists(this.userWidePath) && !Files.isWritable(this.userWidePath)) {
            throw ExceptionFactory.createException("Cannot write to file '" + this.userWidePath + "'.");
        }
        try {
            Path parent = this.userWidePath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Files.write(this.userWidePath, data.toString().getBytes());
        } catch (IOException e) {
            throw ExceptionFactory.createException("Error writing to file '" + this.userWidePath + "'.", e);
        }
    }
}
