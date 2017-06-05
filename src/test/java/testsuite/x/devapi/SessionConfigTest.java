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

package testsuite.x.devapi;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.mysql.cj.api.xdevapi.PasswordHandler;
import com.mysql.cj.api.xdevapi.PersistenceHandler;
import com.mysql.cj.core.exceptions.CJException;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.xdevapi.DbDoc;
import com.mysql.cj.xdevapi.DefaultPersistenceHandler;
import com.mysql.cj.xdevapi.JsonNumber;
import com.mysql.cj.xdevapi.JsonParser;
import com.mysql.cj.xdevapi.JsonString;
import com.mysql.cj.xdevapi.SessionConfig;
import com.mysql.cj.xdevapi.SessionConfigManager;
import com.mysql.cj.xdevapi.SessionConfigManager.Attr;
import com.mysql.cj.xdevapi.SessionFactory;

public class SessionConfigTest extends DevApiBaseTestCase {
    private String sysConfigFile = ".mysql/tests/test_sys_sessions.json";
    private String usrConfigFile = ".mysql/tests/test_usr_sessions.json";

    private PersistenceHandler originalPersistenceHandler = null;
    private PersistenceHandler testPersistenceHandler = null;

    private PasswordHandler originalPasswordHandler = null;
    private PasswordHandler testPasswordHandler = null;

    @Before
    public void setupSessionConfigTest() {
        if (!this.isSetForXTests) {
            return;
        }

        if (!this.baseUrl.contains("?")) {
            this.baseUrl += "?";
        }

        deleteTestSessionConfigurationFiles();

        try {
            Field currentPersistenceHandler = SessionConfigManager.class.getDeclaredField("persistenceHandler");
            currentPersistenceHandler.setAccessible(true);
            this.originalPersistenceHandler = (PersistenceHandler) currentPersistenceHandler.get(null);
        } catch (Exception e) {
            this.originalPersistenceHandler = new DefaultPersistenceHandler();
        }
        this.testPersistenceHandler = new DefaultPersistenceHandler(this.sysConfigFile, this.usrConfigFile);

        try {
            Field currentPasswordHandler = SessionConfigManager.class.getDeclaredField("passwordHandler");
            currentPasswordHandler.setAccessible(true);
            this.originalPasswordHandler = (PasswordHandler) currentPasswordHandler.get(null);
        } catch (Exception e) {
            this.originalPasswordHandler = null;
        }
        this.testPasswordHandler = new PasswordHandler() {
            private Map<String, String> passwords = new HashMap<>();

            @Override
            public void save(String key, String service, String password) {
                this.passwords.put(key + "§" + service, password);
            }

            @Override
            public String load(String key, String service) {
                return this.passwords.get(key + "§" + service);
            }
        };
    }

    @After
    public void teardownSessionConfigTest() {
        if (!this.isSetForXTests) {
            return;
        }

        SessionConfigManager.setPersistenceHandler(this.originalPersistenceHandler);
        SessionConfigManager.setPasswordHandler(this.originalPasswordHandler);

        deleteTestSessionConfigurationFiles();
    }

    private void createTestSessionConfigurationFile(Path sessionPath, String contents) throws IOException {
        Path parent = sessionPath.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.write(sessionPath, contents.getBytes());
    }

    private void deleteTestSessionConfigurationFiles() {
        try {
            Files.delete(Paths.get(this.sysConfigFile));
        } catch (IOException e) {
        }
        try {
            Files.delete(Paths.get(this.usrConfigFile));
        } catch (IOException e) {
        }
        for (Path configsPath = Paths.get(this.usrConfigFile).getParent(); configsPath != null; configsPath = configsPath.getParent()) {
            try {
                Files.delete(configsPath);
            } catch (IOException e) {
            }
        }
    }

    private String readFileToString(String file) {
        Path path = Paths.get(file);
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

    private DbDoc loadJsonFromFile(String fileName) {
        DbDoc dbDoc = null;
        try {
            dbDoc = JsonParser.parseDoc(new StringReader(readFileToString(fileName)));
        } catch (IOException e) {
            fail(e.getMessage());
        }
        return dbDoc;
    }

    /**
     * Tests method {@link SessionConfigManager#save(String, String, String)}.
     */
    @Test
    public void testSaveUriAndJsonString() {
        if (!this.isSetForXTests) {
            return;
        }

        SessionConfigManager.setPersistenceHandler(this.testPersistenceHandler);

        SessionConfigManager.save("session", "mysqlx://testuser:testpassword@testhost:12345/testdb", "{\"key1\": \"value1\", \"key2\": \"value2\"}");

        DbDoc dbDoc = loadJsonFromFile(this.usrConfigFile);
        assertEquals(1, dbDoc.size());
        assertTrue(dbDoc.containsKey("session"));
        assertTrue(((DbDoc) dbDoc.get("session")).containsKey(SessionConfigManager.Attr.URI.getKey()));
        assertEquals("mysqlx://testuser:@testhost:12345/testdb", ((JsonString) ((DbDoc) dbDoc.get("session")).get(Attr.URI.getKey())).getString());
        assertTrue(((DbDoc) dbDoc.get("session")).containsKey(SessionConfigManager.Attr.APPDATA.getKey()));
        assertEquals(2, ((DbDoc) ((DbDoc) dbDoc.get("session")).get(SessionConfigManager.Attr.APPDATA.getKey())).size());
        assertFalse(Files.exists(Paths.get(this.sysConfigFile)));
    }

    /**
     * Tests method {@link SessionConfigManager#save(String, String, Properties)}.
     */
    @Test
    public void testSaveUriAndProps() {
        if (!this.isSetForXTests) {
            return;
        }

        SessionConfigManager.setPersistenceHandler(this.testPersistenceHandler);

        Properties appData = new Properties();
        appData.setProperty("key1", "value1");
        appData.setProperty("key2", "value2");
        SessionConfigManager.save("session", "mysqlx://testuser:testpassword@testhost:12345/testdb", appData);

        DbDoc dbDoc = loadJsonFromFile(this.usrConfigFile);
        assertEquals(1, dbDoc.size());
        assertTrue(dbDoc.containsKey("session"));
        assertTrue(((DbDoc) dbDoc.get("session")).containsKey(SessionConfigManager.Attr.URI.getKey()));
        assertEquals("mysqlx://testuser:@testhost:12345/testdb", ((JsonString) ((DbDoc) dbDoc.get("session")).get(Attr.URI.getKey())).getString());
        assertTrue(((DbDoc) dbDoc.get("session")).containsKey(SessionConfigManager.Attr.APPDATA.getKey()));
        assertEquals(2, ((DbDoc) ((DbDoc) dbDoc.get("session")).get(SessionConfigManager.Attr.APPDATA.getKey())).size());
        assertFalse(Files.exists(Paths.get(this.sysConfigFile)));
    }

    /**
     * Tests method {@link SessionConfigManager#save(String, String, Map)}.
     */
    @Test
    public void testSaveUriAndMap() {
        if (!this.isSetForXTests) {
            return;
        }

        SessionConfigManager.setPersistenceHandler(this.testPersistenceHandler);

        Map<String, String> appData = new HashMap<>();
        appData.put("key1", "value1");
        appData.put("key2", "value2");
        SessionConfigManager.save("session", "mysqlx://testuser:testpassword@testhost:12345/testdb", appData);

        DbDoc dbDoc = loadJsonFromFile(this.usrConfigFile);
        assertEquals(1, dbDoc.size());
        assertTrue(dbDoc.containsKey("session"));
        assertTrue(((DbDoc) dbDoc.get("session")).containsKey(SessionConfigManager.Attr.URI.getKey()));
        assertEquals("mysqlx://testuser:@testhost:12345/testdb", ((JsonString) ((DbDoc) dbDoc.get("session")).get(Attr.URI.getKey())).getString());
        assertTrue(((DbDoc) dbDoc.get("session")).containsKey(SessionConfigManager.Attr.APPDATA.getKey()));
        assertEquals(2, ((DbDoc) ((DbDoc) dbDoc.get("session")).get(SessionConfigManager.Attr.APPDATA.getKey())).size());
        assertFalse(Files.exists(Paths.get(this.sysConfigFile)));
    }

    /**
     * Tests method {@link SessionConfigManager#save(String, String, DbDoc)}.
     */
    @Test
    public void testSaveUriAndDbDoc() {
        if (!this.isSetForXTests) {
            return;
        }

        SessionConfigManager.setPersistenceHandler(this.testPersistenceHandler);

        DbDoc appData = new DbDoc();
        appData.put("key1", new JsonString().setValue("value1"));
        appData.put("key2", new JsonString().setValue("value2"));
        SessionConfigManager.save("session", "mysqlx://testuser:testpassword@testhost:12345/testdb", appData);

        DbDoc dbDoc = loadJsonFromFile(this.usrConfigFile);
        assertEquals(1, dbDoc.size());
        assertTrue(dbDoc.containsKey("session"));
        assertTrue(((DbDoc) dbDoc.get("session")).containsKey(SessionConfigManager.Attr.URI.getKey()));
        assertEquals("mysqlx://testuser:@testhost:12345/testdb", ((JsonString) ((DbDoc) dbDoc.get("session")).get(Attr.URI.getKey())).getString());
        assertTrue(((DbDoc) dbDoc.get("session")).containsKey(SessionConfigManager.Attr.APPDATA.getKey()));
        assertEquals(2, ((DbDoc) ((DbDoc) dbDoc.get("session")).get(SessionConfigManager.Attr.APPDATA.getKey())).size());
        assertFalse(Files.exists(Paths.get(this.sysConfigFile)));
    }

    /**
     * Tests method {@link SessionConfigManager#save(String, String)}.
     */
    @Test
    public void testSaveUriOrJsonString() {
        if (!this.isSetForXTests) {
            return;
        }

        SessionConfigManager.setPersistenceHandler(this.testPersistenceHandler);

        // SessionConfigManager.save(String name, String uriOrJson), with URI.
        SessionConfigManager.save("session1", "mysqlx://testuser1:testpassword1@testhost1:11111/testdb1");
        // SessionConfigManager.save(String name, String uriOrJson), with JSON string with 'uri' + 'appdata'.
        SessionConfigManager.save("session2", "{\"uri\": \"mysqlx:\\/\\/testuser2:testpassword2@testhost2:22222\\/testdb2\", "
                + "\"appdata\": { \"key1\": \"value1\", \"key2\": \"value2\"}}");
        assertThrows(CJException.class, "Invalid JSON document\\. Only the keys 'uri' and 'appdata' are allowed together\\.",
                () -> SessionConfigManager.save("session2bad", "{\"uri\": \"mysqlx:\\/\\/testuser2:testpassword2@testhost2:22222\\/testdb2\", "
                        + "\"appdata\": { \"key1\": \"value1\", \"key2\": \"value2\"}, \"key3\": \"value3\"}"));
        // SessionConfigManager.save(String name, String uriOrJson), with JSON string with URI components + 'appdata'.
        SessionConfigManager.save("session3", "{\"host\": \"testhost3\", \"port\": 33333, \"user\": \"testuser3\", "
                + "\"password\": \"testpassword3\", \"schema\": \"testdb3\", \"appdata\": { \"key1\": \"value1\", \"key2\": \"value2\"}}");
        assertThrows(CJException.class,
                "Invalid JSON document\\. Only the URI component keys 'user, password, host, port and schema', valid "
                        + "connection properties and 'appdata' are allowed together\\.",
                () -> SessionConfigManager.save("session3bad", "{\"host\": \"testhost3\", \"port\": 33333, \"user\": \"testuser3\", \"password\": "
                        + "\"testpassword3\", \"schema\": \"testdb3\", \"appdata\": { \"key1\": \"value1\", \"key2\": \"value2\"}, \"key3\": \"value3\"}"));

        DbDoc dbDoc = loadJsonFromFile(this.usrConfigFile);
        assertEquals(3, dbDoc.size());
        assertTrue(dbDoc.containsKey("session1"));
        assertTrue(((DbDoc) dbDoc.get("session1")).containsKey(SessionConfigManager.Attr.URI.getKey()));
        assertEquals("mysqlx://testuser1:@testhost1:11111/testdb1", ((JsonString) ((DbDoc) dbDoc.get("session1")).get(Attr.URI.getKey())).getString());
        assertFalse(((DbDoc) dbDoc.get("session1")).containsKey(SessionConfigManager.Attr.APPDATA.getKey()));
        assertTrue(dbDoc.containsKey("session2"));
        assertTrue(((DbDoc) dbDoc.get("session2")).containsKey(SessionConfigManager.Attr.URI.getKey()));
        assertEquals("mysqlx://testuser2:@testhost2:22222/testdb2", ((JsonString) ((DbDoc) dbDoc.get("session2")).get(Attr.URI.getKey())).getString());
        assertTrue(((DbDoc) dbDoc.get("session2")).containsKey(SessionConfigManager.Attr.APPDATA.getKey()));
        assertEquals(2, ((DbDoc) ((DbDoc) dbDoc.get("session2")).get(SessionConfigManager.Attr.APPDATA.getKey())).size());
        assertTrue(dbDoc.containsKey("session3"));
        assertTrue(((DbDoc) dbDoc.get("session3")).containsKey(SessionConfigManager.Attr.URI.getKey()));
        assertEquals("mysqlx://testuser3:@testhost3:33333/testdb3", ((JsonString) ((DbDoc) dbDoc.get("session3")).get(Attr.URI.getKey())).getString());
        assertTrue(((DbDoc) dbDoc.get("session3")).containsKey(SessionConfigManager.Attr.APPDATA.getKey()));
        assertEquals(2, ((DbDoc) ((DbDoc) dbDoc.get("session3")).get(SessionConfigManager.Attr.APPDATA.getKey())).size());
        assertFalse(Files.exists(Paths.get(this.sysConfigFile)));
    }

    /**
     * Tests method {@link SessionConfigManager#save(String, Properties)}.
     */
    @Test
    public void testSaveProps() {
        if (!this.isSetForXTests) {
            return;
        }

        SessionConfigManager.setPersistenceHandler(this.testPersistenceHandler);

        // SessionConfigManager.save(String name, Properties data), with 'uri' + additional 'appdata' elements.
        Properties data = new Properties();
        data.setProperty(SessionConfigManager.Attr.URI.getKey(), "mysqlx://testuser1:testpassword1@testhost1:11111/testdb1");
        data.setProperty("key1", "value1");
        data.setProperty("key2", "value2");
        SessionConfigManager.save("session1", data);

        // SessionConfigManager.save(String name, Properties data), with URI components + additional 'appdata' elements.
        data = new Properties();
        data.setProperty(SessionConfigManager.Attr.HOST.getKey(), "testhost2");
        data.setProperty(SessionConfigManager.Attr.PORT.getKey(), "22222");
        data.setProperty(SessionConfigManager.Attr.USER.getKey(), "testuser2");
        data.setProperty(SessionConfigManager.Attr.PASSWORD.getKey(), "testpassword2");
        data.setProperty(SessionConfigManager.Attr.SCHEMA.getKey(), "testdb2");
        data.setProperty("key1", "value1");
        data.setProperty("key2", "value2");
        SessionConfigManager.save("session2", data);

        DbDoc dbDoc = loadJsonFromFile(this.usrConfigFile);
        assertEquals(2, dbDoc.size());
        assertTrue(dbDoc.containsKey("session1"));
        assertTrue(((DbDoc) dbDoc.get("session1")).containsKey(SessionConfigManager.Attr.URI.getKey()));
        assertEquals("mysqlx://testuser1:@testhost1:11111/testdb1", ((JsonString) ((DbDoc) dbDoc.get("session1")).get(Attr.URI.getKey())).getString());
        assertTrue(((DbDoc) dbDoc.get("session1")).containsKey(SessionConfigManager.Attr.APPDATA.getKey()));
        assertEquals(2, ((DbDoc) ((DbDoc) dbDoc.get("session1")).get(SessionConfigManager.Attr.APPDATA.getKey())).size());
        assertTrue(dbDoc.containsKey("session2"));
        assertTrue(((DbDoc) dbDoc.get("session2")).containsKey(SessionConfigManager.Attr.URI.getKey()));
        assertEquals("mysqlx://testuser2:@testhost2:22222/testdb2", ((JsonString) ((DbDoc) dbDoc.get("session2")).get(Attr.URI.getKey())).getString());
        assertTrue(((DbDoc) dbDoc.get("session2")).containsKey(SessionConfigManager.Attr.APPDATA.getKey()));
        assertEquals(2, ((DbDoc) ((DbDoc) dbDoc.get("session2")).get(SessionConfigManager.Attr.APPDATA.getKey())).size());
        assertFalse(Files.exists(Paths.get(this.sysConfigFile)));
    }

    /**
     * Tests method {@link SessionConfigManager#save(String, Map)}.
     */
    @Test
    public void testSaveMap() {
        if (!this.isSetForXTests) {
            return;
        }

        SessionConfigManager.setPersistenceHandler(this.testPersistenceHandler);

        // SessionConfigManager.save(String name, Properties data), with 'uri' + additional 'appdata' elements.
        Map<String, String> data = new HashMap<>();
        data.put(SessionConfigManager.Attr.URI.getKey(), "mysqlx://testuser1:testpassword1@testhost1:11111/testdb1");
        data.put("key1", "value1");
        data.put("key2", "value2");
        SessionConfigManager.save("session1", data);

        // SessionConfigManager.save(String name, Properties data), with URI components + additional 'appdata' elements.
        data = new HashMap<>();
        data.put(SessionConfigManager.Attr.HOST.getKey(), "testhost2");
        data.put(SessionConfigManager.Attr.PORT.getKey(), "22222");
        data.put(SessionConfigManager.Attr.USER.getKey(), "testuser2");
        data.put(SessionConfigManager.Attr.PASSWORD.getKey(), "testpassword2");
        data.put(SessionConfigManager.Attr.SCHEMA.getKey(), "testdb2");
        data.put("key1", "value1");
        data.put("key2", "value2");
        SessionConfigManager.save("session2", data);

        DbDoc dbDoc = loadJsonFromFile(this.usrConfigFile);
        assertEquals(2, dbDoc.size());
        assertTrue(dbDoc.containsKey("session1"));
        assertTrue(((DbDoc) dbDoc.get("session1")).containsKey(SessionConfigManager.Attr.URI.getKey()));
        assertEquals("mysqlx://testuser1:@testhost1:11111/testdb1", ((JsonString) ((DbDoc) dbDoc.get("session1")).get(Attr.URI.getKey())).getString());
        assertTrue(((DbDoc) dbDoc.get("session1")).containsKey(SessionConfigManager.Attr.APPDATA.getKey()));
        assertEquals(2, ((DbDoc) ((DbDoc) dbDoc.get("session1")).get(SessionConfigManager.Attr.APPDATA.getKey())).size());
        assertTrue(dbDoc.containsKey("session2"));
        assertTrue(((DbDoc) dbDoc.get("session2")).containsKey(SessionConfigManager.Attr.URI.getKey()));
        assertEquals("mysqlx://testuser2:@testhost2:22222/testdb2", ((JsonString) ((DbDoc) dbDoc.get("session2")).get(Attr.URI.getKey())).getString());
        assertTrue(((DbDoc) dbDoc.get("session2")).containsKey(SessionConfigManager.Attr.APPDATA.getKey()));
        assertEquals(2, ((DbDoc) ((DbDoc) dbDoc.get("session2")).get(SessionConfigManager.Attr.APPDATA.getKey())).size());
        assertFalse(Files.exists(Paths.get(this.sysConfigFile)));
    }

    /**
     * Tests method {@link SessionConfigManager#save(String, DbDoc)}.
     */
    @Test
    public void testSaveDbDoc() {
        if (!this.isSetForXTests) {
            return;
        }

        SessionConfigManager.setPersistenceHandler(this.testPersistenceHandler);

        DbDoc appData = new DbDoc();
        appData.put("key1", new JsonString().setValue("value1"));
        appData.put("key2", new JsonString().setValue("value2"));

        // SessionConfigManager.save(String name, Properties data), with 'uri' + additional 'appdata' elements.
        DbDoc data1 = new DbDoc();
        data1.put(SessionConfigManager.Attr.URI.getKey(), new JsonString().setValue("mysqlx://testuser1:testpassword1@testhost1:11111/testdb1"));
        data1.put(SessionConfigManager.Attr.APPDATA.getKey(), appData);
        SessionConfigManager.save("session1", data1);

        // Unrecognized top level keys are not accepted.
        data1.put("key3", new JsonString().setValue("value3"));
        assertThrows(CJException.class, "Invalid JSON document\\. Only the keys 'uri' and 'appdata' are allowed together\\.",
                () -> SessionConfigManager.save("session1bad", data1));

        // SessionConfigManager.save(String name, Properties data), with URI components + additional 'appdata' elements.
        DbDoc data2 = new DbDoc();
        data2.put(SessionConfigManager.Attr.HOST.getKey(), new JsonString().setValue("testhost2"));
        data2.put(SessionConfigManager.Attr.PORT.getKey(), new JsonString().setValue("22222"));
        data2.put(SessionConfigManager.Attr.USER.getKey(), new JsonString().setValue("testuser2"));
        data2.put(SessionConfigManager.Attr.PASSWORD.getKey(), new JsonString().setValue("testpassword2"));
        data2.put(SessionConfigManager.Attr.SCHEMA.getKey(), new JsonString().setValue("testdb2"));
        data2.put(SessionConfigManager.Attr.APPDATA.getKey(), appData);
        SessionConfigManager.save("session2", data2);

        // Unrecognized top level keys are not accepted.
        data2.put("key3", new JsonString().setValue("value3"));
        assertThrows(CJException.class, "Invalid JSON document\\. Only the URI component keys 'user, password, host, port and schema', valid "
                + "connection properties and 'appdata' are allowed together\\.", () -> SessionConfigManager.save("session2bad", data2));

        DbDoc dbDoc = loadJsonFromFile(this.usrConfigFile);
        assertEquals(2, dbDoc.size());
        assertTrue(dbDoc.containsKey("session1"));
        assertTrue(((DbDoc) dbDoc.get("session1")).containsKey(SessionConfigManager.Attr.URI.getKey()));
        assertEquals("mysqlx://testuser1:@testhost1:11111/testdb1", ((JsonString) ((DbDoc) dbDoc.get("session1")).get(Attr.URI.getKey())).getString());
        assertTrue(((DbDoc) dbDoc.get("session1")).containsKey(SessionConfigManager.Attr.APPDATA.getKey()));
        assertEquals(2, ((DbDoc) ((DbDoc) dbDoc.get("session1")).get(SessionConfigManager.Attr.APPDATA.getKey())).size());
        assertTrue(dbDoc.containsKey("session2"));
        assertTrue(((DbDoc) dbDoc.get("session2")).containsKey(SessionConfigManager.Attr.URI.getKey()));
        assertEquals("mysqlx://testuser2:@testhost2:22222/testdb2", ((JsonString) ((DbDoc) dbDoc.get("session2")).get(Attr.URI.getKey())).getString());
        assertTrue(((DbDoc) dbDoc.get("session2")).containsKey(SessionConfigManager.Attr.APPDATA.getKey()));
        assertEquals(2, ((DbDoc) ((DbDoc) dbDoc.get("session2")).get(SessionConfigManager.Attr.APPDATA.getKey())).size());
        assertFalse(Files.exists(Paths.get(this.sysConfigFile)));
    }

    /**
     * Tests method {@link SessionConfigManager#save(com.mysql.cj.xdevapi.SessionConfig)}.
     */
    @Test
    public void testSaveSessionConfig() {
        if (!this.isSetForXTests) {
            return;
        }

        SessionConfigManager.setPersistenceHandler(this.testPersistenceHandler);

        SessionConfig sessionCfg = new SessionConfig("session", "mysqlx://testuser:testpassword@testhost:12345/testdb");
        sessionCfg.setAppData("key1", "value1");
        sessionCfg.setAppData("key2", "value2");
        SessionConfigManager.save(sessionCfg);

        DbDoc dbDoc = loadJsonFromFile(this.usrConfigFile);
        assertEquals(1, dbDoc.size());
        assertTrue(dbDoc.containsKey("session"));
        assertTrue(((DbDoc) dbDoc.get("session")).containsKey(SessionConfigManager.Attr.URI.getKey()));
        assertEquals("mysqlx://testuser:@testhost:12345/testdb", ((JsonString) ((DbDoc) dbDoc.get("session")).get(Attr.URI.getKey())).getString());
        assertTrue(((DbDoc) dbDoc.get("session")).containsKey(SessionConfigManager.Attr.APPDATA.getKey()));
        assertEquals(2, ((DbDoc) ((DbDoc) dbDoc.get("session")).get(SessionConfigManager.Attr.APPDATA.getKey())).size());
        assertFalse(Files.exists(Paths.get(this.sysConfigFile)));
    }

    /**
     * Tests method {@link SessionConfigManager#get(String)}, with system-wide vs user-wide configurations.
     */
    @Test
    public void testGetSessionConfigs() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }

        SessionConfigManager.setPersistenceHandler(this.testPersistenceHandler);

        DbDoc sysSessions = JsonParser.parseDoc(new StringReader("{\"session1\": {\"uri\": \"mysqlx://sysuser1:@syshost1:11111/sysdb1\", "
                + "\"appdata\": {\"key1\": \"sysvalue1\", \"key2\": \"sysvalue2\"}}, \"session2\": {\"uri\": \"mysqlx://sysuser2:@syshost2:12222/sysdb2\", "
                + "\"appdata\": {\"key1\": \"sysvalue1\", \"key2\": \"sysvalue2\"}}}"));
        createTestSessionConfigurationFile(Paths.get(this.sysConfigFile), sysSessions.toString());

        SessionConfig sessionConfig = SessionConfigManager.get("session1");
        assertEquals("mysqlx://sysuser1:@syshost1:11111/sysdb1", sessionConfig.getUri());
        assertEquals(2, sessionConfig.getAppData().size());
        assertEquals("sysvalue1", sessionConfig.getAppData("key1"));
        assertEquals("sysvalue2", sessionConfig.getAppData("key2"));
        sessionConfig = SessionConfigManager.get("session2");
        assertEquals("mysqlx://sysuser2:@syshost2:12222/sysdb2", sessionConfig.getUri());
        assertEquals(2, sessionConfig.getAppData().size());
        assertEquals("sysvalue1", sessionConfig.getAppData("key1"));
        assertEquals("sysvalue2", sessionConfig.getAppData("key2"));

        SessionConfigManager.save("session2", "mysqlx://usruser2:usrpassword2@usrhost2:22222/usrdb2", "{\"key1\": \"usrvalue1\", \"key2\": \"usrvalue2\"}");
        SessionConfigManager.save("session3", "mysqlx://usruser3:usrpassword3@usrhost3:23333/usrdb3", "{\"key1\": \"usrvalue1\", \"key2\": \"usrvalue2\"}");

        sessionConfig = SessionConfigManager.get("session1");
        assertEquals("mysqlx://sysuser1:@syshost1:11111/sysdb1", sessionConfig.getUri());
        assertEquals(2, sessionConfig.getAppData().size());
        assertEquals("sysvalue1", sessionConfig.getAppData("key1"));
        assertEquals("sysvalue2", sessionConfig.getAppData("key2"));
        sessionConfig = SessionConfigManager.get("session2");
        assertEquals("mysqlx://usruser2:@usrhost2:22222/usrdb2", sessionConfig.getUri());
        assertEquals(2, sessionConfig.getAppData().size());
        assertEquals("usrvalue1", sessionConfig.getAppData("key1"));
        assertEquals("usrvalue2", sessionConfig.getAppData("key2"));
        sessionConfig = SessionConfigManager.get("session3");
        assertEquals("mysqlx://usruser3:@usrhost3:23333/usrdb3", sessionConfig.getUri());
        assertEquals(2, sessionConfig.getAppData().size());
        assertEquals("usrvalue1", sessionConfig.getAppData("key1"));
        assertEquals("usrvalue2", sessionConfig.getAppData("key2"));

        DbDoc dbDoc = loadJsonFromFile(this.sysConfigFile);
        assertEquals(2, dbDoc.size());
        assertTrue(dbDoc.containsKey("session1"));
        assertTrue(((DbDoc) dbDoc.get("session1")).containsKey(SessionConfigManager.Attr.URI.getKey()));
        assertEquals("mysqlx://sysuser1:@syshost1:11111/sysdb1", ((JsonString) ((DbDoc) dbDoc.get("session1")).get(Attr.URI.getKey())).getString());
        assertTrue(dbDoc.containsKey("session2"));
        assertTrue(((DbDoc) dbDoc.get("session2")).containsKey(SessionConfigManager.Attr.URI.getKey()));
        assertEquals("mysqlx://sysuser2:@syshost2:12222/sysdb2", ((JsonString) ((DbDoc) dbDoc.get("session2")).get(Attr.URI.getKey())).getString());

        dbDoc = loadJsonFromFile(this.usrConfigFile);
        assertEquals(2, dbDoc.size());
        assertTrue(dbDoc.containsKey("session2"));
        assertTrue(((DbDoc) dbDoc.get("session2")).containsKey(SessionConfigManager.Attr.URI.getKey()));
        assertEquals("mysqlx://usruser2:@usrhost2:22222/usrdb2", ((JsonString) ((DbDoc) dbDoc.get("session2")).get(Attr.URI.getKey())).getString());
        assertTrue(dbDoc.containsKey("session3"));
        assertTrue(((DbDoc) dbDoc.get("session3")).containsKey(SessionConfigManager.Attr.URI.getKey()));
        assertEquals("mysqlx://usruser3:@usrhost3:23333/usrdb3", ((JsonString) ((DbDoc) dbDoc.get("session3")).get(Attr.URI.getKey())).getString());
    }

    /**
     * Tests method {@link SessionConfigManager#delete(String)}.
     */
    @Test
    public void testDeleteSessionConfigs() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }

        SessionConfigManager.setPersistenceHandler(this.testPersistenceHandler);

        DbDoc sysSessions = JsonParser.parseDoc(new StringReader("{\"session1\": {\"uri\": \"mysqlx://sysuser1:@syshost1:11111/sysdb1\", "
                + "\"appdata\": {\"key1\": \"sysvalue1\", \"key2\": \"sysvalue2\"}}, \"session2\": {\"uri\": \"mysqlx://sysuser2:@syshost2:12222/sysdb2\", "
                + "\"appdata\": {\"key1\": \"sysvalue1\", \"key2\": \"sysvalue2\"}}}"));
        createTestSessionConfigurationFile(Paths.get(this.sysConfigFile), sysSessions.toString());

        DbDoc dbDoc = loadJsonFromFile(this.sysConfigFile);
        assertEquals(2, dbDoc.size());
        assertTrue(dbDoc.containsKey("session1"));
        assertTrue(dbDoc.containsKey("session2"));

        assertFalse(SessionConfigManager.delete("session1"));
        assertFalse(SessionConfigManager.delete("session2"));

        dbDoc = loadJsonFromFile(this.sysConfigFile);
        assertEquals(2, dbDoc.size());
        assertTrue(dbDoc.containsKey("session1"));
        assertTrue(dbDoc.containsKey("session2"));

        SessionConfigManager.save("session2", "mysqlx://usruser2:usrpassword2@usrhost2:22222/usrdb2", "{\"key1\": \"usrvalue1\", \"key2\": \"usrvalue2\"}");
        SessionConfigManager.save("session3", "mysqlx://usruser3:usrpassword3@usrhost3:23333/usrdb3", "{\"key1\": \"usrvalue1\", \"key2\": \"usrvalue2\"}");

        dbDoc = loadJsonFromFile(this.sysConfigFile);
        assertEquals(2, dbDoc.size());
        assertTrue(dbDoc.containsKey("session1"));
        assertTrue(dbDoc.containsKey("session2"));

        dbDoc = loadJsonFromFile(this.usrConfigFile);
        assertEquals(2, dbDoc.size());
        assertTrue(dbDoc.containsKey("session2"));
        assertTrue(dbDoc.containsKey("session3"));

        assertTrue(SessionConfigManager.delete("session2"));

        dbDoc = loadJsonFromFile(this.sysConfigFile);
        assertEquals(2, dbDoc.size());
        assertTrue(dbDoc.containsKey("session1"));
        assertTrue(dbDoc.containsKey("session2"));

        dbDoc = loadJsonFromFile(this.usrConfigFile);
        assertEquals(1, dbDoc.size());
        assertTrue(dbDoc.containsKey("session3"));

        assertTrue(SessionConfigManager.delete("session3"));

        dbDoc = loadJsonFromFile(this.sysConfigFile);
        assertEquals(2, dbDoc.size());
        assertTrue(dbDoc.containsKey("session1"));
        assertTrue(dbDoc.containsKey("session2"));

        dbDoc = loadJsonFromFile(this.usrConfigFile);
        assertEquals(0, dbDoc.size());

        assertFalse(SessionConfigManager.delete("session4"));
    }

    /**
     * Tests method {@link SessionConfigManager#list()}.
     */
    @Test
    public void testListSessionConfigs() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }

        SessionConfigManager.setPersistenceHandler(this.testPersistenceHandler);

        DbDoc sysSessions = JsonParser.parseDoc(new StringReader("{\"session1\": {\"uri\": \"mysqlx://sysuser1:@syshost1:11111/sysdb1\", "
                + "\"appdata\": {\"key1\": \"sysvalue1\", \"key2\": \"sysvalue2\"}}, \"session2\": {\"uri\": \"mysqlx://sysuser2:@syshost2:12222/sysdb2\", "
                + "\"appdata\": {\"key1\": \"sysvalue1\", \"key2\": \"sysvalue2\"}}}"));
        createTestSessionConfigurationFile(Paths.get(this.sysConfigFile), sysSessions.toString());

        List<String> sessionsList = SessionConfigManager.list();
        Assert.assertArrayEquals(new String[] { "session1", "session2" }, sessionsList.toArray(new String[sessionsList.size()]));

        SessionConfigManager.save("session2", "mysqlx://usruser2:usrpassword2@usrhost2:22222/usrdb2", "{\"key1\": \"usrvalue1\", \"key2\": \"usrvalue2\"}");
        SessionConfigManager.save("session3", "mysqlx://usruser3:usrpassword3@usrhost3:23333/usrdb3", "{\"key1\": \"usrvalue1\", \"key2\": \"usrvalue2\"}");

        sessionsList = SessionConfigManager.list();
        Assert.assertArrayEquals(new String[] { "session1", "session2", "session3" }, sessionsList.toArray(new String[sessionsList.size()]));

        SessionConfigManager.delete("session1");

        sessionsList = SessionConfigManager.list();
        Assert.assertArrayEquals(new String[] { "session1", "session2", "session3" }, sessionsList.toArray(new String[sessionsList.size()]));

        SessionConfigManager.delete("session2");

        sessionsList = SessionConfigManager.list();
        Assert.assertArrayEquals(new String[] { "session1", "session2", "session3" }, sessionsList.toArray(new String[sessionsList.size()]));

        SessionConfigManager.delete("session3");

        sessionsList = SessionConfigManager.list();
        Assert.assertArrayEquals(new String[] { "session1", "session2" }, sessionsList.toArray(new String[sessionsList.size()]));

        Files.delete(Paths.get(this.sysConfigFile));

        sessionsList = SessionConfigManager.list();
        Assert.assertArrayEquals(new String[] {}, sessionsList.toArray(new String[sessionsList.size()]));
    }

    /**
     * Tests support for password persistence.
     */
    @Test
    public void testPasswordPersistence() {
        if (!this.isSetForXTests) {
            return;
        }

        SessionConfigManager.setPersistenceHandler(this.testPersistenceHandler);

        // Save sessions without password handler.
        SessionConfigManager.save("session1", "mysqlx://testuser1:testpassword1@testhost1:11111/testdb1");
        Map<String, String> data = new HashMap<>();
        data.put(SessionConfigManager.Attr.HOST.getKey(), "testhost2");
        data.put(SessionConfigManager.Attr.PORT.getKey(), "22222");
        data.put(SessionConfigManager.Attr.USER.getKey(), "testuser2");
        data.put(SessionConfigManager.Attr.PASSWORD.getKey(), "testpassword2");
        data.put(SessionConfigManager.Attr.SCHEMA.getKey(), "testdb2");
        SessionConfigManager.save("session2", data);

        SessionConfigManager.setPasswordHandler(this.testPasswordHandler);

        // Save sessions with password handler.
        SessionConfigManager.save("session3", "mysqlx://testuser3:testpassword3@testhost3:33333/testdb3");
        data = new HashMap<>();
        data.put(SessionConfigManager.Attr.HOST.getKey(), "testhost4");
        data.put(SessionConfigManager.Attr.PORT.getKey(), "44444");
        data.put(SessionConfigManager.Attr.USER.getKey(), "testuser4");
        data.put(SessionConfigManager.Attr.PASSWORD.getKey(), "testpassword4");
        data.put(SessionConfigManager.Attr.SCHEMA.getKey(), "testdb4");
        SessionConfigManager.save("session4", data);

        assertEquals("mysqlx://testuser1:@testhost1:11111/testdb1", SessionConfigManager.get("session1").getUri());
        assertEquals("mysqlx://testuser2:@testhost2:22222/testdb2", SessionConfigManager.get("session2").getUri());
        assertEquals("mysqlx://testuser3:testpassword3@testhost3:33333/testdb3", SessionConfigManager.get("session3").getUri());
        assertEquals("mysqlx://testuser4:testpassword4@testhost4:44444/testdb4", SessionConfigManager.get("session4").getUri());
    }

    /**
     * Tests password-less vs stripped out password.
     */
    @Test
    public void testPasswordLessVsStrippedPassword() {
        if (!this.isSetForXTests) {
            return;
        }

        SessionConfigManager.setPersistenceHandler(this.testPersistenceHandler);

        // Save password-less sessions.
        SessionConfigManager.save("session1", "mysqlx://testuser1@testhost1:11111/testdb1");
        Map<String, String> data = new HashMap<>();
        data.put(SessionConfigManager.Attr.HOST.getKey(), "testhost2");
        data.put(SessionConfigManager.Attr.PORT.getKey(), "22222");
        data.put(SessionConfigManager.Attr.USER.getKey(), "testuser2");
        data.put(SessionConfigManager.Attr.SCHEMA.getKey(), "testdb2");
        SessionConfigManager.save("session2", data);

        // Save sessions with password stripped out, without password handler.
        SessionConfigManager.save("session3", "mysqlx://testuser3:testpassword3@testhost3:33333/testdb3");
        data = new HashMap<>();
        data.put(SessionConfigManager.Attr.HOST.getKey(), "testhost4");
        data.put(SessionConfigManager.Attr.PORT.getKey(), "44444");
        data.put(SessionConfigManager.Attr.USER.getKey(), "testuser4");
        data.put(SessionConfigManager.Attr.PASSWORD.getKey(), "testpassword4");
        data.put(SessionConfigManager.Attr.SCHEMA.getKey(), "testdb4");
        SessionConfigManager.save("session4", data);

        SessionConfigManager.setPasswordHandler(this.testPasswordHandler);

        // Save sessions with password stripped out, with password handler.
        SessionConfigManager.save("session5", "mysqlx://testuser5:testpassword5@testhost5:55555/testdb5");
        data = new HashMap<>();
        data.put(SessionConfigManager.Attr.HOST.getKey(), "testhost6");
        data.put(SessionConfigManager.Attr.PORT.getKey(), "66666");
        data.put(SessionConfigManager.Attr.USER.getKey(), "testuser6");
        data.put(SessionConfigManager.Attr.PASSWORD.getKey(), "testpassword6");
        data.put(SessionConfigManager.Attr.SCHEMA.getKey(), "testdb6");
        SessionConfigManager.save("session6", data);

        DbDoc dbDoc = loadJsonFromFile(this.usrConfigFile);
        assertEquals(6, dbDoc.size());
        assertTrue(dbDoc.containsKey("session1"));
        assertTrue(((DbDoc) dbDoc.get("session1")).containsKey(SessionConfigManager.Attr.URI.getKey()));
        assertEquals("mysqlx://testuser1@testhost1:11111/testdb1", ((JsonString) ((DbDoc) dbDoc.get("session1")).get(Attr.URI.getKey())).getString());
        assertTrue(dbDoc.containsKey("session2"));
        assertTrue(((DbDoc) dbDoc.get("session2")).containsKey(SessionConfigManager.Attr.URI.getKey()));
        assertEquals("mysqlx://testuser2@testhost2:22222/testdb2", ((JsonString) ((DbDoc) dbDoc.get("session2")).get(Attr.URI.getKey())).getString());
        assertTrue(dbDoc.containsKey("session3"));
        assertTrue(((DbDoc) dbDoc.get("session3")).containsKey(SessionConfigManager.Attr.URI.getKey()));
        assertEquals("mysqlx://testuser3:@testhost3:33333/testdb3", ((JsonString) ((DbDoc) dbDoc.get("session3")).get(Attr.URI.getKey())).getString());
        assertTrue(dbDoc.containsKey("session4"));
        assertTrue(((DbDoc) dbDoc.get("session4")).containsKey(SessionConfigManager.Attr.URI.getKey()));
        assertEquals("mysqlx://testuser4:@testhost4:44444/testdb4", ((JsonString) ((DbDoc) dbDoc.get("session4")).get(Attr.URI.getKey())).getString());
        assertTrue(dbDoc.containsKey("session5"));
        assertTrue(((DbDoc) dbDoc.get("session5")).containsKey(SessionConfigManager.Attr.URI.getKey()));
        assertEquals("mysqlx://testuser5:@testhost5:55555/testdb5", ((JsonString) ((DbDoc) dbDoc.get("session5")).get(Attr.URI.getKey())).getString());
        assertTrue(dbDoc.containsKey("session6"));
        assertTrue(((DbDoc) dbDoc.get("session6")).containsKey(SessionConfigManager.Attr.URI.getKey()));
        assertEquals("mysqlx://testuser6:@testhost6:66666/testdb6", ((JsonString) ((DbDoc) dbDoc.get("session6")).get(Attr.URI.getKey())).getString());
        assertFalse(Files.exists(Paths.get(this.sysConfigFile)));
    }

    /**
     * Tests exceptions thrown on missing session configurations.
     */
    @Test
    public void testMissingSessionConfig() {
        if (!this.isSetForXTests) {
            return;
        }

        SessionConfigManager.setPersistenceHandler(this.testPersistenceHandler);

        for (final String sc : new String[] { "no_session", "", null }) {
            assertThrows(CJException.class, "Failed to get the session configuration '" + sc + "'\\.", () -> SessionConfigManager.get(sc));

            assertThrows(CJException.class, "Failed to get the session configuration '" + sc + "'\\.",
                    () -> this.fact.getSession(this.baseUrl + "&" + SessionFactory.SESSION_NAME + "=" + sc));

            final Properties props = new Properties();
            props.setProperty(SessionFactory.SESSION_NAME, sc == null ? "null" : sc);
            assertThrows(CJException.class, "Failed to get the session configuration '" + sc + "'\\.", () -> this.fact.getSession(props));

            final Map<String, String> map = new HashMap<>();
            map.put(SessionFactory.SESSION_NAME, sc);
            assertThrows(CJException.class, "Failed to get the session configuration '" + sc + "'\\.", () -> this.fact.getSession(map));

            final DbDoc dbDoc = new DbDoc();
            dbDoc.put(SessionFactory.SESSION_NAME, new JsonString().setValue(sc));
            assertThrows(CJException.class, "Failed to get the session configuration '" + sc + "'\\.", () -> this.fact.getSession(dbDoc));
        }
    }

    /**
     * Tests session creation using password-complete session configurations.
     */
    @Test
    public void testCreateSessionPasswordCompleteConfig() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }

        SessionConfigManager.setPersistenceHandler(this.testPersistenceHandler);
        SessionConfigManager.setPasswordHandler(this.testPasswordHandler);

        SessionConfigManager.save("session1", this.baseUrl);
        Files.move(Paths.get(this.usrConfigFile), Paths.get(this.sysConfigFile));
        SessionConfigManager.save("session2", this.baseUrl);

        DbDoc dbDocFile = loadJsonFromFile(this.sysConfigFile);
        assertEquals(1, dbDocFile.size());
        assertTrue(dbDocFile.containsKey("session1"));

        dbDocFile = loadJsonFromFile(this.usrConfigFile);
        assertEquals(1, dbDocFile.size());
        assertTrue(dbDocFile.containsKey("session2"));

        this.fact.getSession(SessionConfigManager.get("session1")).close();
        this.fact.getSession(SessionConfigManager.get("session1"), getTestPassword()).close();
        this.fact.getSession(SessionConfigManager.get("session2")).close();
        this.fact.getSession(SessionConfigManager.get("session2"), getTestPassword()).close();

        this.fact.getSession(this.baseUrl + "&" + SessionFactory.SESSION_NAME + "=session1").close();
        this.fact.getSession(this.baseUrl + "&" + SessionFactory.SESSION_NAME + "=session2").close();

        final Properties props = new Properties();
        props.setProperty(SessionFactory.SESSION_NAME, "session1");
        this.fact.getSession(props).close();
        props.setProperty(SessionFactory.SESSION_NAME, "session2");
        this.fact.getSession(props).close();

        final Map<String, String> map = new HashMap<>();
        map.put(SessionFactory.SESSION_NAME, "session1");
        this.fact.getSession(map).close();
        map.put(SessionFactory.SESSION_NAME, "session2");
        this.fact.getSession(map).close();

        final DbDoc dbDoc = new DbDoc();
        dbDoc.put(SessionFactory.SESSION_NAME, new JsonString().setValue("session1"));
        this.fact.getSession(dbDoc).close();
        dbDoc.put(SessionFactory.SESSION_NAME, new JsonString().setValue("session2"));
        this.fact.getSession(dbDoc).close();
    }

    /**
     * Tests session creation using password-less session configurations.
     */
    @Test
    public void testCreateSessionPasswordLessConfig() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }

        SessionConfigManager.setPersistenceHandler(this.testPersistenceHandler);

        SessionConfigManager.save("session1", this.baseUrl);
        Files.move(Paths.get(this.usrConfigFile), Paths.get(this.sysConfigFile));
        SessionConfigManager.save("session2", this.baseUrl);

        DbDoc dbDocFile = loadJsonFromFile(this.sysConfigFile);
        assertEquals(1, dbDocFile.size());
        assertTrue(dbDocFile.containsKey("session1"));

        dbDocFile = loadJsonFromFile(this.usrConfigFile);
        assertEquals(1, dbDocFile.size());
        assertTrue(dbDocFile.containsKey("session2"));

        if (StringUtils.isNullOrEmpty(getTestPassword())) {
            this.fact.getSession(SessionConfigManager.get("session1")).close();
            this.fact.getSession(SessionConfigManager.get("session2")).close();
        } else {
            assertThrows(CJException.class, ".*Invalid user or password", () -> this.fact.getSession(SessionConfigManager.get("session1")));
            assertThrows(CJException.class, ".*Invalid user or password", () -> this.fact.getSession(SessionConfigManager.get("session2")));
        }

        this.fact.getSession(SessionConfigManager.get("session1"), getTestPassword()).close();
        this.fact.getSession(SessionConfigManager.get("session2"), getTestPassword()).close();
    }

    /**
     * Tests session creation using a password-complete user.
     */
    @Test
    public void testCreateSessionWithPasswordCompleteUser() {
        if (!setupTestSession()) {
            return;
        }

        try {
            this.session.sql("CREATE USER 'userpwdcpml'@'%' IDENTIFIED WITH mysql_native_password BY 'thepassword'").execute();
            this.session.sql("GRANT ALL ON *.* TO 'userpwdcpml'@'%'").execute();

            SessionConfigManager.setPersistenceHandler(this.testPersistenceHandler);

            final Properties props = new Properties();
            props.setProperty(SessionConfigManager.Attr.HOST.getKey(), getTestHost());
            props.setProperty(SessionConfigManager.Attr.PORT.getKey(), String.valueOf(getTestPort()));
            props.setProperty(SessionConfigManager.Attr.USER.getKey(), "userpwdcpml");
            props.setProperty(SessionConfigManager.Attr.PASSWORD.getKey(), "thepassword");
            props.setProperty(SessionConfigManager.Attr.SCHEMA.getKey(), getTestDatabase());
            SessionConfigManager.save("session1", props);

            assertThrows(CJException.class, ".*Invalid user or password", () -> this.fact.getSession(SessionConfigManager.get("session1")));
            this.fact.getSession(SessionConfigManager.get("session1"), "thepassword").close();
            assertThrows(CJException.class, ".*Invalid user or password", () -> this.fact.getSession(SessionConfigManager.get("session1"), "wrongpassword"));

            SessionConfigManager.setPasswordHandler(this.testPasswordHandler);
            SessionConfigManager.save("session2", props);

            this.fact.getSession(SessionConfigManager.get("session2")).close();
            this.fact.getSession(SessionConfigManager.get("session2"), "thepassword").close();
            assertThrows(CJException.class, ".*Invalid user or password", () -> this.fact.getSession(SessionConfigManager.get("session2"), "wrongpassword"));
        } finally {
            this.session.sql("DROP USER 'userpwdcpml'@'%'").execute();
        }
    }

    /**
     * Tests session creation using a password-less user.
     */
    @Test
    public void testCreateSessionWithPasswordLessUser() {
        if (!setupTestSession()) {
            return;
        }

        try {
            this.session.sql("CREATE USER 'userpwdless'@'%' IDENTIFIED WITH mysql_native_password").execute();
            this.session.sql("GRANT ALL ON *.* TO 'userpwdless'@'%'").execute();

            SessionConfigManager.setPersistenceHandler(this.testPersistenceHandler);

            final Properties props = new Properties();
            props.setProperty(SessionConfigManager.Attr.HOST.getKey(), getTestHost());
            props.setProperty(SessionConfigManager.Attr.PORT.getKey(), String.valueOf(getTestPort()));
            props.setProperty(SessionConfigManager.Attr.USER.getKey(), "userpwdless");
            props.setProperty(SessionConfigManager.Attr.PASSWORD.getKey(), "");
            props.setProperty(SessionConfigManager.Attr.SCHEMA.getKey(), getTestDatabase());
            SessionConfigManager.save("session1", props);

            this.fact.getSession(SessionConfigManager.get("session1")).close();
            this.fact.getSession(SessionConfigManager.get("session1"), "").close();
            assertThrows(CJException.class, ".*Invalid user or password", () -> this.fact.getSession(SessionConfigManager.get("session1"), "wrongpassword"));

            SessionConfigManager.setPasswordHandler(this.testPasswordHandler);
            SessionConfigManager.save("session2", props);

            this.fact.getSession(SessionConfigManager.get("session2")).close();
            this.fact.getSession(SessionConfigManager.get("session2"), "").close();
            assertThrows(CJException.class, ".*Invalid user or password", () -> this.fact.getSession(SessionConfigManager.get("session2"), "wrongpassword"));
        } finally {
            this.session.sql("DROP USER 'userpwdless'@'%'").execute();
        }
    }

    /**
     * Tests session creation with overriding data.
     */
    @Test
    public void testCreateSessionWithOverwriding() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }

        SessionConfigManager.setPersistenceHandler(this.testPersistenceHandler);
        SessionConfigManager.setPasswordHandler(this.testPasswordHandler);

        SessionConfigManager.save("session1", "mysqlx://sysuser:syspassword@syshost:11111/sysdb");
        Files.move(Paths.get(this.usrConfigFile), Paths.get(this.sysConfigFile));
        SessionConfigManager.save("session2", "mysqlx://usruser:usrpassword@usrhost:22222/usrdb");

        DbDoc dbDocFile = loadJsonFromFile(this.sysConfigFile);
        assertEquals(1, dbDocFile.size());
        assertTrue(dbDocFile.containsKey("session1"));

        dbDocFile = loadJsonFromFile(this.usrConfigFile);
        assertEquals(1, dbDocFile.size());
        assertTrue(dbDocFile.containsKey("session2"));

        this.fact.getSession(this.baseUrl + "&" + SessionFactory.SESSION_NAME + "=session1").close();
        this.fact.getSession(this.baseUrl + "&" + SessionFactory.SESSION_NAME + "=session2").close();

        final Properties props = new Properties();
        props.setProperty(SessionConfigManager.Attr.HOST.getKey(), getTestHost());
        props.setProperty(SessionConfigManager.Attr.PORT.getKey(), String.valueOf(getTestPort()));
        props.setProperty(SessionConfigManager.Attr.USER.getKey(), getTestUser());
        props.setProperty(SessionConfigManager.Attr.PASSWORD.getKey(), getTestPassword());
        props.setProperty(SessionConfigManager.Attr.SCHEMA.getKey(), getTestDatabase());
        props.setProperty(SessionFactory.SESSION_NAME, "session1");
        this.fact.getSession(props).close();
        props.setProperty(SessionFactory.SESSION_NAME, "session2");
        this.fact.getSession(props).close();

        final Map<String, String> map = new HashMap<>();
        map.put(SessionConfigManager.Attr.HOST.getKey(), getTestHost());
        map.put(SessionConfigManager.Attr.PORT.getKey(), String.valueOf(getTestPort()));
        map.put(SessionConfigManager.Attr.USER.getKey(), getTestUser());
        map.put(SessionConfigManager.Attr.PASSWORD.getKey(), getTestPassword());
        map.put(SessionConfigManager.Attr.SCHEMA.getKey(), getTestDatabase());
        map.put(SessionFactory.SESSION_NAME, "session1");
        this.fact.getSession(map).close();
        map.put(SessionFactory.SESSION_NAME, "session2");
        this.fact.getSession(map).close();

        final DbDoc dbDoc = new DbDoc();
        dbDoc.put(SessionConfigManager.Attr.HOST.getKey(), new JsonString().setValue(getTestHost()));
        dbDoc.put(SessionConfigManager.Attr.PORT.getKey(), new JsonNumber().setValue(String.valueOf(getTestPort())));
        dbDoc.put(SessionConfigManager.Attr.USER.getKey(), new JsonString().setValue(getTestUser()));
        dbDoc.put(SessionConfigManager.Attr.PASSWORD.getKey(), new JsonString().setValue(getTestPassword()));
        dbDoc.put(SessionConfigManager.Attr.SCHEMA.getKey(), new JsonString().setValue(getTestDatabase()));
        dbDoc.put(SessionFactory.SESSION_NAME, new JsonString().setValue("session1"));
        this.fact.getSession(dbDoc).close();
        dbDoc.put(SessionFactory.SESSION_NAME, new JsonString().setValue("session2"));
        this.fact.getSession(dbDoc).close();
    }
}
