/*
 * Copyright (c) 2015, 2018, Oracle and/or its affiliates. All rights reserved.
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

package testsuite.x.devapi;

import java.lang.reflect.Field;
import java.sql.SQLException;

import com.mysql.cj.MysqlxSession;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.protocol.x.XProtocolError;
import com.mysql.cj.xdevapi.Schema;
import com.mysql.cj.xdevapi.Session;
import com.mysql.cj.xdevapi.SessionImpl;
import com.mysql.cj.xdevapi.SqlResult;

import testsuite.x.internal.InternalXBaseTestCase;

/**
 * Utilities for Dev API tests.
 */
public class DevApiBaseTestCase extends InternalXBaseTestCase {
    /**
     * Session for use in tests.
     */
    Session session;
    Schema schema;
    String dbCharset;
    String dbCollation;

    public boolean setupTestSession() {
        if (this.isSetForXTests) {
            this.session = new SessionImpl(this.testHostInfo);
            this.schema = this.session.getDefaultSchema();
            SqlResult rs = this.session.sql("SHOW VARIABLES LIKE 'character_set_database'").execute();
            this.dbCharset = rs.fetchOne().getString(1);
            rs = this.session.sql("SHOW VARIABLES LIKE 'collation_database'").execute();
            this.dbCollation = rs.fetchOne().getString(1);
            return true;
        }
        return false;
    }

    public void destroyTestSession() {
        if (this.session != null && this.session.isOpen()) {
            try {
                this.session.close();
            } catch (Exception ex) {
                System.err.println("Error during cleanup destroyTestSession()");
                ex.printStackTrace();
            }
        }
        this.session = null;
    }

    protected void sqlUpdate(String sql) {
        try {
            Field f = SessionImpl.class.getDeclaredField("session");
            f.setAccessible(true);
            MysqlxSession mysqlxSession = (MysqlxSession) f.get(this.session);
            mysqlxSession.sendMessage(mysqlxSession.getMessageBuilder().buildSqlStatement(sql));
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    protected void dropCollection(String collectionName) {
        if (this.isSetForXTests) {
            try {
                this.schema.dropCollection(collectionName);
            } catch (XProtocolError ex) {
                if (ex.getErrorCode() != MysqlErrorNumbers.ER_BAD_TABLE_ERROR) {
                    throw ex;
                }
            }
        }
    }

    protected String makeParam(PropertyKey key, Enum<?> value) {
        return makeParam(key, value.toString());
    }

    protected String makeParam(PropertyKey key, String value) {
        return makeParam(key, value, false);
    }

    protected String makeParam(PropertyKey key, String value, boolean isFirst) {
        return (isFirst ? "" : "&") + key.getKeyName() + "=" + value;
    }

    protected boolean isServerRunningOnWindows() throws SQLException {
        SqlResult res = this.session.sql("SHOW VARIABLES LIKE 'datadir'").execute();
        return res.fetchOne().getString(1).indexOf('\\') != -1;
    }
}
