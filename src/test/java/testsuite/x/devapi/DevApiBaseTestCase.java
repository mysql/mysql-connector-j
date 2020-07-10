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

package testsuite.x.devapi;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Field;
import java.sql.SQLException;

import com.mysql.cj.MysqlxSession;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.protocol.x.XProtocolError;
import com.mysql.cj.xdevapi.PreparableStatement;
import com.mysql.cj.xdevapi.Schema;
import com.mysql.cj.xdevapi.Session;
import com.mysql.cj.xdevapi.SessionImpl;
import com.mysql.cj.xdevapi.SqlResult;
import com.mysql.cj.xdevapi.Statement;
import com.mysql.cj.xdevapi.UpdateResultBuilder;

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
            mysqlxSession.query(mysqlxSession.getMessageBuilder().buildSqlStatement(sql), new UpdateResultBuilder<>());
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

    protected int getThreadId(Session sess) {
        return sess.sql("SELECT thread_id FROM performance_schema.threads WHERE processlist_id = connection_id()").execute().fetchOne().getInt(0);
    }

    int getPrepPrepareCount(Session sess) {
        return Integer.parseInt(sess.sql("SHOW SESSION STATUS LIKE 'mysqlx_prep_prepare'").execute().fetchOne().getString(1));
    }

    int getPrepExecuteCount(Session sess) {
        return Integer.parseInt(sess.sql("SHOW SESSION STATUS LIKE 'mysqlx_prep_execute'").execute().fetchOne().getString(1));
    }

    int getPrepDeallocateCount(Session sess) {
        return Integer.parseInt(sess.sql("SHOW SESSION STATUS LIKE 'mysqlx_prep_deallocate'").execute().fetchOne().getString(1));
    }

    int getPreparedStatementsCount() {
        return this.session.sql("SELECT COUNT(*) FROM performance_schema.prepared_statements_instances").execute().fetchOne().getInt(0);
    }

    int getPreparedStatementsCount(int threadId) {
        return this.session.sql("SELECT COUNT(*) FROM performance_schema.prepared_statements_instances  WHERE owner_thread_id = " + threadId).execute()
                .fetchOne().getInt(0);
    }

    int getPreparedStatementsCount(Session sess) {
        return sess.sql("SELECT COUNT(*) FROM performance_schema.prepared_statements_instances psi INNER JOIN performance_schema.threads t "
                + "ON psi.owner_thread_id = t.thread_id WHERE t.processlist_id = connection_id()").execute().fetchOne().getInt(0);
    }

    int getPreparedStatementExecutionsCount(Session sess, int prepStmtId) {
        SqlResult res = sess.sql("SELECT psi.count_execute FROM performance_schema.prepared_statements_instances psi INNER JOIN performance_schema.threads t "
                + "ON psi.owner_thread_id = t.thread_id WHERE t.processlist_id = connection_id() AND psi.statement_id = mysqlx_get_prepared_statement_id(?)")
                .bind(prepStmtId).execute();
        if (res.hasNext()) {
            return res.next().getInt(0);
        }
        return -1;
    }

    int getPreparedStatementId(PreparableStatement<?> stmt) {
        try {
            Field prepStmtId = PreparableStatement.class.getDeclaredField("preparedStatementId");
            prepStmtId.setAccessible(true);
            return prepStmtId.getInt(stmt);
        } catch (Exception e) {
            return -1;
        }
    }

    protected void assertPreparedStatementsCountsAndId(Session sess, int expectedPrepStmtsCount, Statement<?, ?> stmt, int expectedId, int expectedExec) {
        assertEquals(expectedPrepStmtsCount, getPreparedStatementsCount(sess));
        assertEquals(expectedId, getPreparedStatementId((PreparableStatement<?>) stmt));
        assertEquals(expectedExec, getPreparedStatementExecutionsCount(sess, expectedId));
    }

    protected void assertPreparedStatementsStatusCounts(Session sess, int expectedPrep, int expectedExec, int expectedDealloc) {
        assertEquals(expectedPrep, getPrepPrepareCount(sess));
        assertEquals(expectedExec, getPrepExecuteCount(sess));
        assertEquals(expectedDealloc, getPrepDeallocateCount(sess));
    }

    protected void assertPreparedStatementsCount(int threadId, int expectedCount, int countdown) {
        /*
         * System table <code>performance_schema.prepared_statements_instances</code> may have some delay in updating its values after a session containing
         * prepared statements is closed.
         */
        int psCount;
        do {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
            psCount = getPreparedStatementsCount(threadId);
        } while (psCount != 0 && --countdown > 0);
        assertEquals(expectedCount, psCount);
    }

    protected static void assertNonSecureSession(Session sess) {
        assertSessionStatusEquals(sess, "mysqlx_ssl_cipher", "");
    }

    protected static void assertSecureSession(Session sess) {
        assertSessionStatusNotEquals(sess, "mysqlx_ssl_cipher", "");
    }

    protected static void assertSecureSession(Session sess, String user) {
        assertSecureSession(sess);
        SqlResult res = sess.sql("SELECT CURRENT_USER()").execute();
        assertEquals(user, res.fetchOne().getString(0).split("@")[0]);
    }
}
