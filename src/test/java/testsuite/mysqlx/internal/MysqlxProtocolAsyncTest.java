/*
  Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.

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

package testsuite.mysqlx.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.mysql.cj.core.io.StatementExecuteOk;
import com.mysql.cj.core.result.Field;
import com.mysql.cj.mysqlx.DocFindParams;
import com.mysql.cj.mysqlx.MysqlxError;
import com.mysql.cj.mysqlx.io.MysqlxProtocol;
import com.mysql.cj.mysqlx.io.ResultListener;
import com.mysql.cj.mysqlx.result.MysqlxRow;

/**
 * Tests for protocol-level <b>async</b> APIs against X Plugin via X Protocol.
 */
@Category(testsuite.mysqlx.AsyncTests.class)
public class MysqlxProtocolAsyncTest extends InternalMysqlxBaseTestCase {
    private MysqlxProtocol protocol;

    @Before
    public void setupTestProtocol() {
        if (this.isSetForMySQLxTests) {
            this.protocol = createAuthenticatedTestProtocol();
        }
    }

    @After
    public void destroyTestProtocol() throws IOException {
        if (this.isSetForMySQLxTests) {
            this.protocol.sendSessionClose();
            this.protocol.readOk();
            this.protocol.close();
        }
    }

    /**
     * Helper class to hold values across threads and closures.
     */
    public static class ValueHolder<T> implements Consumer<T>, Supplier<T> {
        T val;

        public void accept(T t) {
            this.val = t;
        }

        public T get() {
            return this.val;
        }
    }

    @Test
    public void simpleSuccessfulQuery() throws Exception {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        String collName = createTempTestCollection(this.protocol);

        String json = "{'_id': '85983efc2a9a11e5b345feff819cdc9f', 'testVal': 1, 'insertedBy': 'Jess'}".replaceAll("'", "\"");
        this.protocol.sendDocInsert(getTestDatabase(), collName, Arrays.asList(new String[] { json }));
        this.protocol.readStatementExecuteOk();

        final ValueHolder<ArrayList<Field>> metadataHolder = new ValueHolder<>();
        final ValueHolder<ArrayList<MysqlxRow>> rowHolder = new ValueHolder<>();
        rowHolder.accept(new ArrayList<>());
        final ValueHolder<StatementExecuteOk> okHolder = new ValueHolder<>();
        final ValueHolder<MysqlxError> errHolder = new ValueHolder<>();
        final ValueHolder<Throwable> excHolder = new ValueHolder<>();

        this.protocol.asyncFind(new DocFindParams(getTestDatabase(), collName), DEFAULT_METADATA_CHARSET, new ResultListener() {
            public void onMetadata(ArrayList<Field> metadata) {
                metadataHolder.accept(metadata);
            }

            public void onRow(MysqlxRow r) {
                rowHolder.get().add(r);
            }

            public void onComplete(StatementExecuteOk ok) {
                okHolder.accept(ok);
                synchronized (MysqlxProtocolAsyncTest.this) {
                    MysqlxProtocolAsyncTest.this.notify();
                }
            }

            public void onError(MysqlxError err) {
                errHolder.accept(err);
                synchronized (MysqlxProtocolAsyncTest.this) {
                    MysqlxProtocolAsyncTest.this.notify();
                }
            }

            public void onException(Throwable t) {
                excHolder.accept(t);
                synchronized (MysqlxProtocolAsyncTest.this) {
                    MysqlxProtocolAsyncTest.this.notify();
                }
            }
        }, new CompletableFuture<Void>());

        synchronized (this) {
            // timeout in case we get stuck
            this.wait(5000);
        }

        assertEquals(1, metadataHolder.get().size());
        assertEquals(1, rowHolder.get().size());
        assertNotNull(okHolder.get());
        assertNull(errHolder.get());
        assertNull(excHolder.get());
    }
}
