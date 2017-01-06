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

package testsuite.regression.jdbc42;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.mysql.jdbc.ConnectionImpl;
import com.mysql.jdbc.MysqlIO;
import com.mysql.jdbc.SQLError;

import testsuite.BaseTestCase;

public class ConnectionRegressionTest extends BaseTestCase {

    public ConnectionRegressionTest(String name) {
        super(name);
    }

    /**
     * Tests fix for Bug#75615 - Incorrect implementation of Connection.setNetworkTimeout().
     * 
     * Note: this test exploits a non deterministic race condition. Usually the failure was observed under 10 consecutive executions, as such the siginficant
     * part of the test is run up to 25 times.
     */
    private Future<?> testBug75615Future = null;

    public void testBug75615() throws Exception {
        // Main use case: although this could cause an exception due to a race condition in MysqlIO.mysqlConnection it is silently swallowed within the running
        // thread.
        final Connection testConn1 = getConnectionWithProps("");
        testConn1.setNetworkTimeout(Executors.newSingleThreadExecutor(), 1000);
        testConn1.close();

        // Main use case simulation: this simulates the above by capturing an eventual exeption in the main thread. This is where this test would actually fail.
        // This part is repeated several times to increase the chance of hitting the reported bug.
        for (int i = 0; i < 25; i++) {
            final ExecutorService execService = Executors.newSingleThreadExecutor();
            final Connection testConn2 = getConnectionWithProps("");
            testConn2.setNetworkTimeout(new Executor() {
                public void execute(Runnable command) {
                    // Attach the future to the parent object so that it can track the exception in the main thread.
                    ConnectionRegressionTest.this.testBug75615Future = execService.submit(command);
                }
            }, 1000);
            testConn2.close();
            try {
                this.testBug75615Future.get();
            } catch (ExecutionException e) {
                e.getCause().printStackTrace();
                fail("Exception thrown in the thread that was setting the network timeout: " + e.getCause());
            }
            execService.shutdownNow();
        }

        // Test the expected exception on null executor.
        assertThrows(SQLException.class, "Executor can not be null", new Callable<Void>() {
            public Void call() throws Exception {
                Connection testConn = getConnectionWithProps("");
                testConn.setNetworkTimeout(null, 1000);
                testConn.close();
                return null;
            }
        });
    }
}
