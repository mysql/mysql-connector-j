/*
  Copyright (c) 2002, 2014, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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

package testsuite.regression;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import testsuite.BaseTestCase;

/**
 * Tests for multi-thread stress regressions.
 */
public class StressRegressionTest extends BaseTestCase {
    private int numThreadsStarted;

    /**
     * Creates a new StressRegressionTest
     * 
     * @param name
     *            the name of the test.
     */
    public StressRegressionTest(String name) {
        super(name);
    }

    /**
     * Runs all test cases in this test suite
     * 
     * @param args
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(StressRegressionTest.class);
    }

    /**
     * @throws Exception
     */
    public synchronized void testContention() throws Exception {
        if (false) {
            System.out.println("Calculating baseline elapsed time...");

            long start = System.currentTimeMillis();

            contentiousWork(this.conn, this.stmt, 0);

            long singleThreadElapsedTimeMillis = System.currentTimeMillis() - start;

            System.out.println("Single threaded execution took " + singleThreadElapsedTimeMillis + " ms.");

            int numThreadsToStart = 95;

            System.out.println("\nStarting " + numThreadsToStart + " threads.");

            this.numThreadsStarted = numThreadsToStart;

            ContentionThread[] threads = new ContentionThread[this.numThreadsStarted];

            for (int i = 0; i < numThreadsToStart; i++) {
                threads[i] = new ContentionThread(i);
                threads[i].start();
            }

            for (;;) {
                try {
                    wait();

                    if (this.numThreadsStarted == 0) {
                        break;
                    }
                } catch (InterruptedException ie) {
                    // ignore
                }
            }

            // Collect statistics...
            System.out.println("Done!");

            double avgElapsedTimeMillis = 0;

            List<Long> elapsedTimes = new ArrayList<Long>();

            for (int i = 0; i < numThreadsToStart; i++) {
                elapsedTimes.add(new Long(threads[i].elapsedTimeMillis));

                avgElapsedTimeMillis += ((double) threads[i].elapsedTimeMillis / numThreadsToStart);
            }

            Collections.sort(elapsedTimes);

            System.out.println("Average elapsed time per-thread was " + avgElapsedTimeMillis + " ms.");
            System.out.println("Median elapsed time per-thread was " + elapsedTimes.get(elapsedTimes.size() / 2) + " ms.");
            System.out.println("Minimum elapsed time per-thread was " + elapsedTimes.get(0) + " ms.");
            System.out.println("Maximum elapsed time per-thread was " + elapsedTimes.get(elapsedTimes.size() - 1) + " ms.");
        }
    }

    /**
     * @throws Exception
     */
    public void testCreateConnections() throws Exception {
        new CreateThread().start();
    }

    /**
     * @throws Exception
     */
    public void testCreateConnectionsUnderLoad() throws Exception {
        new CreateThread(new BusyThread()).start();
    }

    /**
     * @param threadConn
     * @param threadStmt
     * @param threadNumber
     */
    void contentiousWork(Connection threadConn, Statement threadStmt, int threadNumber) {
        Date now = new Date();

        try {
            for (int i = 0; i < 1000; i++) {
                ResultSet threadRs = threadStmt.executeQuery("SELECT 1, 2");

                while (threadRs.next()) {
                    threadRs.getString(1);
                    threadRs.getString(2);
                }

                threadRs.close();

                PreparedStatement pStmt = threadConn.prepareStatement("SELECT ?");
                pStmt.setTimestamp(1, new Timestamp(now.getTime()));

                threadRs = pStmt.executeQuery();

                while (threadRs.next()) {
                    threadRs.getTimestamp(1);
                }

                threadRs.close();
                pStmt.close();
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex.toString());
        }
    }

    synchronized void reportDone() {
        // TODO: This test should just be refactored to use an executor and futures.
        // this.numThreadsStarted--;
        notify();
    }

    public class BusyThread extends Thread {
        boolean stop = false;

        @Override
        public void run() {
            while (!this.stop) {
            }
        }
    }

    class ContentionThread extends Thread {
        Connection threadConn;

        Statement threadStmt;

        int threadNumber;

        long elapsedTimeMillis;

        public ContentionThread(int num) throws SQLException {
            this.threadNumber = num;
            this.threadConn = getConnectionWithProps(new Properties());
            this.threadStmt = this.threadConn.createStatement();

            System.out.println(this.threadConn);
        }

        @Override
        public void run() {
            long start = System.currentTimeMillis();

            try {
                contentiousWork(this.threadConn, this.threadStmt, this.threadNumber);
                this.elapsedTimeMillis = System.currentTimeMillis() - start;

                System.out.println("Thread " + this.threadNumber + " finished.");
            } finally {
                if (this.elapsedTimeMillis == 0) {
                    this.elapsedTimeMillis = System.currentTimeMillis() - start;
                }

                reportDone();

                try {
                    this.threadStmt.close();
                    this.threadConn.close();
                } catch (SQLException ex) {
                    // ignore
                }
            }
        }
    }

    class CreateThread extends Thread {
        BusyThread busyThread;

        int numConnections = 15;

        public CreateThread() {
        }

        public CreateThread(BusyThread toStop) {
            this.busyThread = toStop;
        }

        public CreateThread(int numConns) {
            this.numConnections = numConns;
        }

        @Override
        public void run() {
            try {
                Connection[] connList = new Connection[this.numConnections];

                long maxConnTime = Long.MIN_VALUE;
                long minConnTime = Long.MAX_VALUE;
                double averageTime = 0;

                Properties nullProps = new Properties();

                for (int i = 0; i < this.numConnections; i++) {
                    long startTime = System.currentTimeMillis();
                    connList[i] = getConnectionWithProps(nullProps);

                    long endTime = System.currentTimeMillis();
                    long ellapsedTime = endTime - startTime;

                    if (ellapsedTime < minConnTime) {
                        minConnTime = ellapsedTime;
                    }

                    if (ellapsedTime > maxConnTime) {
                        maxConnTime = ellapsedTime;
                    }

                    averageTime += ((double) ellapsedTime / this.numConnections);
                }

                if (this.busyThread != null) {
                    this.busyThread.stop = true;
                }

                for (int i = 0; i < this.numConnections; i++) {
                    connList[i].close();
                }

                System.out.println(minConnTime + "/" + maxConnTime + "/" + averageTime);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
