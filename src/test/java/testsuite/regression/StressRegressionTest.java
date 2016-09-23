/*
  Copyright (c) 2002, 2016, Oracle and/or its affiliates. All rights reserved.

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

package testsuite.regression;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
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
        if (!this.DISABLED_testContention) {
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
        Thread t = new CreateThread();
        t.start();
        t.join();
    }

    /**
     * @throws Exception
     */
    public void testCreateConnectionsUnderLoad() throws Exception {
        Thread t = new CreateThread(new BusyThread());
        t.start();
        t.join();
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
            boolean doStop = this.stop;
            while (!doStop) {
                doStop = this.stop;
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

                if (this.busyThread != null) {
                    this.busyThread.start();
                }

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

    /**
     * Tests fix for BUG#67760 - Deadlock when concurrently executing prepared statements with Timestamp objects
     * 
     * Concurrent execution of Timestamp, Date and Time related setters and getters from a PreparedStatement and ResultSet object obtained from a same shared
     * Connection may result in a deadlock.
     * 
     * This test exploits a non-deterministic situation that can end in a deadlock. It executes two concurrent jobs for 10 seconds while stressing the referred
     * methods. The deadlock was observed before 3 seconds have elapsed, all times, in development environment.
     * 
     * WARNING! If this test fails there is no guarantee that the JVM will remain stable and won't affect any other tests. It is imperative that this test
     * passes to ensure other tests results.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug67760() throws Exception {
        /*
         * Use a brand new Connection not shared by anyone else, otherwise it may block later on test teardown.
         */
        final Connection testConn = getConnectionWithProps("");

        /*
         * Thread to execute set[Timestamp|Date|Time]() methods in an instance of a PreparedStatement constructed from a shared Connection.
         */
        Thread job1 = new Thread(new Runnable() {
            public void run() {
                try {
                    System.out.println("Starting job 1 (" + Thread.currentThread().getName() + ") - PreparedStatement.set[Timestamp|Date|Time]()...");
                    PreparedStatement testPstmt = testConn.prepareStatement("SELECT ?, ?, ?");

                    Timestamp ts = new Timestamp(System.currentTimeMillis());
                    java.sql.Date dt = new java.sql.Date(System.currentTimeMillis());
                    Time tm = new Time(System.currentTimeMillis());

                    while (SharedInfoForTestBug67760.running) {
                        SharedInfoForTestBug67760.job1Iterations++;

                        testPstmt.setTimestamp(1, ts);
                        testPstmt.setDate(2, dt);
                        testPstmt.setTime(3, tm);
                        testPstmt.execute();
                    }
                    System.out.println(
                            "Finishing job 1 (" + Thread.currentThread().getName() + ") after " + SharedInfoForTestBug67760.job1Iterations + " iterations...");
                    testPstmt.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        /*
         * Thread to execute get[Timestamp|Date|Time]() methods in an instance of a ResultSet obtained from a PreparedStatement constructed from a shared
         * Connection.
         */
        Thread job2 = new Thread(new Runnable() {
            public void run() {
                try {
                    System.out.println("Starting job 2 (" + Thread.currentThread().getName() + ") - ResultSet.get[Timestamp|Date|Time]()...");
                    PreparedStatement testPstmt = testConn.prepareStatement("SELECT NOW(), CAST(NOW() AS DATE), CAST(NOW() AS TIME)");

                    while (SharedInfoForTestBug67760.running) {
                        SharedInfoForTestBug67760.job2Iterations++;

                        ResultSet testRs = testPstmt.executeQuery();
                        testRs.next();
                        testRs.getTimestamp(1);
                        testRs.getDate(2);
                        testRs.getTime(3);
                        testRs.close();
                    }
                    System.out.println(
                            "Finishing job 2 (" + Thread.currentThread().getName() + ") after " + SharedInfoForTestBug67760.job2Iterations + " iterations...");
                    testPstmt.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        /*
         * Start concurrent jobs and let them run for 10 seconds (100 * 100 milliseconds).
         * Monitor jobs activity while they are running, allowing, at the most, a period of 2 seconds (20 * 100 milliseconds) inactivity before the test fails.
         */
        final int recheckWaitTimeUnit = 100;
        int recheckWaitTimeCountdown = 100;

        final int delta0IterationsCountdownSize = 20;
        int delta0IterationsCountdown = delta0IterationsCountdownSize;

        System.out.println("Start concurrent jobs and let them run for aproximatly " + (recheckWaitTimeUnit * recheckWaitTimeCountdown / 1000) + " seconds...");
        final long startTime = System.currentTimeMillis();
        long elapsedTime = 0;
        job1.start();
        job2.start();

        do {
            if (recheckWaitTimeCountdown == 1) {
                // time to stop monitoring and finish the jobs
                SharedInfoForTestBug67760.running = false;
            }
            Thread.sleep(recheckWaitTimeUnit);

            delta0IterationsCountdown = SharedInfoForTestBug67760.iterationsChanged() ? delta0IterationsCountdownSize : delta0IterationsCountdown - 1;
            if (SharedInfoForTestBug67760.running && (!job1.isAlive() || !job2.isAlive())) {
                fail("Something as failed. At least one of the threads has died.");
            }

            if (delta0IterationsCountdown == 0 || !SharedInfoForTestBug67760.running) {
                if (!SharedInfoForTestBug67760.running && (job1.isAlive() || job2.isAlive())) {
                    // jobs haven't died yet, allow them some more time to die
                    Thread.sleep(1000);
                }

                if (job1.isAlive() && job2.isAlive()) {
                    // there must be a deadlock
                    elapsedTime = System.currentTimeMillis() - startTime;
                    System.out.println("Possible deadlock detected after " + elapsedTime + " milliseconds.");

                    ThreadMXBean threadMXbean = ManagementFactory.getThreadMXBean();

                    ThreadInfo thread1Info = threadMXbean.getThreadInfo(job1.getId(), Integer.MAX_VALUE);
                    System.out.printf("%n%s stopped at iteration %d, blocked by the lock %s, owned by %s%n", job1.getName(),
                            SharedInfoForTestBug67760.job1Iterations, thread1Info.getLockName(), thread1Info.getLockOwnerName());
                    System.out.println("Stacktrace:");
                    for (StackTraceElement element : thread1Info.getStackTrace()) {
                        System.out.println("  " + element);
                    }

                    ThreadInfo thread2Info = threadMXbean.getThreadInfo(job2.getId(), Integer.MAX_VALUE);
                    System.out.printf("%n%s stopped at iteration %d, blocked by the lock %s, owned by %s%n", job2.getName(),
                            SharedInfoForTestBug67760.job2Iterations, thread2Info.getLockName(), thread2Info.getLockOwnerName());
                    System.out.println("Stacktrace:");
                    for (StackTraceElement element : thread2Info.getStackTrace()) {
                        System.out.println("   " + element);
                    }

                    fail("Possible deadlock detected after " + elapsedTime
                            + " milliseconds. See the console output for more details. WARNING: this failure may lead to JVM instability.");
                }
            }
        } while (--recheckWaitTimeCountdown > 0);

        elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println("The test ended gracefully after " + elapsedTime + " milliseconds.");

        assertTrue("Test expected to run at least for " + (recheckWaitTimeUnit * recheckWaitTimeCountdown) + " milliseconds.",
                elapsedTime >= recheckWaitTimeUnit * recheckWaitTimeCountdown);

        testConn.close();
    }

    private static final class SharedInfoForTestBug67760 {
        static volatile boolean running = true;

        static volatile int job1Iterations = 0;
        static volatile int job2Iterations = 0;

        static int prevJob1Iterations = 0;
        static int prevJob2Iterations = 0;

        static boolean iterationsChanged() {
            boolean iterationsChanged = prevJob1Iterations != job1Iterations && prevJob2Iterations != job2Iterations;
            prevJob1Iterations = job1Iterations;
            prevJob2Iterations = job2Iterations;
            return iterationsChanged;
        }
    }
}
