/*
 * Copyright (c) 2002, 2023, Oracle and/or its affiliates.
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

package testsuite.regression;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import testsuite.BaseTestCase;

/**
 * Microperformance benchmarks to track increase/decrease in performance of core methods in the driver over time.
 */
public class MicroPerformanceRegressionTest extends BaseTestCase {

    private static double[] scaleFactorSamples = new double[5];
    private static double scaleFactor = 0.0;

    private final static double ORIGINAL_LOOP_TIME_MS = 2300.0;

    // (Used to be 10.0 for JVM < 1.7 but since HW and VMs are much faster now a minimal disruption can cause significant deviations)
    private final static double LEEWAY = 50.0; // account for VMs

    private final static Map<String, Double> BASELINE_TIMES = new HashMap<>();

    static {
        BASELINE_TIMES.put("ResultSet.getInt()", new Double(0.00661));
        BASELINE_TIMES.put("ResultSet.getDouble()", new Double(0.00671));
        BASELINE_TIMES.put("ResultSet.getTime()", new Double(0.02033));
        BASELINE_TIMES.put("ResultSet.getTimestamp()", new Double(0.02363));
        BASELINE_TIMES.put("ResultSet.getDate()", new Double(0.02223));
        BASELINE_TIMES.put("ResultSet.getString()", new Double(0.00982));
        BASELINE_TIMES.put("ResultSet.getObject() on a string", new Double(0.00861));
        BASELINE_TIMES.put("Connection.prepareStatement()", new Double(0.18547));
        BASELINE_TIMES.put("single selects", new Double(46));
        BASELINE_TIMES.put("5 standalone queries", new Double(146));
        BASELINE_TIMES.put("total time all queries", new Double(190));

        BASELINE_TIMES.put("PreparedStatement.setInt()", new Double(0.0014));
        BASELINE_TIMES.put("PreparedStatement.setTime()", new Double(0.0107));
        BASELINE_TIMES.put("PreparedStatement.setTimestamp()", new Double(0.0182));
        BASELINE_TIMES.put("PreparedStatement.setDate()", new Double(0.0819));
        BASELINE_TIMES.put("PreparedStatement.setString()", new Double(0.0081));
        BASELINE_TIMES.put("PreparedStatement.setObject() on a string", new Double(0.00793));
        BASELINE_TIMES.put("PreparedStatement.setDouble()", new Double(0.0246));

        System.out.println("Calculating global performance scaling factor...");
        for (int i = 0; i < scaleFactorSamples.length; i++) {
            scaleFactorSamples[i] = calculateScaleFactor();
            scaleFactor += scaleFactorSamples[i];
        }
        scaleFactor /= scaleFactorSamples.length;
        System.out.println("Global performance scaling factor is: " + scaleFactor);
    }

    @BeforeEach
    public void setUp() throws Exception {
        System.out.println("Adjusting global performance scaling factor...");
        System.out.println("Gobal performance scaling factor adjusted from: " + scaleFactor + " to: " + adjustScaleFactor());
    }

    private static final double adjustScaleFactor() {
        double newScaleFactor = calculateScaleFactor();
        double maxDeviation = Math.abs(newScaleFactor - scaleFactor);

        // discard the farthest value from previous mean (scaleFactor);
        for (int i = 0; i < scaleFactorSamples.length; i++) {
            double deviation = Math.abs(scaleFactorSamples[i] - scaleFactor);
            if (deviation > maxDeviation) {
                Double swapValue = scaleFactorSamples[i];
                scaleFactorSamples[i] = newScaleFactor;
                newScaleFactor = swapValue;
                maxDeviation = deviation;
            }
        }

        // calculate new mean (scaleFactor)
        newScaleFactor = 0.0;
        for (double d : scaleFactorSamples) {
            newScaleFactor += d;
        }
        scaleFactor = newScaleFactor / scaleFactorSamples.length;

        return scaleFactor;
    }

    private static final double calculateScaleFactor() {
        // Run this simple test to get some sort of performance scaling factor, compared to the development environment. This should help reduce false-positives
        // on this test.
        int numLoops = 10000;

        long start = BaseTestCase.currentTimeMillis();

        for (int j = 0; j < 2000; j++) {
            // StringBuffer below is used for measuring and can't be changed to StringBuilder.
            StringBuffer buf = new StringBuffer(numLoops);

            for (int i = 0; i < numLoops; i++) {
                buf.append('a');
            }
        }

        long elapsedTime = BaseTestCase.currentTimeMillis() - start;
        return elapsedTime / ORIGINAL_LOOP_TIME_MS;
    }

    private synchronized void checkTime(String testType, double avgExecTimeMs) throws Exception {
        double adjustForVendor = 1.0D;

        if (isRunningOnJRockit()) {
            adjustForVendor = 4.0D;
        }

        Double baselineExecTimeMs = BASELINE_TIMES.get(testType);

        if (baselineExecTimeMs == null) {
            throw new Exception("No baseline time recorded for test '" + testType + "'");
        }

        double acceptableTime = LEEWAY * baselineExecTimeMs.doubleValue() * scaleFactor * adjustForVendor;

        System.out.println(testType + ": avg time = " + avgExecTimeMs + ", acceptable time = " + acceptableTime);

        assertTrue(avgExecTimeMs <= acceptableTime,
                "Average execution time of " + avgExecTimeMs + " ms. exceeded baseline * leeway of " + acceptableTime + " ms.");
    }

    /**
     * Tests result set accessors performance.
     *
     * @throws Exception
     */
    @Test
    public void testResultSetAccessors() throws Exception {
        createTable("marktest", "(intField INT, floatField DOUBLE, timeField TIME, datetimeField DATETIME, stringField VARCHAR(64))");
        this.stmt.executeUpdate(
                "INSERT INTO marktest VALUES (123456789, 12345.6789, NOW(), NOW(), 'abcdefghijklmnopqrstuvABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@')");

        this.rs = this.stmt.executeQuery("SELECT intField, floatField, timeField, datetimeField, stringField FROM marktest");

        this.rs.next();

        int numLoops = 100000;

        long start = currentTimeMillis();

        for (int i = 0; i < numLoops; i++) {
            this.rs.getInt(1);
        }

        double getIntAvgMs = (double) (currentTimeMillis() - start) / numLoops;

        checkTime("ResultSet.getInt()", getIntAvgMs);

        start = currentTimeMillis();

        for (int i = 0; i < numLoops; i++) {
            this.rs.getDouble(2);
        }

        double getDoubleAvgMs = (double) (currentTimeMillis() - start) / numLoops;

        checkTime("ResultSet.getDouble()", getDoubleAvgMs);

        start = currentTimeMillis();

        for (int i = 0; i < numLoops; i++) {
            this.rs.getTime(3);
            // If we don't clear the warnings here, we add one for every loop and spend a huge amount of time iterating and updating the linked list
            this.rs.clearWarnings();
        }

        double getTimeAvgMs = (double) (currentTimeMillis() - start) / numLoops;

        checkTime("ResultSet.getTime()", getTimeAvgMs);

        start = currentTimeMillis();

        for (int i = 0; i < numLoops; i++) {
            this.rs.getTimestamp(4);
        }

        double getTimestampAvgMs = (double) (currentTimeMillis() - start) / numLoops;

        checkTime("ResultSet.getTimestamp()", getTimestampAvgMs);

        start = currentTimeMillis();

        for (int i = 0; i < numLoops; i++) {
            this.rs.getDate(4);
            // If we don't clear the warnings here, we add one for every loop and spend a huge amount of time iterating and updating the linked list
            this.rs.clearWarnings();
        }

        double getDateAvgMs = (double) (currentTimeMillis() - start) / numLoops;

        checkTime("ResultSet.getDate()", getDateAvgMs);

        start = currentTimeMillis();

        for (int i = 0; i < numLoops; i++) {
            this.rs.getString(5);
        }

        double getStringAvgMs = (double) (currentTimeMillis() - start) / numLoops;

        checkTime("ResultSet.getString()", getStringAvgMs);

        start = currentTimeMillis();

        for (int i = 0; i < numLoops; i++) {
            this.rs.getObject(5);
        }

        double getStringObjAvgMs = (double) (currentTimeMillis() - start) / numLoops;

        checkTime("ResultSet.getObject() on a string", getStringObjAvgMs);
    }

    @Test
    public void testPreparedStatementTimes() throws Exception {
        createTable("marktest", "(intField INT, floatField DOUBLE, timeField TIME, datetimeField DATETIME, stringField VARCHAR(64))");
        this.stmt.executeUpdate(
                "INSERT INTO marktest VALUES (123456789, 12345.6789, NOW(), NOW(), 'abcdefghijklmnopqrstuvABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@')");

        long start = currentTimeMillis();

        long blockStart = currentTimeMillis();
        long lastBlock = 0;

        int numLoops = 100000;

        int numPrepares = 100000;

        numPrepares = 10000; // we don't need to do so many for server-side prep statements...

        for (int i = 0; i < numPrepares; i++) {
            if (i % 1000 == 0) {

                long blockEnd = currentTimeMillis();

                long totalTime = blockEnd - blockStart;

                blockStart = blockEnd;

                StringBuilder messageBuf = new StringBuilder();

                messageBuf.append(i + " prepares, the last 1000 prepares took " + totalTime + " ms");

                if (lastBlock == 0) {
                    lastBlock = totalTime;
                    messageBuf.append(".");
                } else {
                    double diff = (double) totalTime / (double) lastBlock;

                    messageBuf.append(", difference is " + diff + " x");

                    lastBlock = totalTime;
                }

                System.out.println(messageBuf.toString());

            }

            PreparedStatement pStmt = this.conn.prepareStatement("INSERT INTO test.marktest VALUES (?, ?, ?, ?, ?)");
            pStmt.close();
        }

        @SuppressWarnings("unused")
        double getPrepareStmtAvgMs = (double) (currentTimeMillis() - start) / numPrepares;

        // checkTime("Connection.prepareStatement()", getPrepareStmtAvgMs);

        PreparedStatement pStmt = this.conn.prepareStatement("INSERT INTO marktest VALUES (?, ?, ?, ?, ?)");

        System.out.println(pStmt.toString());

        start = currentTimeMillis();

        for (int i = 0; i < numLoops; i++) {
            pStmt.setInt(1, 1);
        }

        System.out.println(pStmt.toString());

        double setIntAvgMs = (double) (currentTimeMillis() - start) / numLoops;

        checkTime("PreparedStatement.setInt()", setIntAvgMs);

        start = currentTimeMillis();

        for (int i = 0; i < numLoops; i++) {
            pStmt.setDouble(2, 1234567890.1234);
        }

        double setDoubleAvgMs = (double) (currentTimeMillis() - start) / numLoops;

        checkTime("PreparedStatement.setDouble()", setDoubleAvgMs);

        start = currentTimeMillis();

        Time tm = new Time(start);

        for (int i = 0; i < numLoops; i++) {
            pStmt.setTime(3, tm);
        }

        double setTimeAvgMs = (double) (currentTimeMillis() - start) / numLoops;

        checkTime("PreparedStatement.setTime()", setTimeAvgMs);

        start = currentTimeMillis();

        Timestamp ts = new Timestamp(start);

        for (int i = 0; i < numLoops; i++) {
            pStmt.setTimestamp(4, ts);
        }

        double setTimestampAvgMs = (double) (currentTimeMillis() - start) / numLoops;

        checkTime("PreparedStatement.setTimestamp()", setTimestampAvgMs);

        start = currentTimeMillis();

        Date dt = new Date(start);

        for (int i = 0; i < numLoops; i++) {
            pStmt.setDate(4, dt);
        }

        double setDateAvgMs = (double) (currentTimeMillis() - start) / numLoops;

        checkTime("PreparedStatement.setDate()", setDateAvgMs);

        start = currentTimeMillis();

        for (int i = 0; i < numLoops; i++) {
            pStmt.setString(5, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@");
        }

        double setStringAvgMs = (double) (currentTimeMillis() - start) / numLoops;

        checkTime("PreparedStatement.setString()", setStringAvgMs);

        start = currentTimeMillis();

        for (int i = 0; i < numLoops; i++) {
            pStmt.setObject(5, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@");
        }

        double setStringObjAvgMs = (double) (currentTimeMillis() - start) / numLoops;

        checkTime("PreparedStatement.setObject() on a string", setStringObjAvgMs);

        start = currentTimeMillis();
    }

    @Test
    public void testBug6359() throws Exception {
        if (runLongTests()) {
            int numRows = 550000;
            int numSelects = 100000;

            createTable("testBug6359",
                    "(pk_field INT PRIMARY KEY NOT NULL AUTO_INCREMENT, field1 INT, field2 INT, field3 INT, field4 INT, field5 INT, field6 INT, field7 INT, field8 INT, field9 INT,  INDEX (field1))");

            PreparedStatement pStmt = this.conn.prepareStatement(
                    "INSERT INTO testBug6359 (field1, field2, field3, field4, field5, field6, field7, field8, field9) VALUES (?, 1, 2, 3, 4, 5, 6, 7, 8)");

            logDebug("Loading " + numRows + " rows...");

            for (int i = 0; i < numRows; i++) {
                pStmt.setInt(1, i);
                pStmt.executeUpdate();

                if (i % 10000 == 0) {
                    logDebug(i + " rows loaded so far");
                }
            }

            logDebug("Finished loading rows");

            long begin = currentTimeMillis();

            long beginSingleQuery = currentTimeMillis();

            for (int i = 0; i < numSelects; i++) {
                this.rs = this.stmt.executeQuery("SELECT pk_field FROM testBug6359 WHERE field1 BETWEEN 1 AND 5");
            }

            long endSingleQuery = currentTimeMillis();

            double secondsSingleQuery = ((double) endSingleQuery - (double) beginSingleQuery) / 1000;

            logDebug("time to execute " + numSelects + " single queries: " + secondsSingleQuery + " seconds");

            checkTime("single selects", secondsSingleQuery);

            PreparedStatement pStmt2 = this.conn.prepareStatement("SELECT field2, field3, field4, field5 FROM testBug6359 WHERE pk_field=?");

            long beginFiveQueries = currentTimeMillis();

            for (int i = 0; i < numSelects; i++) {

                for (int j = 0; j < 5; j++) {
                    pStmt2.setInt(1, j);
                    this.rs = pStmt2.executeQuery();
                }
            }

            long endFiveQueries = currentTimeMillis();

            double secondsFiveQueries = ((double) endFiveQueries - (double) beginFiveQueries) / 1000;

            logDebug("time to execute " + numSelects + " 5 standalone queries: " + secondsFiveQueries + " seconds");

            checkTime("5 standalone queries", secondsFiveQueries);

            long end = currentTimeMillis();

            double seconds = ((double) end - (double) begin) / 1000;

            logDebug("time to execute " + numSelects + " selects: " + seconds + " seconds");

            checkTime("total time all queries", seconds);
        }
    }

}
