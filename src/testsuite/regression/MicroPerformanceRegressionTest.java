/*
 Copyright (C) 2002-2004 MySQL AB

 This program is free software; you can redistribute it and/or modify
 it under the terms of version 2 of the GNU General Public License as 
 published by the Free Software Foundation.

 There are special exceptions to the terms and conditions of the GPL 
 as it is applied to this software. View the full text of the 
 exception in file EXCEPTIONS-CONNECTOR-J in the directory of this 
 software distribution.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA



 */
package testsuite.regression;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import testsuite.BaseTestCase;

/**
 * Microperformance benchmarks to track increase/decrease in performance of core
 * methods in the driver over time.
 * 
 * @author Mark Matthews
 * 
 * @version $Id: MicroPerformanceRegressionTest.java,v 1.1.2.1 2005/05/13
 *          18:58:38 mmatthews Exp $
 */
public class MicroPerformanceRegressionTest extends BaseTestCase {

	private double scaleFactor = 1.0;

	private final static int ORIGINAL_LOOP_TIME_MS = 2300;

	private final static double LEEWAY = 3.0;

	private final static Map BASELINE_TIMES = new HashMap();

	static {
		BASELINE_TIMES.put("ResultSet.getInt()", new Double(0.00661));
		BASELINE_TIMES.put("ResultSet.getDouble()", new Double(0.00671));
		BASELINE_TIMES.put("ResultSet.getTime()", new Double(0.02033));
		BASELINE_TIMES.put("ResultSet.getTimestamp()", new Double(0.02363));
		BASELINE_TIMES.put("ResultSet.getDate()", new Double(0.02223));
		BASELINE_TIMES.put("ResultSet.getString()", new Double(0.00982));
		BASELINE_TIMES.put("ResultSet.getObject() on a string", new Double(
				0.00861));
		BASELINE_TIMES
				.put("Connection.prepareStatement()", new Double(0.18547));
		BASELINE_TIMES.put("PreparedStatement.setInt()", new Double(0.0011));
		BASELINE_TIMES
				.put("PreparedStatement.setDouble()", new Double(0.00671));
		BASELINE_TIMES.put("PreparedStatement.setTime()", new Double(0.0642));
		BASELINE_TIMES.put("PreparedStatement.setTimestamp()", new Double(
				0.03184));
		BASELINE_TIMES.put("PreparedStatement.setDate()", new Double(0.12248));
		BASELINE_TIMES
				.put("PreparedStatement.setString()", new Double(0.01512));
		BASELINE_TIMES.put("PreparedStatement.setObject() on a string",
				new Double(0.01923));
		BASELINE_TIMES.put("single selects", new Double(46));
		BASELINE_TIMES.put("5 standalone queries", new Double(146));
		BASELINE_TIMES.put("total time all queries", new Double(190));
	}

	public MicroPerformanceRegressionTest(String name) {
		super(name);
	}

	/**
	 * Runs all test cases in this test suite
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		junit.textui.TestRunner.run(MicroPerformanceRegressionTest.class);
	}

	/**
	 * Tests result set accessors performance.
	 * 
	 * @throws Exception
	 *             if the performance of these methods does not meet
	 *             expectations.
	 */
	public void testResultSetAccessors() throws Exception {
		try {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS marktest");
			this.stmt
					.executeUpdate("CREATE TABLE marktest(intField INT, floatField DOUBLE, timeField TIME, datetimeField DATETIME, stringField VARCHAR(64))");
			this.stmt
					.executeUpdate("INSERT INTO marktest VALUES (123456789, 12345.6789, NOW(), NOW(), 'abcdefghijklmnopqrstuvABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@')");

			this.rs = this.stmt
					.executeQuery("SELECT intField, floatField, timeField, datetimeField, stringField FROM marktest");

			this.rs.next();

			int numLoops = 100000;

			long start = System.currentTimeMillis();

			for (int i = 0; i < numLoops; i++) {
				this.rs.getInt(1);
			}

			double getIntAvgMs = (double) (System.currentTimeMillis() - start)
					/ numLoops;

			checkTime("ResultSet.getInt()", getIntAvgMs);

			start = System.currentTimeMillis();

			for (int i = 0; i < numLoops; i++) {
				this.rs.getDouble(2);
			}

			double getDoubleAvgMs = (double) (System.currentTimeMillis() - start)
					/ numLoops;

			checkTime("ResultSet.getDouble()", getDoubleAvgMs);

			start = System.currentTimeMillis();

			for (int i = 0; i < numLoops; i++) {
				this.rs.getTime(3);
			}

			double getTimeAvgMs = (double) (System.currentTimeMillis() - start)
					/ numLoops;

			checkTime("ResultSet.getTime()", getTimeAvgMs);

			start = System.currentTimeMillis();

			for (int i = 0; i < numLoops; i++) {
				this.rs.getTimestamp(4);
			}

			double getTimestampAvgMs = (double) (System.currentTimeMillis() - start)
					/ numLoops;

			checkTime("ResultSet.getTimestamp()", getTimestampAvgMs);

			start = System.currentTimeMillis();

			for (int i = 0; i < numLoops; i++) {
				this.rs.getDate(4);
			}

			double getDateAvgMs = (double) (System.currentTimeMillis() - start)
					/ numLoops;

			checkTime("ResultSet.getDate()", getDateAvgMs);

			start = System.currentTimeMillis();

			for (int i = 0; i < numLoops; i++) {
				this.rs.getString(5);
			}

			double getStringAvgMs = (double) (System.currentTimeMillis() - start)
					/ numLoops;

			checkTime("ResultSet.getString()", getStringAvgMs);

			start = System.currentTimeMillis();

			for (int i = 0; i < numLoops; i++) {
				this.rs.getObject(5);
			}

			double getStringObjAvgMs = (double) (System.currentTimeMillis() - start)
					/ numLoops;

			checkTime("ResultSet.getObject() on a string", getStringObjAvgMs);
		} finally {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS marktest");
		}
	}

	public void testPreparedStatementTimes() throws Exception {
		try {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS marktest");
			this.stmt
					.executeUpdate("CREATE TABLE marktest(intField INT, floatField DOUBLE, timeField TIME, datetimeField DATETIME, stringField VARCHAR(64))");
			this.stmt
					.executeUpdate("INSERT INTO marktest VALUES (123456789, 12345.6789, NOW(), NOW(), 'abcdefghijklmnopqrstuvABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@')");

			long start = System.currentTimeMillis();

			long blockStart = System.currentTimeMillis();
			long lastBlock = 0;

			int numLoops = 100000;

			int numPrepares = 100000;

			if (versionMeetsMinimum(4, 1)) {
				numPrepares = 10000; // we don't need to do so many for
				// server-side prep statements...
			}

			for (int i = 0; i < numPrepares; i++) {
				if (i % 1000 == 0) {

					long blockEnd = System.currentTimeMillis();

					long totalTime = blockEnd - blockStart;

					blockStart = blockEnd;

					StringBuffer messageBuf = new StringBuffer();

					messageBuf.append(i
							+ " prepares, the last 1000 prepares took "
							+ totalTime + " ms");

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

				PreparedStatement pStmt = this.conn
						.prepareStatement("INSERT INTO test.marktest VALUES (?, ?, ?, ?, ?)");
				pStmt.close();
			}

			double getPrepareStmtAvgMs = (double) (System.currentTimeMillis() - start)
					/ numPrepares;

			// checkTime("Connection.prepareStatement()", getPrepareStmtAvgMs);

			PreparedStatement pStmt = this.conn
					.prepareStatement("INSERT INTO marktest VALUES (?, ?, ?, ?, ?)");

			System.out.println(pStmt.toString());

			start = System.currentTimeMillis();

			for (int i = 0; i < numLoops; i++) {
				pStmt.setInt(1, 1);
			}

			System.out.println(pStmt.toString());

			double setIntAvgMs = (double) (System.currentTimeMillis() - start)
					/ numLoops;

			checkTime("PreparedStatement.setInt()", setIntAvgMs);

			start = System.currentTimeMillis();

			for (int i = 0; i < numLoops; i++) {
				pStmt.setDouble(2, 1234567890.1234);
			}

			double setDoubleAvgMs = (double) (System.currentTimeMillis() - start)
					/ numLoops;

			checkTime("PreparedStatement.setDouble()", setDoubleAvgMs);

			start = System.currentTimeMillis();

			Time tm = new Time(start);

			for (int i = 0; i < numLoops; i++) {
				pStmt.setTime(3, tm);
			}

			double setTimeAvgMs = (double) (System.currentTimeMillis() - start)
					/ numLoops;

			checkTime("PreparedStatement.setTime()", setTimeAvgMs);

			start = System.currentTimeMillis();

			Timestamp ts = new Timestamp(start);

			for (int i = 0; i < numLoops; i++) {
				pStmt.setTimestamp(4, ts);
			}

			double setTimestampAvgMs = (double) (System.currentTimeMillis() - start)
					/ numLoops;

			checkTime("PreparedStatement.setTimestamp()", setTimestampAvgMs);

			start = System.currentTimeMillis();

			Date dt = new Date(start);

			for (int i = 0; i < numLoops; i++) {
				pStmt.setDate(4, dt);
			}

			double setDateAvgMs = (double) (System.currentTimeMillis() - start)
					/ numLoops;

			checkTime("PreparedStatement.setDate()", setDateAvgMs);

			start = System.currentTimeMillis();

			for (int i = 0; i < numLoops; i++) {
				pStmt
						.setString(5,
								"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@");
			}

			double setStringAvgMs = (double) (System.currentTimeMillis() - start)
					/ numLoops;

			checkTime("PreparedStatement.setString()", setStringAvgMs);

			start = System.currentTimeMillis();

			for (int i = 0; i < numLoops; i++) {
				pStmt
						.setObject(5,
								"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@");
			}

			double setStringObjAvgMs = (double) (System.currentTimeMillis() - start)
					/ numLoops;

			checkTime("PreparedStatement.setObject() on a string",
					setStringObjAvgMs);

			start = System.currentTimeMillis();

		} finally {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS marktest");
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see junit.framework.TestCase#setUp()
	 */
	public void setUp() throws Exception {
		super.setUp();

		System.out.println("Calculating performance scaling factor...");
		// Run this simple test to get some sort of performance scaling factor,
		// compared to
		// the development environment. This should help reduce false-positives
		// on this test.
		int numLoops = 10000;

		long start = System.currentTimeMillis();

		for (int j = 0; j < 2000; j++) {
			StringBuffer buf = new StringBuffer(numLoops);

			for (int i = 0; i < numLoops; i++) {
				buf.append('a');
			}
		}

		long elapsedTime = System.currentTimeMillis() - start;

		System.out.println("Elapsed time for factor: " + elapsedTime);

		this.scaleFactor = (double) elapsedTime
				/ (double) ORIGINAL_LOOP_TIME_MS;

		System.out
				.println("Performance scaling factor is: " + this.scaleFactor);
	}

	private void checkTime(String testType, double avgExecTimeMs)
			throws Exception {
		
		double adjustForVendor = 1.0D;

		if (isRunningOnJRockit()) {
			adjustForVendor = 4.0D;
		}

		Double baselineExecTimeMs = (Double) BASELINE_TIMES.get(testType);

		if (baselineExecTimeMs == null) {
			throw new Exception("No baseline time recorded for test '"
					+ testType + "'");
		}

		double acceptableTime = LEEWAY * baselineExecTimeMs.doubleValue()
				* this.scaleFactor * adjustForVendor;

		assertTrue("Average execution time of " + avgExecTimeMs
				+ " ms. exceeded baseline * leeway of " + acceptableTime
				+ " ms.", (avgExecTimeMs <= acceptableTime));
	}

	public void testBug6359() throws Exception {
		if (runLongTests()) {
			int numRows = 550000;
			int numSelects = 100000;

			try {
				this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug6359");
				this.stmt
						.executeUpdate("CREATE TABLE testBug6359 (pk_field INT PRIMARY KEY NOT NULL AUTO_INCREMENT, field1 INT, field2 INT, field3 INT, field4 INT, field5 INT, field6 INT, field7 INT, field8 INT, field9 INT,  INDEX (field1))");

				PreparedStatement pStmt = this.conn
						.prepareStatement("INSERT INTO testBug6359 (field1, field2, field3, field4, field5, field6, field7, field8, field9) VALUES (?, 1, 2, 3, 4, 5, 6, 7, 8)");

				logDebug("Loading " + numRows + " rows...");

				for (int i = 0; i < numRows; i++) {
					pStmt.setInt(1, i);
					pStmt.executeUpdate();

					if ((i % 10000) == 0) {
						logDebug(i + " rows loaded so far");
					}
				}

				logDebug("Finished loading rows");

				long begin = System.currentTimeMillis();

				long beginSingleQuery = System.currentTimeMillis();

				for (int i = 0; i < numSelects; i++) {
					this.rs = this.stmt
							.executeQuery("SELECT pk_field FROM testBug6359 WHERE field1 BETWEEN 1 AND 5");
				}

				long endSingleQuery = System.currentTimeMillis();

				double secondsSingleQuery = ((double) endSingleQuery - (double) beginSingleQuery) / 1000;

				logDebug("time to execute " + numSelects + " single queries: "
						+ secondsSingleQuery + " seconds");

				checkTime("single selects", secondsSingleQuery);

				PreparedStatement pStmt2 = this.conn
						.prepareStatement("SELECT field2, field3, field4, field5 FROM testBug6359 WHERE pk_field=?");

				long beginFiveQueries = System.currentTimeMillis();

				for (int i = 0; i < numSelects; i++) {

					for (int j = 0; j < 5; j++) {
						pStmt2.setInt(1, j);
						pStmt2.executeQuery();
					}
				}

				long endFiveQueries = System.currentTimeMillis();

				double secondsFiveQueries = ((double) endFiveQueries - (double) beginFiveQueries) / 1000;

				logDebug("time to execute " + numSelects
						+ " 5 standalone queries: " + secondsFiveQueries
						+ " seconds");

				checkTime("5 standalone queries", secondsFiveQueries);

				long end = System.currentTimeMillis();

				double seconds = ((double) end - (double) begin) / 1000;

				logDebug("time to execute " + numSelects + " selects: "
						+ seconds + " seconds");

				checkTime("total time all queries", seconds);
			} finally {
				this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug6359");
			}
		}
	}

}
