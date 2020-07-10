/*
 * Copyright (c) 2017, 2020, Oracle and/or its affiliates.
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

package com.mysql.cj.log;

public class BaseMetricsHolder {

    private final static int HISTOGRAM_BUCKETS = 20;

    /**
     * If gathering metrics, what was the execution time of the longest query so
     * far ?
     */
    private long longestQueryTimeMs = 0;

    private long maximumNumberTablesAccessed = 0;

    private long minimumNumberTablesAccessed = Long.MAX_VALUE;

    /** When was the last time we reported metrics? */
    //private long metricsLastReportedMs;

    private long numberOfPreparedExecutes = 0;

    private long numberOfPrepares = 0;

    private long numberOfQueriesIssued = 0;

    private long numberOfResultSetsCreated = 0;

    private long[] numTablesMetricsHistBreakpoints;

    private int[] numTablesMetricsHistCounts;

    private long[] oldHistBreakpoints = null;

    private int[] oldHistCounts = null;

    private long shortestQueryTimeMs = Long.MAX_VALUE;

    private double totalQueryTimeMs = 0;

    private long[] perfMetricsHistBreakpoints;

    private int[] perfMetricsHistCounts;

    private long queryTimeCount;
    private double queryTimeSum;
    private double queryTimeSumSquares;
    private double queryTimeMean;

    private void createInitialHistogram(long[] breakpoints, long lowerBound, long upperBound) {

        double bucketSize = (((double) upperBound - (double) lowerBound) / HISTOGRAM_BUCKETS) * 1.25;

        if (bucketSize < 1) {
            bucketSize = 1;
        }

        for (int i = 0; i < HISTOGRAM_BUCKETS; i++) {
            breakpoints[i] = lowerBound;
            lowerBound += bucketSize;
        }
    }

    private void addToHistogram(int[] histogramCounts, long[] histogramBreakpoints, long value, int numberOfTimes, long currentLowerBound,
            long currentUpperBound) {
        if (histogramCounts == null) {
            createInitialHistogram(histogramBreakpoints, currentLowerBound, currentUpperBound);
        } else {
            for (int i = 0; i < HISTOGRAM_BUCKETS; i++) {
                if (histogramBreakpoints[i] >= value) {
                    histogramCounts[i] += numberOfTimes;

                    break;
                }
            }
        }
    }

    private void addToPerformanceHistogram(long value, int numberOfTimes) {
        checkAndCreatePerformanceHistogram();

        addToHistogram(this.perfMetricsHistCounts, this.perfMetricsHistBreakpoints, value, numberOfTimes,
                this.shortestQueryTimeMs == Long.MAX_VALUE ? 0 : this.shortestQueryTimeMs, this.longestQueryTimeMs);
    }

    private void addToTablesAccessedHistogram(long value, int numberOfTimes) {
        checkAndCreateTablesAccessedHistogram();

        addToHistogram(this.numTablesMetricsHistCounts, this.numTablesMetricsHistBreakpoints, value, numberOfTimes,
                this.minimumNumberTablesAccessed == Long.MAX_VALUE ? 0 : this.minimumNumberTablesAccessed, this.maximumNumberTablesAccessed);
    }

    private void checkAndCreatePerformanceHistogram() {
        if (this.perfMetricsHistCounts == null) {
            this.perfMetricsHistCounts = new int[HISTOGRAM_BUCKETS];
        }

        if (this.perfMetricsHistBreakpoints == null) {
            this.perfMetricsHistBreakpoints = new long[HISTOGRAM_BUCKETS];
        }
    }

    private void checkAndCreateTablesAccessedHistogram() {
        if (this.numTablesMetricsHistCounts == null) {
            this.numTablesMetricsHistCounts = new int[HISTOGRAM_BUCKETS];
        }

        if (this.numTablesMetricsHistBreakpoints == null) {
            this.numTablesMetricsHistBreakpoints = new long[HISTOGRAM_BUCKETS];
        }
    }

    /**
     * @param queryTimeMs
     *            query execution time in milliseconds
     */
    public void registerQueryExecutionTime(long queryTimeMs) {
        if (queryTimeMs > this.longestQueryTimeMs) {
            this.longestQueryTimeMs = queryTimeMs;

            repartitionPerformanceHistogram();
        }

        addToPerformanceHistogram(queryTimeMs, 1);

        if (queryTimeMs < this.shortestQueryTimeMs) {
            this.shortestQueryTimeMs = (queryTimeMs == 0) ? 1 : queryTimeMs;
        }

        this.numberOfQueriesIssued++;

        this.totalQueryTimeMs += queryTimeMs;
    }

    private void repartitionHistogram(int[] histCounts, long[] histBreakpoints, long currentLowerBound, long currentUpperBound) {

        if (this.oldHistCounts == null) {
            this.oldHistCounts = new int[histCounts.length];
            this.oldHistBreakpoints = new long[histBreakpoints.length];
        }

        System.arraycopy(histCounts, 0, this.oldHistCounts, 0, histCounts.length);

        System.arraycopy(histBreakpoints, 0, this.oldHistBreakpoints, 0, histBreakpoints.length);

        createInitialHistogram(histBreakpoints, currentLowerBound, currentUpperBound);

        for (int i = 0; i < HISTOGRAM_BUCKETS; i++) {
            addToHistogram(histCounts, histBreakpoints, this.oldHistBreakpoints[i], this.oldHistCounts[i], currentLowerBound, currentUpperBound);
        }
    }

    private void repartitionPerformanceHistogram() {
        checkAndCreatePerformanceHistogram();

        repartitionHistogram(this.perfMetricsHistCounts, this.perfMetricsHistBreakpoints,
                this.shortestQueryTimeMs == Long.MAX_VALUE ? 0 : this.shortestQueryTimeMs, this.longestQueryTimeMs);
    }

    private void repartitionTablesAccessedHistogram() {
        checkAndCreateTablesAccessedHistogram();

        repartitionHistogram(this.numTablesMetricsHistCounts, this.numTablesMetricsHistBreakpoints,
                this.minimumNumberTablesAccessed == Long.MAX_VALUE ? 0 : this.minimumNumberTablesAccessed, this.maximumNumberTablesAccessed);
    }

    public void reportMetrics(Log log) {
        StringBuilder logMessage = new StringBuilder(256);

        logMessage.append("** Performance Metrics Report **\n");
        logMessage.append("\nLongest reported query: " + this.longestQueryTimeMs + " ms");
        logMessage.append("\nShortest reported query: " + this.shortestQueryTimeMs + " ms");
        logMessage.append("\nAverage query execution time: " + (this.totalQueryTimeMs / this.numberOfQueriesIssued) + " ms");
        logMessage.append("\nNumber of statements executed: " + this.numberOfQueriesIssued);
        logMessage.append("\nNumber of result sets created: " + this.numberOfResultSetsCreated);
        logMessage.append("\nNumber of statements prepared: " + this.numberOfPrepares);
        logMessage.append("\nNumber of prepared statement executions: " + this.numberOfPreparedExecutes);

        if (this.perfMetricsHistBreakpoints != null) {
            logMessage.append("\n\n\tTiming Histogram:\n");
            int maxNumPoints = 20;
            int highestCount = Integer.MIN_VALUE;

            for (int i = 0; i < (HISTOGRAM_BUCKETS); i++) {
                if (this.perfMetricsHistCounts[i] > highestCount) {
                    highestCount = this.perfMetricsHistCounts[i];
                }
            }

            if (highestCount == 0) {
                highestCount = 1; // avoid DIV/0
            }

            for (int i = 0; i < (HISTOGRAM_BUCKETS - 1); i++) {

                if (i == 0) {
                    logMessage.append("\n\tless than " + this.perfMetricsHistBreakpoints[i + 1] + " ms: \t" + this.perfMetricsHistCounts[i]);
                } else {
                    logMessage.append("\n\tbetween " + this.perfMetricsHistBreakpoints[i] + " and " + this.perfMetricsHistBreakpoints[i + 1] + " ms: \t"
                            + this.perfMetricsHistCounts[i]);
                }

                logMessage.append("\t");

                int numPointsToGraph = (int) (maxNumPoints * ((double) this.perfMetricsHistCounts[i] / highestCount));

                for (int j = 0; j < numPointsToGraph; j++) {
                    logMessage.append("*");
                }

                if (this.longestQueryTimeMs < this.perfMetricsHistCounts[i + 1]) {
                    break;
                }
            }

            if (this.perfMetricsHistBreakpoints[HISTOGRAM_BUCKETS - 2] < this.longestQueryTimeMs) {
                logMessage.append("\n\tbetween ");
                logMessage.append(this.perfMetricsHistBreakpoints[HISTOGRAM_BUCKETS - 2]);
                logMessage.append(" and ");
                logMessage.append(this.perfMetricsHistBreakpoints[HISTOGRAM_BUCKETS - 1]);
                logMessage.append(" ms: \t");
                logMessage.append(this.perfMetricsHistCounts[HISTOGRAM_BUCKETS - 1]);
            }
        }

        if (this.numTablesMetricsHistBreakpoints != null) {
            logMessage.append("\n\n\tTable Join Histogram:\n");
            int maxNumPoints = 20;
            int highestCount = Integer.MIN_VALUE;

            for (int i = 0; i < (HISTOGRAM_BUCKETS); i++) {
                if (this.numTablesMetricsHistCounts[i] > highestCount) {
                    highestCount = this.numTablesMetricsHistCounts[i];
                }
            }

            if (highestCount == 0) {
                highestCount = 1; // avoid DIV/0
            }

            for (int i = 0; i < (HISTOGRAM_BUCKETS - 1); i++) {

                if (i == 0) {
                    logMessage.append("\n\t" + this.numTablesMetricsHistBreakpoints[i + 1] + " tables or less: \t\t" + this.numTablesMetricsHistCounts[i]);
                } else {
                    logMessage.append("\n\tbetween " + this.numTablesMetricsHistBreakpoints[i] + " and " + this.numTablesMetricsHistBreakpoints[i + 1]
                            + " tables: \t" + this.numTablesMetricsHistCounts[i]);
                }

                logMessage.append("\t");

                int numPointsToGraph = (int) (maxNumPoints * ((double) this.numTablesMetricsHistCounts[i] / highestCount));

                for (int j = 0; j < numPointsToGraph; j++) {
                    logMessage.append("*");
                }

                if (this.maximumNumberTablesAccessed < this.numTablesMetricsHistBreakpoints[i + 1]) {
                    break;
                }
            }

            if (this.numTablesMetricsHistBreakpoints[HISTOGRAM_BUCKETS - 2] < this.maximumNumberTablesAccessed) {
                logMessage.append("\n\tbetween ");
                logMessage.append(this.numTablesMetricsHistBreakpoints[HISTOGRAM_BUCKETS - 2]);
                logMessage.append(" and ");
                logMessage.append(this.numTablesMetricsHistBreakpoints[HISTOGRAM_BUCKETS - 1]);
                logMessage.append(" tables: ");
                logMessage.append(this.numTablesMetricsHistCounts[HISTOGRAM_BUCKETS - 1]);
            }
        }

        log.logInfo(logMessage);

        //this.metricsLastReportedMs = System.currentTimeMillis();
    }

    ///**
    // * Reports currently collected metrics if this feature is enabled and the
    // * timeout has passed.
    // */
    //protected void reportMetricsIfNeeded() {
    //    if (this.gatherPerfMetrics.getValue()) {
    //        if ((System.currentTimeMillis() - this.metricsLastReportedMs) > getPropertySet()
    //                .getIntegerReadableProperty(PropertyDefinitions.PNAME_reportMetricsIntervalMillis).getValue()) {
    //            reportMetrics();
    //        }
    //    }
    //}

    public void reportNumberOfTablesAccessed(int numTablesAccessed) {
        if (numTablesAccessed < this.minimumNumberTablesAccessed) {
            this.minimumNumberTablesAccessed = numTablesAccessed;
        }

        if (numTablesAccessed > this.maximumNumberTablesAccessed) {
            this.maximumNumberTablesAccessed = numTablesAccessed;

            repartitionTablesAccessedHistogram();
        }

        addToTablesAccessedHistogram(numTablesAccessed, 1);
    }

    public void incrementNumberOfPreparedExecutes() {
        this.numberOfPreparedExecutes++;

        // We need to increment this, because server-side prepared statements bypass any execution by the connection itself...
        this.numberOfQueriesIssued++;
    }

    public void incrementNumberOfPrepares() {
        this.numberOfPrepares++;
    }

    public void incrementNumberOfResultSetsCreated() {
        this.numberOfResultSetsCreated++;
    }

    public void reportQueryTime(long millisOrNanos) {
        this.queryTimeCount++;
        this.queryTimeSum += millisOrNanos;
        this.queryTimeSumSquares += (millisOrNanos * millisOrNanos);
        this.queryTimeMean = ((this.queryTimeMean * (this.queryTimeCount - 1)) + millisOrNanos) / this.queryTimeCount;
    }

    /**
     * Update statistics that allows the driver to determine if a query is slow enough to be logged,
     * and return the estimation result for millisOrNanos value.
     * <p>
     * Used in case autoSlowLog=true.
     * 
     * @param millisOrNanos
     *            query execution time
     * @return true if millisOrNanos is outside the 99th percentile?
     */
    public boolean checkAbonormallyLongQuery(long millisOrNanos) {
        boolean res = false;
        if (this.queryTimeCount > 14) { // need a minimum amount for this to make sense
            double stddev = Math.sqrt((this.queryTimeSumSquares - ((this.queryTimeSum * this.queryTimeSum) / this.queryTimeCount)) / (this.queryTimeCount - 1));
            res = millisOrNanos > (this.queryTimeMean + 5 * stddev);
        }
        reportQueryTime(millisOrNanos);
        return res;

    }
}
