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

package testsuite.perf;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;

import testsuite.BaseTestCase;

/**
 * Base class for performance test cases. Handles statistics.
 */
public abstract class BasePerfTest extends BaseTestCase {
    /**
     * Confidence interval lookup table, indexed by degrees of freedom at 95%.
     */
    private static final double[] T95 = { 12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262, 2.228, 2.201, 2.179, 2.160, 2.145, 2.131, 2.120,
            2.110, 2.101, 2.093, 2.086, 2.080, 2.074, 2.069, 2.064, 2.060, 2.056, 2.052, 2.048, 2.045, 2.042 };

    /**
     * Confidence interval lookup table, indexed by degrees of freedom at 99%.
     */
    private static final double[] T99 = { 63.657, 9.925, 5.841, 4.604, 4.032, 3.707, 3.499, 3.355, 3.250, 3.169, 3.106, 3.055, 3.012, 2.977, 2.947, 2.921,
            2.898, 2.878, 2.861, 2.845, 2.831, 2.819, 2.807, 2.797, 2.787, 2.779, 2.771, 2.763, 2.756, 2.750 };

    static NumberFormat numberFormatter = NumberFormat.getInstance();

    static {
        numberFormatter.setMaximumFractionDigits(4);
        numberFormatter.setMinimumFractionDigits(4);
    }

    /**
     * List of values for each iteration
     */
    private List<Double> testValuesList = new ArrayList<Double>();

    private double confidenceLevel = 95; // 95% by default

    private double confidenceValue = 0;

    private double intervalWidth = 0.1;

    private double meanValue = 0;

    private double squareSumValue = 0;

    private double sumValue = 0;

    private double variationValue = 0;

    /**
     * The number of iterations that we have performed
     */
    private int numIterations = 0;

    /**
     * Creates a new BasePerfTest object.
     * 
     * @param name
     *            the testcase name to perform.
     */
    public BasePerfTest(String name) {
        super(name);
    }

    /**
     * Returns the meanValue.
     * 
     * @return double
     */
    public double getMeanValue() {
        return this.meanValue;
    }

    /**
     * Sub-classes should override this to perform the operation to be measured.
     * 
     * @throws Exception
     *             if an error occurs.
     */
    protected abstract void doOneIteration() throws Exception;

    /**
     * Returns the current confidence level.
     * 
     * @return the current confindence level.
     */
    protected double getCurrentConfidence() {
        return (this.intervalWidth - this.confidenceValue) * 100;
    }

    /**
     * Returns the current margin of error.
     * 
     * @return the current margin of error.
     */
    protected double getMarginOfError() {
        return getConfidenceLookup() * (getStandardDeviationP() / Math.sqrt(this.numIterations));
    }

    /**
     * Returns the current STDDEV.
     * 
     * @return the current STDDEV
     */
    protected double getStandardDeviationP() {
        if (this.numIterations < 1) {
            return 0;
        }

        return Math.sqrt(((this.numIterations * this.squareSumValue) - (this.sumValue * this.sumValue)) / (this.numIterations * this.numIterations));
    }

    /**
     * Adds one test result to the statistics.
     * 
     * @param value
     *            a single result representing the value being measured in the
     *            test.
     */
    protected void addResult(double value) {
        this.numIterations++;
        this.testValuesList.add(new Double(value));

        this.sumValue += value;
        this.squareSumValue += (value * value);
        this.meanValue = this.sumValue / this.numIterations;
        this.variationValue = (this.squareSumValue / this.numIterations) - (this.meanValue * this.meanValue);

        // Can only have confidence when more than one test has been completed
        if (this.numIterations > 1) {
            this.confidenceValue = this.intervalWidth
                    - ((2.0 * getConfidenceLookup() * Math.sqrt(this.variationValue / (this.numIterations - 1.0))) / this.meanValue);
        }
    }

    /**
     * Calls doIteration() the <code>numIterations</code> times, displaying
     * the mean, std, margin of error and confidence level.
     * 
     * @param num_iterations
     *            the number of iterations to perform ( < 30)
     * @throws Exception
     *             if an error occurs.
     */
    protected void doIterations(int num_iterations) throws Exception {
        for (int i = 0; i < num_iterations; i++) {
            doOneIteration();
        }
    }

    /**
     * Reports the current results to STDOUT, preceeded by <code>additionalMessage</code> if not null.
     * 
     * @param additionalMessage
     *            the additional message to print, or null if no message.
     */
    protected synchronized void reportResults(String additionalMessage) {
        StringBuffer messageBuf = new StringBuffer();

        if (additionalMessage != null) {
            messageBuf.append(additionalMessage);
            messageBuf.append(": ");
        }

        messageBuf.append(" mean: ");
        messageBuf.append(numberFormatter.format(this.meanValue));
        messageBuf.append(" stdevp: ");
        messageBuf.append(numberFormatter.format(getStandardDeviationP()));
        messageBuf.append(" m-o-e: ");
        messageBuf.append(numberFormatter.format(getMarginOfError()));

        System.out.println(messageBuf.toString());
    }

    private double getConfidenceLookup() {
        if (this.confidenceLevel == 95) {
            return T95[this.numIterations - 1];
        } else if (this.confidenceLevel == 99) {
            return T99[this.numIterations - 1];
        } else {
            throw new IllegalArgumentException("Confidence level must be 95 or 99");
        }
    }
}
