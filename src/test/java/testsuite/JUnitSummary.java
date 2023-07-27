/*
 * Copyright (c) 2020, 2023, Oracle and/or its affiliates.
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

package testsuite;

import java.io.PrintStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;

public class JUnitSummary implements TestExecutionListener {

    private AtomicInteger numSkippedInTestSet = new AtomicInteger();
    private AtomicInteger numAbortedInTestSet = new AtomicInteger();
    private AtomicInteger numSucceededInTestSet = new AtomicInteger();
    private AtomicInteger numFailedInTestSet = new AtomicInteger();
    private Instant testSetStartTime;

    PrintStream out = null;

    public JUnitSummary() {
        this.out = System.out;
    }

    private void resetCountsForNewTestSet() {
        this.numSkippedInTestSet.set(0);
        this.numAbortedInTestSet.set(0);
        this.numSucceededInTestSet.set(0);
        this.numFailedInTestSet.set(0);
        this.testSetStartTime = Instant.now();
    }

    @Override
    public void executionStarted(TestIdentifier testIdentifier) {
        Optional<String> parentId = testIdentifier.getParentId();
        if (parentId.isPresent() && parentId.get().indexOf('/') < 0) {
            println("\nRunning " + testIdentifier.getLegacyReportingName());
            resetCountsForNewTestSet();
        }
    }

    @Override
    public void executionSkipped(TestIdentifier testIdentifier, String reason) {
        this.numSkippedInTestSet.incrementAndGet();
    }

    @Override
    public void executionFinished(TestIdentifier testIdentifier, TestExecutionResult testExecutionResult) {
        Optional<String> parentId = testIdentifier.getParentId();
        if (parentId.isPresent() && parentId.get().indexOf('/') < 0) {
            int totalTestsInClass = this.numSucceededInTestSet.get() + this.numAbortedInTestSet.get() + this.numFailedInTestSet.get()
                    + this.numSkippedInTestSet.get();
            Duration duration = Duration.between(this.testSetStartTime, Instant.now());
            double numSeconds = (double) duration.toMillis() / 1_000;
            String summary = String.format("Tests run: %d, Failures: %d, Aborted: %d, Skipped: %d, Time elapsed: %f sec", totalTestsInClass,
                    this.numFailedInTestSet.get(), this.numAbortedInTestSet.get(), this.numSkippedInTestSet.get(), numSeconds);
            println(summary);
        } else if (testIdentifier.isTest()) {
            switch (testExecutionResult.getStatus()) {
                case SUCCESSFUL:
                    this.numSucceededInTestSet.incrementAndGet();
                    break;
                case ABORTED:
                    println("   Aborted: " + testIdentifier.getDisplayName());
                    this.numAbortedInTestSet.incrementAndGet();
                    break;
                case FAILED:
                    println("   Failed: " + testIdentifier.getDisplayName());
                    this.numFailedInTestSet.incrementAndGet();
                    break;
            }
        }
    }

    private void println(String str) {
        this.out.println(str);
    }

}
