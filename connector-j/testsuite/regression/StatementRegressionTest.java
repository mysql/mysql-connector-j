package testsuite.regression;

import testsuite.BaseTestCase;


/**
 * Regression tests for the Statement class
 * 
 * @author Mark Matthews
 */
public class StatementRegressionTest
    extends BaseTestCase {

    /**
     * Constructor for StatementRegressionTest.
     * @param name
     */
    public StatementRegressionTest(String name) {
        super(name);
    }

    /**
     * Tests a bug where Statement.setFetchSize() does not
     * work for values other than 0 or Integer.MIN_VALUE
     */
    public void testSetFetchSize()
                          throws Exception {

        int oldFetchSize = stmt.getFetchSize();

        try {
            stmt.setFetchSize(10);
        } finally {
            stmt.setFetchSize(oldFetchSize);
        }
    }
}