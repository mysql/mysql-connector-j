ant -lib lib/ant-junitlauncher-1.10.15.jar -lib lib/ant-junit-1.10.15.jar \
  -Dcom.mysql.cj.testsuite.test.class=testsuite.regression.StatementRegressionTest \
  -Dcom.mysql.cj.testsuite.test.methods=testBug119245_IntoInsideVar \
  -Dcom.mysql.cj.testsuite.url=jdbc:mysql://test:test@127.0.0.1:3306/test \
  test
