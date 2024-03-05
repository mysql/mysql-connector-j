/*
 * Copyright (c) 2021, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package testsuite.x.devapi;

import static com.mysql.cj.xdevapi.Expression.expr;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.mysql.cj.ServerVersion;
import com.mysql.cj.xdevapi.Collection;
import com.mysql.cj.xdevapi.DbDoc;
import com.mysql.cj.xdevapi.DbDocImpl;
import com.mysql.cj.xdevapi.DocResult;
import com.mysql.cj.xdevapi.JsonNumber;
import com.mysql.cj.xdevapi.JsonString;
import com.mysql.cj.xdevapi.RowResult;
import com.mysql.cj.xdevapi.Session;
import com.mysql.cj.xdevapi.SessionFactory;
import com.mysql.cj.xdevapi.Table;

public class RowLockingTest extends BaseCollectionTestCase {

    int CheckFlag = 0;

    static Throwable initException[] = null;

    static class MyUncaughtExceptionHandler implements UncaughtExceptionHandler {

        private int index = 0;

        MyUncaughtExceptionHandler(int n) {
            this.index = n;
        }

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            System.out.println("I caught the exception");
            initException[this.index] = e;
        }

    }

    public class SelectRowLock extends Thread {

        private int action;
        private int lock;
        private int shouldWait;
        private int bindVal;
        private String condition;

        SelectRowLock(int action, int lock, int shouldWait, int bindVal, String condition) {
            this.action = action;
            this.lock = lock;
            this.shouldWait = shouldWait;
            this.bindVal = bindVal;
            this.condition = condition;
        }

        @Override
        public void run() {
            System.out.println("Action is " + this.action);
            System.out.println("Lock is " + this.lock);
            System.out.println("Condition is " + this.condition);
            System.out.println("CheckFlag is " + RowLockingTest.this.CheckFlag);

            RowResult rows = null;
            Session sess = null;

            try {
                sess = new SessionFactory().getSession(RowLockingTest.this.baseUrl);
                Table table = sess.getDefaultSchema().getCollectionAsTable(RowLockingTest.this.collectionName);

                if (this.shouldWait == 1) {
                    System.out.println("wait started");
                    do {
                        Thread.sleep(1000);
                    } while (RowLockingTest.this.CheckFlag != 1);
                    System.out.println("wait ended");
                }

                sess.startTransaction();
                switch (this.action) {
                    case 1: {
                        if (this.lock == 1) {
                            rows = table.select("doc->$.F2").where(this.condition).bind("bVal", this.bindVal).lockExclusive().execute();
                        } else if (this.lock == 2) {
                            rows = table.select("doc->$.F2").where(this.condition).bind("bVal", this.bindVal).lockShared().execute();
                        }
                        rows.next();
                        break;
                    }
                    case 2: {
                        Map<String, Object> params = new HashMap<>();
                        params.put("bVal", this.bindVal);
                        if (this.lock == 1) {
                            rows = table.select("doc->$.F2").where(this.condition).bind(params).lockExclusive().execute();
                        } else if (this.lock == 2) {
                            rows = table.select("doc->$.F2").where(this.condition).bind(params).lockShared().execute();
                        }
                        rows.next();
                        break;
                    }
                    case 3: {
                        table.update().set("doc", expr("JSON_REPLACE(doc, \"$.F2\", \"NewData\")")).where(this.condition).bind("bVal", this.bindVal).execute();
                        break;
                    }
                    case 4: {
                        table.delete().where(this.condition).bind("bVal", this.bindVal).execute();
                        break;
                    }
                }

                if (this.shouldWait == 0) {
                    RowLockingTest.this.CheckFlag = 1;
                    Thread.sleep(10000);
                    if (2 == RowLockingTest.this.CheckFlag) {
                        throw new RuntimeException("Second thread moved ahead");
                    }
                } else if (this.shouldWait == 1) {
                    RowLockingTest.this.CheckFlag = 2;
                } else if (this.shouldWait == 2) {
                    RowLockingTest.this.CheckFlag = 1;
                    Thread.sleep(10000);
                    if (2 == RowLockingTest.this.CheckFlag) {
                        throw new RuntimeException("Second thread stuck even thoung different conditions executed");
                    }
                }

                sess.commit();

            } catch (Exception e) {
                System.err.print("InterruptedException: ");
                System.err.println(e.getMessage());
                e.printStackTrace();
                throw new RuntimeException(e);
            } finally {
                if (this.shouldWait != 1) {
                    RowLockingTest.this.CheckFlag = 1;
                }
                if (sess != null) {
                    sess.close();
                }
            }
        }

    }

    public class FindRowLock extends Thread {

        private int action;
        private int lock;
        private int shouldWait;
        private int bindVal;
        private String condition;

        FindRowLock(int action, int lock, int shouldWait, int bindVal, String condition) {
            this.action = action;
            this.lock = lock;
            this.shouldWait = shouldWait;
            this.bindVal = bindVal;
            this.condition = condition;
        }

        @SuppressWarnings("deprecation")
        @Override
        public void run() {
            System.out.println("Action is " + this.action);
            System.out.println("Lock is " + this.lock);
            System.out.println("Condition is " + this.condition);
            System.out.println("CheckFlag is " + RowLockingTest.this.CheckFlag);

            DocResult docs = null;
            Session sess = null;

            try {
                sess = new SessionFactory().getSession(RowLockingTest.this.baseUrl);
                Collection coll = sess.getDefaultSchema().getCollection(RowLockingTest.this.collectionName);

                if (this.shouldWait == 1) {
                    System.out.println("wait started");
                    do {
                        Thread.sleep(1000);
                    } while (RowLockingTest.this.CheckFlag != 1);
                    System.out.println("wait ended");
                }

                sess.startTransaction();
                switch (this.action) {
                    case 1: {
                        if (this.lock == 1) {
                            docs = coll.find(this.condition).bind(new Object[] { this.bindVal }).fields("$.F2 as F2").orderBy("$.F1").lockExclusive().execute();
                        } else if (this.lock == 2) {
                            docs = coll.find(this.condition).bind(new Object[] { this.bindVal }).fields("$.F2 as F2").orderBy("$.F1").lockShared().execute();
                        }
                        docs.next();
                        break;
                    }
                    case 2: {
                        coll.modify(this.condition).set("$.F2", "Data_New").bind(new Object[] { this.bindVal }).sort("$.F1 asc").execute();
                        break;
                    }
                    case 3: {
                        coll.remove(this.condition).bind(new Object[] { this.bindVal }).orderBy("$.F1 asc").execute();
                        break;
                    }
                }

                if (this.shouldWait == 0) {
                    RowLockingTest.this.CheckFlag = 1;
                    Thread.sleep(10000);
                    if (2 == RowLockingTest.this.CheckFlag) {
                        throw new RuntimeException("Second thread moved ahead");
                    }
                } else if (this.shouldWait == 1) {
                    RowLockingTest.this.CheckFlag = 2;
                } else if (this.shouldWait == 2) {
                    RowLockingTest.this.CheckFlag = 1;
                    Thread.sleep(10000);
                    if (2 == RowLockingTest.this.CheckFlag) {
                        throw new RuntimeException("Second thread stuck even thoung different conditions executed");
                    }
                }

                sess.commit();

            } catch (Exception e) {
                System.err.print("InterruptedException: ");
                System.err.println(e.getMessage());
                e.printStackTrace();
                throw new RuntimeException(e);
            } finally {
                if (this.shouldWait != 1) {
                    RowLockingTest.this.CheckFlag = 1;
                }
                if (sess != null) {
                    sess.close();
                }
            }
        }

    }

    public class SelectRowDeadLock extends Thread {

        private int action;
        private int lock;
        private String condition;

        SelectRowDeadLock(int action, int lock, String condition) {
            this.action = action;
            this.lock = lock;
            this.condition = condition;
        }

        @Override
        public void run() {
            System.out.println("Action is " + this.action);
            System.out.println("Lock is " + this.lock);
            System.out.println("Condition is " + this.condition);
            System.out.println("CheckFlag is " + RowLockingTest.this.CheckFlag);

            String tabname = "newtable";
            Session sess = null;

            try {
                sess = new SessionFactory().getSession(RowLockingTest.this.baseUrl);
                Table table = sess.getDefaultSchema().getTable(tabname);

                sess.startTransaction();
                switch (this.action) {
                    case 1: {
                        if (this.lock == 1) {
                            table.select("F1").where("F0 = 1").lockExclusive().execute();
                        } else if (this.lock == 2) {
                            table.select("F1").where("F0 = 1").lockShared().execute();
                        }

                        RowLockingTest.this.CheckFlag = 1;
                        do {
                            Thread.sleep(1000);
                        } while (RowLockingTest.this.CheckFlag != 2);

                        try {
                            if (this.lock == 1) {
                                table.select("F1").where("F0 = 2").lockExclusive().execute();
                            } else if (this.lock == 2) {
                                table.select("F1").where("F0 = 2").lockShared().execute();
                            }
                        } catch (Exception e) {
                            System.out.println("ERROR(3) " + e.getMessage());
                            assertTrue(e.getMessage().contains("Deadlock"));
                        }

                        break;
                    }
                    case 2: {
                        do {
                            Thread.sleep(1000);
                        } while (RowLockingTest.this.CheckFlag != 1);

                        if (this.lock == 1) {
                            table.select("F1").where("F0 = 2").lockExclusive().execute();
                        } else if (this.lock == 2) {
                            table.select("F1").where("F0 = 2").lockShared().execute();
                        }

                        RowLockingTest.this.CheckFlag = 2;

                        try {
                            if (this.lock == 1) {
                                table.select("F1").where("F0 = 1").lockExclusive().execute();
                            } else if (this.lock == 2) {
                                table.select("F1").where("F0 = 1").lockShared().execute();
                            }
                        } catch (Exception e) {
                            System.out.println("ERROR(3) " + e.getMessage());
                            assertTrue(e.getMessage().contains("Deadlock"));
                        }

                        break;
                    }
                }

                sess.commit();

            } catch (Exception e) {
                System.err.print("InterruptedException: ");
                System.err.println(e.getMessage());
                e.printStackTrace();
                throw new RuntimeException(e);
            } finally {
                if (sess != null) {
                    sess.close();
                }
            }
        }

    }

    public class FindRowDeadLock extends Thread {

        private int action;
        private int lock;
        private String condition;

        FindRowDeadLock(int action, int lock, String condition) {
            this.action = action;
            this.lock = lock;
            this.condition = condition;
        }

        @Override
        public void run() {
            System.out.println("Action is " + this.action);
            System.out.println("Lock is " + this.lock);
            System.out.println("Condition is " + this.condition);
            System.out.println("CheckFlag is " + RowLockingTest.this.CheckFlag);

            Session sess = null;

            try {
                sess = new SessionFactory().getSession(RowLockingTest.this.baseUrl);
                Collection coll = RowLockingTest.this.schema.getCollection(RowLockingTest.this.collectionName);

                sess.startTransaction();
                switch (this.action) {
                    case 1: {
                        if (this.lock == 1) {
                            coll.find("$.F1 = 1").fields("$.F2 as F2").lockExclusive().execute();
                        } else if (this.lock == 2) {
                            coll.find("$.F1 = 1").fields("$.F2 as F2").lockShared().execute();
                        }

                        RowLockingTest.this.CheckFlag = 1;
                        do {
                            Thread.sleep(1000);
                        } while (RowLockingTest.this.CheckFlag != 2);

                        if (this.lock == 1) {
                            coll.find("$.F1 = 2").fields("$.F2 as F2").lockExclusive().execute();
                        } else if (this.lock == 2) {
                            coll.find("$.F1 = 2").fields("$.F2 as F2").lockShared().execute();
                        }

                        break;
                    }
                    case 2: {
                        do {
                            Thread.sleep(1000);
                        } while (RowLockingTest.this.CheckFlag != 1);

                        if (this.lock == 1) {
                            coll.find("$.F1 = 2").fields("$.F2 as F2").lockExclusive().execute();
                        } else if (this.lock == 2) {
                            coll.find("$.F1 = 2").fields("$.F2 as F2").lockShared().execute();
                        }

                        RowLockingTest.this.CheckFlag = 2;

                        if (this.lock == 1) {
                            coll.find("$.F1 = 1").fields("$.F2 as F2").lockExclusive().execute();
                        } else if (this.lock == 2) {
                            coll.find("$.F1 = 1").fields("$.F2 as F2").lockShared().execute();
                        }

                        break;
                    }
                }

                sess.commit();

            } catch (Exception e) {
                System.err.print("InterruptedException: ");
                System.err.println(e.getMessage());
                e.printStackTrace();
                throw new RuntimeException(e);
            } finally {
                if (sess != null) {
                    sess.close();
                }
            }
        }

    }

    /**
     * START collection.find() tests
     *
     * @throws Exception
     */

    @Test
    public void testFindRowLockingValid() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.0")), "MySQL 8.0+ is required to run this test.");

        int i = 0;
        try {

            DbDoc[] jsonlist = new DbDocImpl[10];

            for (i = 1; i <= 10; i++) {
                DbDoc newDoc2 = new DbDocImpl();
                newDoc2.add("F1", new JsonNumber().setValue(String.valueOf(i)));
                newDoc2.add("F2", new JsonString().setValue("Field-1-Data-" + i));
                newDoc2.add("F3", new JsonNumber().setValue(String.valueOf(10 * i + 0.1234)));
                jsonlist[i - 1] = newDoc2;
                newDoc2 = null;
            }
            this.collection.add(jsonlist).execute();

            assertEquals(10, this.collection.count());

            /* Two threads with same conditions, select for update in one and update in second */
            this.CheckFlag = 0;
            initException = new Throwable[2];
            FindRowLock[] Thrd = new FindRowLock[2];

            Thrd[0] = new FindRowLock(1, 1, 0, 5, "$.F1 = ?");
            Thrd[0].setUncaughtExceptionHandler(new MyUncaughtExceptionHandler(0));
            Thrd[0].start();

            Thrd[1] = new FindRowLock(2, 0, 1, 5, "$.F1 = ?");
            Thrd[1].setUncaughtExceptionHandler(new MyUncaughtExceptionHandler(1));
            Thrd[1].start();

            for (i = 0; i < 2; i++) {
                Thrd[i].join();
            }
            for (i = 0; i < 2; i++) {
                if (initException[i] != null) {
                    throw new RuntimeException(initException[i]);
                }
            }

            /* Two threads with same conditions, select for share in one and update in second */
            this.CheckFlag = 0;
            initException = new Throwable[2];

            Thrd[0] = new FindRowLock(1, 2, 0, 5, "$.F1 = ?");
            Thrd[0].setUncaughtExceptionHandler(new MyUncaughtExceptionHandler(0));
            Thrd[0].start();

            Thrd[1] = new FindRowLock(2, 0, 1, 5, "$.F1 = ?");
            Thrd[1].setUncaughtExceptionHandler(new MyUncaughtExceptionHandler(1));
            Thrd[1].start();

            for (i = 0; i < 2; i++) {
                Thrd[i].join();
            }
            for (i = 0; i < 2; i++) {
                if (initException[i] != null) {
                    throw new RuntimeException(initException[i]);
                }
            }

            /* Two threads with same conditions selecting multiple records, select for update in one and update in second */
            this.CheckFlag = 0;
            initException = new Throwable[2];

            Thrd[0] = new FindRowLock(1, 2, 0, 5, "$.F1 < ?");
            Thrd[0].setUncaughtExceptionHandler(new MyUncaughtExceptionHandler(0));
            Thrd[0].start();

            Thrd[1] = new FindRowLock(2, 0, 1, 5, "$.F1 < ?");
            Thrd[1].setUncaughtExceptionHandler(new MyUncaughtExceptionHandler(1));
            Thrd[1].start();

            for (i = 0; i < 2; i++) {
                Thrd[i].join();
            }
            for (i = 0; i < 2; i++) {
                if (initException[i] != null) {
                    throw new RuntimeException(initException[i]);
                }
            }

            /* Two threads with same conditions selecting multiple rows, select for share in one and update in second */
            this.CheckFlag = 0;
            initException = new Throwable[2];

            Thrd[0] = new FindRowLock(1, 1, 0, 5, "$.F1 < ?");
            Thrd[0].setUncaughtExceptionHandler(new MyUncaughtExceptionHandler(0));
            Thrd[0].start();

            Thrd[1] = new FindRowLock(2, 0, 1, 5, "$.F1 < ?");
            Thrd[1].setUncaughtExceptionHandler(new MyUncaughtExceptionHandler(1));
            Thrd[1].start();

            for (i = 0; i < 2; i++) {
                Thrd[i].join();
            }
            for (i = 0; i < 2; i++) {
                if (initException[i] != null) {
                    throw new RuntimeException(initException[i]);
                }
            }

            /* Two threads with same conditions, select for update in one and select for share in second */
            this.CheckFlag = 0;
            initException = new Throwable[2];

            Thrd[0] = new FindRowLock(1, 2, 0, 5, "$.F1 < ?");
            Thrd[0].setUncaughtExceptionHandler(new MyUncaughtExceptionHandler(0));
            Thrd[0].start();

            Thrd[1] = new FindRowLock(1, 1, 1, 5, "$.F1 < ?");
            Thrd[1].setUncaughtExceptionHandler(new MyUncaughtExceptionHandler(1));
            Thrd[1].start();

            for (i = 0; i < 2; i++) {
                Thrd[i].join();
            }
            for (i = 0; i < 2; i++) {
                if (initException[i] != null) {
                    throw new RuntimeException(initException[i]);
                }
            }

        } catch (RuntimeException e) {
            System.out.print("**************RuntimeException: " + i);
            System.out.println(e.getMessage());
            throw e;
        } catch (InterruptedException e) {
            System.out.print("InterruptedException: " + i);
            System.out.println(e.getMessage());
            throw e;
        }
    }

    @Test
    public void testSelectRowLockingValid() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.0")), "MySQL 8.0+ is required to run this test.");

        int i = 0;
        try {
            /* add(DbDoc[] docs) */
            DbDoc[] jsonlist = new DbDocImpl[10];

            for (i = 1; i <= 10; i++) {
                DbDoc newDoc2 = new DbDocImpl();
                newDoc2.add("F1", new JsonNumber().setValue(String.valueOf(i)));
                newDoc2.add("F2", new JsonString().setValue("Field-1-Data-" + i));
                newDoc2.add("F3", new JsonNumber().setValue(String.valueOf(10 * i + 0.1234)));
                jsonlist[i - 1] = newDoc2;
                newDoc2 = null;
            }
            this.collection.add(jsonlist).execute();

            assertEquals(10, this.collection.count());
            this.CheckFlag = 0;
            initException = new Throwable[2];
            SelectRowLock[] Thrd = new SelectRowLock[2];

            /* Two threads with same conditions, select for update in one and update in second */
            Thrd[0] = new SelectRowLock(1, 1, 0, 5, "doc->$.F1 = :bVal");
            Thrd[0].setUncaughtExceptionHandler(new MyUncaughtExceptionHandler(0));
            Thrd[0].start();

            Thrd[1] = new SelectRowLock(3, 0, 1, 5, "doc->$.F1 = :bVal");
            Thrd[1].setUncaughtExceptionHandler(new MyUncaughtExceptionHandler(1));
            Thrd[1].start();

            for (i = 0; i < 2; i++) {
                Thrd[i].join();
            }
            for (i = 0; i < 2; i++) {
                if (initException[i] != null) {
                    throw new RuntimeException(initException[i]);
                }
            }

            /* Two threads with same conditions, select for share in one and update in second */
            this.CheckFlag = 0;
            initException = new Throwable[2];

            Thrd[0] = new SelectRowLock(1, 2, 0, 5, "doc->$.F1 = :bVal");
            Thrd[0].setUncaughtExceptionHandler(new MyUncaughtExceptionHandler(0));
            Thrd[0].start();

            Thrd[1] = new SelectRowLock(3, 0, 1, 5, "doc->$.F1 = :bVal");
            Thrd[1].setUncaughtExceptionHandler(new MyUncaughtExceptionHandler(1));
            Thrd[1].start();

            for (i = 0; i < 2; i++) {
                Thrd[i].join();
            }
            for (i = 0; i < 2; i++) {
                if (initException[i] != null) {
                    throw new RuntimeException(initException[i]);
                }
            }

            /* Two threads with same conditions, select for update in one and update in second */
            this.CheckFlag = 0;
            initException = new Throwable[2];

            Thrd[0] = new SelectRowLock(2, 1, 0, 5, "doc->$.F1 = :bVal");
            Thrd[0].setUncaughtExceptionHandler(new MyUncaughtExceptionHandler(0));
            Thrd[0].start();

            Thrd[1] = new SelectRowLock(3, 0, 1, 5, "doc->$.F1 = :bVal");
            Thrd[1].setUncaughtExceptionHandler(new MyUncaughtExceptionHandler(1));
            Thrd[1].start();

            for (i = 0; i < 2; i++) {
                Thrd[i].join();
            }
            for (i = 0; i < 2; i++) {
                if (initException[i] != null) {
                    throw new RuntimeException(initException[i]);
                }
            }

            /* Two threads with same conditions, select for share in one and update in second */
            this.CheckFlag = 0;
            initException = new Throwable[2];

            Thrd[0] = new SelectRowLock(2, 2, 0, 5, "doc->$.F1 = :bVal");
            Thrd[0].setUncaughtExceptionHandler(new MyUncaughtExceptionHandler(0));
            Thrd[0].start();

            Thrd[1] = new SelectRowLock(3, 0, 1, 5, "doc->$.F1 = :bVal");
            Thrd[1].setUncaughtExceptionHandler(new MyUncaughtExceptionHandler(1));
            Thrd[1].start();

            for (i = 0; i < 2; i++) {
                Thrd[i].join();
            }
            for (i = 0; i < 2; i++) {
                if (initException[i] != null) {
                    throw new RuntimeException(initException[i]);
                }
            }

            /* Two threads with same conditions selecting multiple records, select for update in one and update in second */
            this.CheckFlag = 0;
            initException = new Throwable[2];

            Thrd[0] = new SelectRowLock(2, 2, 0, 5, "doc->$.F1 < :bVal");
            Thrd[0].setUncaughtExceptionHandler(new MyUncaughtExceptionHandler(0));
            Thrd[0].start();

            Thrd[1] = new SelectRowLock(3, 0, 1, 5, "doc->$.F1 < :bVal");
            Thrd[1].setUncaughtExceptionHandler(new MyUncaughtExceptionHandler(1));
            Thrd[1].start();

            for (i = 0; i < 2; i++) {
                Thrd[i].join();
            }
            for (i = 0; i < 2; i++) {
                if (initException[i] != null) {
                    throw new RuntimeException(initException[i]);
                }
            }

            /* Two threads with same conditions selecting multiple records, select for share in one and update in second */
            this.CheckFlag = 0;
            initException = new Throwable[2];

            Thrd[0] = new SelectRowLock(2, 1, 0, 5, "doc->$.F1 < :bVal");
            Thrd[0].setUncaughtExceptionHandler(new MyUncaughtExceptionHandler(0));
            Thrd[0].start();

            Thrd[1] = new SelectRowLock(3, 0, 1, 5, "doc->$.F1 < :bVal");
            Thrd[1].setUncaughtExceptionHandler(new MyUncaughtExceptionHandler(1));
            Thrd[1].start();

            for (i = 0; i < 2; i++) {
                Thrd[i].join();
            }
            for (i = 0; i < 2; i++) {
                if (initException[i] != null) {
                    throw new RuntimeException(initException[i]);
                }
            }

        } catch (RuntimeException e) {
            System.out.print("**************RuntimeException: " + i);
            System.out.println(e.getMessage());
            throw e;
        } catch (InterruptedException e) {
            System.out.print("InterruptedException: " + i);
            System.out.println(e.getMessage());
            throw e;
        }
    }

    /* Simulate deadlock using table select */
    @Test
    public void testSelectRowLockingDeadlock() throws Exception {
        int i = 0;
        try {
            this.session.sql("drop table if exists newtable").execute();

            this.session.sql("create table newtable(F0 int auto_increment, F1 varchar(1024), PRIMARY KEY (f0))").execute();
            Table table = this.schema.getTable("newtable");

            for (i = 1; i <= 10; i++) {
                table.insert().values(i, "Data").execute();
            }

            this.CheckFlag = 0;
            initException = new Throwable[2];
            SelectRowDeadLock[] Thrd = new SelectRowDeadLock[2];

            Thrd[0] = new SelectRowDeadLock(1, 1, "");
            Thrd[0].setUncaughtExceptionHandler(new MyUncaughtExceptionHandler(0));
            Thrd[0].start();

            Thrd[1] = new SelectRowDeadLock(2, 1, "");
            Thrd[1].setUncaughtExceptionHandler(new MyUncaughtExceptionHandler(1));
            Thrd[1].start();

            for (i = 0; i < 2; i++) {
                Thrd[i].join();
            }
            for (i = 0; i < 2; i++) {
                if (initException[i] != null) {
                    throw new RuntimeException(initException[i]);
                }
            }

            this.CheckFlag = 0;
            initException = new Throwable[2];

            Thrd[0] = new SelectRowDeadLock(1, 2, "");
            Thrd[0].setUncaughtExceptionHandler(new MyUncaughtExceptionHandler(0));
            Thrd[0].start();

            Thrd[1] = new SelectRowDeadLock(2, 1, "");
            Thrd[1].setUncaughtExceptionHandler(new MyUncaughtExceptionHandler(1));
            Thrd[1].start();

            for (i = 0; i < 2; i++) {
                Thrd[i].join();
            }
            for (i = 0; i < 2; i++) {
                if (initException[i] != null) {
                    throw new RuntimeException(initException[i]);
                }
            }

        } catch (RuntimeException e) {
            System.out.print("**************RuntimeException: " + i);
            System.out.println(e.getMessage());
            throw e;
        } catch (InterruptedException e) {
            System.out.print("InterruptedException: " + i);
            System.out.println(e.getMessage());
            throw e;
        } finally {
            this.session.sql("drop table if exists newtable").execute();
        }
    }

}
