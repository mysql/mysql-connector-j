/*
 * Copyright (c) 2015, 2018, Oracle and/or its affiliates. All rights reserved.
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

package instrumentation;

import java.sql.Savepoint;
import java.util.Map;

import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.jdbc.ConnectionWrapper;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;

public class CommonChecks {
    private static boolean verbose = false;

    public static void main(String[] args) throws Exception {

        System.out.println("Applying CommonChecks.");

        verbose = "true".equalsIgnoreCase(args[1]);

        ClassPool pool = ClassPool.getDefault();
        pool.insertClassPath(args[0]);

        // params classes
        CtClass ctClazz = pool.get(Class.class.getName());
        //CtClass ctClob = pool.get(java.sql.Clob.class.getName());
        //CtClass ctBindValue = pool.get(BindValue.class.getName());
        CtClass ctBool = pool.get(boolean.class.getName());
        //CtClass ctBoolArray = pool.get(boolean[].class.getName());
        //CtClass ctByteArray2 = pool.get(byte[][].class.getName());
        //CtClass ctBuffer = pool.get(Buffer.class.getName());
        //CtClass ctExecutor = pool.get(Executor.class.getName());
        //CtClass ctFieldArray = pool.get(Field[].class.getName());
        CtClass ctInt = pool.get(int.class.getName());
        CtClass ctIntArray = pool.get(int[].class.getName());
        //CtClass ctInputStreamArray = pool.get(InputStream[].class.getName());
        //CtClass ctLong = pool.get(long.class.getName());
        CtClass ctMap = pool.get(Map.class.getName());
        //CtClass ctMysqlSavepoint = pool.get(MysqlSavepoint.class.getName());
        CtClass ctObjectArray = pool.get(Object[].class.getName());
        //CtClass ctProperties = pool.get(Properties.class.getName());
        //CtClass ctReader = pool.get(Reader.class.getName());
        CtClass ctSavepoint = pool.get(Savepoint.class.getName());
        //CtClass ctStatement = pool.get(Statement.class.getName());
        CtClass ctString = pool.get(String.class.getName());
        CtClass ctStringArray = pool.get(String[].class.getName());

        CtClass clazz = pool.get(ConnectionImpl.class.getName());
        // addClosedCheck(clazz.getDeclaredMethod("changeUser", new CtClass[] { ctString, ctString }));
        addClosedCheck(clazz.getDeclaredMethod("clientPrepareStatement", new CtClass[] { ctString, ctInt, ctInt, ctBool }));
        // addClosedCheck(clazz.getDeclaredMethod("commit", new CtClass[] {}));
        addClosedCheck(clazz.getDeclaredMethod("createStatement", new CtClass[] { ctInt, ctInt }));
        // addClosedCheck(clazz.getDeclaredMethod("getMetaData", new CtClass[] { ctBool, ctBool }));
        // addClosedCheck(clazz.getDeclaredMethod("getNetworkTimeout", new CtClass[] {}));
        // addClosedCheck(clazz.getDeclaredMethod("getSchema", new CtClass[] {}));
        // addClosedCheck(clazz.getDeclaredMethod("isAutoCommitNonDefaultOnServer", new CtClass[] {}));
        // addClosedCheck(clazz.getDeclaredMethod("isServerLocal", new CtClass[] {}));
        addClosedCheck(clazz.getDeclaredMethod("isWrapperFor", new CtClass[] { ctClazz }));
        // addClosedCheck(clazz.getDeclaredMethod("prepareStatement", new CtClass[] { ctString, ctInt, ctInt }));
        // addClosedCheck(clazz.getDeclaredMethod("rollback", new CtClass[] {}));
        // addClosedCheck(clazz.getDeclaredMethod("rollback", new CtClass[] { ctSavepoint }));
        // addClosedCheck(clazz.getDeclaredMethod("setAutoCommit", new CtClass[] { ctBool }));
        // addClosedCheck(clazz.getDeclaredMethod("setCatalog", new CtClass[] { ctString }));
        // addClosedCheck(clazz.getDeclaredMethod("setNetworkTimeout", new CtClass[] { ctExecutor, ctInt }));
        addClosedCheck(clazz.getDeclaredMethod("setReadOnly", new CtClass[] { ctBool }));
        // addClosedCheck(clazz.getDeclaredMethod("setSavepoint", new CtClass[] { ctMysqlSavepoint }));
        // addClosedCheck(clazz.getDeclaredMethod("setSchema", new CtClass[] { ctString }));
        // addClosedCheck(clazz.getDeclaredMethod("setTransactionIsolation", new CtClass[] { ctInt }));
        addClosedCheck(clazz.getDeclaredMethod("versionMeetsMinimum", new CtClass[] { ctInt, ctInt, ctInt }));
        clazz.writeFile(args[0]);

        clazz = pool.get(ConnectionWrapper.class.getName());
        addClosedCheck(clazz.getDeclaredMethod("changeUser", new CtClass[] { ctString, ctString }));
        addClosedCheck(clazz.getDeclaredMethod("clientPrepare", new CtClass[] { ctString }));
        addClosedCheck(clazz.getDeclaredMethod("clientPrepare", new CtClass[] { ctString, ctInt, ctInt }));
        addClosedCheck(clazz.getDeclaredMethod("clientPrepareStatement", new CtClass[] { ctString }));
        addClosedCheck(clazz.getDeclaredMethod("clientPrepareStatement", new CtClass[] { ctString, ctInt }));
        addClosedCheck(clazz.getDeclaredMethod("clientPrepareStatement", new CtClass[] { ctString, ctIntArray }));
        addClosedCheck(clazz.getDeclaredMethod("clientPrepareStatement", new CtClass[] { ctString, ctStringArray }));
        addClosedCheck(clazz.getDeclaredMethod("clientPrepareStatement", new CtClass[] { ctString, ctInt, ctInt }));
        addClosedCheck(clazz.getDeclaredMethod("clientPrepareStatement", new CtClass[] { ctString, ctInt, ctInt, ctInt }));
        addClosedCheck(clazz.getDeclaredMethod("commit", new CtClass[] {}));
        addClosedCheck(clazz.getDeclaredMethod("clearWarnings", new CtClass[] {}));
        addClosedCheck(clazz.getDeclaredMethod("createArrayOf", new CtClass[] { ctString, ctObjectArray }));
        addClosedCheck(clazz.getDeclaredMethod("createBlob", new CtClass[] {}));
        addClosedCheck(clazz.getDeclaredMethod("createClob", new CtClass[] {}));
        addClosedCheck(clazz.getDeclaredMethod("createNClob", new CtClass[] {}));
        addClosedCheck(clazz.getDeclaredMethod("createStatement", new CtClass[] {}));
        addClosedCheck(clazz.getDeclaredMethod("createStatement", new CtClass[] { ctInt, ctInt }));
        addClosedCheck(clazz.getDeclaredMethod("createStatement", new CtClass[] { ctInt, ctInt, ctInt }));
        addClosedCheck(clazz.getDeclaredMethod("createSQLXML", new CtClass[] {}));
        addClosedCheck(clazz.getDeclaredMethod("createStruct", new CtClass[] { ctString, ctObjectArray }));
        addClosedCheck(clazz.getDeclaredMethod("getAutoCommit", new CtClass[] {}));
        addClosedCheck(clazz.getDeclaredMethod("getCatalog", new CtClass[] {}));
        addClosedCheck(clazz.getDeclaredMethod("getClientInfo", new CtClass[] {}));
        addClosedCheck(clazz.getDeclaredMethod("getClientInfo", new CtClass[] { ctString }));
        addClosedCheck(clazz.getDeclaredMethod("getHoldability", new CtClass[] {}));
        // addClosedCheck(clazz.getDeclaredMethod("getProcessHost", new CtClass[] {}));
        addClosedCheck(clazz.getDeclaredMethod("getMetaData", new CtClass[] {}));
        // addClosedCheck(clazz.getDeclaredMethod("getNetworkTimeout", new CtClass[] {}));
        // addClosedCheck(clazz.getDeclaredMethod("getSchema", new CtClass[] {}));
        addClosedCheck(clazz.getDeclaredMethod("getTransactionIsolation", new CtClass[] {}));
        addClosedCheck(clazz.getDeclaredMethod("getTypeMap", new CtClass[] {}));
        addClosedCheck(clazz.getDeclaredMethod("getWarnings", new CtClass[] {}));
        addClosedCheck(clazz.getDeclaredMethod("isReadOnly", new CtClass[] {}));
        addClosedCheck(clazz.getDeclaredMethod("isReadOnly", new CtClass[] { ctBool }));
        //addClosedCheck(clazz.getDeclaredMethod("isWrapperFor", new CtClass[] { ctClazz }));
        addClosedCheck(clazz.getDeclaredMethod("nativeSQL", new CtClass[] { ctString }));
        addClosedCheck(clazz.getDeclaredMethod("prepareCall", new CtClass[] { ctString }));
        addClosedCheck(clazz.getDeclaredMethod("prepareCall", new CtClass[] { ctString, ctInt, ctInt }));
        addClosedCheck(clazz.getDeclaredMethod("prepareCall", new CtClass[] { ctString, ctInt, ctInt, ctInt }));
        addClosedCheck(clazz.getDeclaredMethod("prepareStatement", new CtClass[] { ctString }));
        addClosedCheck(clazz.getDeclaredMethod("prepareStatement", new CtClass[] { ctString, ctInt }));
        // addClosedCheck(clazz.getDeclaredMethod("prepareStatement", new CtClass[] { ctString, ctStringArray }));
        addClosedCheck(clazz.getDeclaredMethod("prepareStatement", new CtClass[] { ctString, ctIntArray }));
        addClosedCheck(clazz.getDeclaredMethod("prepareStatement", new CtClass[] { ctString, ctInt, ctInt }));
        addClosedCheck(clazz.getDeclaredMethod("prepareStatement", new CtClass[] { ctString, ctInt, ctInt, ctInt }));
        addClosedCheck(clazz.getDeclaredMethod("prepareStatement", new CtClass[] { ctString, ctStringArray }));
        addClosedCheck(clazz.getDeclaredMethod("releaseSavepoint", new CtClass[] { ctSavepoint }));
        addClosedCheck(clazz.getDeclaredMethod("resetServerState", new CtClass[] {}));
        addClosedCheck(clazz.getDeclaredMethod("rollback", new CtClass[] {}));
        addClosedCheck(clazz.getDeclaredMethod("rollback", new CtClass[] { ctSavepoint }));
        addClosedCheck(clazz.getDeclaredMethod("serverPrepareStatement", new CtClass[] { ctString }));
        addClosedCheck(clazz.getDeclaredMethod("serverPrepareStatement", new CtClass[] { ctString, ctInt }));
        addClosedCheck(clazz.getDeclaredMethod("serverPrepareStatement", new CtClass[] { ctString, ctIntArray }));
        addClosedCheck(clazz.getDeclaredMethod("serverPrepareStatement", new CtClass[] { ctString, ctStringArray }));
        addClosedCheck(clazz.getDeclaredMethod("serverPrepareStatement", new CtClass[] { ctString, ctInt, ctInt }));
        addClosedCheck(clazz.getDeclaredMethod("serverPrepareStatement", new CtClass[] { ctString, ctInt, ctInt, ctInt }));
        addClosedCheck(clazz.getDeclaredMethod("setAutoCommit", new CtClass[] { ctBool }));
        addClosedCheck(clazz.getDeclaredMethod("setCatalog", new CtClass[] { ctString }));
        // addClosedCheck(clazz.getDeclaredMethod("setClientInfo", new CtClass[] { ctString, ctString }));
        // addClosedCheck(clazz.getDeclaredMethod("setClientInfo", new CtClass[] { ctProperties }));
        addClosedCheck(clazz.getDeclaredMethod("setHoldability", new CtClass[] { ctInt }));
        // addClosedCheck(clazz.getDeclaredMethod("setNetworkTimeout", new CtClass[] { ctExecutor, ctInt }));
        addClosedCheck(clazz.getDeclaredMethod("setReadOnly", new CtClass[] { ctBool }));
        addClosedCheck(clazz.getDeclaredMethod("setSavepoint", new CtClass[] {}));
        addClosedCheck(clazz.getDeclaredMethod("setSavepoint", new CtClass[] { ctString }));
        // addClosedCheck(clazz.getDeclaredMethod("setSchema", new CtClass[] { ctString }));
        addClosedCheck(clazz.getDeclaredMethod("setTransactionIsolation", new CtClass[] { ctInt }));
        addClosedCheck(clazz.getDeclaredMethod("setTypeMap", new CtClass[] { ctMap }));
        addClosedCheck(clazz.getDeclaredMethod("shutdownServer", new CtClass[] {}));
        //addClosedCheck(clazz.getDeclaredMethod("versionMeetsMinimum", new CtClass[] { ctInt, ctInt, ctInt }));
        clazz.writeFile(args[0]);

        //clazz = pool.get(MultiHostMySQLConnection.class.getName());
        //addClosedCheck(clazz.getDeclaredMethod("isWrapperFor", new CtClass[] { ctClazz }));
        //clazz.writeFile(args[0]);

    }

    private static void addClosedCheck(CtMethod m) throws Exception {
        sysOut(m.toString());
        m.insertBefore("checkClosed();");
    }

    private static void sysOut(String s) {
        if (verbose) {
            System.out.println(s);
        }
    }

}
