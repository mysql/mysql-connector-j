/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

package instrumentation;

import java.io.InputStream;
import java.io.Reader;
import java.sql.Savepoint;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;

import com.mysql.cj.core.exception.CJException;
import com.mysql.cj.core.io.Buffer;
import com.mysql.jdbc.ConnectionImpl;
import com.mysql.jdbc.Field;
import com.mysql.jdbc.MultiHostMySQLConnection;
import com.mysql.jdbc.MysqlSavepoint;
import com.mysql.jdbc.NonRegisteringDriver;
import com.mysql.jdbc.PreparedStatement;
import com.mysql.jdbc.ServerPreparedStatement;
import com.mysql.jdbc.ServerPreparedStatement.BindValue;
import com.mysql.jdbc.StatementImpl;
import com.mysql.jdbc.UpdatableResultSet;
import com.mysql.jdbc.exceptions.SQLExceptionsMapping;
import com.mysql.jdbc.jdbc2.optional.ConnectionWrapper;

public class TranslateExceptions {

    private static CtClass runTimeException = null;

    public static void main(String[] args) throws Exception {
        ClassPool pool = ClassPool.getDefault();
        pool.insertClassPath(args[0]);

        runTimeException = pool.get(CJException.class.getName());

        // params classes
        CtClass ctClazz = pool.get(Class.class.getName());
        CtClass ctClob = pool.get(java.sql.Clob.class.getName());
        CtClass ctBindValue = pool.get(BindValue.class.getName());
        CtClass ctBool = pool.get(boolean.class.getName());
        CtClass ctBoolArray = pool.get(boolean[].class.getName());
        CtClass ctByteArray2 = pool.get(byte[][].class.getName());
        CtClass ctBuffer = pool.get(Buffer.class.getName());
        CtClass ctExecutor = pool.get(Executor.class.getName());
        CtClass ctFieldArray = pool.get(Field[].class.getName());
        CtClass ctInt = pool.get(int.class.getName());
        CtClass ctIntArray = pool.get(int[].class.getName());
        CtClass ctInputStreamArray = pool.get(InputStream[].class.getName());
        CtClass ctLong = pool.get(long.class.getName());
        CtClass ctMap = pool.get(Map.class.getName());
        CtClass ctMysqlSavepoint = pool.get(MysqlSavepoint.class.getName());
        CtClass ctObjectArray = pool.get(Object[].class.getName());
        CtClass ctProperties = pool.get(Properties.class.getName());
        CtClass ctReader = pool.get(Reader.class.getName());
        CtClass ctSavepoint = pool.get(Savepoint.class.getName());
        CtClass ctString = pool.get(String.class.getName());
        CtClass ctStringArray = pool.get(String[].class.getName());

        // class we want to instrument

        /*
         * java.sql.Driver
         */
        CtClass clazz = pool.get(NonRegisteringDriver.class.getName());
        instrumentMethodCatch(clazz.getDeclaredMethod("connect", new CtClass[] { ctString, ctProperties }));
        instrumentMethodCatch(clazz.getDeclaredMethod("acceptsURL", new CtClass[] { ctString }));
        instrumentMethodCatch(clazz.getDeclaredMethod("getPropertyInfo", new CtClass[] { ctString, ctProperties }));
        clazz.writeFile(args[0]);

        /*
         * java.sql.Clob
         */
        //clazz = pool.get(Clob.class.getName());
        //instrumentMethodCatch(clazz.getDeclaredMethod("setString", new CtClass[] { ctLong, ctString, ctInt, ctInt }), "this.exceptionInterceptor");
        //clazz.writeFile(args[0]);

        /*
         * java.sql.Connection
         * com.mysql.jdbc.JdbcConnection
         * com.mysql.jdbc.MysqlJdbcConnection
         */
        clazz = pool.get(ConnectionImpl.class.getName());
        instrumentMethodCatch(clazz.getDeclaredMethod("changeUser", new CtClass[] { ctString, ctString }), "getExceptionInterceptor()");
        instrumentMethodCatch(clazz.getDeclaredMethod("clientPrepareStatement", new CtClass[] { ctString, ctInt, ctInt, ctBool }), "getExceptionInterceptor()");
        instrumentMethodCatch(clazz.getDeclaredMethod("commit", new CtClass[] {}), "getExceptionInterceptor()");
        instrumentMethodCatch(clazz.getDeclaredMethod("createStatement", new CtClass[] { ctInt, ctInt }), "getExceptionInterceptor()");
        instrumentMethodCatch(clazz.getDeclaredMethod("getMetaData", new CtClass[] { ctBool, ctBool }), "getExceptionInterceptor()");
        instrumentMethodCatch(clazz.getDeclaredMethod("getNetworkTimeout", new CtClass[] {}), "getExceptionInterceptor()");
        instrumentMethodCatch(clazz.getDeclaredMethod("getSchema", new CtClass[] {}), "getExceptionInterceptor()");
        instrumentMethodCatch(clazz.getDeclaredMethod("isAutoCommitNonDefaultOnServer", new CtClass[] {}), "getExceptionInterceptor()");
        instrumentMethodCatch(clazz.getDeclaredMethod("isServerLocal", new CtClass[] {}), "getExceptionInterceptor()");
        instrumentMethodCatch(clazz.getDeclaredMethod("isWrapperFor", new CtClass[] { ctClazz }), "getExceptionInterceptor()");
        instrumentMethodCatch(clazz.getDeclaredMethod("pingInternal", new CtClass[] { ctBool, ctInt }), "getExceptionInterceptor()");
        instrumentMethodCatch(clazz.getDeclaredMethod("prepareStatement", new CtClass[] { ctString, ctInt, ctInt }), "getExceptionInterceptor()");
        instrumentMethodCatch(clazz.getDeclaredMethod("rollback", new CtClass[] {}), "getExceptionInterceptor()");
        instrumentMethodCatch(clazz.getDeclaredMethod("rollback", new CtClass[] { ctSavepoint }), "getExceptionInterceptor()");
        instrumentMethodCatch(clazz.getDeclaredMethod("setAutoCommit", new CtClass[] { ctBool }), "getExceptionInterceptor()");
        instrumentMethodCatch(clazz.getDeclaredMethod("setCatalog", new CtClass[] { ctString }), "getExceptionInterceptor()");
        instrumentMethodCatch(clazz.getDeclaredMethod("setNetworkTimeout", new CtClass[] { ctExecutor, ctInt }), "getExceptionInterceptor()");
        instrumentMethodCatch(clazz.getDeclaredMethod("setReadOnly", new CtClass[] { ctBool }), "getExceptionInterceptor()");
        instrumentMethodCatch(clazz.getDeclaredMethod("setSavepoint", new CtClass[] { ctMysqlSavepoint }), "getExceptionInterceptor()");
        instrumentMethodCatch(clazz.getDeclaredMethod("setSchema", new CtClass[] { ctString }), "getExceptionInterceptor()");
        instrumentMethodCatch(clazz.getDeclaredMethod("setTransactionIsolation", new CtClass[] { ctInt }), "getExceptionInterceptor()");
        instrumentMethodCatch(clazz.getDeclaredMethod("versionMeetsMinimum", new CtClass[] { ctInt, ctInt, ctInt }), "getExceptionInterceptor()");
        clazz.writeFile(args[0]);

        clazz = pool.get(ConnectionWrapper.class.getName());
        instrumentMethodCatch(clazz.getDeclaredMethod("changeUser", new CtClass[] { ctString, ctString }), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("clientPrepare", new CtClass[] { ctString }), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("clientPrepare", new CtClass[] { ctString, ctInt, ctInt }), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("clientPrepareStatement", new CtClass[] { ctString }), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("commit", new CtClass[] {}), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("clearWarnings", new CtClass[] {}), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("createArrayOf", new CtClass[] { ctString, ctObjectArray }), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("createBlob", new CtClass[] {}), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("createClob", new CtClass[] {}), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("createNClob", new CtClass[] {}), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("createStatement", new CtClass[] {}), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("createStatement", new CtClass[] { ctInt, ctInt }), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("createStatement", new CtClass[] { ctInt, ctInt, ctInt }), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("createSQLXML", new CtClass[] {}), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("createStruct", new CtClass[] { ctString, ctObjectArray }), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("exposeAsXml", new CtClass[] {}), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("getAutoCommit", new CtClass[] {}), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("getCatalog", new CtClass[] {}), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("getClientInfo", new CtClass[] {}), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("getClientInfo", new CtClass[] { ctString }), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("getHoldability", new CtClass[] {}), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("getProcessHost", new CtClass[] {}), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("getMetaData", new CtClass[] {}), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("getNetworkTimeout", new CtClass[] {}), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("getSchema", new CtClass[] {}), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("getTransactionIsolation", new CtClass[] {}), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("getTypeMap", new CtClass[] {}), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("getWarnings", new CtClass[] {}), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("isReadOnly", new CtClass[] {}), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("isWrapperFor", new CtClass[] { ctClazz }), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("nativeSQL", new CtClass[] { ctString }), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("prepareCall", new CtClass[] { ctString }), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("prepareCall", new CtClass[] { ctString, ctInt, ctInt }), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("prepareCall", new CtClass[] { ctString, ctInt, ctInt, ctInt }), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("prepareStatement", new CtClass[] { ctString }), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("prepareStatement", new CtClass[] { ctString, ctInt }), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("prepareStatement", new CtClass[] { ctString, ctStringArray }), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("prepareStatement", new CtClass[] { ctString, ctIntArray }), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("prepareStatement", new CtClass[] { ctString, ctInt, ctInt }), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("prepareStatement", new CtClass[] { ctString, ctInt, ctInt, ctInt }), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("prepareStatement", new CtClass[] { ctString, ctStringArray }), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("releaseSavepoint", new CtClass[] { ctSavepoint }), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("resetServerState", new CtClass[] {}), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("rollback", new CtClass[] {}), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("rollback", new CtClass[] { ctSavepoint }), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("serverPrepareStatement", new CtClass[] { ctString }), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("setAutoCommit", new CtClass[] { ctBool }), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("setCatalog", new CtClass[] { ctString }), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("setClientInfo", new CtClass[] { ctString, ctString }), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("setClientInfo", new CtClass[] { ctProperties }), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("setHoldability", new CtClass[] { ctInt }), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("setNetworkTimeout", new CtClass[] { ctExecutor, ctInt }), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("setReadOnly", new CtClass[] { ctBool }), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("setSavepoint", new CtClass[] {}), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("setSavepoint", new CtClass[] { ctString }), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("setSchema", new CtClass[] { ctString }), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("setTransactionIsolation", new CtClass[] { ctInt }), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("setTypeMap", new CtClass[] { ctMap }), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("shutdownServer", new CtClass[] {}), "this.exceptionInterceptor");
        instrumentMethodCatch(clazz.getDeclaredMethod("versionMeetsMinimum", new CtClass[] { ctInt, ctInt, ctInt }), "this.exceptionInterceptor");
        clazz.writeFile(args[0]);

        clazz = pool.get(MultiHostMySQLConnection.class.getName());
        instrumentMethodCatch(clazz.getDeclaredMethod("isWrapperFor", new CtClass[] { ctClazz }), "getExceptionInterceptor()");
        clazz.writeFile(args[0]);

        /*
         * java.sql.PreparedStatement
         */
        clazz = pool.get(PreparedStatement.class.getName());
        instrumentMethodCatch(clazz.getDeclaredMethod("executePreparedBatchAsMultiStatement", new CtClass[] { ctInt }), "getExceptionInterceptor()");
        instrumentMethodCatch(clazz.getDeclaredMethod("fillSendPacket", new CtClass[] { ctByteArray2, ctInputStreamArray, ctBoolArray, ctIntArray }),
                "getExceptionInterceptor()");
        clazz.writeFile(args[0]);

        clazz = pool.get(ServerPreparedStatement.class.getName());
        instrumentMethodCatch(clazz.getDeclaredMethod("executeInternal", new CtClass[] { ctInt, ctBuffer, ctBool, ctBool, ctFieldArray, ctBool }),
                "getExceptionInterceptor()");
        instrumentMethodCatch(clazz.getDeclaredMethod("getBytes", new CtClass[] { ctInt }), "getExceptionInterceptor()");
        instrumentMethodCatch(clazz.getDeclaredMethod("realClose", new CtClass[] { ctBool, ctBool }), "getExceptionInterceptor()");
        instrumentMethodCatch(clazz.getDeclaredMethod("serverExecute", new CtClass[] { ctInt, ctBool, ctFieldArray }), "getExceptionInterceptor()");
        instrumentMethodCatch(clazz.getDeclaredMethod("serverLongData", new CtClass[] { ctInt, ctBindValue }), "getExceptionInterceptor()");
        instrumentMethodCatch(clazz.getDeclaredMethod("serverPrepare", new CtClass[] { ctString }), "getExceptionInterceptor()");
        instrumentMethodCatch(clazz.getDeclaredMethod("serverResetStatement", new CtClass[] {}), "getExceptionInterceptor()");
        clazz.writeFile(args[0]);

        clazz = pool.get(StatementImpl.class.getName());
        instrumentMethodCatch(clazz.getDeclaredMethod("cancel", new CtClass[] {}), "getExceptionInterceptor()");
        instrumentMethodCatch(clazz.getDeclaredMethod("executeBatchUsingMultiQueries", new CtClass[] { ctBool, ctInt, ctInt }), "getExceptionInterceptor()");
        clazz.writeFile(args[0]);

        clazz = pool.get(UpdatableResultSet.class.getName());
        instrumentMethodCatch(clazz.getDeclaredMethod("generateStatements", new CtClass[] {}), "getExceptionInterceptor()");
        clazz.writeFile(args[0]);

        /*
         * TODO: analyze other JDBC classes
         * 
         * java.sql.Array
         * java.sql.BatchUpdateException
         * java.sql.Blob
         * java.sql.CallableStatement
         * java.sql.ClientInfoStatus
         * java.sql.Connection
         * java.sql.DatabaseMetaData
         * java.sql.DataTruncation
         * java.sql.Date
         * java.sql.DriverManager
         * java.sql.DriverPropertyInfo
         * java.sql.ParameterMetaData
         * java.sql.PseudoColumnUsage
         * java.sql.Ref
         * java.sql.ResultSet
         * java.sql.ResultSetMetaData
         * java.sql.RowId
         * java.sql.RowIdLifetime
         * java.sql.Savepoint
         * java.sql.SQLClientInfoException
         * java.sql.SQLData
         * java.sql.SQLDataException
         * java.sql.SQLException
         * java.sql.SQLFeatureNotSupportedException
         * java.sql.SQLInput
         * java.sql.SQLIntegrityConstraintViolationException
         * java.sql.SQLInvalidAuthorizationSpecException
         * java.sql.SQLNonTransientException
         * java.sql.SQLNonTransientConnectionException
         * java.sql.SQLOutput
         * java.sql.SQLPermission
         * java.sql.SQLRecoverableException
         * java.sql.SQLSyntaxErrorException
         * java.sql.SQLTimeoutException
         * java.sql.SQLTransactionRollbackException
         * java.sql.SQLTransientConnectionException
         * java.sql.SQLTransientException
         * java.sql.SQLWarning
         * java.sql.SQLXML
         * java.sql.Statement
         * java.sql.Struct
         * java.sql.Time
         * java.sql.Timestamp
         * java.sql.Types
         * java.sql.Wrapper
         * 
         * javax.sql.CommonDataSource
         * javax.sql.ConnectionEvent
         * javax.sql.ConnectionEventListener
         * javax.sql.ConnectionPoolDataSource
         * javax.sql.DataSource
         * javax.sql.PooledConnection
         * javax.sql.RowSet
         * javax.sql.RowSetEvent
         * javax.sql.RowSetInternal
         * javax.sql.RowSetListener
         * javax.sql.RowSetMetaData
         * javax.sql.RowSetReader
         * javax.sql.RowSetWriter
         * 
         * javax.sql.rowset.BaseRowSet
         * javax.sql.rowset.CachedRowSet
         * javax.sql.rowset.FilteredRowSet
         * javax.sql.rowset.JdbcRowSet
         * javax.sql.rowset.Joinable
         * javax.sql.rowset.JoinRowSet
         * javax.sql.rowset.Predicate
         * javax.sql.rowset.RowSetFactory
         * javax.sql.rowset.RowSetMetaDataImpl
         * javax.sql.rowset.RowSetProvider
         * javax.sql.rowset.RowSetWarning
         * javax.sql.rowset.WebRowSet
         * 
         * javax.sql.rowset.serial.SerialArray
         * javax.sql.rowset.serial.SerialBlob
         * javax.sql.rowset.serial.SerialClob
         * javax.sql.rowset.serial.SerialDatalink
         * javax.sql.rowset.serial.SerialException
         * javax.sql.rowset.serial.SerialJavaObject
         * javax.sql.rowset.serial.SerialRef
         * javax.sql.rowset.serial.SerialStruct
         * javax.sql.rowset.serial.SQLInputImpl
         * javax.sql.rowset.serial.SQLOutputImpl
         * 
         * javax.sql.rowset.spi.SyncFactory
         * javax.sql.rowset.spi.SyncFactoryException
         * javax.sql.rowset.spi.SyncProvider
         * javax.sql.rowset.spi.SyncProviderException
         * javax.sql.rowset.spi.SyncResolver
         * javax.sql.rowset.spi.TransactionalWriter
         * javax.sql.rowset.spi.XmlReader
         * javax.sql.rowset.spi.XmlWriter
         */

    }

    private static void instrumentMethodCatch(CtMethod m) throws Exception {
        System.out.println(m);
        m.addCatch("{throw " + SQLExceptionsMapping.class.getName() + ".translateException(ex);}", runTimeException, "ex");
    }

    private static void instrumentMethodCatch(CtMethod m, String exceptionInterceptorStr) throws Exception {
        System.out.println(m + ", " + exceptionInterceptorStr);
        m.addCatch("{throw " + SQLExceptionsMapping.class.getName() + ".translateException(ex, " + exceptionInterceptorStr + ");}", runTimeException, "ex");
    }

}
