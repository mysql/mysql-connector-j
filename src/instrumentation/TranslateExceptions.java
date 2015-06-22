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
import java.lang.reflect.Method;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.TreeMap;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.NotFoundException;

import com.mysql.cj.core.exception.CJException;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.mysqla.io.Buffer;
import com.mysql.fabric.jdbc.FabricMySQLConnection;
import com.mysql.fabric.jdbc.FabricMySQLConnectionProxy;
import com.mysql.fabric.jdbc.FabricMySQLDataSource;
import com.mysql.fabric.jdbc.FabricMySQLDriver;
import com.mysql.jdbc.Blob;
import com.mysql.jdbc.BlobFromLocator;
import com.mysql.jdbc.CallableStatement;
import com.mysql.jdbc.CallableStatement.CallableStatementParamInfo;
import com.mysql.jdbc.Clob;
import com.mysql.jdbc.ConnectionImpl;
import com.mysql.jdbc.DatabaseMetaData;
import com.mysql.jdbc.DatabaseMetaDataUsingInfoSchema;
import com.mysql.jdbc.Field;
import com.mysql.jdbc.LoadBalancedConnection;
import com.mysql.jdbc.LoadBalancedMySQLConnection;
import com.mysql.jdbc.MultiHostMySQLConnection;
import com.mysql.jdbc.MysqlParameterMetadata;
import com.mysql.jdbc.MysqlSQLXML;
import com.mysql.jdbc.MysqlSavepoint;
import com.mysql.jdbc.NClob;
import com.mysql.jdbc.NonRegisteringDriver;
import com.mysql.jdbc.PreparedStatement;
import com.mysql.jdbc.ReplicationConnection;
import com.mysql.jdbc.ResultSetImpl;
import com.mysql.jdbc.ResultSetInternalMethods;
import com.mysql.jdbc.ResultSetMetaData;
import com.mysql.jdbc.ServerPreparedStatement;
import com.mysql.jdbc.ServerPreparedStatement.BindValue;
import com.mysql.jdbc.Statement;
import com.mysql.jdbc.StatementImpl;
import com.mysql.jdbc.UpdatableResultSet;
import com.mysql.jdbc.exceptions.SQLExceptionsMapping;
import com.mysql.jdbc.jdbc2.optional.ConnectionWrapper;
import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import com.mysql.jdbc.jdbc2.optional.MysqlPooledConnection;
import com.mysql.jdbc.jdbc2.optional.MysqlXAConnection;
import com.mysql.jdbc.jdbc2.optional.MysqlXADataSource;
import com.mysql.jdbc.jdbc2.optional.MysqlXid;
import com.mysql.jdbc.jdbc2.optional.SuspendableXAConnection;

public class TranslateExceptions {

    private static CtClass runTimeException = null;
    private static ClassPool pool = ClassPool.getDefault();
    private static Map<String, List<CtMethod>> processed = new TreeMap<String, List<CtMethod>>();

    private static String EXCEPTION_INTERCEPTOR_GETTER = "getExceptionInterceptor()";
    private static String EXCEPTION_INTERCEPTOR_MEMBER = "this.exceptionInterceptor";

    public static void main(String[] args) throws Exception {
        pool.insertClassPath(args[0]);
        processed.clear();

        runTimeException = pool.get(CJException.class.getName());

        // params classes
        CtClass ctBindValue = pool.get(BindValue.class.getName());
        CtClass ctBoolArray = pool.get(boolean[].class.getName());
        CtClass ctByteArray = pool.get(byte[].class.getName());
        CtClass ctByteArray2 = pool.get(byte[][].class.getName());
        CtClass ctBuffer = pool.get(Buffer.class.getName());
        CtClass ctFieldArray = pool.get(Field[].class.getName());
        CtClass ctIntArray = pool.get(int[].class.getName());
        CtClass ctInputStream = pool.get(InputStream.class.getName());
        CtClass ctInputStreamArray = pool.get(InputStream[].class.getName());
        CtClass ctJdbcConnection = pool.get(JdbcConnection.class.getName());
        CtClass ctMysqlSavepoint = pool.get(MysqlSavepoint.class.getName());
        CtClass ctProperties = pool.get(Properties.class.getName());
        CtClass ctResultSet = pool.get(ResultSet.class.getName());
        CtClass ctResultSetInternalMethods = pool.get(ResultSetInternalMethods.class.getName());
        CtClass ctSqlDate = pool.get(Date.class.getName());
        CtClass ctStatement = pool.get(java.sql.Statement.class.getName());
        CtClass ctStatementImpl = pool.get(StatementImpl.class.getName());
        CtClass ctString = pool.get(String.class.getName());
        CtClass ctTime = pool.get(Time.class.getName());
        CtClass ctTimestamp = pool.get(Timestamp.class.getName());
        CtClass ctTimeZone = pool.get(TimeZone.class.getName());

        // class we want to instrument
        CtClass clazz;

        /*
         * java.sql.Blob
         */
        // com.mysql.jdbc.Blob implements java.sql.Blob, OutputStreamWatcher
        clazz = pool.get(Blob.class.getName());
        instrumentJdbcMethods(clazz, java.sql.Blob.class, false, EXCEPTION_INTERCEPTOR_MEMBER);
        clazz.writeFile(args[0]);

        // com.mysql.jdbc.BlobFromLocator implements java.sql.Blob
        clazz = pool.get(BlobFromLocator.class.getName());
        instrumentJdbcMethods(clazz, java.sql.Blob.class, false, EXCEPTION_INTERCEPTOR_MEMBER);
        clazz.writeFile(args[0]);

        /*
         * java.sql.CallableStatement
         */
        // com.mysql.jdbc.CallableStatement extends PreparedStatement implements java.sql.CallableStatement
        clazz = pool.get(CallableStatement.class.getName());
        instrumentJdbcMethods(clazz, java.sql.CallableStatement.class, false, EXCEPTION_INTERCEPTOR_GETTER);
        instrumentJdbcMethods(clazz, Statement.class, true, EXCEPTION_INTERCEPTOR_GETTER);
        // non-JDBC
        catchRuntimeException(clazz, clazz.getDeclaredMethod("checkIsOutputParam", new CtClass[] { CtClass.intType }), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("checkParameterIndexBounds", new CtClass[] { CtClass.intType }), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("checkReadOnlyProcedure", new CtClass[] {}), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("convertGetProcedureColumnsToInternalDescriptors", new CtClass[] { ctResultSet }),
                EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("determineParameterTypes", new CtClass[] {}), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("fakeParameterTypes", new CtClass[] { CtClass.booleanType }), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("fixParameterName", new CtClass[] { ctString }), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("generateParameterMap", new CtClass[] {}), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("getNamedParamIndex", new CtClass[] { ctString, CtClass.booleanType }),
                EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("getOutputParameters", new CtClass[] { CtClass.intType }), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("mapOutputParameterIndexToRsIndex", new CtClass[] { CtClass.intType }),
                EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("retrieveOutParams", new CtClass[] {}), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("setInOutParamsOnServer", new CtClass[] {}), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("setOutParams", new CtClass[] {}), EXCEPTION_INTERCEPTOR_GETTER);
        clazz.writeFile(args[0]);

        /*
         * com.mysql.jdbc.jdbc2.optional.StatementWrapper extends WrapperBase implements Statement
         */
        // TODO: Does it's own typical exception wrapping, could be instrumented with different catch method

        /*
         * com.mysql.jdbc.jdbc2.optional.PreparedStatementWrapper extends StatementWrapper implements PreparedStatement
         */
        // TODO: Does it's own typical exception wrapping, could be instrumented with different catch method

        /*
         * com.mysql.jdbc.jdbc2.optional.CallableStatementWrapper extends PreparedStatementWrapper implements CallableStatement
         */
        // TODO: Does it's own typical exception wrapping, could be instrumented with different catch method

        /*
         * java.sql.Clob
         */
        // com.mysql.jdbc.Clob implements java.sql.Clob, OutputStreamWatcher, WriterWatcher
        clazz = pool.get(Clob.class.getName());
        instrumentJdbcMethods(clazz, java.sql.Clob.class, false, EXCEPTION_INTERCEPTOR_MEMBER);
        clazz.writeFile(args[0]);

        /*
         * 
         * java.sql.Connection extends java.sql.Wrapper
         * ----> com.mysql.jdbc.JdbcConnection extends java.sql.Connection, MysqlConnection
         * ----------> FabricMySQLConnection extends JdbcConnection
         * -------------> com.mysql.fabric.jdbc.FabricMySQLConnectionProxy extends AbstractJdbcConnection implements FabricMySQLConnection
         * ----------> com.mysql.jdbc.ConnectionImpl
         * ----------> com.mysql.jdbc.LoadBalancedConnection extends JdbcConnection
         * -------------> com.mysql.jdbc.LoadBalancedMySQLConnection extends MultiHostMySQLConnection implements LoadBalancedConnection
         * ----------> com.mysql.jdbc.MultiHostMySQLConnection
         * -------> com.mysql.jdbc.ReplicationConnection implements JdbcConnection, PingTarget
         * -------> com.mysql.jdbc.jdbc2.optional.ConnectionWrapper
         */
        // FabricMySQLConnectionProxy extends AbstractJdbcConnection implements FabricMySQLConnection
        clazz = pool.get(FabricMySQLConnectionProxy.class.getName());
        instrumentJdbcMethods(clazz, FabricMySQLConnection.class, false, EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("createNewIO", new CtClass[] { CtClass.booleanType }));
        clazz.writeFile(args[0]);

        // ConnectionImpl extends AbstractJdbcConnection implements JdbcConnection
        clazz = pool.get(ConnectionImpl.class.getName());
        instrumentJdbcMethods(clazz, JdbcConnection.class, false, EXCEPTION_INTERCEPTOR_GETTER);
        // non-JDBC
        catchRuntimeException(clazz,
                clazz.getDeclaredMethod("clientPrepareStatement", new CtClass[] { ctString, CtClass.intType, CtClass.intType, CtClass.booleanType }),
                EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("createNewIO", new CtClass[] { CtClass.booleanType }), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("getMetaData", new CtClass[] { CtClass.booleanType, CtClass.booleanType }),
                EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("isAutoCommitNonDefaultOnServer", new CtClass[] {}), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("setSavepoint", new CtClass[] { ctMysqlSavepoint }), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("versionMeetsMinimum", new CtClass[] { CtClass.intType, CtClass.intType, CtClass.intType }),
                EXCEPTION_INTERCEPTOR_GETTER);
        clazz.writeFile(args[0]);

        // com.mysql.jdbc.LoadBalancedMySQLConnection extends MultiHostMySQLConnection implements LoadBalancedConnection
        clazz = pool.get(LoadBalancedMySQLConnection.class.getName());
        instrumentJdbcMethods(clazz, LoadBalancedConnection.class, false, EXCEPTION_INTERCEPTOR_GETTER);
        clazz.writeFile(args[0]);

        // MultiHostMySQLConnection implements JdbcConnection
        clazz = pool.get(MultiHostMySQLConnection.class.getName());
        instrumentJdbcMethods(clazz, JdbcConnection.class, false, EXCEPTION_INTERCEPTOR_GETTER);
        clazz.writeFile(args[0]);

        // com.mysql.jdbc.ReplicationConnection implements JdbcConnection, PingTarget
        clazz = pool.get(ReplicationConnection.class.getName());
        instrumentJdbcMethods(clazz, JdbcConnection.class, false, EXCEPTION_INTERCEPTOR_GETTER);
        clazz.writeFile(args[0]);

        // ConnectionWrapper extends WrapperBase implements JdbcConnection
        clazz = pool.get(ConnectionWrapper.class.getName());
        instrumentJdbcMethods(clazz, JdbcConnection.class, false, EXCEPTION_INTERCEPTOR_MEMBER);
        // non-JDBC
        catchRuntimeException(clazz, clazz.getDeclaredMethod("clientPrepare", new CtClass[] { ctString }), EXCEPTION_INTERCEPTOR_MEMBER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("clientPrepare", new CtClass[] { ctString, CtClass.intType, CtClass.intType }),
                EXCEPTION_INTERCEPTOR_MEMBER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("getProcessHost", new CtClass[] {}), EXCEPTION_INTERCEPTOR_MEMBER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("setClientInfo", new CtClass[] { ctString, ctString }), EXCEPTION_INTERCEPTOR_MEMBER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("setClientInfo", new CtClass[] { ctProperties }), EXCEPTION_INTERCEPTOR_MEMBER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("versionMeetsMinimum", new CtClass[] { CtClass.intType, CtClass.intType, CtClass.intType }),
                EXCEPTION_INTERCEPTOR_MEMBER);
        clazz.writeFile(args[0]);

        /*
         * java.sql.DatabaseMetaData extends java.sql.Wrapper
         */
        // com.mysql.jdbc.DatabaseMetaData implements java.sql.DatabaseMetaData
        clazz = pool.get(DatabaseMetaData.class.getName());
        instrumentJdbcMethods(clazz, java.sql.DatabaseMetaData.class, false, EXCEPTION_INTERCEPTOR_GETTER);
        clazz.writeFile(args[0]);

        // com.mysql.jdbc.DatabaseMetaDataUsingInfoSchema extends DatabaseMetaData
        clazz = pool.get(DatabaseMetaDataUsingInfoSchema.class.getName());
        instrumentJdbcMethods(clazz, java.sql.DatabaseMetaData.class, false, EXCEPTION_INTERCEPTOR_GETTER);
        clazz.writeFile(args[0]);

        /*
         * java.sql.Driver
         */
        // com.mysql.jdbc.Driver extends NonRegisteringDriver implements java.sql.Driver
        clazz = pool.get(NonRegisteringDriver.class.getName());
        instrumentJdbcMethods(clazz, java.sql.Driver.class);
        clazz.writeFile(args[0]);

        // FabricMySQLDriver extends NonRegisteringDriver implements Driver
        clazz = pool.get(FabricMySQLDriver.class.getName());
        instrumentJdbcMethods(clazz, java.sql.Driver.class);
        clazz.writeFile(args[0]);

        /*
         * java.sql.NClob
         */
        // com.mysql.jdbc.NClob extends Clob implements java.sql.NClob
        clazz = pool.get(NClob.class.getName());
        instrumentJdbcMethods(clazz, java.sql.NClob.class);
        clazz.writeFile(args[0]);

        /*
         * java.sql.ParameterMetaData extends java.sql.Wrapper
         */
        // com.mysql.jdbc.CallableStatement.CallableStatementParamInfo implements ParameterMetaData
        clazz = pool.get(CallableStatementParamInfo.class.getName());
        instrumentJdbcMethods(clazz, java.sql.ParameterMetaData.class);
        clazz.writeFile(args[0]);

        // com.mysql.jdbc.MysqlParameterMetadata implements ParameterMetaData
        clazz = pool.get(MysqlParameterMetadata.class.getName());
        instrumentJdbcMethods(clazz, java.sql.ParameterMetaData.class, false, EXCEPTION_INTERCEPTOR_MEMBER);
        clazz.writeFile(args[0]);

        /*
         * java.sql.PreparedStatement extends java.sql.Statement (java.sql.Statement extends java.sql.Wrapper)
         */
        // com.mysql.jdbc.PreparedStatement extends com.mysql.jdbc.StatementImpl implements java.sql.PreparedStatement
        clazz = pool.get(PreparedStatement.class.getName());
        instrumentJdbcMethods(clazz, java.sql.PreparedStatement.class, false, EXCEPTION_INTERCEPTOR_GETTER);
        instrumentJdbcMethods(clazz, Statement.class, true, EXCEPTION_INTERCEPTOR_GETTER);
        // non-JDBC
        catchRuntimeException(clazz, clazz.getDeclaredMethod("asSql", new CtClass[] { CtClass.booleanType }), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("checkBounds", new CtClass[] { CtClass.intType, CtClass.intType }), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("checkReadOnlySafeStatement", new CtClass[] {}), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("computeBatchSize", new CtClass[] { CtClass.intType }), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("computeMaxParameterSetSizeAndBatchSize", new CtClass[] { CtClass.intType }),
                EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("executeBatchedInserts", new CtClass[] { CtClass.intType }), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("executeBatchSerially", new CtClass[] { CtClass.intType }), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(
                clazz,
                clazz.getDeclaredMethod("executeInternal", new CtClass[] { CtClass.intType, ctBuffer, CtClass.booleanType, CtClass.booleanType, ctFieldArray,
                        CtClass.booleanType }), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("executePreparedBatchAsMultiStatement", new CtClass[] { CtClass.intType }),
                EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("executeUpdate", new CtClass[] { CtClass.booleanType, CtClass.booleanType }),
                EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(
                clazz,
                clazz.getDeclaredMethod("executeUpdate", new CtClass[] { ctByteArray2, ctInputStreamArray, ctBoolArray, ctIntArray, ctBoolArray,
                        CtClass.booleanType }), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("fillSendPacket", new CtClass[] {}), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("fillSendPacket", new CtClass[] { ctByteArray2, ctInputStreamArray, ctBoolArray, ctIntArray }),
                EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("generateMultiStatementForBatch", new CtClass[] { CtClass.intType }), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("getBytesRepresentation", new CtClass[] { CtClass.intType }), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("getBytesRepresentationForBatch", new CtClass[] { CtClass.intType, CtClass.intType }),
                EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("getNonRewrittenSql", new CtClass[] {}), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("getParameterBindings", new CtClass[] {}), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("initializeFromParseInfo", new CtClass[] {}), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("isNull", new CtClass[] { CtClass.intType }), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("isSelectQuery", new CtClass[] {}), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("prepareBatchedInsertSQL", new CtClass[] { ctJdbcConnection, CtClass.intType }),
                EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz,
                clazz.getDeclaredMethod("setBytes", new CtClass[] { CtClass.intType, ctByteArray, CtClass.booleanType, CtClass.booleanType }),
                EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("setInternal", new CtClass[] { CtClass.intType, ctByteArray }), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("setInternal", new CtClass[] { CtClass.intType, ctString }), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("setRetrieveGeneratedKeys", new CtClass[] { CtClass.booleanType }), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz,
                clazz.getDeclaredMethod("streamToBytes", new CtClass[] { ctBuffer, ctInputStream, CtClass.booleanType, CtClass.intType, CtClass.booleanType }),
                EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz,
                clazz.getDeclaredMethod("streamToBytes", new CtClass[] { ctInputStream, CtClass.booleanType, CtClass.intType, CtClass.booleanType }),
                EXCEPTION_INTERCEPTOR_GETTER);
        clazz.writeFile(args[0]);

        /*
         * com.mysql.jdbc.ServerPreparedStatement extends PreparedStatement
         */
        clazz = pool.get(ServerPreparedStatement.class.getName());
        instrumentJdbcMethods(clazz, java.sql.PreparedStatement.class, false, EXCEPTION_INTERCEPTOR_GETTER);
        instrumentJdbcMethods(clazz, Statement.class, true, EXCEPTION_INTERCEPTOR_GETTER);
        // non-JDBC
        catchRuntimeException(
                clazz,
                clazz.getDeclaredMethod("executeInternal", new CtClass[] { CtClass.intType, ctBuffer, CtClass.booleanType, CtClass.booleanType, ctFieldArray,
                        CtClass.booleanType }), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("getBytes", new CtClass[] { CtClass.intType }), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("realClose", new CtClass[] { CtClass.booleanType, CtClass.booleanType }),
                EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("serverExecute", new CtClass[] { CtClass.intType, CtClass.booleanType, ctFieldArray }),
                EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("serverLongData", new CtClass[] { CtClass.intType, ctBindValue }), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("serverPrepare", new CtClass[] { ctString }), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("serverResetStatement", new CtClass[] {}), EXCEPTION_INTERCEPTOR_GETTER);
        clazz.writeFile(args[0]);

        /*
         * java.sql.ResultSet extends java.sql.Wrapper
         */
        // com.mysql.jdbc.ResultSetImpl implements com.mysql.jdbc.ResultSetInternalMethods (extends java.sql.ResultSet)
        clazz = pool.get(ResultSetImpl.class.getName());
        instrumentJdbcMethods(clazz, ResultSetInternalMethods.class, false, EXCEPTION_INTERCEPTOR_GETTER);
        clazz.writeFile(args[0]);

        // com.mysql.jdbc.UpdatableResultSet extends ResultSetImpl
        clazz = pool.get(UpdatableResultSet.class.getName());
        instrumentJdbcMethods(clazz, ResultSetInternalMethods.class, false, EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("generateStatements", new CtClass[] {}), EXCEPTION_INTERCEPTOR_GETTER);
        clazz.writeFile(args[0]);

        /*
         * java.sql.ResultSetMetaData extends java.sql.Wrapper
         */
        // com.mysql.jdbc.ResultSetMetaData implements java.sql.ResultSetMetaData
        clazz = pool.get(ResultSetMetaData.class.getName());
        instrumentJdbcMethods(clazz, java.sql.ResultSetMetaData.class, false, EXCEPTION_INTERCEPTOR_MEMBER);
        clazz.writeFile(args[0]);

        /*
         * java.sql.Savepoint
         */
        // com.mysql.jdbc.MysqlSavepoint implements java.sql.Savepoint
        clazz = pool.get(MysqlSavepoint.class.getName());
        instrumentJdbcMethods(clazz, Savepoint.class, false, EXCEPTION_INTERCEPTOR_MEMBER);
        clazz.writeFile(args[0]);

        /*
         * java.sql.Statement extends java.sql.Wrapper
         */
        // com.mysql.jdbc.StatementImpl implements com.mysql.jdbc.Statement (extends java.sql.Statement)
        clazz = pool.get(StatementImpl.class.getName());
        instrumentJdbcMethods(clazz, Statement.class, false, EXCEPTION_INTERCEPTOR_GETTER);
        // non-JDBC
        catchRuntimeException(clazz, clazz.getDeclaredMethod("createResultSetUsingServerFetch", new CtClass[] { ctString }), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("doPingInstead", new CtClass[] {}), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("execute", new CtClass[] { ctString, CtClass.booleanType }), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz,
                clazz.getDeclaredMethod("executeBatchUsingMultiQueries", new CtClass[] { CtClass.booleanType, CtClass.intType, CtClass.intType }),
                EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("executeUpdate", new CtClass[] { ctString, CtClass.booleanType, CtClass.booleanType }),
                EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("generatePingResultSet", new CtClass[] {}), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("getBatchedGeneratedKeys", new CtClass[] { CtClass.intType }), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("getBatchedGeneratedKeys", new CtClass[] { ctStatement }), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("getGeneratedKeysInternal", new CtClass[] { CtClass.intType }), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("getLastInsertID", new CtClass[] {}), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("getLongUpdateCount", new CtClass[] {}), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("getOpenResultSetCount", new CtClass[] {}), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("getResultSetInternal", new CtClass[] {}), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("processMultiCountsAndKeys", new CtClass[] { ctStatementImpl, CtClass.intType, ctIntArray }),
                EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("removeOpenResultSet", new CtClass[] { ctResultSetInternalMethods }), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("resetCancelledState", new CtClass[] {}), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("setHoldResultsOpenOverClose", new CtClass[] { CtClass.booleanType }),
                EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("setResultSetConcurrency", new CtClass[] { CtClass.intType }), EXCEPTION_INTERCEPTOR_GETTER);
        catchRuntimeException(clazz, clazz.getDeclaredMethod("useServerFetch", new CtClass[] {}), EXCEPTION_INTERCEPTOR_GETTER);
        clazz.writeFile(args[0]);

        /*
         * java.sql.SQLXML
         */
        // com.mysql.jdbc.MysqlSQLXML implements SQLXML
        clazz = pool.get(MysqlSQLXML.class.getName());
        instrumentJdbcMethods(clazz, java.sql.SQLXML.class, false, EXCEPTION_INTERCEPTOR_MEMBER);
        clazz.writeFile(args[0]);

        /*
         * javax.sql.ConnectionPoolDataSource
         */
        // MysqlConnectionPoolDataSource extends MysqlDataSource implements ConnectionPoolDataSource
        clazz = pool.get(MysqlConnectionPoolDataSource.class.getName());
        instrumentJdbcMethods(clazz, javax.sql.ConnectionPoolDataSource.class);
        clazz.writeFile(args[0]);

        /*
         * javax.sql.DataSource
         */
        // MysqlDataSource extends JdbcPropertySetImpl implements DataSource, Referenceable, Serializable, JdbcPropertySet
        clazz = pool.get(MysqlDataSource.class.getName());
        instrumentJdbcMethods(clazz, javax.sql.DataSource.class);
        clazz.writeFile(args[0]);

        // com.mysql.fabric.jdbc.FabricMySQLDataSource extends MysqlDataSource implements FabricPropertySet
        clazz = pool.get(FabricMySQLDataSource.class.getName());
        instrumentJdbcMethods(clazz, javax.sql.DataSource.class);
        clazz.writeFile(args[0]);

        /*
         * javax.sql.PooledConnection
         */
        // com.mysql.jdbc.jdbc2.optional.MysqlPooledConnection
        clazz = pool.get(MysqlPooledConnection.class.getName());
        instrumentJdbcMethods(clazz, javax.sql.PooledConnection.class, false, EXCEPTION_INTERCEPTOR_MEMBER);
        clazz.writeFile(args[0]);

        /*
         * javax.sql.XAConnection
         * javax.transaction.xa.XAResource
         */
        // com.mysql.jdbc.jdbc2.optional.MysqlXAConnection extends MysqlPooledConnection implements XAConnection, XAResource
        clazz = pool.get(MysqlXAConnection.class.getName());
        instrumentJdbcMethods(clazz, javax.sql.XAConnection.class);
        clazz.writeFile(args[0]);

        // com.mysql.jdbc.jdbc2.optional.SuspendableXAConnection extends MysqlPooledConnection implements XAConnection, XAResource
        clazz = pool.get(SuspendableXAConnection.class.getName());
        instrumentJdbcMethods(clazz, javax.sql.XAConnection.class);
        clazz.writeFile(args[0]);

        /*
         * javax.sql.XADataSource
         */
        // com.mysql.jdbc.jdbc2.optional.MysqlXADataSource extends MysqlDataSource implements javax.sql.XADataSource
        clazz = pool.get(MysqlXADataSource.class.getName());
        instrumentJdbcMethods(clazz, javax.sql.DataSource.class);
        instrumentJdbcMethods(clazz, javax.sql.XADataSource.class);
        clazz.writeFile(args[0]);

        /*
         * javax.transaction.xa.Xid
         */
        // com.mysql.jdbc.jdbc2.optional.MysqlXid implements Xid
        clazz = pool.get(MysqlXid.class.getName());
        instrumentJdbcMethods(clazz, javax.transaction.xa.Xid.class);
        clazz.writeFile(args[0]);

        /*
         * TODO:
         * java.sql.DataTruncation
         */
        // com.mysql.jdbc.exceptions.MysqlDataTruncation extends DataTruncation

        /*
         * TODO:
         * java.sql.SQLException
         */
        // com.mysql.fabric.xmlrpc.exceptions.MySQLFabricException extends SQLException
        // com.mysql.jdbc.exceptions.NotUpdatable extends SQLException
        // com.mysql.jdbc.exceptions.OperationNotSupportedException extends SQLException
        // com.mysql.jdbc.exceptions.PacketTooBigException extends SQLException

        /*
         * TODO:
         * java.sql.SQLNonTransientException
         */
        // com.mysql.jdbc.exceptions.MySQLQueryInterruptedException extends SQLNonTransientException
        // com.mysql.jdbc.exceptions.MySQLStatementCancelledException extends SQLNonTransientException

        /*
         * TODO:
         * java.sql.SQLRecoverableException
         */
        // com.mysql.jdbc.exceptions.CommunicationsException extends SQLRecoverableException implements StreamingNotifiable
        // ---> com.mysql.jdbc.exceptions.ConnectionFeatureNotAvailableException extends CommunicationsException

        /*
         * TODO:
         * java.sql.SQLTransientException
         * ---> java.sql.SQLTimeoutException
         */
        // com.mysql.jdbc.exceptions.MySQLTimeoutException extends SQLTimeoutException

        /*
         * TODO:
         * java.sql.SQLTransientException
         * ---> java.sql.SQLTransactionRollbackException
         */
        // com.mysql.jdbc.exceptions.MySQLTransactionRollbackException extends SQLTransactionRollbackException implements DeadlockTimeoutRollbackMarker

        /*
         * TODO:
         * com.mysql.jdbc.jdbc2.optional.MysqlXAException extends javax.transaction.xa.XAException
         */

        /*
         * These classes have no implementations in c/J:
         * 
         * java.sql.Array
         * java.sql.BatchUpdateException
         * java.sql.ClientInfoStatus
         * java.sql.Date
         * java.sql.DriverManager
         * java.sql.DriverPropertyInfo
         * java.sql.PseudoColumnUsage
         * java.sql.Ref
         * java.sql.RowId
         * java.sql.RowIdLifetime
         * java.sql.SQLClientInfoException
         * java.sql.SQLData
         * java.sql.SQLDataException
         * java.sql.SQLFeatureNotSupportedException
         * java.sql.SQLInput
         * java.sql.SQLIntegrityConstraintViolationException
         * java.sql.SQLInvalidAuthorizationSpecException
         * java.sql.SQLNonTransientConnectionException
         * java.sql.SQLOutput
         * java.sql.SQLPermission
         * java.sql.SQLSyntaxErrorException
         * java.sql.SQLTransientConnectionException
         * java.sql.SQLWarning
         * java.sql.Struct
         * java.sql.Time
         * java.sql.Timestamp
         * java.sql.Types
         * 
         * javax.sql.CommonDataSource
         * javax.sql.ConnectionEvent
         * javax.sql.ConnectionEventListener
         * javax.sql.RowSet
         * javax.sql.RowSetEvent
         * javax.sql.RowSetInternal
         * javax.sql.RowSetListener
         * javax.sql.RowSetMetaData
         * javax.sql.RowSetReader
         * javax.sql.RowSetWriter
         * javax.sql.StatementEvent
         * javax.sql.StatementEventListener
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

    private static void instrumentJdbcMethods(CtClass cjClazz, Class<?> jdbcClass) throws Exception {
        instrumentJdbcMethods(cjClazz, jdbcClass, false, null);
    }

    /**
     * Instruments methods of cjClazz defined in jdbcClass.
     * 
     * @param cjClazz
     *            CtClass to be instrumented.
     * @param jdbcClass
     *            Class from JDBC specification where methods descriptors to be get.
     * @param declaredMethodsOnly
     *            true - instrument methods declared in this class, false - also instrument inherited methods
     * @return Array of CtMethods to be instrumented
     * @throws Exception
     */
    private static void instrumentJdbcMethods(CtClass cjClazz, Class<?> jdbcClass, boolean declaredMethodsOnly, String exceptionInterceptorStr)
            throws Exception {
        System.out.println("---");
        System.out.println(cjClazz.getName());

        Method[] methods;
        if (declaredMethodsOnly) {
            // instrument methods declared in this class which throws SQLException
            methods = jdbcClass.getDeclaredMethods();
        } else {
            // instrument all methods, declared in this class and it's superclasses, which throws SQLException
            methods = jdbcClass.getMethods();
        }

        for (Method method : methods) {
            CtMethod ctm = null;
            String prefix = "SKIPPED:         ";
            for (Class<?> exc : method.getExceptionTypes()) {
                if (exc.equals(SQLException.class)) {
                    prefix = "INSTRUMENTING... ";
                    String jdbcClassName = method.getName();
                    List<CtClass> params = new LinkedList<CtClass>();
                    for (Class<?> param : method.getParameterTypes()) {
                        params.add(pool.get(param.getName()));
                    }
                    try {
                        ctm = cjClazz.getDeclaredMethod(jdbcClassName, params.toArray(new CtClass[0]));
                    } catch (NotFoundException ex) {
                        // Just ignoring because the only reason is that the method is implemented in superclass
                        prefix = "NOT FOUND:       ";
                    }
                    break;
                }
            }
            System.out.print(prefix);
            System.out.print(method.toGenericString());
            if (ctm != null) {
                if (catchRuntimeException(cjClazz, ctm, exceptionInterceptorStr, false)) {
                    System.out.print(" ... DONE.");
                } else {
                    System.out.print(" ... ALREADY PROCESSED!!!");
                }
            }
            System.out.println();
        }

    }

    private static void catchRuntimeException(CtClass clazz, CtMethod m) throws Exception {
        catchRuntimeException(clazz, m, null, true);
    }

    private static void catchRuntimeException(CtClass clazz, CtMethod m, String exceptionInterceptorStr) throws Exception {
        catchRuntimeException(clazz, m, exceptionInterceptorStr, true);
    }

    private static boolean catchRuntimeException(CtClass clazz, CtMethod m, String exceptionInterceptorStr, boolean log) throws Exception {
        if (isProcessed(clazz.getClassFile().getName(), m)) {
            if (log) {
                System.out.println("ALREADY PROCESSED!!! " + m);
            }
            return false;
        }
        if (log) {
            System.out.println(m + ", " + exceptionInterceptorStr);
        }
        if (exceptionInterceptorStr == null) {
            m.addCatch("{throw " + SQLExceptionsMapping.class.getName() + ".translateException(ex);}", runTimeException, "ex");
        } else {
            m.addCatch("{throw " + SQLExceptionsMapping.class.getName() + ".translateException(ex, " + exceptionInterceptorStr + ");}", runTimeException, "ex");
        }
        processed.get(clazz.getClassFile().getName()).add(m);
        return true;
    }

    private static boolean isProcessed(String fileName, CtMethod m) throws Exception {
        List<CtMethod> methods = processed.get(fileName);
        if (methods != null) {
            if (methods.contains(m)) {
                return true;
            }
        } else {
            processed.put(fileName, new LinkedList<CtMethod>());
        }
        return false;
    }

}
