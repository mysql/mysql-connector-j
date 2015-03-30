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

import java.io.Reader;
import java.util.Properties;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;

import com.mysql.cj.core.exception.CJException;
import com.mysql.jdbc.Clob;
import com.mysql.jdbc.NonRegisteringDriver;
import com.mysql.jdbc.exceptions.SQLExceptionsMapping;

public class TranslateExceptions {

    private static CtClass runTimeException = null;

    public static void main(String[] args) throws Exception {
        ClassPool pool = ClassPool.getDefault();
        pool.insertClassPath(args[0]);

        runTimeException = pool.get(CJException.class.getName());

        // params classes
        CtClass ctClob = pool.get(java.sql.Clob.class.getName());
        CtClass ctInt = pool.get(int.class.getName());
        CtClass ctLong = pool.get(long.class.getName());
        CtClass ctReader = pool.get(Reader.class.getName());
        CtClass ctString = pool.get(String.class.getName());
        CtClass ctProperties = pool.get(Properties.class.getName());

        // class we want to instrument

        /*
         * java.sql.Driver
         */
        CtClass clazz = pool.get(NonRegisteringDriver.class.getName());
        instrumentMethodCatch(clazz.getDeclaredMethod("connect", new CtClass[] { ctString, ctProperties }));
        instrumentMethodCatch(clazz.getDeclaredMethod("acceptsURL", new CtClass[] { ctString }));
        instrumentMethodCatch(clazz.getDeclaredMethod("getPropertyInfo", new CtClass[] { ctString, ctProperties }));

        /*
         * java.sql.PreparedStatement
         */
        //clazz = pool.get(PreparedStatement.class.getName());
        //instrumentMethodCatch(clazz.getDeclaredMethod("getBytesRepresentationForBatch", new CtClass[] { ctInt, ctInt })); // TODO: internal method
        //instrumentMethodCatch(clazz.getDeclaredMethod("setCharacterStream", new CtClass[] { ctInt, ctReader, ctInt }));
        //instrumentMethodCatch(clazz.getDeclaredMethod("setClob", new CtClass[] { ctInt, ctClob }));

        /*
         * java.sql.Clob
         */
        //clazz = pool.get(Clob.class.getName());
        //instrumentMethodCatch(clazz.getDeclaredMethod("setString", new CtClass[] { ctLong, ctString, ctInt, ctInt }), "this.exceptionInterceptor");

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

        // write out the modified class file
        clazz.writeFile(args[0]);

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
