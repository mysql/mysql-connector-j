/*
  Copyright (c) 2002, 2016, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FOSS License Exception
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

package com.mysql.cj.jdbc.util;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.sql.DataTruncation;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.mysql.cj.api.MysqlConnection;
import com.mysql.cj.api.jdbc.JdbcConnection;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exceptions.CJException;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.exceptions.MysqlErrorNumbers;
import com.mysql.cj.core.util.Util;
import com.mysql.cj.jdbc.exceptions.MysqlDataTruncation;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.jdbc.io.ResultSetFactory;
import com.mysql.cj.mysqla.MysqlaSession;
import com.mysql.cj.mysqla.io.MysqlaProtocol;

/**
 * Utilities for dealing with result sets (used in testcases and profiler).
 */
public class ResultSetUtil {

    public static StringBuilder appendResultSetSlashGStyle(StringBuilder appendTo, ResultSet rs) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();

        int numFields = rsmd.getColumnCount();
        int maxWidth = 0;

        String[] fieldNames = new String[numFields];

        for (int i = 0; i < numFields; i++) {
            fieldNames[i] = rsmd.getColumnLabel(i + 1);

            if (fieldNames[i].length() > maxWidth) {
                maxWidth = fieldNames[i].length();
            }
        }

        int rowCount = 1;

        while (rs.next()) {
            appendTo.append("*************************** ");
            appendTo.append(rowCount++);
            appendTo.append(". row ***************************\n");

            for (int i = 0; i < numFields; i++) {
                int leftPad = maxWidth - fieldNames[i].length();

                for (int j = 0; j < leftPad; j++) {
                    appendTo.append(" ");
                }

                appendTo.append(fieldNames[i]);
                appendTo.append(": ");

                String stringVal = rs.getString(i + 1);

                if (stringVal != null) {
                    appendTo.append(stringVal);
                } else {
                    appendTo.append("NULL");
                }

                appendTo.append("\n");
            }

            appendTo.append("\n");
        }

        return appendTo;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static void resultSetToMap(Map mappedValues, ResultSet rs) throws SQLException {
        while (rs.next()) {
            mappedValues.put(rs.getObject(1), rs.getObject(2));
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static void resultSetToMap(Map mappedValues, java.sql.ResultSet rs, int key, int value) throws SQLException {
        while (rs.next()) {
            mappedValues.put(rs.getObject(key), rs.getObject(value));
        }
    }

    /**
     * Given a ResultSet and an index into the columns of that ResultSet, read
     * binary data from the column which represents a serialized object, and
     * re-create the object.
     * 
     * @param resultSet
     *            the ResultSet to use.
     * @param index
     *            an index into the ResultSet.
     * @return the object if it can be de-serialized
     * @throws SQLException
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws Exception
     *             if an error occurs
     */
    public static Object readObject(java.sql.ResultSet resultSet, int index) throws IOException, SQLException, ClassNotFoundException {
        ObjectInputStream objIn = new ObjectInputStream(resultSet.getBinaryStream(index));
        Object obj = objIn.readObject();
        objIn.close();

        return obj;
    }

    /**
     * Turns output of 'SHOW WARNINGS' into JDBC SQLWarning instances.
     * 
     * If 'forTruncationOnly' is true, only looks for truncation warnings, and
     * actually throws DataTruncation as an exception.
     * 
     * @param connection
     *            the connection to use for getting warnings.
     * 
     * @return the SQLWarning chain (or null if no warnings)
     * 
     * @throws SQLException
     *             if the warnings could not be retrieved
     */
    public static SQLWarning convertShowWarningsToSQLWarnings(MysqlConnection connection) throws SQLException {
        return convertShowWarningsToSQLWarnings(connection, 0, false);
    }

    /**
     * Turns output of 'SHOW WARNINGS' into JDBC SQLWarning instances.
     * 
     * If 'forTruncationOnly' is true, only looks for truncation warnings, and
     * actually throws DataTruncation as an exception.
     * 
     * @param connection
     *            the connection to use for getting warnings.
     * @param warningCountIfKnown
     *            the warning count (if known), otherwise set it to 0.
     * @param forTruncationOnly
     *            if this method should only scan for data truncation warnings
     * 
     * @return the SQLWarning chain (or null if no warnings)
     * 
     * @throws SQLException
     *             if the warnings could not be retrieved, or if data truncation
     *             is being scanned for and truncations were found.
     */
    public static SQLWarning convertShowWarningsToSQLWarnings(MysqlConnection connection, int warningCountIfKnown, boolean forTruncationOnly)
            throws SQLException {
        java.sql.Statement stmt = null;
        java.sql.ResultSet warnRs = null;

        SQLWarning currentWarning = null;

        try {
            if (warningCountIfKnown < 100) {
                stmt = ((JdbcConnection) connection).createStatement();

                if (stmt.getMaxRows() != 0) {
                    stmt.setMaxRows(0);
                }
            } else {
                // stream large warning counts
                stmt = ((JdbcConnection) connection).createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
                stmt.setFetchSize(Integer.MIN_VALUE);
            }

            /*
             * +---------+------+---------------------------------------------+ |
             * Level | Code | Message |
             * +---------+------+---------------------------------------------+ |
             * Warning | 1265 | Data truncated for column 'field1' at row 1 |
             * +---------+------+---------------------------------------------+
             */
            warnRs = stmt.executeQuery("SHOW WARNINGS");

            while (warnRs.next()) {
                int code = warnRs.getInt("Code");

                if (forTruncationOnly) {
                    if (code == MysqlErrorNumbers.ER_WARN_DATA_TRUNCATED || code == MysqlErrorNumbers.ER_WARN_DATA_OUT_OF_RANGE) {
                        DataTruncation newTruncation = new MysqlDataTruncation(warnRs.getString("Message"), 0, false, false, 0, 0, code);

                        if (currentWarning == null) {
                            currentWarning = newTruncation;
                        } else {
                            currentWarning.setNextWarning(newTruncation);
                        }
                    }
                } else {
                    //String level = warnRs.getString("Level"); 
                    String message = warnRs.getString("Message");

                    SQLWarning newWarning = new SQLWarning(message, SQLError.mysqlToSqlState(code), code);

                    if (currentWarning == null) {
                        currentWarning = newWarning;
                    } else {
                        currentWarning.setNextWarning(newWarning);
                    }
                }
            }

            if (forTruncationOnly && (currentWarning != null)) {
                throw currentWarning;
            }

            return currentWarning;
        } finally {
            SQLException reThrow = null;

            if (warnRs != null) {
                try {
                    warnRs.close();
                } catch (SQLException sqlEx) {
                    reThrow = sqlEx;
                }
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlEx) {
                    // ideally, we'd use chained exceptions here, but we still support JDK-1.2.x with this driver which doesn't have them....
                    reThrow = sqlEx;
                }
            }

            if (reThrow != null) {
                throw reThrow;
            }
        }
    }

    public static void appendDeadlockStatusInformation(MysqlConnection connection, String xOpen, StringBuilder errorBuf) {
        MysqlaSession session = (MysqlaSession) connection.getSession();
        MysqlaProtocol protocol = session.getProtocol();
        if (session.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_includeInnodbStatusInDeadlockExceptions).getValue() && xOpen != null
                && (xOpen.startsWith("40") || xOpen.startsWith("41")) && protocol.getStreamingData() == null) {
            ResultSet rs = null;

            try {
                rs = protocol.sqlQueryDirect(null, "SHOW ENGINE INNODB STATUS",
                        session.getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_characterEncoding).getValue(), null, -1, false,
                        ((JdbcConnection) connection).getCatalog(), null, session::getProfilerEventHandlerInstanceFunction,
                        new ResultSetFactory((JdbcConnection) connection, null));

                if (rs.next()) {
                    errorBuf.append("\n\n");
                    errorBuf.append(rs.getString("Status"));
                } else {
                    errorBuf.append("\n\n");
                    errorBuf.append(Messages.getString("MysqlIO.NoInnoDBStatusFound"));
                }
            } catch (IOException | SQLException | CJException ex) {
                errorBuf.append("\n\n");
                errorBuf.append(Messages.getString("MysqlIO.InnoDBStatusFailed"));
                errorBuf.append("\n\n");
                errorBuf.append(Util.stackTraceToString(ex));
            } finally {
                if (rs != null) {
                    try {
                        rs.close();
                    } catch (SQLException ex) {
                        throw ExceptionFactory.createException(ex.getMessage(), ex);
                    }
                }
            }
        }

        if (session.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_includeThreadDumpInDeadlockExceptions).getValue()) {
            errorBuf.append("\n\n*** Java threads running at time of deadlock ***\n\n");

            ThreadMXBean threadMBean = ManagementFactory.getThreadMXBean();
            long[] threadIds = threadMBean.getAllThreadIds();

            ThreadInfo[] threads = threadMBean.getThreadInfo(threadIds, Integer.MAX_VALUE);
            List<ThreadInfo> activeThreads = new ArrayList<ThreadInfo>();

            for (ThreadInfo info : threads) {
                if (info != null) {
                    activeThreads.add(info);
                }
            }

            for (ThreadInfo threadInfo : activeThreads) {
                // "Thread-60" daemon prio=1 tid=0x093569c0 nid=0x1b99 in Object.wait()

                errorBuf.append('"');
                errorBuf.append(threadInfo.getThreadName());
                errorBuf.append("\" tid=");
                errorBuf.append(threadInfo.getThreadId());
                errorBuf.append(" ");
                errorBuf.append(threadInfo.getThreadState());

                if (threadInfo.getLockName() != null) {
                    errorBuf.append(" on lock=" + threadInfo.getLockName());
                }
                if (threadInfo.isSuspended()) {
                    errorBuf.append(" (suspended)");
                }
                if (threadInfo.isInNative()) {
                    errorBuf.append(" (running in native)");
                }

                StackTraceElement[] stackTrace = threadInfo.getStackTrace();

                if (stackTrace.length > 0) {
                    errorBuf.append(" in ");
                    errorBuf.append(stackTrace[0].getClassName());
                    errorBuf.append(".");
                    errorBuf.append(stackTrace[0].getMethodName());
                    errorBuf.append("()");
                }

                errorBuf.append("\n");

                if (threadInfo.getLockOwnerName() != null) {
                    errorBuf.append("\t owned by " + threadInfo.getLockOwnerName() + " Id=" + threadInfo.getLockOwnerId());
                    errorBuf.append("\n");
                }

                for (int j = 0; j < stackTrace.length; j++) {
                    StackTraceElement ste = stackTrace[j];
                    errorBuf.append("\tat " + ste.toString());
                    errorBuf.append("\n");
                }
            }
        }
    }

}
