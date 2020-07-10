/*
 * Copyright (c) 2020, Oracle and/or its affiliates.
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

package testsuite.simple;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.Callable;

import org.junit.jupiter.api.Test;

import com.mysql.cj.MysqlType;
import com.mysql.cj.conf.PropertyKey;

import testsuite.BaseTestCase;

public class DateTimeTest extends BaseTestCase {

    @Test
    public void testSetObjectLocalDateInDifferentTimezone() throws Exception {
        boolean withFract = versionMeetsMinimum(5, 6, 4); // fractional seconds are not supported in previous versions
        final String tableYear = "testSetObjectYear";
        final String tableDate = "testSetObjectDate";
        final String tableTime = "testSetObjectTime";
        final String tableDatetime = "testSetObjectDatetime";
        final String tableTimestamp = "testSetObjectTimestamp";
        final String tableVarchar = "testSetObjectVarchar";
        createTable(tableYear, "(id INT, d YEAR)");
        createTable(tableDate, "(id INT, d DATE)");
        createTable(tableTime, "(id INT, d TIME)");
        createTable(tableDatetime, "(id INT, d DATETIME)");
        createTable(tableTimestamp, "(id INT, d TIMESTAMP)");
        createTable(tableVarchar, "(id INT, d VARCHAR(30))");

        Properties props = new Properties();

        for (boolean useSSPS : new boolean[] { false, true }) {
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "" + useSSPS);

            /* Unsupported conversions */

            assertThrows(SQLException.class, ".* Conversion from java.time.LocalDate to TIME is not supported.", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableVarchar, LocalDate.of(2019, 12, 31), MysqlType.TIME, "00:00:00", null, props);
                    return null;
                }
            });

            assertThrows(SQLException.class, ".* Conversion from java.time.LocalDate to INT is not supported.", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableDate, LocalDate.of(2019, 12, 31), MysqlType.INT, "2019-12-31", null, props);
                    return null;
                }
            });

            /* Into YEAR field */

            if (useSSPS && withFract) {
                // TODO different behaviour for CSPS and SSPS. Server bug?
                setObjectInDifferentTimezone(tableYear, LocalDate.of(2019, 12, 31), null, "2019", LocalDate.of(2019, 01, 01), props);
                setObjectInDifferentTimezone(tableYear, LocalDate.of(2019, 12, 31), MysqlType.DATE, "2019", LocalDate.of(2019, 01, 01), props);
            } else {
                assertThrows(SQLException.class, "Data truncated for column 'd' at row 1", new Callable<Void>() {
                    public Void call() throws Exception {
                        setObjectInDifferentTimezone(tableYear, LocalDate.of(2019, 12, 31), null, "2019", null, props);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Data truncated for column 'd' at row 1", new Callable<Void>() {
                    public Void call() throws Exception {
                        setObjectInDifferentTimezone(tableYear, LocalDate.of(2019, 12, 31), MysqlType.DATE, "2019", null, props);
                        return null;
                    }
                });
            }
            assertThrows(SQLException.class, "Data truncated for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableYear, LocalDate.of(2019, 12, 31), MysqlType.CHAR, "2019", null, props);
                    return null;
                }
            });
            assertThrows(SQLException.class, "Data truncated for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableYear, LocalDate.of(2019, 12, 31), MysqlType.VARCHAR, "2019", null, props);
                    return null;
                }
            });
            assertThrows(SQLException.class, "Data truncated for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableYear, LocalDate.of(2019, 12, 31), MysqlType.TINYTEXT, "2019", null, props);
                    return null;
                }
            });
            assertThrows(SQLException.class, "Data truncated for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableYear, LocalDate.of(2019, 12, 31), MysqlType.TEXT, "2019", null, props);
                    return null;
                }
            });
            assertThrows(SQLException.class, "Data truncated for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableYear, LocalDate.of(2019, 12, 31), MysqlType.MEDIUMTEXT, "2019", null, props);
                    return null;
                }
            });
            assertThrows(SQLException.class, "Data truncated for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableYear, LocalDate.of(2019, 12, 31), MysqlType.LONGTEXT, "2019", null, props);
                    return null;
                }
            });
            if (useSSPS && withFract) {
                // TODO different behaviour for CSPS and SSPS. Server bug?
                setObjectInDifferentTimezone(tableYear, LocalDate.of(2019, 12, 31), MysqlType.DATETIME, "2019", LocalDate.of(2019, 01, 01), props);
                setObjectInDifferentTimezone(tableYear, LocalDate.of(2019, 12, 31), MysqlType.TIMESTAMP, "2019", LocalDate.of(2019, 01, 01), props);
            } else {
                assertThrows(SQLException.class, "Data truncated for column 'd' at row 1", new Callable<Void>() {
                    public Void call() throws Exception {
                        setObjectInDifferentTimezone(tableYear, LocalDate.of(2019, 12, 31), MysqlType.DATETIME, "2019", null, props);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Data truncated for column 'd' at row 1", new Callable<Void>() {
                    public Void call() throws Exception {
                        setObjectInDifferentTimezone(tableYear, LocalDate.of(2019, 12, 31), MysqlType.TIMESTAMP, "2019", null, props);
                        return null;
                    }
                });
            }
            setObjectInDifferentTimezone(tableYear, LocalDate.of(2019, 12, 31), MysqlType.YEAR, "2019", LocalDate.of(2019, 01, 01), props);

            /* Into DATE field */

            setObjectInDifferentTimezone(tableDate, LocalDate.of(2019, 12, 31), null, "2019-12-31", LocalDate.of(2019, 12, 31), props);
            setObjectInDifferentTimezone(tableDate, LocalDate.of(2019, 12, 31), MysqlType.DATE, "2019-12-31", LocalDate.of(2019, 12, 31), props);
            setObjectInDifferentTimezone(tableDate, LocalDate.of(2019, 12, 31), MysqlType.CHAR, "2019-12-31", LocalDate.of(2019, 12, 31), props);
            setObjectInDifferentTimezone(tableDate, LocalDate.of(2019, 12, 31), MysqlType.VARCHAR, "2019-12-31", LocalDate.of(2019, 12, 31), props);
            setObjectInDifferentTimezone(tableDate, LocalDate.of(2019, 12, 31), MysqlType.TINYTEXT, "2019-12-31", LocalDate.of(2019, 12, 31), props);
            setObjectInDifferentTimezone(tableDate, LocalDate.of(2019, 12, 31), MysqlType.TEXT, "2019-12-31", LocalDate.of(2019, 12, 31), props);
            setObjectInDifferentTimezone(tableDate, LocalDate.of(2019, 12, 31), MysqlType.MEDIUMTEXT, "2019-12-31", LocalDate.of(2019, 12, 31), props);
            setObjectInDifferentTimezone(tableDate, LocalDate.of(2019, 12, 31), MysqlType.LONGTEXT, "2019-12-31", LocalDate.of(2019, 12, 31), props);
            setObjectInDifferentTimezone(tableDate, LocalDate.of(2019, 12, 31), MysqlType.DATETIME, "2019-12-31 00:00:00", LocalDate.of(2019, 12, 31), props);
            setObjectInDifferentTimezone(tableDate, LocalDate.of(2019, 12, 31), MysqlType.TIMESTAMP, "2019-12-31 00:00:00", LocalDate.of(2019, 12, 31), props);
            assertThrows(SQLException.class, "Data truncation: Incorrect date value: '2019' for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableDate, LocalDate.of(2019, 12, 31), MysqlType.YEAR, "2019", null, props);
                    return null;
                }
            });

            /* Into TIME field */

            if (useSSPS) {
                // TODO different behaviour for CSPS and SSPS. Server bug?
                setObjectInDifferentTimezone(tableTime, LocalDate.of(2019, 12, 31), null, "00:00:00",
                        new SQLDataException("Unsupported conversion from TIME to java.time.LocalDate"), props);
                setObjectInDifferentTimezone(tableTime, LocalDate.of(2019, 12, 31), MysqlType.DATE, "00:00:00",
                        new SQLDataException("Unsupported conversion from TIME to java.time.LocalDate"), props);
            } else {
                assertThrows(SQLException.class, "Data truncation: Incorrect time value: '2019-12-31' for column 'd' at row 1", new Callable<Void>() {
                    public Void call() throws Exception {
                        setObjectInDifferentTimezone(tableTime, LocalDate.of(2019, 12, 31), null, "00:00:00", null, props);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Data truncation: Incorrect time value: '2019-12-31' for column 'd' at row 1", new Callable<Void>() {
                    public Void call() throws Exception {
                        setObjectInDifferentTimezone(tableTime, LocalDate.of(2019, 12, 31), MysqlType.DATE, "00:00:00", null, props);
                        return null;
                    }
                });
            }
            assertThrows(SQLException.class, "Data truncation: Incorrect time value: '2019-12-31' for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableTime, LocalDate.of(2019, 12, 31), MysqlType.CHAR, "00:00:00", null, props);
                    return null;
                }
            });
            assertThrows(SQLException.class, "Data truncation: Incorrect time value: '2019-12-31' for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableTime, LocalDate.of(2019, 12, 31), MysqlType.VARCHAR, "00:00:00", null, props);
                    return null;
                }
            });
            assertThrows(SQLException.class, "Data truncation: Incorrect time value: '2019-12-31' for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableTime, LocalDate.of(2019, 12, 31), MysqlType.TINYTEXT, "00:00:00", null, props);
                    return null;
                }
            });
            assertThrows(SQLException.class, "Data truncation: Incorrect time value: '2019-12-31' for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableTime, LocalDate.of(2019, 12, 31), MysqlType.TEXT, "00:00:00", null, props);
                    return null;
                }
            });
            assertThrows(SQLException.class, "Data truncation: Incorrect time value: '2019-12-31' for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableTime, LocalDate.of(2019, 12, 31), MysqlType.MEDIUMTEXT, "00:00:00", null, props);
                    return null;
                }
            });
            assertThrows(SQLException.class, "Data truncation: Incorrect time value: '2019-12-31' for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableTime, LocalDate.of(2019, 12, 31), MysqlType.LONGTEXT, "00:00:00", null, props);
                    return null;
                }
            });
            setObjectInDifferentTimezone(tableTime, LocalDate.of(2019, 12, 31), MysqlType.DATETIME, "00:00:00",
                    new SQLDataException("Unsupported conversion from TIME to java.time.LocalDate"), props);
            setObjectInDifferentTimezone(tableTime, LocalDate.of(2019, 12, 31), MysqlType.TIMESTAMP, "00:00:00",
                    new SQLDataException("Unsupported conversion from TIME to java.time.LocalDate"), props);
            setObjectInDifferentTimezone(tableTime, LocalDate.of(2019, 12, 31), MysqlType.YEAR, "2019",
                    new SQLDataException("Unsupported conversion from TIME to java.time.LocalDate"), props); // TIME can recognize numbers as a short notation, thus it works here
            setObjectInDifferentTimezone(tableTime, LocalDate.of(2019, 12, 31), MysqlType.YEAR, "00:20:19.0",
                    new SQLDataException("Unsupported conversion from TIME to java.time.LocalDate"), props); // TIME can recognize numbers as a short notation, thus it works here

            /* Into DATETIME field */

            setObjectInDifferentTimezone(tableDatetime, LocalDate.of(2019, 12, 31), null, "2019-12-31 00:00:00", LocalDate.of(2019, 12, 31), props);
            setObjectInDifferentTimezone(tableDatetime, LocalDate.of(2019, 12, 31), MysqlType.DATE, "2019-12-31 00:00:00", LocalDate.of(2019, 12, 31), props);
            setObjectInDifferentTimezone(tableDatetime, LocalDate.of(2019, 12, 31), MysqlType.CHAR, "2019-12-31 00:00:00", LocalDate.of(2019, 12, 31), props);
            setObjectInDifferentTimezone(tableDatetime, LocalDate.of(2019, 12, 31), MysqlType.VARCHAR, "2019-12-31 00:00:00", LocalDate.of(2019, 12, 31),
                    props);
            setObjectInDifferentTimezone(tableDatetime, LocalDate.of(2019, 12, 31), MysqlType.TINYTEXT, "2019-12-31 00:00:00", LocalDate.of(2019, 12, 31),
                    props);
            setObjectInDifferentTimezone(tableDatetime, LocalDate.of(2019, 12, 31), MysqlType.TEXT, "2019-12-31 00:00:00", LocalDate.of(2019, 12, 31), props);
            setObjectInDifferentTimezone(tableDatetime, LocalDate.of(2019, 12, 31), MysqlType.MEDIUMTEXT, "2019-12-31 00:00:00", LocalDate.of(2019, 12, 31),
                    props);
            setObjectInDifferentTimezone(tableDatetime, LocalDate.of(2019, 12, 31), MysqlType.LONGTEXT, "2019-12-31 00:00:00", LocalDate.of(2019, 12, 31),
                    props);
            setObjectInDifferentTimezone(tableDatetime, LocalDate.of(2019, 12, 31), MysqlType.DATETIME, "2019-12-31 00:00:00", LocalDate.of(2019, 12, 31),
                    props);
            setObjectInDifferentTimezone(tableDatetime, LocalDate.of(2019, 12, 31), MysqlType.TIMESTAMP, "2019-12-31 00:00:00", LocalDate.of(2019, 12, 31),
                    props);
            assertThrows(SQLException.class, "Data truncation: Incorrect datetime value: '2019' for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableDatetime, LocalDate.of(2019, 12, 31), MysqlType.YEAR, "2019", null, props);
                    return null;
                }
            });

            /* Into TIMESTAMP field */

            setObjectInDifferentTimezone(tableTimestamp, LocalDate.of(2019, 12, 31), null, "2019-12-31 00:00:00", LocalDate.of(2019, 12, 31), props);
            setObjectInDifferentTimezone(tableTimestamp, LocalDate.of(2019, 12, 31), MysqlType.DATE, "2019-12-31 00:00:00", LocalDate.of(2019, 12, 31), props);
            setObjectInDifferentTimezone(tableTimestamp, LocalDate.of(2019, 12, 31), MysqlType.CHAR, "2019-12-31 00:00:00", LocalDate.of(2019, 12, 31), props);
            setObjectInDifferentTimezone(tableTimestamp, LocalDate.of(2019, 12, 31), MysqlType.VARCHAR, "2019-12-31 00:00:00", LocalDate.of(2019, 12, 31),
                    props);
            setObjectInDifferentTimezone(tableTimestamp, LocalDate.of(2019, 12, 31), MysqlType.TINYTEXT, "2019-12-31 00:00:00", LocalDate.of(2019, 12, 31),
                    props);
            setObjectInDifferentTimezone(tableTimestamp, LocalDate.of(2019, 12, 31), MysqlType.TEXT, "2019-12-31 00:00:00", LocalDate.of(2019, 12, 31), props);
            setObjectInDifferentTimezone(tableTimestamp, LocalDate.of(2019, 12, 31), MysqlType.MEDIUMTEXT, "2019-12-31 00:00:00", LocalDate.of(2019, 12, 31),
                    props);
            setObjectInDifferentTimezone(tableTimestamp, LocalDate.of(2019, 12, 31), MysqlType.LONGTEXT, "2019-12-31 00:00:00", LocalDate.of(2019, 12, 31),
                    props);
            setObjectInDifferentTimezone(tableTimestamp, LocalDate.of(2019, 12, 31), MysqlType.DATETIME, "2019-12-31 00:00:00", LocalDate.of(2019, 12, 31),
                    props);
            setObjectInDifferentTimezone(tableTimestamp, LocalDate.of(2019, 12, 31), MysqlType.TIMESTAMP, "2019-12-31 00:00:00", LocalDate.of(2019, 12, 31),
                    props);
            assertThrows(SQLException.class, "Data truncation: Incorrect datetime value: '2019' for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableTimestamp, LocalDate.of(2019, 12, 31), MysqlType.YEAR, "2019", null, props);
                    return null;
                }
            });

            /* Into VARCHAR field */

            setObjectInDifferentTimezone(tableVarchar, LocalDate.of(2019, 12, 31), null, "2019-12-31", LocalDate.of(2019, 12, 31), props);
            setObjectInDifferentTimezone(tableVarchar, LocalDate.of(2019, 12, 31), MysqlType.DATE, "2019-12-31", LocalDate.of(2019, 12, 31), props);
            setObjectInDifferentTimezone(tableVarchar, LocalDate.of(2019, 12, 31), MysqlType.CHAR, "2019-12-31", LocalDate.of(2019, 12, 31), props);
            setObjectInDifferentTimezone(tableVarchar, LocalDate.of(2019, 12, 31), MysqlType.VARCHAR, "2019-12-31", LocalDate.of(2019, 12, 31), props);
            setObjectInDifferentTimezone(tableVarchar, LocalDate.of(2019, 12, 31), MysqlType.TINYTEXT, "2019-12-31", LocalDate.of(2019, 12, 31), props);
            setObjectInDifferentTimezone(tableVarchar, LocalDate.of(2019, 12, 31), MysqlType.TEXT, "2019-12-31", LocalDate.of(2019, 12, 31), props);
            setObjectInDifferentTimezone(tableVarchar, LocalDate.of(2019, 12, 31), MysqlType.MEDIUMTEXT, "2019-12-31", LocalDate.of(2019, 12, 31), props);
            setObjectInDifferentTimezone(tableVarchar, LocalDate.of(2019, 12, 31), MysqlType.LONGTEXT, "2019-12-31", LocalDate.of(2019, 12, 31), props);
            setObjectInDifferentTimezone(tableVarchar, LocalDate.of(2019, 12, 31), MysqlType.DATETIME, "2019-12-31 00:00:00", LocalDate.of(2019, 12, 31),
                    props);
            setObjectInDifferentTimezone(tableVarchar, LocalDate.of(2019, 12, 31), MysqlType.TIMESTAMP, "2019-12-31 00:00:00", LocalDate.of(2019, 12, 31),
                    props);
            setObjectInDifferentTimezone(tableVarchar, LocalDate.of(2019, 12, 31), MysqlType.YEAR, "2019",
                    new SQLDataException("Cannot convert string '2019' to java.time.LocalDate value"), props);
        }
    }

    @Test
    public void testSetObjectLocalTimeInDifferentTimezone() throws Exception {
        boolean withFract = versionMeetsMinimum(5, 6, 4); // fractional seconds are not supported in previous versions
        final String tableYear = "testSetObjectYear";
        final String tableDate = "testSetObjectDate";
        final String tableTime = "testSetObjectTime";
        final String tableDatetime = "testSetObjectDatetime";
        final String tableTimestamp = "testSetObjectTimestamp";
        final String tableVarchar = "testSetObjectVarchar";
        createTable(tableYear, "(id INT, d YEAR)");
        createTable(tableDate, "(id INT, d DATE)");
        createTable(tableTime, withFract ? "(id INT, d TIME(6))" : "(id INT, d TIME)");
        createTable(tableDatetime, withFract ? "(id INT, d DATETIME(6))" : "(id INT, d DATETIME)");
        createTable(tableTimestamp, withFract ? "(id INT, d TIMESTAMP(6))" : "(id INT, d TIMESTAMP)");
        createTable(tableVarchar, "(id INT, d VARCHAR(30))");

        // truncation/rounding is tested in StatementRegressionTest#testBug77449
        Properties props = new Properties();
        props.setProperty(PropertyKey.useSSL.getKeyName(), "false");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");

        if (!withFract) {
            // applying 8.0 defaults to old servers
            props.setProperty(PropertyKey.sessionVariables.getKeyName(), "sql_mode='NO_ZERO_IN_DATE'");
        }

        String dateErr = withFract ? "Data truncation: Incorrect date value: '11:22:33.123456' for column 'd' at row 1"
                : "Data truncation: Incorrect date value: '11:22:33' for column 'd' at row 1";

        for (boolean useSSPS : new boolean[] { false, true }) {
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "" + useSSPS);

            /* Unsupported conversions */

            assertThrows(SQLException.class, ".* Conversion from java.time.LocalTime to DATE is not supported.", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableVarchar, LocalTime.of(11, 22, 33, 123456000), MysqlType.DATE,
                            withFract ? "1970-01-01 11:22:33.123456" : "1970-01-01 11:22:33.123456", null, props);
                    return null;
                }
            });
            assertThrows(SQLException.class, ".* Conversion from java.time.LocalTime to DATETIME is not supported.", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableVarchar, LocalTime.of(11, 22, 33, 123456000), MysqlType.DATETIME,
                            withFract ? "1970-01-01 11:22:33.123456" : "1970-01-01 11:22:33.123456", null, props);
                    return null;
                }
            });
            assertThrows(SQLException.class, ".* Conversion from java.time.LocalTime to TIMESTAMP is not supported.", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableVarchar, LocalTime.of(11, 22, 33, 123456000), MysqlType.TIMESTAMP,
                            withFract ? "1970-01-01 11:22:33.123456" : "1970-01-01 11:22:33", null, props);
                    return null;
                }
            });
            assertThrows(SQLException.class, ".* Conversion from java.time.LocalTime to YEAR is not supported.", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableVarchar, LocalTime.of(11, 22, 33, 123456000), MysqlType.YEAR, "1971", null, props);
                    return null;
                }
            });

            /* Into YEAR field */

            if (useSSPS && withFract) {
                // TODO different behaviour for CSPS and SSPS. Server bug?
                setObjectInDifferentTimezone(tableYear, LocalTime.of(11, 22, 33, 123456000), null, LocalDate.now().getYear() + "",
                        new SQLDataException("Unsupported conversion from YEAR to java.time.LocalTime"), props);
            } else {
                assertThrows(SQLException.class, "Data truncated for column 'd' at row 1", new Callable<Void>() {
                    public Void call() throws Exception {
                        setObjectInDifferentTimezone(tableYear, LocalTime.of(11, 22, 33, 123456000), null, "1970", null, props);
                        return null;
                    }
                });
            }
            assertThrows(SQLException.class, "Data truncated for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableYear, LocalTime.of(11, 22, 33, 123456000), MysqlType.CHAR, "1970", null, props);
                    return null;
                }
            });
            assertThrows(SQLException.class, "Data truncated for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableYear, LocalTime.of(11, 22, 33, 123456000), MysqlType.VARCHAR, "1970", null, props);
                    return null;
                }
            });
            assertThrows(SQLException.class, "Data truncated for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableYear, LocalTime.of(11, 22, 33, 123456000), MysqlType.TINYTEXT, "1970", null, props);
                    return null;
                }
            });
            assertThrows(SQLException.class, "Data truncated for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableYear, LocalTime.of(11, 22, 33, 123456000), MysqlType.TEXT, "1970", null, props);
                    return null;
                }
            });
            assertThrows(SQLException.class, "Data truncated for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableYear, LocalTime.of(11, 22, 33, 123456000), MysqlType.MEDIUMTEXT, "1970", null, props);
                    return null;
                }
            });
            assertThrows(SQLException.class, "Data truncated for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableYear, LocalTime.of(11, 22, 33, 123456000), MysqlType.LONGTEXT, "1970", null, props);
                    return null;
                }
            });
            if (useSSPS && withFract) {
                // TODO different behaviour for CSPS and SSPS. Server bug?
                setObjectInDifferentTimezone(tableYear, LocalTime.of(11, 22, 33, 123456000), MysqlType.TIME, LocalDate.now().getYear() + "",
                        new SQLDataException("Unsupported conversion from YEAR to java.time.LocalTime"), props);
            } else {
                assertThrows(SQLException.class, "Data truncated for column 'd' at row 1", new Callable<Void>() {
                    public Void call() throws Exception {
                        setObjectInDifferentTimezone(tableYear, LocalTime.of(11, 22, 33, 123456000), MysqlType.TIME, "1970", null, props);
                        return null;
                    }
                });
            }

            /* Into DATE field */

            if (useSSPS && withFract) {
                // TODO different behaviour for CSPS and SSPS. Server bug?
                setObjectInDifferentTimezone(tableDate, LocalTime.of(11, 22, 33, 123456000), null, LocalDate.now().toString(),
                        new SQLDataException("Unsupported conversion from DATE to java.time.LocalTime"), props);
            } else {
                assertThrows(SQLException.class, useSSPS ? "Data truncated for column 'd' at row 1" : dateErr, new Callable<Void>() {
                    public Void call() throws Exception {
                        setObjectInDifferentTimezone(tableDate, LocalTime.of(11, 22, 33, 123456000), null, "1970-01-01", null, props);
                        return null;
                    }
                });
            }
            assertThrows(SQLException.class, "Data truncation: Incorrect date value: '11:22:33.123456' for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableDate, LocalTime.of(11, 22, 33, 123456000), MysqlType.CHAR, "1970-01-01", null, props);
                    return null;
                }
            });
            assertThrows(SQLException.class, "Data truncation: Incorrect date value: '11:22:33.123456' for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableDate, LocalTime.of(11, 22, 33, 123456000), MysqlType.VARCHAR, "1970-01-01", null, props);
                    return null;
                }
            });
            assertThrows(SQLException.class, "Data truncation: Incorrect date value: '11:22:33.123456' for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableDate, LocalTime.of(11, 22, 33, 123456000), MysqlType.TINYTEXT, "1970-01-01", null, props);
                    return null;
                }
            });
            assertThrows(SQLException.class, "Data truncation: Incorrect date value: '11:22:33.123456' for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableDate, LocalTime.of(11, 22, 33, 123456000), MysqlType.TEXT, "1970-01-01", null, props);
                    return null;
                }
            });
            assertThrows(SQLException.class, "Data truncation: Incorrect date value: '11:22:33.123456' for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableDate, LocalTime.of(11, 22, 33, 123456000), MysqlType.MEDIUMTEXT, "1970-01-01", null, props);
                    return null;
                }
            });
            assertThrows(SQLException.class, "Data truncation: Incorrect date value: '11:22:33.123456' for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableDate, LocalTime.of(11, 22, 33, 123456000), MysqlType.LONGTEXT, "1970-01-01", null, props);
                    return null;
                }
            });
            if (useSSPS && withFract) {
                // TODO different behaviour for CSPS and SSPS. Server bug?
                setObjectInDifferentTimezone(tableDate, LocalTime.of(11, 22, 33, 123456000), MysqlType.TIME, LocalDate.now().toString(),
                        new SQLDataException("Unsupported conversion from DATE to java.time.LocalTime"), props);
            } else {
                assertThrows(SQLException.class, useSSPS ? "Data truncated for column 'd' at row 1" : dateErr, new Callable<Void>() {
                    public Void call() throws Exception {
                        setObjectInDifferentTimezone(tableDate, LocalTime.of(11, 22, 33, 123456000), MysqlType.TIME, "1970-01-01", null, props);
                        return null;
                    }
                });
            }

            /* Into TIME field */

            LocalTime expLT = withFract ? LocalTime.of(11, 22, 33, 123456000) : LocalTime.of(11, 22, 33, 0);

            setObjectInDifferentTimezone(tableTime, LocalTime.of(11, 22, 33, 123456000), null, "11:22:33.123456", expLT, props);
            setObjectInDifferentTimezone(tableTime, LocalTime.of(11, 22, 33), MysqlType.CHAR, "11:22:33", LocalTime.of(11, 22, 33), props);
            setObjectInDifferentTimezone(tableTime, LocalTime.of(11, 22, 33, 123456000), MysqlType.CHAR, "11:22:33.123456", expLT, props);
            setObjectInDifferentTimezone(tableTime, LocalTime.of(11, 22, 33, 123456000), MysqlType.VARCHAR, "11:22:33.123456", expLT, props);
            setObjectInDifferentTimezone(tableTime, LocalTime.of(11, 22, 33, 123456000), MysqlType.TINYTEXT, "11:22:33.123456", expLT, props);
            setObjectInDifferentTimezone(tableTime, LocalTime.of(11, 22, 33, 123456000), MysqlType.TEXT, "11:22:33.123456", expLT, props);
            setObjectInDifferentTimezone(tableTime, LocalTime.of(11, 22, 33, 123456000), MysqlType.MEDIUMTEXT, "11:22:33.123456", expLT, props);
            setObjectInDifferentTimezone(tableTime, LocalTime.of(11, 22, 33, 123456000), MysqlType.LONGTEXT, "11:22:33.123456", expLT, props);
            setObjectInDifferentTimezone(tableTime, LocalTime.of(11, 22, 33), MysqlType.TIME, "11:22:33", LocalTime.of(11, 22, 33), props);
            setObjectInDifferentTimezone(tableTime, LocalTime.of(11, 22, 33, 123456000), MysqlType.TIME, "11:22:33.123456", expLT, props);

            /* Into DATETIME field */

            String datetimeErr = withFract ? "Data truncation: Incorrect datetime value: '11:22:33.123456' for column 'd' at row 1"
                    : "Data truncation: Incorrect datetime value: '11:22:33' for column 'd' at row 1";

            if (useSSPS && withFract) {
                // TODO different behaviour for CSPS and SSPS. Server bug?
                setObjectInDifferentTimezone(tableDatetime, LocalTime.of(11, 22, 33, 123456000), null, LocalDate.now() + " 11:22:33.123456",
                        LocalTime.of(11, 22, 33, 123456000), props);
            } else {
                assertThrows(SQLException.class, useSSPS ? "Data truncated for column 'd' at row 1" : datetimeErr, new Callable<Void>() {
                    public Void call() throws Exception {
                        setObjectInDifferentTimezone(tableDatetime, LocalTime.of(11, 22, 33, 123456000), null, "1970-01-01 11:22:33.123456", null, props);
                        return null;
                    }
                });
            }
            assertThrows(SQLException.class, "Data truncation: Incorrect datetime value: '11:22:33.123456' for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableDatetime, LocalTime.of(11, 22, 33, 123456000), MysqlType.CHAR, "1970-01-01 11:22:33.123456", null, props);
                    return null;
                }
            });
            assertThrows(SQLException.class, "Data truncation: Incorrect datetime value: '11:22:33.123456' for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableDatetime, LocalTime.of(11, 22, 33, 123456000), MysqlType.VARCHAR, "1970-01-01 11:22:33.123456", null,
                            props);
                    return null;
                }
            });
            assertThrows(SQLException.class, "Data truncation: Incorrect datetime value: '11:22:33.123456' for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableDatetime, LocalTime.of(11, 22, 33, 123456000), MysqlType.TINYTEXT, "1970-01-01 11:22:33.123456", null,
                            props);
                    return null;
                }
            });
            assertThrows(SQLException.class, "Data truncation: Incorrect datetime value: '11:22:33.123456' for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableDatetime, LocalTime.of(11, 22, 33, 123456000), MysqlType.TEXT, "1970-01-01 11:22:33.123456", null, props);
                    return null;
                }
            });
            assertThrows(SQLException.class, "Data truncation: Incorrect datetime value: '11:22:33.123456' for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableDatetime, LocalTime.of(11, 22, 33, 123456000), MysqlType.MEDIUMTEXT, "1970-01-01 11:22:33.123456", null,
                            props);
                    return null;
                }
            });
            assertThrows(SQLException.class, "Data truncation: Incorrect datetime value: '11:22:33.123456' for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableDatetime, LocalTime.of(11, 22, 33, 123456000), MysqlType.LONGTEXT, "1970-01-01 11:22:33.123456", null,
                            props);
                    return null;
                }
            });
            if (useSSPS && withFract) {
                // TODO different behaviour for CSPS and SSPS. Server bug?
                setObjectInDifferentTimezone(tableDatetime, LocalTime.of(11, 22, 33, 123456000), MysqlType.TIME, LocalDate.now() + " 11:22:33.123456",
                        LocalTime.of(11, 22, 33, 123456000), props);
            } else {
                assertThrows(SQLException.class, useSSPS ? "Data truncated for column 'd' at row 1" : datetimeErr, new Callable<Void>() {
                    public Void call() throws Exception {
                        setObjectInDifferentTimezone(tableDatetime, LocalTime.of(11, 22, 33, 123456000), MysqlType.TIME, "1970-01-01 11:22:33.123456", null,
                                props);
                        return null;
                    }
                });
            }

            /* Into TIMESTAMP field */

            if (useSSPS && withFract) {
                // TODO different behaviour for CSPS and SSPS. Server bug?
                setObjectInDifferentTimezone(tableTimestamp, LocalTime.of(11, 22, 33), null, LocalDate.now() + " 11:22:33", LocalTime.of(11, 22, 33), props);
                setObjectInDifferentTimezone(tableTimestamp, LocalTime.of(11, 22, 33, 123456000), null, LocalDate.now() + " 11:22:33.123456",
                        LocalTime.of(11, 22, 33, 123456000), props);
            } else {
                assertThrows(SQLException.class, datetimeErr, new Callable<Void>() {
                    public Void call() throws Exception {
                        setObjectInDifferentTimezone(tableTimestamp, LocalTime.of(11, 22, 33, 123456000), null, "1970-01-01 11:22:33.123456", null, props);
                        return null;
                    }
                });
            }
            assertThrows(SQLException.class, "Data truncation: Incorrect datetime value: '11:22:33.123456' for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableTimestamp, LocalTime.of(11, 22, 33, 123456000), MysqlType.CHAR, "11:22:33.123456", null, props);
                    return null;
                }
            });
            assertThrows(SQLException.class, "Data truncation: Incorrect datetime value: '11:22:33.123456' for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableTimestamp, LocalTime.of(11, 22, 33, 123456000), MysqlType.VARCHAR, "11:22:33.123456", null, props);
                    return null;
                }
            });
            assertThrows(SQLException.class, "Data truncation: Incorrect datetime value: '11:22:33.123456' for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableTimestamp, LocalTime.of(11, 22, 33, 123456000), MysqlType.TINYTEXT, "11:22:33.123456", null, props);
                    return null;
                }
            });
            assertThrows(SQLException.class, "Data truncation: Incorrect datetime value: '11:22:33.123456' for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableTimestamp, LocalTime.of(11, 22, 33, 123456000), MysqlType.TEXT, "11:22:33.123456", null, props);
                    return null;
                }
            });
            assertThrows(SQLException.class, "Data truncation: Incorrect datetime value: '11:22:33.123456' for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableTimestamp, LocalTime.of(11, 22, 33, 123456000), MysqlType.MEDIUMTEXT, "11:22:33.123456", null, props);
                    return null;
                }
            });
            assertThrows(SQLException.class, "Data truncation: Incorrect datetime value: '11:22:33.123456' for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableTimestamp, LocalTime.of(11, 22, 33, 123456000), MysqlType.LONGTEXT, "11:22:33.123456", null, props);
                    return null;
                }
            });
            if (useSSPS && withFract) {
                // TODO different behaviour for CSPS and SSPS. Server bug?
                setObjectInDifferentTimezone(tableTimestamp, LocalTime.of(11, 22, 33, 123456000), MysqlType.TIME, LocalDate.now() + " 11:22:33.123456",
                        LocalTime.of(11, 22, 33, 123456000), props);
            } else {
                assertThrows(SQLException.class, datetimeErr, new Callable<Void>() {
                    public Void call() throws Exception {
                        setObjectInDifferentTimezone(tableTimestamp, LocalTime.of(11, 22, 33, 123456000), MysqlType.TIME, "1970-01-01 11:22:33.123456", null,
                                props);
                        return null;
                    }
                });
            }

            /* Into VARCHAR field */

            if (useSSPS) {
                // TODO milliseconds are ignored by server. Bug ?
                setObjectInDifferentTimezone(tableVarchar, LocalTime.of(11, 22, 33, 123456000), null, "11:22:33", LocalTime.of(11, 22, 33), props);
            } else {
                setObjectInDifferentTimezone(tableVarchar, LocalTime.of(11, 22, 33, 123456000), null, withFract ? "11:22:33.123456" : "11:22:33",
                        withFract ? LocalTime.of(11, 22, 33, 123456000) : LocalTime.of(11, 22, 33, 0), props);
            }
            setObjectInDifferentTimezone(tableVarchar, LocalTime.of(11, 22, 33, 123456000), MysqlType.CHAR, "11:22:33.123456",
                    LocalTime.of(11, 22, 33, 123456000), props);
            setObjectInDifferentTimezone(tableVarchar, LocalTime.of(11, 22, 33, 123456000), MysqlType.VARCHAR, "11:22:33.123456",
                    LocalTime.of(11, 22, 33, 123456000), props);
            setObjectInDifferentTimezone(tableVarchar, LocalTime.of(11, 22, 33, 123456000), MysqlType.TINYTEXT, "11:22:33.123456",
                    LocalTime.of(11, 22, 33, 123456000), props);
            setObjectInDifferentTimezone(tableVarchar, LocalTime.of(11, 22, 33, 123456000), MysqlType.TEXT, "11:22:33.123456",
                    LocalTime.of(11, 22, 33, 123456000), props);
            setObjectInDifferentTimezone(tableVarchar, LocalTime.of(11, 22, 33, 123456000), MysqlType.MEDIUMTEXT, "11:22:33.123456",
                    LocalTime.of(11, 22, 33, 123456000), props);
            setObjectInDifferentTimezone(tableVarchar, LocalTime.of(11, 22, 33, 123456000), MysqlType.LONGTEXT, "11:22:33.123456",
                    LocalTime.of(11, 22, 33, 123456000), props);
            if (useSSPS) {
                // TODO milliseconds are ignored by server. Bug ?
                setObjectInDifferentTimezone(tableVarchar, LocalTime.of(11, 22, 33, 123456000), MysqlType.TIME, "11:22:33", LocalTime.of(11, 22, 33), props);
            } else {
                setObjectInDifferentTimezone(tableVarchar, LocalTime.of(11, 22, 33, 123456000), MysqlType.TIME, withFract ? "11:22:33.123456" : "11:22:33",
                        LocalTime.of(11, 22, 33, withFract ? 123456000 : 0), props);
            }
        }
    }

    @Test
    public void testSetObjectLocalDateTimeInDifferentTimezone() throws Exception {
        boolean withFract = versionMeetsMinimum(5, 6, 4); // fractional seconds are not supported in previous versions
        final String tableYear = "testSetObjectYear";
        final String tableDate = "testSetObjectDate";
        final String tableTime = "testSetObjectTime";
        final String tableDatetime = "testSetObjectDatetime";
        final String tableTimestamp = "testSetObjectTimestamp";
        final String tableVarchar = "testSetObjectVarchar";
        createTable(tableYear, "(id INT, d YEAR)");
        createTable(tableDate, "(id INT, d DATE)");
        createTable(tableTime, withFract ? "(id INT, d TIME(6))" : "(id INT, d TIME)");
        createTable(tableDatetime, withFract ? "(id INT, d DATETIME(6))" : "(id INT, d DATETIME)");
        createTable(tableTimestamp, withFract ? "(id INT, d TIMESTAMP(6))" : "(id INT, d TIMESTAMP)");
        createTable(tableVarchar, "(id INT, d VARCHAR(30))");

        // truncation/rounding is tested in StatementRegressionTest#testBug77449
        Properties props = new Properties();
        if (!withFract) {
            // applying 8.0 defaults to old servers
            props.setProperty(PropertyKey.sessionVariables.getKeyName(), "sql_mode='NO_ZERO_IN_DATE'");
        }
        for (boolean useSSPS : new boolean[] { false, true }) {
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "" + useSSPS);

            /* Into YEAR field */

            if (useSSPS && withFract) {
                // TODO different behaviour for CSPS and SSPS. Server bug?
                setObjectInDifferentTimezone(tableYear, LocalDateTime.of(2019, 1, 1, 11, 22, 33, 123456000), null, "2019", LocalDateTime.of(2019, 1, 1, 0, 0),
                        props);
                setObjectInDifferentTimezone(tableYear, LocalDateTime.of(2019, 1, 1, 11, 22, 33, 123456000), MysqlType.DATE, "2019",
                        LocalDateTime.of(2019, 1, 1, 0, 0), props);
            } else {
                assertThrows(SQLException.class, "Data truncated for column 'd' at row 1", new Callable<Void>() {
                    public Void call() throws Exception {
                        setObjectInDifferentTimezone(tableYear, LocalDateTime.of(2019, 1, 1, 11, 22, 33, 123456000), null, "2019", null, props);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Data truncated for column 'd' at row 1", new Callable<Void>() {
                    public Void call() throws Exception {
                        setObjectInDifferentTimezone(tableYear, LocalDateTime.of(2019, 1, 1, 11, 22, 33, 123456000), MysqlType.DATE, "2019", null, props);
                        return null;
                    }
                });
            }
            assertThrows(SQLException.class, "Data truncated for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableYear, LocalDateTime.of(2019, 1, 1, 11, 22, 33, 123456000), MysqlType.CHAR, "2019", null, props);
                    return null;
                }
            });
            assertThrows(SQLException.class, "Data truncated for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableYear, LocalDateTime.of(2019, 1, 1, 11, 22, 33, 123456000), MysqlType.VARCHAR, "2019", null, props);
                    return null;
                }
            });
            assertThrows(SQLException.class, "Data truncated for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableYear, LocalDateTime.of(2019, 1, 1, 11, 22, 33, 123456000), MysqlType.TINYTEXT, "2019", null, props);
                    return null;
                }
            });
            assertThrows(SQLException.class, "Data truncated for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableYear, LocalDateTime.of(2019, 1, 1, 11, 22, 33, 123456000), MysqlType.TEXT, "2019", null, props);
                    return null;
                }
            });
            assertThrows(SQLException.class, "Data truncated for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableYear, LocalDateTime.of(2019, 1, 1, 11, 22, 33, 123456000), MysqlType.MEDIUMTEXT, "2019", null, props);
                    return null;
                }
            });
            assertThrows(SQLException.class, "Data truncated for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableYear, LocalDateTime.of(2019, 1, 1, 11, 22, 33, 123456000), MysqlType.LONGTEXT, "2019", null, props);
                    return null;
                }
            });
            if (useSSPS && withFract) {
                // TODO different behaviour for CSPS and SSPS. Server bug?
                setObjectInDifferentTimezone(tableYear, LocalDateTime.of(2019, 1, 1, 11, 22, 33, 123456000), MysqlType.TIME, LocalDate.now().getYear() + "",
                        LocalDateTime.of(LocalDate.now().getYear(), 1, 1, 0, 0), props);
                setObjectInDifferentTimezone(tableYear, LocalDateTime.of(2019, 1, 1, 11, 22, 33, 123456000), MysqlType.DATETIME, "2019",
                        LocalDateTime.of(2019, 1, 1, 0, 0), props);
                setObjectInDifferentTimezone(tableYear, LocalDateTime.of(2019, 1, 1, 11, 22, 33, 123456000), MysqlType.TIMESTAMP, "2019",
                        LocalDateTime.of(2019, 1, 1, 0, 0), props);
            } else {
                assertThrows(SQLException.class, "Data truncated for column 'd' at row 1", new Callable<Void>() {
                    public Void call() throws Exception {
                        setObjectInDifferentTimezone(tableYear, LocalDateTime.of(2019, 1, 1, 11, 22, 33, 123456000), MysqlType.TIME, "2019", null, props);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Data truncated for column 'd' at row 1", new Callable<Void>() {
                    public Void call() throws Exception {
                        setObjectInDifferentTimezone(tableYear, LocalDateTime.of(2019, 1, 1, 11, 22, 33, 123456000), MysqlType.DATETIME, "2019", null, props);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Data truncated for column 'd' at row 1", new Callable<Void>() {
                    public Void call() throws Exception {
                        setObjectInDifferentTimezone(tableYear, LocalDateTime.of(2019, 1, 1, 11, 22, 33, 123456000), MysqlType.TIMESTAMP, "2019", null, props);
                        return null;
                    }
                });
            }
            setObjectInDifferentTimezone(tableYear, LocalDateTime.of(2019, 1, 1, 11, 22, 33, 123456000), MysqlType.YEAR, "2019",
                    LocalDateTime.of(2019, 1, 1, 0, 0), props);

            /* Into DATE field */

            String dateErr = withFract ? "Data truncation: Incorrect date value: '11:22:33.123456' for column 'd' at row 1"
                    : "Data truncation: Incorrect date value: '11:22:33' for column 'd' at row 1";

            setObjectInDifferentTimezone(tableDate, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), null, "2019-12-31",
                    LocalDateTime.of(2019, 12, 31, 0, 0), props);
            setObjectInDifferentTimezone(tableDate, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.DATE, "2019-12-31",
                    LocalDateTime.of(2019, 12, 31, 0, 0), props);
            setObjectInDifferentTimezone(tableDate, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.CHAR, "2019-12-31",
                    LocalDateTime.of(2019, 12, 31, 0, 0), props);
            setObjectInDifferentTimezone(tableDate, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.VARCHAR, "2019-12-31",
                    LocalDateTime.of(2019, 12, 31, 0, 0), props);
            setObjectInDifferentTimezone(tableDate, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.TINYTEXT, "2019-12-31",
                    LocalDateTime.of(2019, 12, 31, 0, 0), props);
            setObjectInDifferentTimezone(tableDate, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.TEXT, "2019-12-31",
                    LocalDateTime.of(2019, 12, 31, 0, 0), props);
            setObjectInDifferentTimezone(tableDate, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.MEDIUMTEXT, "2019-12-31",
                    LocalDateTime.of(2019, 12, 31, 0, 0), props);
            setObjectInDifferentTimezone(tableDate, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.LONGTEXT, "2019-12-31",
                    LocalDateTime.of(2019, 12, 31, 0, 0), props);
            if (useSSPS && withFract) {
                // TODO different behaviour for CSPS and SSPS. Server bug?
                setObjectInDifferentTimezone(tableDate, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.TIME, LocalDate.now().toString(), null,
                        props);
            } else {
                assertThrows(SQLException.class, useSSPS ? "Data truncated for column 'd' at row 1" : dateErr, new Callable<Void>() {
                    public Void call() throws Exception {
                        setObjectInDifferentTimezone(tableDate, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.TIME, "2019-12-31", null,
                                props);
                        return null;
                    }
                });
            }
            setObjectInDifferentTimezone(tableDate, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.DATETIME, "2019-12-31",
                    LocalDateTime.of(2019, 12, 31, 0, 0), props);
            setObjectInDifferentTimezone(tableDate, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.TIMESTAMP, "2019-12-31",
                    LocalDateTime.of(2019, 12, 31, 0, 0), props);
            assertThrows(SQLException.class, "Data truncation: Incorrect date value: '2019' for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableDate, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.YEAR, "2019", null, props);
                    return null;
                }
            });

            /* Into TIME field */

            String expTime = withFract ? "11:22:33.123456" : "11:22:33";
            LocalDateTime expLDT1970 = LocalDateTime.of(1970, 1, 1, 11, 22, 33, withFract ? 123456000 : 0);

            setObjectInDifferentTimezone(tableTime, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), null, expTime,
                    LocalDateTime.of(1970, 1, 1, 11, 22, 33, withFract ? 123456000 : 0), props);
            if (useSSPS) {
                // TODO different behaviour for CSPS and SSPS. Server bug?
                setObjectInDifferentTimezone(tableTime, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.DATE, "00:00:00.000000",
                        LocalDateTime.of(1970, 1, 1, 0, 0), props);
            } else {
                assertThrows(SQLException.class, "Data truncation: Incorrect time value: '2019-12-31' for column 'd' at row 1", new Callable<Void>() {
                    public Void call() throws Exception {
                        setObjectInDifferentTimezone(tableTime, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.DATE, "00:00:00.000000", null,
                                props);
                        return null;
                    }
                });
            }
            setObjectInDifferentTimezone(tableTime, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.CHAR, "11:22:33.123456", expLDT1970,
                    props);
            setObjectInDifferentTimezone(tableTime, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.VARCHAR, "11:22:33.123456", expLDT1970,
                    props);
            setObjectInDifferentTimezone(tableTime, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.TINYTEXT, "11:22:33.123456", expLDT1970,
                    props);
            setObjectInDifferentTimezone(tableTime, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.TEXT, "11:22:33.123456", expLDT1970,
                    props);
            setObjectInDifferentTimezone(tableTime, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.MEDIUMTEXT, "11:22:33.123456", expLDT1970,
                    props);
            setObjectInDifferentTimezone(tableTime, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.LONGTEXT, "11:22:33.123456", expLDT1970,
                    props);
            setObjectInDifferentTimezone(tableTime, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.TIME, "11:22:33.123456", expLDT1970,
                    props);
            setObjectInDifferentTimezone(tableTime, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.DATETIME, "11:22:33.123456", expLDT1970,
                    props);
            setObjectInDifferentTimezone(tableTime, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.TIMESTAMP, "11:22:33.123456", expLDT1970,
                    props);
            setObjectInDifferentTimezone(tableTime, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.YEAR, "2019",
                    LocalDateTime.of(1970, 1, 1, 0, 20, 19), props); // TIME can recognize numbers as a short notation, thus it works here
            setObjectInDifferentTimezone(tableTime, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.YEAR, "00:20:19.000000",
                    LocalDateTime.of(1970, 1, 1, 0, 20, 19), props); // TIME can recognize numbers as a short notation, thus it works here

            /* Into DATETIME field */

            LocalDateTime expLDT = LocalDateTime.of(2019, 12, 31, 11, 22, 33, withFract ? 123456000 : 0);
            String datetimeErr = withFract ? "Data truncation: Incorrect datetime value: '11:22:33.123456' for column 'd' at row 1"
                    : "Data truncation: Incorrect datetime value: '11:22:33' for column 'd' at row 1";

            setObjectInDifferentTimezone(tableDatetime, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), null, "2019-12-31 11:22:33.123456", expLDT,
                    props);
            setObjectInDifferentTimezone(tableDatetime, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.DATE, "2019-12-31 00:00:00.000000",
                    LocalDateTime.of(2019, 12, 31, 0, 0), props);
            setObjectInDifferentTimezone(tableDatetime, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.CHAR, "2019-12-31 11:22:33.123456",
                    expLDT, props);
            setObjectInDifferentTimezone(tableDatetime, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.VARCHAR, "2019-12-31 11:22:33.123456",
                    expLDT, props);
            setObjectInDifferentTimezone(tableDatetime, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.TINYTEXT, "2019-12-31 11:22:33.123456",
                    expLDT, props);
            setObjectInDifferentTimezone(tableDatetime, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.TEXT, "2019-12-31 11:22:33.123456",
                    expLDT, props);
            setObjectInDifferentTimezone(tableDatetime, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.MEDIUMTEXT,
                    "2019-12-31 11:22:33.123456", expLDT, props);
            setObjectInDifferentTimezone(tableDatetime, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.LONGTEXT, "2019-12-31 11:22:33.123456",
                    expLDT, props);
            if (useSSPS && withFract) {
                // TODO different behaviour for CSPS and SSPS. Server bug?
                setObjectInDifferentTimezone(tableDatetime, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.TIME,
                        LocalDate.now() + " 11:22:33.123456", LocalDateTime.of(LocalDate.now(), LocalTime.of(11, 22, 33, 123456000)), props);
            } else {
                assertThrows(SQLException.class, useSSPS ? "Data truncated for column 'd' at row 1" : datetimeErr, new Callable<Void>() {
                    public Void call() throws Exception {
                        setObjectInDifferentTimezone(tableDatetime, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.TIME,
                                "2019-12-31 11:22:33.123456", null, props);
                        return null;
                    }
                });
            }

            setObjectInDifferentTimezone(tableDatetime, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.DATETIME, "2019-12-31 11:22:33.123456",
                    expLDT, props);
            setObjectInDifferentTimezone(tableDatetime, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.TIMESTAMP,
                    "2019-12-31 11:22:33.123456", expLDT, props);
            assertThrows(SQLException.class, "Data truncation: Incorrect datetime value: '2019' for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableDatetime, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.YEAR, "2019", null, props);
                    return null;
                }
            });

            /* Into TIMESTAMP field */

            setObjectInDifferentTimezone(tableTimestamp, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), null, "2019-12-31 11:22:33.123456", expLDT,
                    props);
            setObjectInDifferentTimezone(tableTimestamp, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.DATE, "2019-12-31 00:00:00.0",
                    LocalDateTime.of(2019, 12, 31, 0, 0), props);
            setObjectInDifferentTimezone(tableTimestamp, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.CHAR, "2019-12-31 11:22:33.123456",
                    expLDT, props);
            setObjectInDifferentTimezone(tableTimestamp, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.VARCHAR, "2019-12-31 11:22:33.123456",
                    expLDT, props);
            setObjectInDifferentTimezone(tableTimestamp, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.TINYTEXT,
                    "2019-12-31 11:22:33.123456", expLDT, props);
            setObjectInDifferentTimezone(tableTimestamp, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.TEXT, "2019-12-31 11:22:33.123456",
                    expLDT, props);
            setObjectInDifferentTimezone(tableTimestamp, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.MEDIUMTEXT,
                    "2019-12-31 11:22:33.123456", expLDT, props);
            setObjectInDifferentTimezone(tableTimestamp, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.LONGTEXT,
                    "2019-12-31 11:22:33.123456", expLDT, props);
            if (useSSPS && withFract) {
                // TODO different behaviour for CSPS and SSPS. Server bug?
                setObjectInDifferentTimezone(tableTimestamp, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.TIME,
                        LocalDate.now() + " 11:22:33.123456", LocalDateTime.of(LocalDate.now(), LocalTime.of(11, 22, 33, 123456000)), props);
            } else {
                assertThrows(SQLException.class, datetimeErr, new Callable<Void>() {
                    public Void call() throws Exception {
                        setObjectInDifferentTimezone(tableTimestamp, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.TIME,
                                "2019-12-31 11:22:33.123456", null, props);
                        return null;
                    }
                });
            }
            setObjectInDifferentTimezone(tableTimestamp, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.DATETIME,
                    "2019-12-31 11:22:33.123456", expLDT, props);
            setObjectInDifferentTimezone(tableTimestamp, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.TIMESTAMP,
                    "2019-12-31 11:22:33.123456", LocalDateTime.of(2019, 12, 31, 11, 22, 33, withFract ? 123456000 : 0), props);
            assertThrows(SQLException.class, "Data truncation: Incorrect datetime value: '2019' for column 'd' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    setObjectInDifferentTimezone(tableTimestamp, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.YEAR, "2019-12-31 00:00:00.0",
                            null, props);
                    return null;
                }
            });

            /* Into VARCHAR field */

            String expVal = withFract ? "2019-12-31 11:22:33.123456" : "2019-12-31 11:22:33";
            if (useSSPS) {
                // TODO milliseconds are ignored by server. Bug ?
                setObjectInDifferentTimezone(tableVarchar, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), null, "2019-12-31 11:22:33",
                        LocalDateTime.of(2019, 12, 31, 11, 22, 33, 0), props);
            } else {
                setObjectInDifferentTimezone(tableVarchar, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), null, expVal, expLDT, props);
            }
            setObjectInDifferentTimezone(tableVarchar, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.DATE, "2019-12-31",
                    LocalDateTime.of(2019, 12, 31, 0, 0), props);
            setObjectInDifferentTimezone(tableVarchar, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.CHAR, "2019-12-31 11:22:33.123456",
                    LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), props);
            setObjectInDifferentTimezone(tableVarchar, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.VARCHAR, "2019-12-31 11:22:33.123456",
                    LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), props);
            setObjectInDifferentTimezone(tableVarchar, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.TINYTEXT, "2019-12-31 11:22:33.123456",
                    LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), props);
            setObjectInDifferentTimezone(tableVarchar, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.TEXT, "2019-12-31 11:22:33.123456",
                    LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), props);
            setObjectInDifferentTimezone(tableVarchar, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.MEDIUMTEXT,
                    "2019-12-31 11:22:33.123456", LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), props);
            setObjectInDifferentTimezone(tableVarchar, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.LONGTEXT, "2019-12-31 11:22:33.123456",
                    LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), props);
            if (useSSPS) {
                // TODO milliseconds are ignored by server. Bug ?
                setObjectInDifferentTimezone(tableVarchar, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.TIME, "11:22:33",
                        LocalDateTime.of(1970, 1, 1, 11, 22, 33, 0), props);
                setObjectInDifferentTimezone(tableVarchar, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.DATETIME, "2019-12-31 11:22:33",
                        LocalDateTime.of(2019, 12, 31, 11, 22, 33, 0), props);
                setObjectInDifferentTimezone(tableVarchar, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.TIMESTAMP, "2019-12-31 11:22:33",
                        LocalDateTime.of(2019, 12, 31, 11, 22, 33, 0), props);
            } else {
                setObjectInDifferentTimezone(tableVarchar, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.TIME,
                        withFract ? "11:22:33.123456" : "11:22:33", expLDT1970, props);
                setObjectInDifferentTimezone(tableVarchar, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.DATETIME, expVal, expLDT, props);
                setObjectInDifferentTimezone(tableVarchar, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.TIMESTAMP, expVal, expLDT, props);
            }
            setObjectInDifferentTimezone(tableVarchar, LocalDateTime.of(2019, 12, 31, 11, 22, 33, 123456000), MysqlType.YEAR, "2019",
                    new SQLDataException("Cannot convert string '2019' to java.time.LocalDateTime value"), props);
        }
    }

    void setObjectInDifferentTimezone(String tableName, Object parameter, SQLType targetSqlType, String expectedValue, Object expectedResult, Properties props)
            throws Exception {
        if (props == null) {
            props = new Properties();
        }
        final TimeZone origTz = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("GMT+23:50"));
            try (Connection testConn = getConnectionWithProps(props)) {
                testConn.createStatement().execute("truncate table " + tableName);
                try (PreparedStatement localPstmt = testConn.prepareStatement("INSERT INTO " + tableName + " VALUES (?, ?)")) {
                    localPstmt.setInt(1, 1);
                    if (targetSqlType == null) {
                        localPstmt.setObject(2, parameter);
                    } else {
                        localPstmt.setObject(2, parameter, targetSqlType);
                    }
                    assertEquals(1, localPstmt.executeUpdate());
                }
                try (Statement localStmt = testConn.createStatement();
                        ResultSet localRs = localStmt.executeQuery("SELECT COUNT(*) FROM " + tableName + " WHERE id = 1 AND d = '" + expectedValue + "'")) {
                    assertTrue(localRs.next());
                    assertEquals(1, localRs.getInt(1),
                            String.format("table: '%s', parameter: '%s', sqlType: '%s', properties: '%s'.", tableName, parameter, targetSqlType, props));
                }
                if (expectedResult != null) {
                    TimeZone.setDefault(TimeZone.getTimeZone("GMT-12:50"));
                    try (Statement localStmt = testConn.createStatement(); ResultSet localRs = localStmt.executeQuery("SELECT * FROM " + tableName)) {
                        assertTrue(localRs.next());

                        if (expectedResult instanceof Exception) {
                            assertThrows(((Exception) expectedResult).getClass(), ((Exception) expectedResult).getMessage(), new Callable<Void>() {
                                public Void call() throws Exception {
                                    localRs.getObject(2, parameter.getClass());
                                    return null;
                                }
                            });
                        } else {
                            assertEquals(expectedResult, localRs.getObject(2, parameter.getClass()));
                        }

                    }
                }
            }
        } finally {
            TimeZone.setDefault(origTz);
        }
    }
}
