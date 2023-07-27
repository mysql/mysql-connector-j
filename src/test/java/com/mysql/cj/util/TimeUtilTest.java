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

package com.mysql.cj.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import org.junit.jupiter.api.Test;

import com.mysql.cj.MysqlType;
import com.mysql.cj.exceptions.WrongArgumentException;

/**
 * Tests for {@link TimeUtil}.
 */
public class TimeUtilTest {

    @Test
    public void testAdjustTimestampNanosPrecision() {
        assertTrue(Timestamp.valueOf("2020-02-26 14:30:11").equals(TimeUtil.adjustNanosPrecision(Timestamp.valueOf("2020-02-26 14:30:10.999999999"), 3, true)));
        assertTrue(Timestamp.valueOf("2020-02-26 14:30:10.999")
                .equals(TimeUtil.adjustNanosPrecision(Timestamp.valueOf("2020-02-26 14:30:10.999999999"), 3, false)));
        assertTrue(Timestamp.valueOf("2020-02-26 14:30:10.778")
                .equals(TimeUtil.adjustNanosPrecision(Timestamp.valueOf("2020-02-26 14:30:10.777777777"), 3, true)));
        assertTrue(Timestamp.valueOf("2020-02-26 14:30:10.777")
                .equals(TimeUtil.adjustNanosPrecision(Timestamp.valueOf("2020-02-26 14:30:10.777777777"), 3, false)));

        assertTrue(LocalDateTime.of(2020, 02, 26, 14, 30, 11)
                .equals(TimeUtil.adjustNanosPrecision(LocalDateTime.of(2020, 02, 26, 14, 30, 10, 999999999), 3, true)));
        assertTrue(LocalDateTime.of(2020, 02, 26, 14, 30, 10, 999000000)
                .equals(TimeUtil.adjustNanosPrecision(LocalDateTime.of(2020, 02, 26, 14, 30, 10, 999999999), 3, false)));
        assertTrue(LocalDateTime.of(2020, 02, 26, 14, 30, 10, 778000000)
                .equals(TimeUtil.adjustNanosPrecision(LocalDateTime.of(2020, 02, 26, 14, 30, 10, 777777777), 3, true)));
        assertTrue(LocalDateTime.of(2020, 02, 26, 14, 30, 10, 777000000)
                .equals(TimeUtil.adjustNanosPrecision(LocalDateTime.of(2020, 02, 26, 14, 30, 10, 777777777), 3, false)));
    }

    @Test
    public void testParseToDateTimeObject() throws IOException {
        /* DATE literals */

        assertEquals(LocalDate.of(2020, 1, 1), TimeUtil.parseToDateTimeObject("2020-01-01", MysqlType.DATE));
        assertEquals(LocalDate.of(2020, 1, 1), TimeUtil.parseToDateTimeObject("20-01-01", MysqlType.DATE));
        assertEquals(LocalDate.of(2020, 1, 1), TimeUtil.parseToDateTimeObject("2020#01$01", MysqlType.DATE));
        assertEquals(LocalDate.of(2020, 1, 1), TimeUtil.parseToDateTimeObject("20%01%01", MysqlType.DATE));
        assertEquals(LocalDate.of(2020, 1, 1), TimeUtil.parseToDateTimeObject("2020-1-1", MysqlType.DATE));
        assertEquals(LocalDate.of(2020, 1, 1), TimeUtil.parseToDateTimeObject("20-1-1", MysqlType.DATE));
        assertEquals(LocalDate.of(2020, 1, 1), TimeUtil.parseToDateTimeObject("20200101", MysqlType.DATE));
        assertEquals(LocalDate.of(2020, 1, 1), TimeUtil.parseToDateTimeObject("200101", MysqlType.DATE));
        assertEquals(LocalDate.of(1970, 1, 1), TimeUtil.parseToDateTimeObject("70-1-1", MysqlType.DATE));

        assertEquals(LocalDate.of(2020, 1, 1), TimeUtil.parseToDateTimeObject("2020-01-01", MysqlType.DATETIME));
        assertEquals(LocalDate.of(2020, 1, 1), TimeUtil.parseToDateTimeObject("20-01-01", MysqlType.DATETIME));
        assertEquals(LocalDate.of(2020, 1, 1), TimeUtil.parseToDateTimeObject("2020#01$01", MysqlType.DATETIME));
        assertEquals(LocalDate.of(2020, 1, 1), TimeUtil.parseToDateTimeObject("20%01%01", MysqlType.DATETIME));
        assertEquals(LocalDate.of(2020, 1, 1), TimeUtil.parseToDateTimeObject("2020-1-1", MysqlType.DATETIME));
        assertEquals(LocalDate.of(2020, 1, 1), TimeUtil.parseToDateTimeObject("20-1-1", MysqlType.DATETIME));
        assertEquals(LocalDate.of(2020, 1, 1), TimeUtil.parseToDateTimeObject("20200101", MysqlType.DATETIME));
        assertEquals(LocalDate.of(2020, 1, 1), TimeUtil.parseToDateTimeObject("200101", MysqlType.DATETIME));
        assertEquals(LocalDate.of(1970, 1, 1), TimeUtil.parseToDateTimeObject("70-1-1", MysqlType.DATETIME));

        assertEquals(LocalTime.of(20, 13, 32), TimeUtil.parseToDateTimeObject("201332", MysqlType.DATETIME));
        assertEquals(LocalTime.of(20, 00, 32), TimeUtil.parseToDateTimeObject("200032", MysqlType.DATETIME));

        assertEquals(LocalDate.of(2020, 1, 1), TimeUtil.parseToDateTimeObject("2020-01-01", MysqlType.TIME));
        assertEquals(LocalDate.of(2020, 1, 1), TimeUtil.parseToDateTimeObject("20-01-01", MysqlType.TIME));
        assertEquals(LocalDate.of(2020, 1, 1), TimeUtil.parseToDateTimeObject("2020#01$01", MysqlType.TIME));
        assertEquals(LocalDate.of(2020, 1, 1), TimeUtil.parseToDateTimeObject("20%01%01", MysqlType.TIME));
        assertEquals(LocalDate.of(2020, 1, 1), TimeUtil.parseToDateTimeObject("2020-1-1", MysqlType.TIME));
        assertEquals(LocalDate.of(2020, 2, 1), TimeUtil.parseToDateTimeObject("20-2-1", MysqlType.TIME));
        assertEquals(LocalDate.of(1970, 1, 1), TimeUtil.parseToDateTimeObject("70-1-1", MysqlType.TIME));
        assertEquals(LocalDate.of(2020, 1, 1), TimeUtil.parseToDateTimeObject("20200101", MysqlType.TIME));
        assertEquals(LocalTime.of(20, 1, 1), TimeUtil.parseToDateTimeObject("200101", MysqlType.TIME));

        assertThrows(WrongArgumentException.class, () -> {
            TimeUtil.parseToDateTimeObject("2020-00-01", MysqlType.DATE);
        }, "There is no known date-time pattern for.*");
        assertThrows(WrongArgumentException.class, () -> {
            TimeUtil.parseToDateTimeObject("2020-01-00", MysqlType.DATE);
        }, "There is no known date-time pattern for.*");
        assertThrows(WrongArgumentException.class, () -> {
            TimeUtil.parseToDateTimeObject("2020-13-01", MysqlType.DATE);
        }, "There is no known date-time pattern for.*");
        assertThrows(WrongArgumentException.class, () -> {
            TimeUtil.parseToDateTimeObject("2020-01-32", MysqlType.DATE);
        }, "There is no known date-time pattern for.*");

        /* TIME literals */

        assertEquals(LocalTime.of(13, 4, 17), TimeUtil.parseToDateTimeObject("13:04:17", MysqlType.DATE));
        assertEquals(LocalTime.of(13, 4), TimeUtil.parseToDateTimeObject("13:04", MysqlType.DATE));
        assertEquals(LocalDate.of(2003, 11, 07), TimeUtil.parseToDateTimeObject("031107", MysqlType.DATE)); // it could be both DATE or TIME literal
        assertEquals(LocalTime.of(0, 4, 17), TimeUtil.parseToDateTimeObject("0417", MysqlType.DATE));
        assertEquals(LocalTime.of(3, 14, 7, 12000000), TimeUtil.parseToDateTimeObject("03:14:07.012", MysqlType.DATE));
        assertEquals(LocalTime.of(3, 14, 7, 123000000), TimeUtil.parseToDateTimeObject("031407.123", MysqlType.DATE));

        assertEquals(LocalTime.of(13, 4, 17), TimeUtil.parseToDateTimeObject("13:04:17", MysqlType.TIME));
        assertEquals(LocalTime.of(13, 4), TimeUtil.parseToDateTimeObject("13:04", MysqlType.TIME));
        assertEquals(LocalTime.of(3, 11, 07), TimeUtil.parseToDateTimeObject("031107", MysqlType.TIME)); // it could be both DATE or TIME literal

        assertEquals(LocalTime.of(0, 4, 17), TimeUtil.parseToDateTimeObject("0417", MysqlType.TIME));
        assertEquals(LocalTime.of(0, 4, 17, 123400000), TimeUtil.parseToDateTimeObject("0417.1234", MysqlType.TIME));
        assertEquals(LocalTime.of(0, 0, 17, 123456789), TimeUtil.parseToDateTimeObject("17.123456789", MysqlType.TIME));

        assertEquals(LocalTime.of(3, 14, 7, 12000000), TimeUtil.parseToDateTimeObject("03:14:07.012", MysqlType.TIME));
        assertEquals(LocalTime.of(3, 14, 7, 123000000), TimeUtil.parseToDateTimeObject("031407.123", MysqlType.TIME));

        assertEquals(LocalTime.of(13, 4, 17), TimeUtil.parseToDateTimeObject("13:04:17", MysqlType.DATETIME));
        assertEquals(LocalTime.of(13, 4), TimeUtil.parseToDateTimeObject("13:04", MysqlType.DATETIME));
        assertEquals(LocalDate.of(2003, 11, 07), TimeUtil.parseToDateTimeObject("031107", MysqlType.DATETIME)); // it could be both DATE or TIME literal
        assertEquals(LocalTime.of(0, 4, 17), TimeUtil.parseToDateTimeObject("0417", MysqlType.DATETIME));
        assertEquals(LocalTime.of(3, 14, 7, 12000000), TimeUtil.parseToDateTimeObject("03:14:07.012", MysqlType.DATETIME));
        assertEquals(LocalTime.of(3, 14, 7, 123000000), TimeUtil.parseToDateTimeObject("031407.123", MysqlType.DATETIME));

        assertThrows(WrongArgumentException.class, () -> {
            TimeUtil.parseToDateTimeObject("23:64:07.12", MysqlType.DATE);
        }, "There is no known date-time pattern for.*");
        assertThrows(WrongArgumentException.class, () -> {
            TimeUtil.parseToDateTimeObject("23:64:07.12", MysqlType.TIME);
        }, "There is no known date-time pattern for.*");
        assertThrows(WrongArgumentException.class, () -> {
            TimeUtil.parseToDateTimeObject("23:64:07.12", MysqlType.DATETIME);
        }, "There is no known date-time pattern for.*");

        assertThrows(WrongArgumentException.class, () -> {
            TimeUtil.parseToDateTimeObject("23:14:67.12", MysqlType.DATE);
        }, "There is no known date-time pattern for.*");
        assertThrows(WrongArgumentException.class, () -> {
            TimeUtil.parseToDateTimeObject("23:14:67.12", MysqlType.TIME);
        }, "There is no known date-time pattern for.*");
        assertThrows(WrongArgumentException.class, () -> {
            TimeUtil.parseToDateTimeObject("23:14:67.12", MysqlType.DATETIME);
        }, "There is no known date-time pattern for.*");

        assertThrows(WrongArgumentException.class, () -> {
            TimeUtil.parseToDateTimeObject("031407#12", MysqlType.DATE);
        }, "There is no known date-time pattern for.*");
        assertThrows(WrongArgumentException.class, () -> {
            TimeUtil.parseToDateTimeObject("031407#12", MysqlType.TIME);
        }, "There is no known date-time pattern for.*");
        assertThrows(WrongArgumentException.class, () -> {
            TimeUtil.parseToDateTimeObject("031407#12", MysqlType.DATETIME);
        }, "There is no known date-time pattern for.*");

        /* Duration literals */

        assertEquals(Duration.parse("P12DT1H"), TimeUtil.parseToDateTimeObject("12 1", MysqlType.DATE));
        assertEquals(Duration.parse("P12DT1H"), TimeUtil.parseToDateTimeObject("12 01", MysqlType.DATE));
        assertEquals(Duration.parse("P12DT12H"), TimeUtil.parseToDateTimeObject("12 12", MysqlType.DATE));
        assertEquals(Duration.parse("P12DT13H4M"), TimeUtil.parseToDateTimeObject("12 13:4", MysqlType.DATE));
        assertEquals(Duration.parse("P12DT13H4M"), TimeUtil.parseToDateTimeObject("12 13:04", MysqlType.DATE));
        assertEquals(Duration.parse("P12DT13H14M"), TimeUtil.parseToDateTimeObject("12 13:14", MysqlType.DATE));
        assertEquals(Duration.parse("P12DT13H4M7S"), TimeUtil.parseToDateTimeObject("12 13:04:7", MysqlType.DATE));
        assertEquals(Duration.parse("P1DT13H4M7S"), TimeUtil.parseToDateTimeObject("1 13:04:07", MysqlType.DATE));
        assertEquals(Duration.parse("P12DT13H4M17S"), TimeUtil.parseToDateTimeObject("12 13:04:17", MysqlType.DATE));
        assertEquals(Duration.parse("P12DT1H4M1S"), TimeUtil.parseToDateTimeObject("12 1:4:1", MysqlType.DATE));
        assertEquals(Duration.parse("P12DT1H4M21S"), TimeUtil.parseToDateTimeObject("12 1:4:21", MysqlType.DATE));
        assertEquals(Duration.parse("-P12DT1H4M21S"), TimeUtil.parseToDateTimeObject("-12 1:4:21", MysqlType.DATE));
        assertEquals(Duration.parse("-P12DT1H04M17S"), TimeUtil.parseToDateTimeObject("-12 1:04:17", MysqlType.DATE));
        assertEquals(Duration.parse("P12DT1H04M17.123456789S"), TimeUtil.parseToDateTimeObject("12 1:04:17.123456789", MysqlType.DATE));
        assertEquals(Duration.parse("-PT121H4M21S"), TimeUtil.parseToDateTimeObject("-121:4:21", MysqlType.DATE));
        assertEquals(Duration.parse("-PT121H4M21.12345S"), TimeUtil.parseToDateTimeObject("-121:4:21.12345", MysqlType.DATE));
        assertEquals(Duration.parse("PT25H14M7.12S"), TimeUtil.parseToDateTimeObject("25:14:07.12", MysqlType.DATE));

        assertEquals(Duration.parse("P12DT1H"), TimeUtil.parseToDateTimeObject("12 1", MysqlType.TIME));
        assertEquals(Duration.parse("P12DT1H"), TimeUtil.parseToDateTimeObject("12 01", MysqlType.TIME));
        assertEquals(Duration.parse("P12DT12H"), TimeUtil.parseToDateTimeObject("12 12", MysqlType.TIME));
        assertEquals(Duration.parse("P12DT13H4M"), TimeUtil.parseToDateTimeObject("12 13:4", MysqlType.TIME));
        assertEquals(Duration.parse("P12DT13H4M"), TimeUtil.parseToDateTimeObject("12 13:04", MysqlType.TIME));
        assertEquals(Duration.parse("P12DT13H14M"), TimeUtil.parseToDateTimeObject("12 13:14", MysqlType.TIME));
        assertEquals(Duration.parse("P12DT13H4M7S"), TimeUtil.parseToDateTimeObject("12 13:04:7", MysqlType.TIME));
        assertEquals(Duration.parse("P1DT13H4M7S"), TimeUtil.parseToDateTimeObject("1 13:04:07", MysqlType.TIME));
        assertEquals(Duration.parse("P12DT13H4M17S"), TimeUtil.parseToDateTimeObject("12 13:04:17", MysqlType.TIME));
        assertEquals(Duration.parse("P12DT1H4M1S"), TimeUtil.parseToDateTimeObject("12 1:4:1", MysqlType.TIME));
        assertEquals(Duration.parse("P12DT1H4M21S"), TimeUtil.parseToDateTimeObject("12 1:4:21", MysqlType.TIME));
        assertEquals(Duration.parse("-P12DT1H4M21S"), TimeUtil.parseToDateTimeObject("-12 1:4:21", MysqlType.TIME));
        assertEquals(Duration.parse("-P12DT1H04M17S"), TimeUtil.parseToDateTimeObject("-12 1:04:17", MysqlType.TIME));
        assertEquals(Duration.parse("P12DT1H04M17.123456789S"), TimeUtil.parseToDateTimeObject("12 1:04:17.123456789", MysqlType.TIME));
        assertEquals(Duration.parse("-PT121H4M21S"), TimeUtil.parseToDateTimeObject("-121:4:21", MysqlType.TIME));
        assertEquals(Duration.parse("-PT121H4M21.12345S"), TimeUtil.parseToDateTimeObject("-121:4:21.12345", MysqlType.TIME));
        assertEquals(Duration.parse("PT25H14M7.12S"), TimeUtil.parseToDateTimeObject("25:14:07.12", MysqlType.TIME));

        assertEquals(Duration.parse("P12DT1H"), TimeUtil.parseToDateTimeObject("12 1", MysqlType.DATETIME));
        assertEquals(Duration.parse("P12DT1H"), TimeUtil.parseToDateTimeObject("12 01", MysqlType.DATETIME));
        assertEquals(Duration.parse("P12DT12H"), TimeUtil.parseToDateTimeObject("12 12", MysqlType.DATETIME));
        assertEquals(Duration.parse("P12DT13H4M"), TimeUtil.parseToDateTimeObject("12 13:4", MysqlType.DATETIME));
        assertEquals(Duration.parse("P12DT13H4M"), TimeUtil.parseToDateTimeObject("12 13:04", MysqlType.DATETIME));
        assertEquals(Duration.parse("P12DT13H14M"), TimeUtil.parseToDateTimeObject("12 13:14", MysqlType.DATETIME));
        assertEquals(Duration.parse("P12DT13H4M7S"), TimeUtil.parseToDateTimeObject("12 13:04:7", MysqlType.DATETIME));
        assertEquals(Duration.parse("P1DT13H4M7S"), TimeUtil.parseToDateTimeObject("1 13:04:07", MysqlType.DATETIME));
        assertEquals(Duration.parse("P12DT13H4M17S"), TimeUtil.parseToDateTimeObject("12 13:04:17", MysqlType.DATETIME));
        assertEquals(Duration.parse("P12DT1H4M1S"), TimeUtil.parseToDateTimeObject("12 1:4:1", MysqlType.DATETIME));
        assertEquals(Duration.parse("P12DT1H4M21S"), TimeUtil.parseToDateTimeObject("12 1:4:21", MysqlType.DATETIME));
        assertEquals(Duration.parse("-P12DT1H4M21S"), TimeUtil.parseToDateTimeObject("-12 1:4:21", MysqlType.DATETIME));
        assertEquals(Duration.parse("-P12DT1H04M17S"), TimeUtil.parseToDateTimeObject("-12 1:04:17", MysqlType.DATETIME));
        assertEquals(Duration.parse("P12DT1H04M17.123456789S"), TimeUtil.parseToDateTimeObject("12 1:04:17.123456789", MysqlType.DATETIME));
        assertEquals(Duration.parse("-PT121H4M21S"), TimeUtil.parseToDateTimeObject("-121:4:21", MysqlType.DATETIME));
        assertEquals(Duration.parse("-PT121H4M21.12345S"), TimeUtil.parseToDateTimeObject("-121:4:21.12345", MysqlType.DATETIME));
        assertEquals(Duration.parse("PT25H14M7.12S"), TimeUtil.parseToDateTimeObject("25:14:07.12", MysqlType.DATETIME));

        assertThrows(WrongArgumentException.class, () -> {
            TimeUtil.parseToDateTimeObject("12 25:4:21", MysqlType.TIME);
        }, "There is no known date-time pattern for.*");
        assertThrows(WrongArgumentException.class, () -> {
            TimeUtil.parseToDateTimeObject("12 2:64:21", MysqlType.TIME);
        }, "There is no known date-time pattern for.*");
        assertThrows(WrongArgumentException.class, () -> {
            TimeUtil.parseToDateTimeObject("12 2:4:61", MysqlType.TIME);
        }, "There is no known date-time pattern for.*");

        /* DATETIME literals */

        assertEquals(LocalDateTime.of(2013, 4, 17, 3, 14, 7), TimeUtil.parseToDateTimeObject("2013-04-17 03:14:07", MysqlType.DATE));
        assertEquals(LocalDateTime.of(2013, 4, 17, 3, 14, 7), TimeUtil.parseToDateTimeObject("13-04-17 03:14:07", MysqlType.DATE));
        assertEquals(LocalDateTime.of(2013, 4, 17, 3, 14, 7), TimeUtil.parseToDateTimeObject("2013#04$17 03@14%07", MysqlType.DATE));
        assertEquals(LocalDateTime.of(2013, 4, 17, 3, 14, 7), TimeUtil.parseToDateTimeObject("13#04$17 03@14%07", MysqlType.DATE));
        assertEquals(LocalDateTime.of(2013, 4, 17, 3, 14, 7), TimeUtil.parseToDateTimeObject("2013#04$17 03@14%07", MysqlType.DATE));
        assertEquals(LocalDateTime.of(2013, 4, 17, 3, 14, 7), TimeUtil.parseToDateTimeObject("13#04$17 03@14%07", MysqlType.DATE));
        assertEquals(LocalDateTime.of(2013, 4, 17, 3, 14, 7), TimeUtil.parseToDateTimeObject("2013#04$17T03@14%07", MysqlType.DATE));
        assertEquals(LocalDateTime.of(2013, 4, 17, 3, 14, 7), TimeUtil.parseToDateTimeObject("13#04$17T03@14%07", MysqlType.DATE));
        assertEquals(LocalDateTime.of(2013, 4, 7, 3, 4, 7), TimeUtil.parseToDateTimeObject("2013-4-7 3:4:7", MysqlType.DATE));
        assertEquals(LocalDateTime.of(2013, 4, 7, 3, 4, 7), TimeUtil.parseToDateTimeObject("13-4-7 3:4:7", MysqlType.DATE));
        assertEquals(LocalDateTime.of(2013, 4, 17, 3, 14, 7), TimeUtil.parseToDateTimeObject("20130417031407", MysqlType.DATE));
        assertEquals(LocalDateTime.of(2013, 4, 17, 3, 14, 7), TimeUtil.parseToDateTimeObject("130417031407", MysqlType.DATE));
        assertEquals(LocalDateTime.of(2013, 4, 17, 3, 14, 7, 100000000), TimeUtil.parseToDateTimeObject("2013-04-17 03:14:07.1", MysqlType.DATE));
        assertEquals(LocalDateTime.of(2013, 4, 17, 3, 14, 7, 12345678), TimeUtil.parseToDateTimeObject("2013-04-17 03:14:07.012345678", MysqlType.DATE));

        assertEquals(LocalDateTime.of(2013, 4, 17, 3, 14, 7), TimeUtil.parseToDateTimeObject("2013-04-17 03:14:07", MysqlType.TIME));
        assertEquals(LocalDateTime.of(2013, 4, 17, 3, 14, 7), TimeUtil.parseToDateTimeObject("13-04-17 03:14:07", MysqlType.TIME));
        assertEquals(LocalDateTime.of(2013, 4, 17, 3, 14, 7), TimeUtil.parseToDateTimeObject("2013#04$17 03@14%07", MysqlType.TIME));
        assertEquals(LocalDateTime.of(2013, 4, 17, 3, 14, 7), TimeUtil.parseToDateTimeObject("13#04$17 03@14%07", MysqlType.TIME));
        assertEquals(LocalDateTime.of(2013, 4, 17, 3, 14, 7), TimeUtil.parseToDateTimeObject("2013#04$17 03@14%07", MysqlType.TIME));
        assertEquals(LocalDateTime.of(2013, 4, 17, 3, 14, 7), TimeUtil.parseToDateTimeObject("13#04$17 03@14%07", MysqlType.TIME));
        assertEquals(LocalDateTime.of(2013, 4, 17, 3, 14, 7), TimeUtil.parseToDateTimeObject("2013#04$17T03@14%07", MysqlType.TIME));
        assertEquals(LocalDateTime.of(2013, 4, 17, 3, 14, 7), TimeUtil.parseToDateTimeObject("13#04$17T03@14%07", MysqlType.TIME));
        assertEquals(LocalDateTime.of(2013, 4, 7, 3, 4, 7), TimeUtil.parseToDateTimeObject("2013-4-7 3:4:7", MysqlType.TIME));
        assertEquals(LocalDateTime.of(2013, 4, 7, 3, 4, 7), TimeUtil.parseToDateTimeObject("13-4-7 3:4:7", MysqlType.TIME));
        assertEquals(LocalDateTime.of(2013, 4, 17, 3, 14, 7), TimeUtil.parseToDateTimeObject("20130417031407", MysqlType.TIME));
        assertEquals(LocalDateTime.of(2013, 4, 17, 3, 14, 7), TimeUtil.parseToDateTimeObject("130417031407", MysqlType.TIME));
        assertEquals(LocalDateTime.of(2013, 4, 17, 3, 14, 7, 100000000), TimeUtil.parseToDateTimeObject("2013-04-17 03:14:07.1", MysqlType.TIME));
        assertEquals(LocalDateTime.of(2013, 4, 17, 3, 14, 7, 12345678), TimeUtil.parseToDateTimeObject("2013-04-17 03:14:07.012345678", MysqlType.TIME));

        assertEquals(LocalDateTime.of(2013, 4, 17, 3, 14, 7), TimeUtil.parseToDateTimeObject("2013-04-17 03:14:07", MysqlType.DATETIME));
        assertEquals(LocalDateTime.of(2013, 4, 17, 3, 14, 7), TimeUtil.parseToDateTimeObject("13-04-17 03:14:07", MysqlType.DATETIME));
        assertEquals(LocalDateTime.of(2013, 4, 17, 3, 14, 7), TimeUtil.parseToDateTimeObject("2013#04$17 03@14%07", MysqlType.DATETIME));
        assertEquals(LocalDateTime.of(2013, 4, 17, 3, 14, 7), TimeUtil.parseToDateTimeObject("13#04$17 03@14%07", MysqlType.DATETIME));
        assertEquals(LocalDateTime.of(2013, 4, 17, 3, 14, 7), TimeUtil.parseToDateTimeObject("2013#04$17 03@14%07", MysqlType.DATETIME));
        assertEquals(LocalDateTime.of(2013, 4, 17, 3, 14, 7), TimeUtil.parseToDateTimeObject("13#04$17 03@14%07", MysqlType.DATETIME));
        assertEquals(LocalDateTime.of(2013, 4, 17, 3, 14, 7), TimeUtil.parseToDateTimeObject("2013#04$17T03@14%07", MysqlType.DATETIME));
        assertEquals(LocalDateTime.of(2013, 4, 17, 3, 14, 7), TimeUtil.parseToDateTimeObject("13#04$17T03@14%07", MysqlType.DATETIME));
        assertEquals(LocalDateTime.of(2013, 4, 7, 3, 4, 7), TimeUtil.parseToDateTimeObject("2013-4-7 3:4:7", MysqlType.DATETIME));
        assertEquals(LocalDateTime.of(2013, 4, 7, 3, 4, 7), TimeUtil.parseToDateTimeObject("13-4-7 3:4:7", MysqlType.DATETIME));
        assertEquals(LocalDateTime.of(2013, 4, 17, 3, 14, 7), TimeUtil.parseToDateTimeObject("20130417031407", MysqlType.DATETIME));
        assertEquals(LocalDateTime.of(2013, 4, 17, 3, 14, 7), TimeUtil.parseToDateTimeObject("130417031407", MysqlType.DATETIME));
        assertEquals(LocalDateTime.of(2013, 4, 17, 3, 14, 7, 100000000), TimeUtil.parseToDateTimeObject("2013-04-17 03:14:07.1", MysqlType.DATETIME));
        assertEquals(LocalDateTime.of(2013, 4, 17, 3, 14, 7, 12345678), TimeUtil.parseToDateTimeObject("2013-04-17 03:14:07.012345678", MysqlType.DATETIME));

        assertThrows(WrongArgumentException.class, () -> {
            TimeUtil.parseToDateTimeObject("2013-14-17 03:14:07.1", MysqlType.DATETIME);
        }, "There is no known date-time pattern for.*");
        assertThrows(WrongArgumentException.class, () -> {
            TimeUtil.parseToDateTimeObject("2013-04-37 03:14:07.1", MysqlType.DATETIME);
        }, "There is no known date-time pattern for.*");
        assertThrows(WrongArgumentException.class, () -> {
            TimeUtil.parseToDateTimeObject("2013-04-17 33:14:07.1", MysqlType.DATETIME);
        }, "There is no known date-time pattern for.*");
        assertThrows(WrongArgumentException.class, () -> {
            TimeUtil.parseToDateTimeObject("2013-04-17 03:64:07.1", MysqlType.DATETIME);
        }, "There is no known date-time pattern for.*");
        assertThrows(WrongArgumentException.class, () -> {
            TimeUtil.parseToDateTimeObject("2013-04-17 03:14:67.1", MysqlType.DATETIME);
        }, "There is no known date-time pattern for.*");

        assertThrows(WrongArgumentException.class, () -> {
            TimeUtil.parseToDateTimeObject("2013-04-17 03:14:07#1", MysqlType.DATE);
        }, "There is no known date-time pattern for.*");
        assertThrows(WrongArgumentException.class, () -> {
            TimeUtil.parseToDateTimeObject("2013#04$17$03@14%07", MysqlType.DATE);
        }, "There is no known date-time pattern for.*");
        assertThrows(WrongArgumentException.class, () -> {
            TimeUtil.parseToDateTimeObject("2013-04-17 03:14:07#1", MysqlType.TIME);
        }, "There is no known date-time pattern for.*");
        assertThrows(WrongArgumentException.class, () -> {
            TimeUtil.parseToDateTimeObject("2013#04$17$03@14%07", MysqlType.TIME);
        }, "There is no known date-time pattern for.*");
        assertThrows(WrongArgumentException.class, () -> {
            TimeUtil.parseToDateTimeObject("2013-04-17 03:14:07#1", MysqlType.DATETIME);
        }, "There is no known date-time pattern for.*");
        assertThrows(WrongArgumentException.class, () -> {
            TimeUtil.parseToDateTimeObject("2013#04$17$03@14%07", MysqlType.DATETIME);
        }, "There is no known date-time pattern for.*");
    }

}
