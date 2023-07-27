/*
 * Copyright (c) 2019, 2023, Oracle and/or its affiliates.
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

package com.mysql.cj.protocol;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;

public class InternalTimestamp extends InternalDate {

    private int hours = 0;
    private int minutes = 0;
    private int seconds = 0;
    private int nanos = 0;
    private int scale = 0;
    private int offset = 0;

    public static InternalTimestamp from(LocalDate x) {
        return new InternalTimestamp(x.getYear(), x.getMonthValue(), x.getDayOfMonth(), 0, 0, 0, 0, -1);
    }

    public static InternalTimestamp from(LocalDateTime x) {
        return new InternalTimestamp(x.getYear(), x.getMonthValue(), x.getDayOfMonth(), x.getHour(), x.getMinute(), x.getSecond(), x.getNano(), -1);
    }

    public static InternalTimestamp from(OffsetDateTime x) {
        InternalTimestamp internalTimestamp = new InternalTimestamp(x.getYear(), x.getMonthValue(), x.getDayOfMonth(), x.getHour(), x.getMinute(),
                x.getSecond(), x.getNano(), -1);
        internalTimestamp.setOffset((int) TimeUnit.SECONDS.toMinutes(x.getOffset().getTotalSeconds()));
        return internalTimestamp;
    }

    public static InternalTimestamp from(ZonedDateTime x) {
        InternalTimestamp internalTimestamp = new InternalTimestamp(x.getYear(), x.getMonthValue(), x.getDayOfMonth(), x.getHour(), x.getMinute(),
                x.getSecond(), x.getNano(), -1);
        internalTimestamp.setOffset((int) TimeUnit.SECONDS.toMinutes(x.getOffset().getTotalSeconds()));
        return internalTimestamp;
    }

    public static InternalTimestamp from(Calendar x, int nanos) {
        return new InternalTimestamp(x.get(Calendar.YEAR), x.get(Calendar.MONTH) + 1, x.get(Calendar.DAY_OF_MONTH), x.get(Calendar.HOUR_OF_DAY),
                x.get(Calendar.MINUTE), x.get(Calendar.SECOND), nanos, -1);
    }

    /**
     * Constructs a zero datetime
     */
    public InternalTimestamp() {
        super();
    }

    public InternalTimestamp(int year, int month, int day, int hours, int minutes, int seconds, int nanos, int scale) {
        this.year = year;
        this.month = month;
        this.day = day;
        this.hours = hours;
        this.minutes = minutes;
        this.seconds = seconds;
        this.nanos = nanos;
        this.scale = scale;
    }

    public int getHours() {
        return this.hours;
    }

    public void setHours(int hours) {
        this.hours = hours;
    }

    public int getMinutes() {
        return this.minutes;
    }

    public void setMinutes(int minutes) {
        this.minutes = minutes;
    }

    public int getSeconds() {
        return this.seconds;
    }

    public void setSeconds(int seconds) {
        this.seconds = seconds;
    }

    public int getNanos() {
        return this.nanos;
    }

    public void setNanos(int nanos) {
        this.nanos = nanos;
    }

    public int getScale() {
        return this.scale;
    }

    public void setScale(int scale) {
        this.scale = scale;
    }

    public int getOffset() {
        return this.offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    @Override
    public boolean isZero() {
        return super.isZero() && this.hours == 0 && this.minutes == 0 && this.seconds == 0 && this.nanos == 0;
    }

}
