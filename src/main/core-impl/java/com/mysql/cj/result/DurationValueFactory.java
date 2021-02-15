/*
 * Copyright (c) 2021, Oracle and/or its affiliates.
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

package com.mysql.cj.result;

import java.time.Duration;

import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.protocol.InternalDate;
import com.mysql.cj.protocol.InternalTime;
import com.mysql.cj.protocol.InternalTimestamp;

/**
 * A value factory to create {@link Duration} instances.
 */
public class DurationValueFactory extends AbstractDateTimeValueFactory<Duration> {

    public DurationValueFactory(PropertySet pset) {
        super(pset);
    }

    @Override
    Duration localCreateFromDate(InternalDate idate) {
        return unsupported("DATE");
    }

    @Override
    public Duration localCreateFromTime(InternalTime it) {
        String ptn = (it.getHours() < 0 ? "-PT" : "PT") + (it.getHours() < 0 ? -it.getHours() : it.getHours()) + "H" + it.getMinutes() + "M" + it.getSeconds()
                + "." + it.getNanos() + "S";
        return Duration.parse(ptn);
    }

    @Override
    public Duration localCreateFromTimestamp(InternalTimestamp its) {
        return unsupported("TIMESTAMP");
    }

    @Override
    public Duration localCreateFromDatetime(InternalTimestamp its) {
        return unsupported("DATETIME");
    }

    public String getTargetTypeName() {
        return Duration.class.getName();
    }

}
