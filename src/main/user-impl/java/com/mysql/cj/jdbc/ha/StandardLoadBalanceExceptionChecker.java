/*
 * Copyright (c) 2010, 2018, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.jdbc.ha;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.jdbc.exceptions.CommunicationsException;
import com.mysql.cj.util.StringUtils;

public class StandardLoadBalanceExceptionChecker implements LoadBalanceExceptionChecker {

    private List<String> sqlStateList;
    private List<Class<?>> sqlExClassList;

    @Override
    public boolean shouldExceptionTriggerFailover(Throwable ex) {
        String sqlState = ex instanceof SQLException ? ((SQLException) ex).getSQLState() : null;

        if (sqlState != null) {
            if (sqlState.startsWith("08")) {
                // connection error
                return true;
            }
            if (this.sqlStateList != null) {
                // check against SQLState list
                for (Iterator<String> i = this.sqlStateList.iterator(); i.hasNext();) {
                    if (sqlState.startsWith(i.next().toString())) {
                        return true;
                    }
                }
            }
        }

        // always handle CommunicationException
        if (ex instanceof CommunicationsException || ex instanceof CJCommunicationsException) {
            return true;
        }

        if (this.sqlExClassList != null) {
            // check against configured class lists
            for (Iterator<Class<?>> i = this.sqlExClassList.iterator(); i.hasNext();) {
                if (i.next().isInstance(ex)) {
                    return true;
                }
            }
        }
        // no matches
        return false;
    }

    @Override
    public void destroy() {
    }

    @Override
    public void init(Properties props) {
        configureSQLStateList(props.getProperty(PropertyDefinitions.PNAME_loadBalanceSQLStateFailover, null));
        configureSQLExceptionSubclassList(props.getProperty(PropertyDefinitions.PNAME_loadBalanceSQLExceptionSubclassFailover, null));
    }

    private void configureSQLStateList(String sqlStates) {
        if (sqlStates == null || "".equals(sqlStates)) {
            return;
        }
        List<String> states = StringUtils.split(sqlStates, ",", true);
        List<String> newStates = new ArrayList<>();

        for (String state : states) {
            if (state.length() > 0) {
                newStates.add(state);
            }
        }
        if (newStates.size() > 0) {
            this.sqlStateList = newStates;
        }
    }

    private void configureSQLExceptionSubclassList(String sqlExClasses) {
        if (sqlExClasses == null || "".equals(sqlExClasses)) {
            return;
        }
        List<String> classes = StringUtils.split(sqlExClasses, ",", true);
        List<Class<?>> newClasses = new ArrayList<>();

        for (String exClass : classes) {
            try {
                Class<?> c = Class.forName(exClass);
                newClasses.add(c);
            } catch (Exception e) {
                // ignore and don't check, class doesn't exist
            }
        }
        if (newClasses.size() > 0) {
            this.sqlExClassList = newClasses;
        }
    }
}
