/*
  Copyright (c) 2010, 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.jdbc.ha;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import com.mysql.cj.api.MysqlConnection;
import com.mysql.cj.api.jdbc.ha.LoadBalanceExceptionChecker;
import com.mysql.cj.api.log.Log;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exceptions.CJCommunicationsException;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.jdbc.exceptions.CommunicationsException;

public class StandardLoadBalanceExceptionChecker implements LoadBalanceExceptionChecker {

    private List<String> sqlStateList;
    private List<Class<?>> sqlExClassList;

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

    public void destroy() {
    }

    public void init(MysqlConnection conn, Properties props, Log log) {
        configureSQLStateList(props.getProperty(PropertyDefinitions.PNAME_loadBalanceSQLStateFailover, null));
        configureSQLExceptionSubclassList(props.getProperty(PropertyDefinitions.PNAME_loadBalanceSQLExceptionSubclassFailover, null));
    }

    private void configureSQLStateList(String sqlStates) {
        if (sqlStates == null || "".equals(sqlStates)) {
            return;
        }
        List<String> states = StringUtils.split(sqlStates, ",", true);
        List<String> newStates = new ArrayList<String>();

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
        List<Class<?>> newClasses = new ArrayList<Class<?>>();

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
