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

package com.mysql.cj.mysqlx;

import java.util.Map;

import com.mysql.cj.api.Session;
import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.api.io.Protocol;
import com.mysql.cj.core.ServerVersion;
import com.mysql.cj.mysqlx.io.MysqlxProtocol;

public class MysqlxSession implements Session {
    MysqlxProtocol protocol;

    public MysqlxSession(MysqlxProtocol protocol) {
        this.protocol = protocol;
    }

    public PropertySet getPropertySet() {
        throw new NullPointerException("TODO: justify use of this method");
    }

    public Protocol getProtocol() {
        throw new NullPointerException("TODO: You are not allowed to have my protocol");
    }

    public void changeUser(String userName, String password, String database) {
        throw new NullPointerException("TODO: implement?");
    }

    public ExceptionInterceptor getExceptionInterceptor() {
        throw new NullPointerException("TODO: You are not allowed to have this");
    }

    public void setExceptionInterceptor(ExceptionInterceptor exceptionInterceptor) {
        throw new NullPointerException("TODO: I don't need your stinkin exception interceptor");
    }

    public boolean characterSetNamesMatches(String mysqlEncodingName) {
        throw new NullPointerException("TODO: don't implement this method here");
    }

    public boolean inTransactionOnServer() {
        throw new NullPointerException("TODO: who wants to know? Also, check NEW tx state in OK packet extensions");
    }

    public String getServerVariable(String name) {
        throw new NullPointerException("TODO: ");
    }

    public Map<String, String> getServerVariables() {
        throw new NullPointerException("TODO: ");
    }

    public void setServerVariables(Map<String, String> serverVariables) {
        throw new NullPointerException("TODO: ");
    }

    public int getServerCharsetIndex() {
        throw new NullPointerException("TODO: ");
    }

    public void setServerCharsetIndex(int serverCharsetIndex) {
        throw new NullPointerException("TODO: ");
    }

    public void abortInternal() {
        throw new NullPointerException("TODO: REPLACE ME WITH close() unless there's different semantics here");
    }

    public void quit() {
        throw new NullPointerException("TODO: REPLACE ME WITH close() unless there's different semantics here");
    }

    public void forceClose() {
        throw new NullPointerException("TODO: REPLACE ME WITH close() unless there's different semantics here");
    }

    public ServerVersion getServerVersion() {
        throw new NullPointerException("TODO: isn't this in server session?");
    }

    public boolean versionMeetsMinimum(int major, int minor, int subminor) {
        throw new NullPointerException("TODO: ");
    }

    public long getThreadId() {
        throw new NullPointerException("TODO: ");
    }

    public boolean isSetNeededForAutoCommitMode(boolean autoCommitFlag) {
        throw new NullPointerException("TODO: ");
    }
}
