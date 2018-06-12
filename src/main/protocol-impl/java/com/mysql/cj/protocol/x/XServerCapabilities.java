/*
 * Copyright (c) 2018, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.protocol.x;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.mysql.cj.ServerVersion;
import com.mysql.cj.protocol.ServerCapabilities;
import com.mysql.cj.x.protobuf.MysqlxDatatypes.Any;
import com.mysql.cj.xdevapi.ExprUtil;

public class XServerCapabilities implements ServerCapabilities {

    private Map<String, Any> capabilities;

    public XServerCapabilities(Map<String, Any> capabilities) {
        this.capabilities = capabilities;
    }

    public void setCapability(String name, Object value) {
        this.capabilities.put(name, ExprUtil.argObjectToScalarAny(value));
    }

    public boolean hasCapability(String name) {
        return this.capabilities.containsKey(name);
    }

    public String getNodeType() {
        return this.capabilities.get("node_type").getScalar().getVString().getValue().toStringUtf8();
    }

    public boolean getTls() {
        return hasCapability("tls") ? this.capabilities.get("tls").getScalar().getVBool() : false;
    }

    public boolean getClientPwdExpireOk() {
        return this.capabilities.get("client.pwd_expire_ok").getScalar().getVBool();
    }

    public List<String> getAuthenticationMechanisms() {
        return this.capabilities.get("authentication.mechanisms").getArray().getValueList().stream()
                .map(v -> v.getScalar().getVString().getValue().toStringUtf8()).collect(Collectors.toList());
    }

    public String getDocFormats() {
        return this.capabilities.get("doc.formats").getScalar().getVString().getValue().toStringUtf8();
    }

    @Override
    public int getCapabilityFlags() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void setCapabilityFlags(int capabilityFlags) {
        // TODO Auto-generated method stub

    }

    @Override
    public ServerVersion getServerVersion() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setServerVersion(ServerVersion serverVersion) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean serverSupportsFracSecs() {
        return true;
    }
}
