/*
 * Copyright (c) 2018, 2021, Oracle and/or its affiliates.
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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.mysql.cj.ServerVersion;
import com.mysql.cj.protocol.ServerCapabilities;
import com.mysql.cj.x.protobuf.MysqlxDatatypes.Any;
import com.mysql.cj.xdevapi.ExprUtil;

public class XServerCapabilities implements ServerCapabilities {

    private Map<String, Any> capabilities;

    static String KEY_COMPRESSION = "compression";
    static String KEY_SESSION_CONNECT_ATTRS = "session_connect_attrs";
    static String KEY_TLS = "tls";
    static String KEY_NODE_TYPE = "node_type";
    static String KEY_CLIENT_PWD_EXPIRE_OK = "client.pwd_expire_ok";
    static String KEY_AUTHENTICATION_MECHANISMS = "authentication.mechanisms";
    static String KEY_DOC_FORMATS = "doc.formats";

    static String SUBKEY_COMPRESSION_ALGORITHM = "algorithm";
    static String SUBKEY_COMPRESSION_SERVER_COMBINE_MIXED_MESSAGES = "server_combine_mixed_messages";
    static String SUBKEY_COMPRESSION_SERVER_MAX_COMBINE_MESSAGES = "server_max_combine_messages";

    /** Server-assigned client-id. */
    private long clientId = -1;

    public XServerCapabilities(Map<String, Any> capabilities) {
        this.capabilities = capabilities;
    }

    public void setCapability(String name, Object value) {
        if (!KEY_SESSION_CONNECT_ATTRS.equals(name) && !KEY_COMPRESSION.equals(name)) {
            this.capabilities.put(name, ExprUtil.argObjectToScalarAny(value));
        }
    }

    public boolean hasCapability(String name) {
        return this.capabilities.containsKey(name);
    }

    public String getNodeType() {
        return this.capabilities.get(KEY_NODE_TYPE).getScalar().getVString().getValue().toStringUtf8();
    }

    public boolean getTls() {
        return hasCapability(KEY_TLS) ? this.capabilities.get(KEY_TLS).getScalar().getVBool() : false;
    }

    public boolean getClientPwdExpireOk() {
        return this.capabilities.get(KEY_CLIENT_PWD_EXPIRE_OK).getScalar().getVBool();
    }

    public List<String> getAuthenticationMechanisms() {
        return this.capabilities.get(KEY_AUTHENTICATION_MECHANISMS).getArray().getValueList().stream()
                .map(v -> v.getScalar().getVString().getValue().toStringUtf8()).collect(Collectors.toList());
    }

    public String getDocFormats() {
        return this.capabilities.get(KEY_DOC_FORMATS).getScalar().getVString().getValue().toStringUtf8();
    }

    public Map<String, List<String>> getCompression() {
        if (this.hasCapability(KEY_COMPRESSION)) {
            return this.capabilities.get(KEY_COMPRESSION).getObj().getFldList().stream()
                    .collect(Collectors.toMap(f -> f.getKey().toLowerCase(), f -> f.getValue().getArray().getValueList().stream()
                            .map(v -> v.getScalar().getVString().getValue().toStringUtf8().toLowerCase()).collect(Collectors.toList())));
        }
        return Collections.emptyMap();
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
    public boolean serverSupportsFracSecs() {
        return true;
    }

    @Override
    public int getServerDefaultCollationIndex() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getThreadId() {
        return this.clientId;
    }

    @Override
    public void setThreadId(long threadId) {
        this.clientId = threadId;
    }
}
