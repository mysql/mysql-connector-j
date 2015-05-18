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

package com.mysql.cj.core.conf;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.mysql.cj.api.conf.BooleanModifiableProperty;
import com.mysql.cj.api.conf.BooleanReadableProperty;
import com.mysql.cj.api.conf.ConnectionProperties;
import com.mysql.cj.api.conf.IntegerModifiableProperty;
import com.mysql.cj.api.conf.IntegerReadableProperty;
import com.mysql.cj.api.conf.LongModifiableProperty;
import com.mysql.cj.api.conf.LongReadableProperty;
import com.mysql.cj.api.conf.MemorySizeModifiableProperty;
import com.mysql.cj.api.conf.MemorySizeReadableProperty;
import com.mysql.cj.api.conf.ModifiableProperty;
import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.conf.ReadableProperty;
import com.mysql.cj.api.conf.RuntimeProperty;
import com.mysql.cj.api.conf.StringModifiableProperty;
import com.mysql.cj.api.conf.StringReadableProperty;
import com.mysql.cj.api.exception.ExceptionInterceptor;

public class CommonConnectionProperties implements PropertySet, ConnectionProperties, Serializable {

    private static final long serialVersionUID = 3764240467061778179L;

    private final Map<String, RuntimeProperty> PROPERTY_NAME_TO_RUNTIME_PROPERTY = new HashMap<String, RuntimeProperty>();

    protected boolean useUnicodeAsBoolean = true;

    protected String characterEncodingAsString = null;

    public CommonConnectionProperties() {

        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_paranoid));

        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_passwordCharacterEncoding));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_serverRSAPublicKeyFile));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_allowPublicKeyRetrieval));

        // SSL Options

        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_clientCertificateKeyStoreUrl));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_trustCertificateKeyStoreUrl));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_clientCertificateKeyStoreType));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_clientCertificateKeyStorePassword));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_trustCertificateKeyStoreType));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_trustCertificateKeyStorePassword));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_verifyServerCertificate));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_enabledSSLCipherSuites));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useUnbufferedInput));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_profilerEventHandler));
    }

    @Override
    public void addProperty(RuntimeProperty prop) {
        this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.put(prop.getPropertyDefinition().getName(), prop);
        this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.put(prop.getPropertyDefinition().getAlias(), prop);
    }

    @Override
    public void removeProperty(String name) {
        RuntimeProperty prop = this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.remove(name);
        if (prop != null) {
            if (name.equals(prop.getPropertyDefinition().getName())) {
                this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.remove(prop.getPropertyDefinition().getAlias());
            } else {
                this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.remove(prop.getPropertyDefinition().getName());
            }
        }
    }

    @Override
    public ReadableProperty getReadableProperty(String name) {
        // TODO check property type
        return (ReadableProperty) this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.get(name);
    }

    @Override
    public BooleanReadableProperty getBooleanReadableProperty(String name) {
        // TODO check property type
        return (BooleanReadableProperty) this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.get(name);
    }

    @Override
    public IntegerReadableProperty getIntegerReadableProperty(String name) {
        // TODO check property type
        return (IntegerReadableProperty) this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.get(name);
    }

    @Override
    public LongReadableProperty getLongReadableProperty(String name) {
        // TODO check property type
        return (LongReadableProperty) this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.get(name);
    }

    @Override
    public MemorySizeReadableProperty getMemorySizeReadableProperty(String name) {
        // TODO check property type
        return (MemorySizeReadableProperty) this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.get(name);
    }

    @Override
    public StringReadableProperty getStringReadableProperty(String name) {
        // TODO check property type
        return (StringReadableProperty) this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.get(name);
    }

    @Override
    public ModifiableProperty getModifiableProperty(String name) {
        // TODO check property type
        return (ModifiableProperty) this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.get(name);
    }

    @Override
    public BooleanModifiableProperty getBooleanModifiableProperty(String name) {
        // TODO check property type
        return (BooleanModifiableProperty) this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.get(name);
    }

    @Override
    public IntegerModifiableProperty getIntegerModifiableProperty(String name) {
        // TODO check property type
        return (IntegerModifiableProperty) this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.get(name);
    }

    @Override
    public LongModifiableProperty getLongModifiableProperty(String name) {
        // TODO check property type
        return (LongModifiableProperty) this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.get(name);
    }

    @Override
    public MemorySizeModifiableProperty getMemorySizeModifiableProperty(String name) {
        // TODO check property type
        return (MemorySizeModifiableProperty) this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.get(name);
    }

    @Override
    public StringModifiableProperty getStringModifiableProperty(String name) {
        // TODO check property type
        return (StringModifiableProperty) this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.get(name);
    }

    // --------------------------------
    public boolean getParanoid() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_paranoid).getValueAsBoolean();
    }

    public void setParanoid(boolean property) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_paranoid).setValue(property);
    }

    public String getPasswordCharacterEncoding() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_passwordCharacterEncoding).getStringValue();
    }

    public void setPasswordCharacterEncoding(String characterSet) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_passwordCharacterEncoding).setValue(characterSet);
    }

    public ExceptionInterceptor getExceptionInterceptor() {
        return null;
    }

    public String getServerRSAPublicKeyFile() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_serverRSAPublicKeyFile).getStringValue();
    }

    public boolean getAllowPublicKeyRetrieval() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_allowPublicKeyRetrieval).getValueAsBoolean();
    }

    public String getEncoding() {
        return this.characterEncodingAsString;
    }

    public boolean getUseUnicode() {
        return this.useUnicodeAsBoolean;
    }

    public String getClientCertificateKeyStorePassword() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_clientCertificateKeyStorePassword).getStringValue();
    }

    public void setClientCertificateKeyStorePassword(String value) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_clientCertificateKeyStorePassword).setValue(value);
    }

    public String getClientCertificateKeyStoreType() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_clientCertificateKeyStoreType).getStringValue();
    }

    public void setClientCertificateKeyStoreType(String value) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_clientCertificateKeyStoreType).setValue(value);
    }

    public String getClientCertificateKeyStoreUrl() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_clientCertificateKeyStoreUrl).getStringValue();
    }

    public void setClientCertificateKeyStoreUrl(String value) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_clientCertificateKeyStoreUrl).setValue(value);
    }

    public String getTrustCertificateKeyStorePassword() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_trustCertificateKeyStorePassword).getStringValue();
    }

    public void setTrustCertificateKeyStorePassword(String value) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_trustCertificateKeyStorePassword).setValue(value);
    }

    public String getTrustCertificateKeyStoreType() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_trustCertificateKeyStoreType).getStringValue();
    }

    public void setTrustCertificateKeyStoreType(String value) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_trustCertificateKeyStoreType).setValue(value);
    }

    public String getTrustCertificateKeyStoreUrl() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_trustCertificateKeyStoreUrl).getStringValue();
    }

    public void setTrustCertificateKeyStoreUrl(String value) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_trustCertificateKeyStoreUrl).setValue(value);
    }

    public boolean getVerifyServerCertificate() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_verifyServerCertificate).getValueAsBoolean();
    }

    public void setVerifyServerCertificate(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_verifyServerCertificate).setValue(flag);
    }

    public String getEnabledSSLCipherSuites() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_enabledSSLCipherSuites).getStringValue();
    }

    public void setEnabledSSLCipherSuites(String cipherSuites) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_enabledSSLCipherSuites).setValue(cipherSuites);
    }

    public boolean getUseUnbufferedInput() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_useUnbufferedInput).getValueAsBoolean();
    }

    public String getProfilerEventHandler() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_profilerEventHandler).getStringValue();
    }

    public void setProfilerEventHandler(String handler) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_profilerEventHandler).setValue(handler);
    }
}
