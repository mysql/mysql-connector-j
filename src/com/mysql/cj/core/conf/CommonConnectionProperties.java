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

import com.mysql.cj.api.conf.ConnectionProperties;
import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.conf.RuntimeProperty;
import com.mysql.cj.api.exception.ExceptionInterceptor;

public class CommonConnectionProperties implements PropertySet, ConnectionProperties, Serializable {

    private static final long serialVersionUID = 3764240467061778179L;

    private final Map<String, RuntimeProperty> PROPERTY_NAME_TO_RUNTIME_PROPERTY = new HashMap<String, RuntimeProperty>();

    //protected static final ArrayList<java.lang.reflect.Field> PROPERTY_LIST = new ArrayList<java.lang.reflect.Field>();

    protected boolean useUnicodeAsBoolean = true;

    protected String characterEncodingAsString = null;

    public CommonConnectionProperties() {

        addProperty(new BooleanConnectionProperty(PropertyDefinitions.PNAME_paranoid));

        addProperty(new StringConnectionProperty(PropertyDefinitions.PNAME_passwordCharacterEncoding));
        addProperty(new StringConnectionProperty(PropertyDefinitions.PNAME_serverRSAPublicKeyFile));
        addProperty(new BooleanConnectionProperty(PropertyDefinitions.PNAME_allowPublicKeyRetrieval));

        // SSL Options

        addProperty(new StringConnectionProperty(PropertyDefinitions.PNAME_clientCertificateKeyStoreUrl));
        addProperty(new StringConnectionProperty(PropertyDefinitions.PNAME_trustCertificateKeyStoreUrl));
        addProperty(new StringConnectionProperty(PropertyDefinitions.PNAME_clientCertificateKeyStoreType));
        addProperty(new StringConnectionProperty(PropertyDefinitions.PNAME_clientCertificateKeyStorePassword));
        addProperty(new StringConnectionProperty(PropertyDefinitions.PNAME_trustCertificateKeyStoreType));
        addProperty(new StringConnectionProperty(PropertyDefinitions.PNAME_trustCertificateKeyStorePassword));
        addProperty(new BooleanConnectionProperty(PropertyDefinitions.PNAME_verifyServerCertificate));
        addProperty(new StringConnectionProperty(PropertyDefinitions.PNAME_enabledSSLCipherSuites));
        addProperty(new BooleanConnectionProperty(PropertyDefinitions.PNAME_useUnbufferedInput));
        addProperty(new StringConnectionProperty(PropertyDefinitions.PNAME_profilerEventHandler));
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
    public RuntimeProperty getProperty(String name) {
        return this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.get(name);
    }

    public boolean getParanoid() {
        return ((BooleanConnectionProperty) getProperty(PropertyDefinitions.PNAME_paranoid)).getValueAsBoolean();
    }

    public void setParanoid(boolean property) {
        ((BooleanConnectionProperty) getProperty(PropertyDefinitions.PNAME_paranoid)).setValue(property);
    }

    public String getPasswordCharacterEncoding() {
        return ((StringConnectionProperty) getProperty(PropertyDefinitions.PNAME_passwordCharacterEncoding)).getValueAsString();
    }

    public void setPasswordCharacterEncoding(String characterSet) {
        ((StringConnectionProperty) getProperty(PropertyDefinitions.PNAME_passwordCharacterEncoding)).setValue(characterSet);
    }

    public ExceptionInterceptor getExceptionInterceptor() {
        return null;
    }

    public String getServerRSAPublicKeyFile() {
        return ((StringConnectionProperty) getProperty(PropertyDefinitions.PNAME_serverRSAPublicKeyFile)).getValueAsString();
    }

    public boolean getAllowPublicKeyRetrieval() {
        return ((BooleanConnectionProperty) getProperty(PropertyDefinitions.PNAME_allowPublicKeyRetrieval)).getValueAsBoolean();
    }

    public String getEncoding() {
        return this.characterEncodingAsString;
    }

    public boolean getUseUnicode() {
        return this.useUnicodeAsBoolean;
    }

    public String getClientCertificateKeyStorePassword() {
        return getProperty(PropertyDefinitions.PNAME_clientCertificateKeyStorePassword).getValue(String.class);
    }

    public void setClientCertificateKeyStorePassword(String value) {
        ((StringConnectionProperty) getProperty(PropertyDefinitions.PNAME_clientCertificateKeyStorePassword)).setValue(value);
    }

    public String getClientCertificateKeyStoreType() {
        return ((StringConnectionProperty) getProperty(PropertyDefinitions.PNAME_clientCertificateKeyStoreType)).getValueAsString();
    }

    public void setClientCertificateKeyStoreType(String value) {
        ((StringConnectionProperty) getProperty(PropertyDefinitions.PNAME_clientCertificateKeyStoreType)).setValue(value);
    }

    public String getClientCertificateKeyStoreUrl() {
        return ((StringConnectionProperty) getProperty(PropertyDefinitions.PNAME_clientCertificateKeyStoreUrl)).getValueAsString();
    }

    public void setClientCertificateKeyStoreUrl(String value) {
        ((StringConnectionProperty) getProperty(PropertyDefinitions.PNAME_clientCertificateKeyStoreUrl)).setValue(value);
    }

    public String getTrustCertificateKeyStorePassword() {
        return ((StringConnectionProperty) getProperty(PropertyDefinitions.PNAME_trustCertificateKeyStorePassword)).getValueAsString();
    }

    public void setTrustCertificateKeyStorePassword(String value) {
        ((StringConnectionProperty) getProperty(PropertyDefinitions.PNAME_trustCertificateKeyStorePassword)).setValue(value);
    }

    public String getTrustCertificateKeyStoreType() {
        return ((StringConnectionProperty) getProperty(PropertyDefinitions.PNAME_trustCertificateKeyStoreType)).getValueAsString();
    }

    public void setTrustCertificateKeyStoreType(String value) {
        ((StringConnectionProperty) getProperty(PropertyDefinitions.PNAME_trustCertificateKeyStoreType)).setValue(value);
    }

    public String getTrustCertificateKeyStoreUrl() {
        return ((StringConnectionProperty) getProperty(PropertyDefinitions.PNAME_trustCertificateKeyStoreUrl)).getValueAsString();
    }

    public void setTrustCertificateKeyStoreUrl(String value) {
        ((StringConnectionProperty) getProperty(PropertyDefinitions.PNAME_trustCertificateKeyStoreUrl)).setValue(value);
    }

    public boolean getVerifyServerCertificate() {
        return ((BooleanConnectionProperty) getProperty(PropertyDefinitions.PNAME_verifyServerCertificate)).getValueAsBoolean();
    }

    public void setVerifyServerCertificate(boolean flag) {
        ((BooleanConnectionProperty) getProperty(PropertyDefinitions.PNAME_verifyServerCertificate)).setValue(flag);
    }

    public String getEnabledSSLCipherSuites() {
        return ((StringConnectionProperty) getProperty(PropertyDefinitions.PNAME_enabledSSLCipherSuites)).getValueAsString();
    }

    public void setEnabledSSLCipherSuites(String cipherSuites) {
        ((StringConnectionProperty) getProperty(PropertyDefinitions.PNAME_enabledSSLCipherSuites)).setValue(cipherSuites);
    }

    public boolean getUseUnbufferedInput() {
        return ((BooleanConnectionProperty) getProperty(PropertyDefinitions.PNAME_useUnbufferedInput)).getValueAsBoolean();
    }

    public String getProfilerEventHandler() {
        return ((StringConnectionProperty) getProperty(PropertyDefinitions.PNAME_profilerEventHandler)).getValueAsString();
    }

    public void setProfilerEventHandler(String handler) {
        ((StringConnectionProperty) getProperty(PropertyDefinitions.PNAME_profilerEventHandler)).setValue(handler);
    }
}
