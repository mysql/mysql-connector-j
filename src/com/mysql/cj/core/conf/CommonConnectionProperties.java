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

import com.mysql.cj.api.conf.ConnectionProperties;
import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.exception.ExceptionInterceptor;

public class CommonConnectionProperties implements ConnectionProperties, Serializable {

    private static final long serialVersionUID = 3764240467061778179L;

    protected PropertySet propertySet = null;

    public CommonConnectionProperties() {
        this.propertySet = new DefaultPropertySet();
    }

    @Override
    public PropertySet getPropertySet() {
        return this.propertySet;
    }

    public boolean getParanoid() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_paranoid).getValue();
    }

    public void setParanoid(boolean property) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_paranoid).setValue(property);
    }

    public String getPasswordCharacterEncoding() {
        String encoding;
        if ((encoding = getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_passwordCharacterEncoding).getStringValue()) != null) {
            return encoding;
        }
        if ((encoding = getCharacterEncoding()) != null) {
            return encoding;
        }
        return "UTF-8";

    }

    public void setPasswordCharacterEncoding(String characterSet) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_passwordCharacterEncoding).setValue(characterSet);
    }

    public ExceptionInterceptor getExceptionInterceptor() {
        return null;
    }

    public String getServerRSAPublicKeyFile() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_serverRSAPublicKeyFile).getStringValue();
    }

    public boolean getAllowPublicKeyRetrieval() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_allowPublicKeyRetrieval).getValue();
    }

    public String getCharacterEncoding() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_characterEncoding).getValue();
    }

    public void setCharacterEncoding(String encoding) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_characterEncoding).setValue(encoding);
    }

    public String getClientCertificateKeyStorePassword() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_clientCertificateKeyStorePassword).getStringValue();
    }

    public void setClientCertificateKeyStorePassword(String value) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_clientCertificateKeyStorePassword).setValue(value);
    }

    public String getClientCertificateKeyStoreType() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_clientCertificateKeyStoreType).getStringValue();
    }

    public void setClientCertificateKeyStoreType(String value) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_clientCertificateKeyStoreType).setValue(value);
    }

    public String getClientCertificateKeyStoreUrl() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_clientCertificateKeyStoreUrl).getStringValue();
    }

    public void setClientCertificateKeyStoreUrl(String value) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_clientCertificateKeyStoreUrl).setValue(value);
    }

    public String getTrustCertificateKeyStorePassword() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_trustCertificateKeyStorePassword).getStringValue();
    }

    public void setTrustCertificateKeyStorePassword(String value) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_trustCertificateKeyStorePassword).setValue(value);
    }

    public String getTrustCertificateKeyStoreType() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_trustCertificateKeyStoreType).getStringValue();
    }

    public void setTrustCertificateKeyStoreType(String value) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_trustCertificateKeyStoreType).setValue(value);
    }

    public String getTrustCertificateKeyStoreUrl() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_trustCertificateKeyStoreUrl).getStringValue();
    }

    public void setTrustCertificateKeyStoreUrl(String value) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_trustCertificateKeyStoreUrl).setValue(value);
    }

    public boolean getVerifyServerCertificate() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_verifyServerCertificate).getValue();
    }

    public void setVerifyServerCertificate(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_verifyServerCertificate).setValue(flag);
    }

    public String getEnabledSSLCipherSuites() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_enabledSSLCipherSuites).getStringValue();
    }

    public void setEnabledSSLCipherSuites(String cipherSuites) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_enabledSSLCipherSuites).setValue(cipherSuites);
    }

    public boolean getUseUnbufferedInput() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useUnbufferedInput).getValue();
    }

    public String getProfilerEventHandler() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_profilerEventHandler).getStringValue();
    }

    public void setProfilerEventHandler(String handler) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_profilerEventHandler).setValue(handler);
    }
}
