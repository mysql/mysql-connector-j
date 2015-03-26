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

import java.util.ArrayList;

import com.mysql.cj.api.ExceptionInterceptor;
import com.mysql.cj.api.conf.ConnectionProperties;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.profiler.LoggingProfilerEventHandler;

public class CommonConnectionProperties implements ConnectionProperties {

    protected static final String CONNECTION_AND_AUTH_CATEGORY = Messages.getString("ConnectionProperties.categoryConnectionAuthentication");

    protected static final String NETWORK_CATEGORY = Messages.getString("ConnectionProperties.categoryNetworking");

    protected static final String SECURITY_CATEGORY = Messages.getString("ConnectionProperties.categorySecurity");

    protected static final String DEBUGING_PROFILING_CATEGORY = Messages.getString("ConnectionProperties.categoryDebuggingProfiling");

    protected static final String HA_CATEGORY = Messages.getString("ConnectionProperties.categorryHA");

    protected static final String MISC_CATEGORY = Messages.getString("ConnectionProperties.categoryMisc");

    protected static final String PERFORMANCE_CATEGORY = Messages.getString("ConnectionProperties.categoryPerformance");

    protected static final String[] PROPERTY_CATEGORIES = new String[] { CONNECTION_AND_AUTH_CATEGORY, NETWORK_CATEGORY, HA_CATEGORY, SECURITY_CATEGORY,
            PERFORMANCE_CATEGORY, DEBUGING_PROFILING_CATEGORY, MISC_CATEGORY };

    protected static final ArrayList<java.lang.reflect.Field> PROPERTY_LIST = new ArrayList<java.lang.reflect.Field>();

    protected boolean useUnicodeAsBoolean = true;

    protected String characterEncodingAsString = null;

    protected BooleanConnectionProperty paranoid = new BooleanConnectionProperty("paranoid", false, Messages.getString("ConnectionProperties.paranoid"),
            "3.0.1", SECURITY_CATEGORY, Integer.MIN_VALUE);

    protected StringConnectionProperty passwordCharacterEncoding = new StringConnectionProperty("passwordCharacterEncoding", null,
            Messages.getString("ConnectionProperties.passwordCharacterEncoding"), "5.1.7", SECURITY_CATEGORY, Integer.MIN_VALUE);

    protected StringConnectionProperty serverRSAPublicKeyFile = new StringConnectionProperty("serverRSAPublicKeyFile", null,
            Messages.getString("ConnectionProperties.serverRSAPublicKeyFile"), "5.1.31", SECURITY_CATEGORY, Integer.MIN_VALUE);

    protected BooleanConnectionProperty allowPublicKeyRetrieval = new BooleanConnectionProperty("allowPublicKeyRetrieval", false,
            Messages.getString("ConnectionProperties.allowPublicKeyRetrieval"), "5.1.31", SECURITY_CATEGORY, Integer.MIN_VALUE);

    // SSL Options

    protected StringConnectionProperty clientCertificateKeyStoreUrl = new StringConnectionProperty("clientCertificateKeyStoreUrl", null,
            Messages.getString("ConnectionProperties.clientCertificateKeyStoreUrl"), "5.1.0", SECURITY_CATEGORY, 5);

    protected StringConnectionProperty trustCertificateKeyStoreUrl = new StringConnectionProperty("trustCertificateKeyStoreUrl", null,
            Messages.getString("ConnectionProperties.trustCertificateKeyStoreUrl"), "5.1.0", SECURITY_CATEGORY, 8);

    protected StringConnectionProperty clientCertificateKeyStoreType = new StringConnectionProperty("clientCertificateKeyStoreType", "JKS",
            Messages.getString("ConnectionProperties.clientCertificateKeyStoreType"), "5.1.0", SECURITY_CATEGORY, 6);

    protected StringConnectionProperty clientCertificateKeyStorePassword = new StringConnectionProperty("clientCertificateKeyStorePassword", null,
            Messages.getString("ConnectionProperties.clientCertificateKeyStorePassword"), "5.1.0", SECURITY_CATEGORY, 7);

    protected StringConnectionProperty trustCertificateKeyStoreType = new StringConnectionProperty("trustCertificateKeyStoreType", "JKS",
            Messages.getString("ConnectionProperties.trustCertificateKeyStoreType"), "5.1.0", SECURITY_CATEGORY, 9);

    protected StringConnectionProperty trustCertificateKeyStorePassword = new StringConnectionProperty("trustCertificateKeyStorePassword", null,
            Messages.getString("ConnectionProperties.trustCertificateKeyStorePassword"), "5.1.0", SECURITY_CATEGORY, 10);

    protected BooleanConnectionProperty verifyServerCertificate = new BooleanConnectionProperty("verifyServerCertificate", true,
            Messages.getString("ConnectionProperties.verifyServerCertificate"), "5.1.6", SECURITY_CATEGORY, 4);

    protected StringConnectionProperty enabledSSLCipherSuites = new StringConnectionProperty("enabledSSLCipherSuites", null,
            Messages.getString("ConnectionProperties.enabledSSLCipherSuites"), "5.1.35", SECURITY_CATEGORY, 11);

    protected BooleanConnectionProperty useUnbufferedInput = new BooleanConnectionProperty("useUnbufferedInput", true,
            Messages.getString("ConnectionProperties.useUnbufferedInput"), "3.0.11", MISC_CATEGORY, Integer.MIN_VALUE);

    protected StringConnectionProperty profilerEventHandler = new StringConnectionProperty("profilerEventHandler", LoggingProfilerEventHandler.class.getName(),
            Messages.getString("ConnectionProperties.profilerEventHandler"), "5.1.6", DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE);

    public boolean getParanoid() {
        return this.paranoid.getValueAsBoolean();
    }

    public void setParanoid(boolean property) {
        this.paranoid.setValue(property);
    }

    public String getPasswordCharacterEncoding() {
        return this.passwordCharacterEncoding.getValueAsString();
    }

    public void setPasswordCharacterEncoding(String characterSet) {
        this.passwordCharacterEncoding.setValue(characterSet);
    }

    public ExceptionInterceptor getExceptionInterceptor() {
        return null;
    }

    public String getServerRSAPublicKeyFile() {
        return this.serverRSAPublicKeyFile.getValueAsString();
    }

    public boolean getAllowPublicKeyRetrieval() {
        return this.allowPublicKeyRetrieval.getValueAsBoolean();
    }

    public String getEncoding() {
        return this.characterEncodingAsString;
    }

    public boolean getUseUnicode() {
        return this.useUnicodeAsBoolean;
    }

    public String getClientCertificateKeyStorePassword() {
        return this.clientCertificateKeyStorePassword.getValueAsString();
    }

    public void setClientCertificateKeyStorePassword(String value) {
        this.clientCertificateKeyStorePassword.setValue(value);
    }

    public String getClientCertificateKeyStoreType() {
        return this.clientCertificateKeyStoreType.getValueAsString();
    }

    public void setClientCertificateKeyStoreType(String value) {
        this.clientCertificateKeyStoreType.setValue(value);
    }

    public String getClientCertificateKeyStoreUrl() {
        return this.clientCertificateKeyStoreUrl.getValueAsString();
    }

    public void setClientCertificateKeyStoreUrl(String value) {
        this.clientCertificateKeyStoreUrl.setValue(value);
    }

    public String getTrustCertificateKeyStorePassword() {
        return this.trustCertificateKeyStorePassword.getValueAsString();
    }

    public void setTrustCertificateKeyStorePassword(String value) {
        this.trustCertificateKeyStorePassword.setValue(value);
    }

    public String getTrustCertificateKeyStoreType() {
        return this.trustCertificateKeyStoreType.getValueAsString();
    }

    public void setTrustCertificateKeyStoreType(String value) {
        this.trustCertificateKeyStoreType.setValue(value);
    }

    public String getTrustCertificateKeyStoreUrl() {
        return this.trustCertificateKeyStoreUrl.getValueAsString();
    }

    public void setTrustCertificateKeyStoreUrl(String value) {
        this.trustCertificateKeyStoreUrl.setValue(value);
    }

    public boolean getVerifyServerCertificate() {
        return this.verifyServerCertificate.getValueAsBoolean();
    }

    public void setVerifyServerCertificate(boolean flag) {
        this.verifyServerCertificate.setValue(flag);
    }

    public String getEnabledSSLCipherSuites() {
        return this.enabledSSLCipherSuites.getValueAsString();
    }

    public void setEnabledSSLCipherSuites(String cipherSuites) {
        this.enabledSSLCipherSuites.setValue(cipherSuites);
    }

    public boolean getUseUnbufferedInput() {
        return this.useUnbufferedInput.getValueAsBoolean();
    }

    public String getProfilerEventHandler() {
        return this.profilerEventHandler.getValueAsString();
    }

    public void setProfilerEventHandler(String handler) {
        this.profilerEventHandler.setValue(handler);
    }
}
