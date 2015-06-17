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

package com.mysql.cj.api.conf;

import com.mysql.cj.api.exception.ExceptionInterceptor;

public interface ConnectionProperties {

    PropertySet getPropertySet();

    boolean getParanoid();

    void setParanoid(boolean property);

    void setPasswordCharacterEncoding(String characterSet);

    ExceptionInterceptor getExceptionInterceptor();

    String getServerRSAPublicKeyFile();

    boolean getAllowPublicKeyRetrieval();

    String getCharacterEncoding();

    /**
     * @param encoding
     *            The characterEncoding to set.
     */
    void setCharacterEncoding(String encoding);

    String getClientCertificateKeyStorePassword();

    void setClientCertificateKeyStorePassword(String value);

    String getClientCertificateKeyStoreType();

    void setClientCertificateKeyStoreType(String value);

    String getClientCertificateKeyStoreUrl();

    void setClientCertificateKeyStoreUrl(String value);

    String getTrustCertificateKeyStorePassword();

    void setTrustCertificateKeyStorePassword(String value);

    String getTrustCertificateKeyStoreType();

    void setTrustCertificateKeyStoreType(String value);

    String getTrustCertificateKeyStoreUrl();

    void setTrustCertificateKeyStoreUrl(String value);

    boolean getVerifyServerCertificate();

    void setVerifyServerCertificate(boolean flag);

    String getEnabledSSLCipherSuites();

    void setEnabledSSLCipherSuites(String cipherSuites);

    boolean getUseUnbufferedInput();

    String getProfilerEventHandler();

    void setProfilerEventHandler(String handler);
}
