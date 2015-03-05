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

package com.mysql.api.conf;

import com.mysql.api.ExceptionInterceptor;

public interface ConnectionProperties {

    public abstract boolean getParanoid();

    public abstract void setParanoid(boolean property);

    public abstract String getPasswordCharacterEncoding();

    public abstract void setPasswordCharacterEncoding(String characterSet);

    public ExceptionInterceptor getExceptionInterceptor();

    public abstract String getServerRSAPublicKeyFile();

    public abstract boolean getAllowPublicKeyRetrieval();

    public abstract boolean getUseUnicode();

    public abstract String getEncoding();

    public abstract String getClientCertificateKeyStorePassword();

    public abstract void setClientCertificateKeyStorePassword(String value);

    public abstract String getClientCertificateKeyStoreType();

    public abstract void setClientCertificateKeyStoreType(String value);

    public abstract String getClientCertificateKeyStoreUrl();

    public abstract void setClientCertificateKeyStoreUrl(String value);

    public abstract String getTrustCertificateKeyStorePassword();

    public abstract void setTrustCertificateKeyStorePassword(String value);

    public abstract String getTrustCertificateKeyStoreType();

    public abstract void setTrustCertificateKeyStoreType(String value);

    public abstract String getTrustCertificateKeyStoreUrl();

    public abstract void setTrustCertificateKeyStoreUrl(String value);

    public boolean getVerifyServerCertificate();

    public abstract void setVerifyServerCertificate(boolean flag);

    public abstract boolean getUseUnbufferedInput();

    public abstract String getProfilerEventHandler();

    public abstract void setProfilerEventHandler(String handler);

}
