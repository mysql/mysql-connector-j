/*
  Copyright (c) 2017, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.api;

import java.io.InputStream;

import com.mysql.cj.core.MysqlType;

public interface BindValue {

    BindValue clone();

    void reset();

    boolean isNull();

    void setNull(boolean isNull);

    boolean isStream();

    void setIsStream(boolean isStream);

    MysqlType getParameterType();

    void setParameterType(MysqlType type);

    byte[] getParameterValue();

    void setParameterValue(byte[] parameterValue);

    InputStream getParameterStream();

    void setParameterStream(InputStream parameterStream, int streamLength);

    int getStreamLength();

    boolean isSet();
}
