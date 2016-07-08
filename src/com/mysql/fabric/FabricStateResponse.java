/*
  Copyright (c) 2013, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.fabric;

import java.util.concurrent.TimeUnit;

public class FabricStateResponse<T> {
    private T data;
    private int secsTtl;
    private long expireTimeMillis;

    public FabricStateResponse(T data, int secsTtl) {
        this.data = data;
        this.secsTtl = secsTtl;
        this.expireTimeMillis = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(secsTtl);
    }

    public FabricStateResponse(T data, int secsTtl, long presetExpireTimeMillis) {
        this.data = data;
        this.secsTtl = secsTtl;
        this.expireTimeMillis = presetExpireTimeMillis;
    }

    public T getData() {
        return this.data;
    }

    public int getTtl() {
        return this.secsTtl;
    }

    /**
     * The expiration time of this data. Should be compared to {@link System.currentTimeMillis()}.
     */
    public long getExpireTimeMillis() {
        return this.expireTimeMillis;
    }
}
