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

package com.mysql.cj.mysqla;

import com.mysql.cj.api.BindValue;
import com.mysql.cj.api.QueryBindings;

public class ServerPreparedQueryBindings extends ClientPreparedQueryBindings implements QueryBindings {

    /** Bind values for individual fields */
    private ServerPreparedQueryBindValue[] bindValues;

    public ServerPreparedQueryBindings(int parameterCount) {
        super(parameterCount);
    }

    @Override
    protected void initBindValues(int parameterCount) {
        this.bindValues = new ServerPreparedQueryBindValue[parameterCount];
        for (int i = 0; i < parameterCount; i++) {
            this.bindValues[i] = new ServerPreparedQueryBindValue();
        }
    }

    @Override
    public ServerPreparedQueryBindings clone() {
        ServerPreparedQueryBindings newBindings = new ServerPreparedQueryBindings(this.bindValues.length);
        ServerPreparedQueryBindValue[] bvs = new ServerPreparedQueryBindValue[this.bindValues.length];
        for (int i = 0; i < this.bindValues.length; i++) {
            bvs[i] = this.bindValues[i].clone();
        }
        newBindings.setBindValues(bvs);
        return newBindings;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends BindValue> T[] getBindValues() {
        return (T[]) this.bindValues;
    }

    @Override
    public <T extends BindValue> void setBindValues(T[] bindValues) {
        this.bindValues = (ServerPreparedQueryBindValue[]) bindValues;
    }

}
