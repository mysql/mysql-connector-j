/*
  Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.mysqla.result;

import com.mysql.cj.api.mysqla.io.NativeProtocol.IntegerDataType;
import com.mysql.cj.api.mysqla.io.NativeProtocol.StringSelfDataType;
import com.mysql.cj.api.mysqla.io.PacketPayload;
import com.mysql.cj.api.mysqla.result.ProtocolEntity;

public class OkPacket implements ProtocolEntity {

    private long updateCount = -1;
    private long updateID = -1;
    private int statusFlags = 0;
    private int warningCount = 0;
    private String info = null;

    public OkPacket() {
    }

    public static OkPacket parse(PacketPayload buf, boolean isReadInfoMsgEnabled, String errorMessageEncoding) {
        OkPacket ok = new OkPacket();

        buf.setPosition(1); // skips the 'last packet' flag (packet signature)

        // read OK packet
        ok.setUpdateCount(buf.readInteger(IntegerDataType.INT_LENENC)); // affected_rows
        ok.setUpdateID(buf.readInteger(IntegerDataType.INT_LENENC)); // last_insert_id
        ok.setStatusFlags((int) buf.readInteger(IntegerDataType.INT2));
        ok.setWarningCount((int) buf.readInteger(IntegerDataType.INT2));
        if (isReadInfoMsgEnabled) {
            ok.setInfo(buf.readString(StringSelfDataType.STRING_TERM, errorMessageEncoding)); // info
        }
        return ok;
    }

    public long getUpdateCount() {
        return this.updateCount;
    }

    public void setUpdateCount(long updateCount) {
        this.updateCount = updateCount;
    }

    public long getUpdateID() {
        return this.updateID;
    }

    public void setUpdateID(long updateID) {
        this.updateID = updateID;
    }

    public String getInfo() {
        return this.info;
    }

    public void setInfo(String info) {
        this.info = info;
    }

    public int getStatusFlags() {
        return this.statusFlags;
    }

    public void setStatusFlags(int statusFlags) {
        this.statusFlags = statusFlags;
    }

    public int getWarningCount() {
        return this.warningCount;
    }

    public void setWarningCount(int warningCount) {
        this.warningCount = warningCount;
    }
}
