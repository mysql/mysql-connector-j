/*
 * Copyright (c) 2016, 2021, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.mysql.cj.protocol.a.result;

import com.mysql.cj.protocol.ProtocolEntity;
import com.mysql.cj.protocol.a.NativeConstants.IntegerDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringSelfDataType;
import com.mysql.cj.protocol.a.NativePacketPayload;
import com.mysql.cj.protocol.a.NativeServerSessionStateController;
import com.mysql.cj.protocol.a.NativeServerSessionStateController.NativeServerSessionStateChanges;

public class OkPacket implements ProtocolEntity {
    private long updateCount = -1;
    private long updateID = -1;
    private int statusFlags = 0;
    private int warningCount = 0;
    private String info = null;
    private NativeServerSessionStateChanges sessionStateChanges = new NativeServerSessionStateChanges();

    public OkPacket() {
    }

    public static OkPacket parse(NativePacketPayload buf, String errorMessageEncoding) {
        OkPacket ok = new OkPacket();

        buf.setPosition(1); // skips the 'last packet' flag (packet signature)

        // read OK packet
        ok.setUpdateCount(buf.readInteger(IntegerDataType.INT_LENENC)); // affected_rows
        ok.setUpdateID(buf.readInteger(IntegerDataType.INT_LENENC)); // last_insert_id
        ok.setStatusFlags((int) buf.readInteger(IntegerDataType.INT2));
        ok.setWarningCount((int) buf.readInteger(IntegerDataType.INT2));
        ok.setInfo(buf.readString(StringSelfDataType.STRING_TERM, errorMessageEncoding)); // info

        // read session state changes info
        if ((ok.getStatusFlags() & NativeServerSessionStateController.SERVER_SESSION_STATE_CHANGED) > 0) {
            ok.sessionStateChanges.init(buf, errorMessageEncoding);
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

    public NativeServerSessionStateChanges getSessionStateChanges() {
        return this.sessionStateChanges;
    }

}