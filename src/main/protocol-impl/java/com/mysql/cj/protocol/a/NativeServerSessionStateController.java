/*
 * Copyright (c) 2021, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package com.mysql.cj.protocol.a;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;

import com.mysql.cj.protocol.ServerSessionStateController;
import com.mysql.cj.protocol.a.NativeConstants.IntegerDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringLengthDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringSelfDataType;

public class NativeServerSessionStateController implements ServerSessionStateController {

    private NativeServerSessionStateChanges sessionStateChanges;
    private List<WeakReference<SessionStateChangesListener>> listeners;

    @Override
    public void setSessionStateChanges(ServerSessionStateChanges changes) {
        this.sessionStateChanges = (NativeServerSessionStateChanges) changes;
        if (this.listeners != null) {
            for (WeakReference<SessionStateChangesListener> wr : this.listeners) {
                SessionStateChangesListener l = wr.get();
                if (l != null) {
                    l.handleSessionStateChanges(changes);
                } else {
                    this.listeners.remove(wr);
                }
            }
        }
    }

    @Override
    public NativeServerSessionStateChanges getSessionStateChanges() {
        return this.sessionStateChanges;
    }

    @Override
    public void addSessionStateChangesListener(SessionStateChangesListener l) {
        if (this.listeners == null) {
            this.listeners = new ArrayList<>();
        }
        for (WeakReference<SessionStateChangesListener> wr : this.listeners) {
            if (l.equals(wr.get())) {
                return;
            }
        }
        this.listeners.add(new WeakReference<>(l));
    }

    @Override
    public void removeSessionStateChangesListener(SessionStateChangesListener listener) {
        if (this.listeners != null) {
            for (WeakReference<SessionStateChangesListener> wr : this.listeners) {
                SessionStateChangesListener l = wr.get();
                if (l == null || l.equals(listener)) {
                    this.listeners.remove(wr);
                    break;
                }
            }
        }
    }

    public static class NativeServerSessionStateChanges implements ServerSessionStateChanges {

        private List<SessionStateChange> sessionStateChanges = new ArrayList<>();

        @Override
        public List<SessionStateChange> getSessionStateChangesList() {
            return this.sessionStateChanges;
        }

        public NativeServerSessionStateChanges() {
        }

        public NativeServerSessionStateChanges init(NativePacketPayload buf, String encoding) {
            int totalLen = (int) buf.readInteger(IntegerDataType.INT_LENENC);
            int start = buf.getPosition();
            int end = start + totalLen;
            while (totalLen > 0 && end > start) {
                int type = (int) buf.readInteger(IntegerDataType.INT1);
                NativePacketPayload b = new NativePacketPayload(buf.readBytes(StringSelfDataType.STRING_LENENC));
                switch (type) {
                    case SESSION_TRACK_SYSTEM_VARIABLES:
                        this.sessionStateChanges.add(new SessionStateChange(type) //
                                .addValue(b.readString(StringSelfDataType.STRING_LENENC, encoding)) //
                                .addValue(b.readString(StringSelfDataType.STRING_LENENC, encoding)));
                        break;
                    case SESSION_TRACK_SCHEMA:
                    case SESSION_TRACK_TRANSACTION_CHARACTERISTICS:
                    case SESSION_TRACK_TRANSACTION_STATE:
                        this.sessionStateChanges.add(new SessionStateChange(type) //
                                .addValue(b.readString(StringSelfDataType.STRING_LENENC, encoding)));
                        break;
                    case SESSION_TRACK_GTIDS:
                        b.readInteger(IntegerDataType.INT1); // skip the byte reserved for the encoding specification, see WL#6128
                        this.sessionStateChanges.add(new SessionStateChange(type) //
                                .addValue(b.readString(StringSelfDataType.STRING_LENENC, encoding)));
                        break;
                    case SESSION_TRACK_STATE_CHANGE:
                    default:
                        // store the payload as it is
                        this.sessionStateChanges.add(new SessionStateChange(type) //
                                .addValue(b.readString(StringLengthDataType.STRING_FIXED, encoding, b.getPayloadLength())));
                }
                start = buf.getPosition();
            }
            return this;
        }

    }

}
