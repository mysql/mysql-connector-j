/*
 * Copyright (c) 2015, 2023, Oracle and/or its affiliates.
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

package com.mysql.cj.exceptions;

import java.net.BindException;
import java.net.NetworkInterface;
import java.net.SocketException;

import com.mysql.cj.Messages;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.protocol.PacketReceivedTimeHolder;
import com.mysql.cj.protocol.PacketSentTimeHolder;
import com.mysql.cj.protocol.ServerSession;

public class ExceptionFactory {

    private static final long DEFAULT_WAIT_TIMEOUT_SECONDS = 28800;

    private static final int DUE_TO_TIMEOUT_FALSE = 0;

    private static final int DUE_TO_TIMEOUT_MAYBE = 2;

    private static final int DUE_TO_TIMEOUT_TRUE = 1;

    public static CJException createException(String message) {
        return createException(CJException.class, message);
    }

    @SuppressWarnings("unchecked")
    public static <T extends CJException> T createException(Class<T> clazz, String message) {

        T sqlEx;
        try {
            sqlEx = clazz.getConstructor(String.class).newInstance(message);
        } catch (Throwable e) {
            sqlEx = (T) new CJException(message);
        }
        return sqlEx;
    }

    public static CJException createException(String message, ExceptionInterceptor interceptor) {
        return createException(CJException.class, message, interceptor);
    }

    /**
     * 
     * @param clazz
     *            exception class
     * @param message
     *            message
     * @param interceptor
     *            exception interceptor
     * @param <T>
     *            {@link CJException}
     * @return {@link CJException} instance
     */
    public static <T extends CJException> T createException(Class<T> clazz, String message, ExceptionInterceptor interceptor) {
        T sqlEx = createException(clazz, message);

        // TODO: Decide whether we need to intercept exceptions at this level
        //if (interceptor != null) {
        //    @SuppressWarnings("unchecked")
        //    T interceptedEx = (T) interceptor.interceptException(sqlEx, null);
        //    if (interceptedEx != null) {
        //        return interceptedEx;
        //    }
        //}

        return sqlEx;
    }

    public static CJException createException(String message, Throwable cause) {
        return createException(CJException.class, message, cause);
    }

    public static <T extends CJException> T createException(Class<T> clazz, String message, Throwable cause) {
        T sqlEx = createException(clazz, message);
        if (cause != null) {
            try {
                sqlEx.initCause(cause);
            } catch (Throwable t) {
                // we're not going to muck with that here, since it's an error condition anyway!
            }

            if (cause instanceof CJException) {
                sqlEx.setSQLState(((CJException) cause).getSQLState());
                sqlEx.setVendorCode(((CJException) cause).getVendorCode());
                sqlEx.setTransient(((CJException) cause).isTransient());
            }
        }
        return sqlEx;
    }

    public static CJException createException(String message, Throwable cause, ExceptionInterceptor interceptor) {
        return createException(CJException.class, message, cause, interceptor);
    }

    public static CJException createException(String message, String sqlState, int vendorErrorCode, boolean isTransient, Throwable cause,
            ExceptionInterceptor interceptor) {
        CJException ex = createException(CJException.class, message, cause, interceptor);
        ex.setSQLState(sqlState);
        ex.setVendorCode(vendorErrorCode);
        ex.setTransient(isTransient);
        return ex;
    }

    /**
     * 
     * @param clazz
     *            exception class
     * @param message
     *            message
     * @param cause
     *            exception caused this one
     * @param interceptor
     *            exception interceptor
     * @param <T>
     *            {@link CJException}
     * @return {@link CJException} instance
     */
    public static <T extends CJException> T createException(Class<T> clazz, String message, Throwable cause, ExceptionInterceptor interceptor) {
        T sqlEx = createException(clazz, message, cause);

        // TODO: Decide whether we need to intercept exceptions at this level
        //if (interceptor != null) {
        //    @SuppressWarnings("unchecked")
        //    T interceptedEx = (T) interceptor.interceptException(sqlEx, null);
        //    if (interceptedEx != null) {
        //        return interceptedEx;
        //    }
        //}

        return sqlEx;
    }

    public static CJCommunicationsException createCommunicationsException(PropertySet propertySet, ServerSession serverSession,
            PacketSentTimeHolder packetSentTimeHolder, PacketReceivedTimeHolder packetReceivedTimeHolder, Throwable cause, ExceptionInterceptor interceptor) {
        CJCommunicationsException sqlEx = createException(CJCommunicationsException.class, null, cause, interceptor);
        sqlEx.init(propertySet, serverSession, packetSentTimeHolder, packetReceivedTimeHolder);

        // TODO: Decide whether we need to intercept exceptions at this level
        //if (interceptor != null) {
        //    @SuppressWarnings("unchecked")
        //    T interceptedEx = (T) interceptor.interceptException(sqlEx, null);
        //    if (interceptedEx != null) {
        //        return interceptedEx;
        //    }
        //}

        return sqlEx;
    }

    /**
     * Creates a communications link failure message to be used in CommunicationsException
     * that (hopefully) has some better information and suggestions based on heuristics.
     * 
     * @param propertySet
     *            property set
     * @param serverSession
     *            server session
     * @param packetSentTimeHolder
     *            packetSentTimeHolder
     * @param packetReceivedTimeHolder
     *            packetReceivedTimeHolder
     * @param underlyingException
     *            underlyingException
     * @return message
     */
    public static String createLinkFailureMessageBasedOnHeuristics(PropertySet propertySet, ServerSession serverSession,
            PacketSentTimeHolder packetSentTimeHolder, PacketReceivedTimeHolder packetReceivedTimeHolder, Throwable underlyingException) {
        long serverTimeoutSeconds = 0;
        boolean isInteractiveClient = false;

        long lastPacketReceivedTimeMs = packetReceivedTimeHolder == null ? 0L : packetReceivedTimeHolder.getLastPacketReceivedTime();
        long lastPacketSentTimeMs = packetSentTimeHolder.getLastPacketSentTime();
        if (lastPacketSentTimeMs > lastPacketReceivedTimeMs) {
            lastPacketSentTimeMs = packetSentTimeHolder.getPreviousPacketSentTime();
        }

        if (propertySet != null) {
            isInteractiveClient = propertySet.getBooleanProperty(PropertyKey.interactiveClient).getValue();

            String serverTimeoutSecondsStr = null;

            if (serverSession != null) {
                serverTimeoutSecondsStr = isInteractiveClient ? serverSession.getServerVariable("interactive_timeout")
                        : serverSession.getServerVariable("wait_timeout");
            }

            if (serverTimeoutSecondsStr != null) {
                try {
                    serverTimeoutSeconds = Long.parseLong(serverTimeoutSecondsStr);
                } catch (NumberFormatException nfe) {
                    serverTimeoutSeconds = 0;
                }
            }
        }

        StringBuilder exceptionMessageBuf = new StringBuilder();

        long nowMs = System.currentTimeMillis();

        if (lastPacketSentTimeMs == 0) {
            lastPacketSentTimeMs = nowMs;
        }

        long timeSinceLastPacketSentMs = (nowMs - lastPacketSentTimeMs);
        long timeSinceLastPacketSeconds = timeSinceLastPacketSentMs / 1000;

        long timeSinceLastPacketReceivedMs = (nowMs - lastPacketReceivedTimeMs);

        int dueToTimeout = DUE_TO_TIMEOUT_FALSE;

        StringBuilder timeoutMessageBuf = null;

        if (serverTimeoutSeconds != 0) {
            if (timeSinceLastPacketSeconds > serverTimeoutSeconds) {
                dueToTimeout = DUE_TO_TIMEOUT_TRUE;

                timeoutMessageBuf = new StringBuilder();
                timeoutMessageBuf.append(Messages.getString("CommunicationsException.2"));
                timeoutMessageBuf.append(Messages.getString(isInteractiveClient ? "CommunicationsException.4" : "CommunicationsException.3"));
            }

        } else if (timeSinceLastPacketSeconds > DEFAULT_WAIT_TIMEOUT_SECONDS) {
            dueToTimeout = DUE_TO_TIMEOUT_MAYBE;

            timeoutMessageBuf = new StringBuilder();
            timeoutMessageBuf.append(Messages.getString("CommunicationsException.5"));
            timeoutMessageBuf.append(Messages.getString("CommunicationsException.6"));
            timeoutMessageBuf.append(Messages.getString("CommunicationsException.7"));
            timeoutMessageBuf.append(Messages.getString("CommunicationsException.8"));
        }

        if (dueToTimeout == DUE_TO_TIMEOUT_TRUE || dueToTimeout == DUE_TO_TIMEOUT_MAYBE) {

            exceptionMessageBuf.append(lastPacketReceivedTimeMs != 0
                    ? Messages.getString("CommunicationsException.ServerPacketTimingInfo",
                            new Object[] { Long.valueOf(timeSinceLastPacketReceivedMs), Long.valueOf(timeSinceLastPacketSentMs) })
                    : Messages.getString("CommunicationsException.ServerPacketTimingInfoNoRecv", new Object[] { Long.valueOf(timeSinceLastPacketSentMs) }));

            if (timeoutMessageBuf != null) {
                exceptionMessageBuf.append(timeoutMessageBuf);
            }

            exceptionMessageBuf.append(Messages.getString("CommunicationsException.11"));
            exceptionMessageBuf.append(Messages.getString("CommunicationsException.12"));
            exceptionMessageBuf.append(Messages.getString("CommunicationsException.13"));

        } else {
            //
            // Attempt to determine the reason for the underlying exception (we can only make a best-guess here)
            //
            if (underlyingException instanceof BindException) {
                String localSocketAddress = propertySet.getStringProperty(PropertyKey.localSocketAddress).getValue();
                boolean interfaceNotAvaliable;
                try {
                    interfaceNotAvaliable = localSocketAddress != null && NetworkInterface.getByName(localSocketAddress) == null;
                } catch (SocketException e1) {
                    interfaceNotAvaliable = false;
                }
                exceptionMessageBuf.append(interfaceNotAvaliable ? Messages.getString("CommunicationsException.LocalSocketAddressNotAvailable")
                        : Messages.getString("CommunicationsException.TooManyClientConnections"));
            }
        }

        if (exceptionMessageBuf.length() == 0) {
            // We haven't figured out a good reason, so copy it.
            exceptionMessageBuf.append(Messages.getString("CommunicationsException.20"));

            if (propertySet.getBooleanProperty(PropertyKey.maintainTimeStats).getValue() && !propertySet.getBooleanProperty(PropertyKey.paranoid).getValue()) {
                exceptionMessageBuf.append("\n\n");
                exceptionMessageBuf.append(lastPacketReceivedTimeMs != 0
                        ? Messages.getString("CommunicationsException.ServerPacketTimingInfo",
                                new Object[] { Long.valueOf(timeSinceLastPacketReceivedMs), Long.valueOf(timeSinceLastPacketSentMs) })
                        : Messages.getString("CommunicationsException.ServerPacketTimingInfoNoRecv", new Object[] { Long.valueOf(timeSinceLastPacketSentMs) }));
            }
        }

        return exceptionMessageBuf.toString();
    }
}
