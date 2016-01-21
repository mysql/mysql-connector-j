/*
  Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.core.exceptions;

import java.net.BindException;

import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.api.io.ServerSession;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.util.Util;

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
     * @param message
     * @param interceptor
     * @return
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
     * @param message
     * @param cause
     * @param interceptor
     * @return
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

    public static CJCommunicationsException createCommunicationsException(PropertySet propertySet, ServerSession serverSession, long lastPacketSentTimeMs,
            long lastPacketReceivedTimeMs, Throwable cause, ExceptionInterceptor interceptor) {
        CJCommunicationsException sqlEx = createException(CJCommunicationsException.class, null, cause, interceptor);
        sqlEx.init(propertySet, serverSession, lastPacketSentTimeMs, lastPacketReceivedTimeMs);

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
     * Creates a communications link failure message to be used
     * in CommunicationsException that (hopefully) has some better
     * information and suggestions based on heuristics.
     * 
     * @param conn
     * @param lastPacketSentTimeMs
     * @param underlyingException
     */
    public static String createLinkFailureMessageBasedOnHeuristics(PropertySet propertySet, ServerSession serverSession, long lastPacketSentTimeMs,
            long lastPacketReceivedTimeMs, Throwable underlyingException) {
        long serverTimeoutSeconds = 0;
        boolean isInteractiveClient = false;

        if (propertySet != null) {
            isInteractiveClient = propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_interactiveClient).getValue();

            String serverTimeoutSecondsStr = null;

            if (serverSession != null) {
                if (isInteractiveClient) {
                    serverTimeoutSecondsStr = serverSession.getServerVariable("interactive_timeout");
                } else {
                    serverTimeoutSecondsStr = serverSession.getServerVariable("wait_timeout");
                }
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

                if (!isInteractiveClient) {
                    timeoutMessageBuf.append(Messages.getString("CommunicationsException.3"));
                } else {
                    timeoutMessageBuf.append(Messages.getString("CommunicationsException.4"));
                }

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

            if (lastPacketReceivedTimeMs != 0) {
                Object[] timingInfo = { Long.valueOf(timeSinceLastPacketReceivedMs), Long.valueOf(timeSinceLastPacketSentMs) };
                exceptionMessageBuf.append(Messages.getString("CommunicationsException.ServerPacketTimingInfo", timingInfo));
            } else {
                exceptionMessageBuf.append(
                        Messages.getString("CommunicationsException.ServerPacketTimingInfoNoRecv", new Object[] { Long.valueOf(timeSinceLastPacketSentMs) }));
            }

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
                String localSocketAddress = propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_localSocketAddress).getValue();
                if (localSocketAddress != null && !Util.interfaceExists(localSocketAddress)) {
                    exceptionMessageBuf.append(Messages.getString("CommunicationsException.LocalSocketAddressNotAvailable"));
                } else {
                    // too many client connections???
                    exceptionMessageBuf.append(Messages.getString("CommunicationsException.TooManyClientConnections"));
                }
            }
        }

        if (exceptionMessageBuf.length() == 0) {
            // We haven't figured out a good reason, so copy it.
            exceptionMessageBuf.append(Messages.getString("CommunicationsException.20"));

            if (propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_maintainTimeStats).getValue()
                    && !propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_paranoid).getValue()) {
                exceptionMessageBuf.append("\n\n");
                if (lastPacketReceivedTimeMs != 0) {
                    Object[] timingInfo = { Long.valueOf(timeSinceLastPacketReceivedMs), Long.valueOf(timeSinceLastPacketSentMs) };
                    exceptionMessageBuf.append(Messages.getString("CommunicationsException.ServerPacketTimingInfo", timingInfo));
                } else {
                    exceptionMessageBuf.append(Messages.getString("CommunicationsException.ServerPacketTimingInfoNoRecv",
                            new Object[] { Long.valueOf(timeSinceLastPacketSentMs) }));
                }
            }
        }

        return exceptionMessageBuf.toString();
    }

}
