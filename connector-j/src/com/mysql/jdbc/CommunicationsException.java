/*
 Copyright (C) 2002-2004 MySQL AB

 This program is free software; you can redistribute it and/or modify
 it under the terms of version 2 of the GNU General Public License as 
 published by the Free Software Foundation.

 There are special exceptions to the terms and conditions of the GPL 
 as it is applied to this software. View the full text of the 
 exception in file EXCEPTIONS-CONNECTOR-J in the directory of this 
 software distribution.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA



 */
package com.mysql.jdbc;

import java.net.BindException;

import java.sql.SQLException;

/**
 * An exception to represent communications errors with the database.
 * 
 * Attempts to provide 'friendler' error messages to end-users, including last
 * time a packet was sent to the database, what the client-timeout is set to,
 * and whether the idle time has been exceeded.
 * 
 * @author Mark Matthews
 * 
 * @version $Id: CommunicationsException.java,v 1.1.2.1 2005/05/13 18:58:37
 *          mmatthews Exp $
 */
public class CommunicationsException extends SQLException {

	private static final long DEFAULT_WAIT_TIMEOUT_SECONDS = 28800;

	private static final int DUE_TO_TIMEOUT_FALSE = 0;

	private static final int DUE_TO_TIMEOUT_MAYBE = 2;

	private static final int DUE_TO_TIMEOUT_TRUE = 1;

	private String exceptionMessage;

	public CommunicationsException(Connection conn, long lastPacketSentTimeMs,
			Exception underlyingException) {

		long serverTimeoutSeconds = 0;
		boolean isInteractiveClient = false;

		if (conn != null) {
			isInteractiveClient = conn.getInteractiveClient();

			String serverTimeoutSecondsStr = null;

			if (isInteractiveClient) {
				serverTimeoutSecondsStr = conn
						.getServerVariable("interactive_timeout"); //$NON-NLS-1$
			} else {
				serverTimeoutSecondsStr = conn
						.getServerVariable("wait_timeout"); //$NON-NLS-1$
			}

			if (serverTimeoutSecondsStr != null) {
				try {
					serverTimeoutSeconds = Long
							.parseLong(serverTimeoutSecondsStr);
				} catch (NumberFormatException nfe) {
					serverTimeoutSeconds = 0;
				}
			}
		}

		StringBuffer exceptionMessageBuf = new StringBuffer();

		if (lastPacketSentTimeMs == 0) {
			lastPacketSentTimeMs = System.currentTimeMillis();
		}

		long timeSinceLastPacket = (System.currentTimeMillis() - lastPacketSentTimeMs) / 1000;

		int dueToTimeout = DUE_TO_TIMEOUT_FALSE;

		StringBuffer timeoutMessageBuf = null;

		if (serverTimeoutSeconds != 0) {
			if (timeSinceLastPacket > serverTimeoutSeconds) {
				dueToTimeout = DUE_TO_TIMEOUT_TRUE;

				timeoutMessageBuf = new StringBuffer();

				timeoutMessageBuf.append(Messages
						.getString("CommunicationsException.2")); //$NON-NLS-1$

				if (!isInteractiveClient) {
					timeoutMessageBuf.append(Messages
							.getString("CommunicationsException.3")); //$NON-NLS-1$
				} else {
					timeoutMessageBuf.append(Messages
							.getString("CommunicationsException.4")); //$NON-NLS-1$
				}

			}
		} else if (timeSinceLastPacket > DEFAULT_WAIT_TIMEOUT_SECONDS) {
			dueToTimeout = DUE_TO_TIMEOUT_MAYBE;

			timeoutMessageBuf = new StringBuffer();

			timeoutMessageBuf.append(Messages
					.getString("CommunicationsException.5")); //$NON-NLS-1$
			timeoutMessageBuf.append(Messages
					.getString("CommunicationsException.6")); //$NON-NLS-1$
			timeoutMessageBuf.append(Messages
					.getString("CommunicationsException.7")); //$NON-NLS-1$
			timeoutMessageBuf.append(Messages
					.getString("CommunicationsException.8")); //$NON-NLS-1$
		}

		if (dueToTimeout == DUE_TO_TIMEOUT_TRUE
				|| dueToTimeout == DUE_TO_TIMEOUT_MAYBE) {

			exceptionMessageBuf.append(Messages
					.getString("CommunicationsException.9")); //$NON-NLS-1$
			exceptionMessageBuf.append(timeSinceLastPacket);
			exceptionMessageBuf.append(Messages
					.getString("CommunicationsException.10")); //$NON-NLS-1$

			if (timeoutMessageBuf != null) {
				exceptionMessageBuf.append(timeoutMessageBuf);
			}

			exceptionMessageBuf.append(Messages
					.getString("CommunicationsException.11")); //$NON-NLS-1$
			exceptionMessageBuf.append(Messages
					.getString("CommunicationsException.12")); //$NON-NLS-1$
			exceptionMessageBuf.append(Messages
					.getString("CommunicationsException.13")); //$NON-NLS-1$

		} else {
			//
			// Attempt to determine the reason for the underlying exception
			// (we can only make a best-guess here)
			//

			if (underlyingException instanceof BindException) {
				// too many client connections???
				exceptionMessageBuf.append(Messages
						.getString("CommunicationsException.14")); //$NON-NLS-1$
				exceptionMessageBuf.append(Messages
						.getString("CommunicationsException.15")); //$NON-NLS-1$
				exceptionMessageBuf.append(Messages
						.getString("CommunicationsException.16")); //$NON-NLS-1$
				exceptionMessageBuf.append(Messages
						.getString("CommunicationsException.17")); //$NON-NLS-1$
				exceptionMessageBuf.append(Messages
						.getString("CommunicationsException.18")); //$NON-NLS-1$
				exceptionMessageBuf.append(Messages
						.getString("CommunicationsException.19")); //$NON-NLS-1$
			}
		}

		if (exceptionMessageBuf.length() == 0) {
			// We haven't figured out a good reason, so copy it.
			exceptionMessageBuf.append(Messages
					.getString("CommunicationsException.20")); //$NON-NLS-1$

			if (underlyingException != null) {
				exceptionMessageBuf.append(Messages
						.getString("CommunicationsException.21")); //$NON-NLS-1$
				exceptionMessageBuf.append(Util
						.stackTraceToString(underlyingException));
			}
			
			if (conn != null && conn.getMaintainTimeStats() && 
					!conn.getParanoid()) {
				exceptionMessageBuf.append("\n\nLast packet sent to the server was ");
				exceptionMessageBuf.append(System.currentTimeMillis() - lastPacketSentTimeMs);
				exceptionMessageBuf.append(" ms ago.");
			}
		}

		this.exceptionMessage = exceptionMessageBuf.toString();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Throwable#getMessage()
	 */
	public String getMessage() {
		return this.exceptionMessage;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.SQLException#getSQLState()
	 */
	public String getSQLState() {
		return SQLError.SQL_STATE_COMMUNICATION_LINK_FAILURE;
	}

}
