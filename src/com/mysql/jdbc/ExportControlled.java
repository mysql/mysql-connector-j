/*
  Copyright (c) 2002, 2014, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.jdbc;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.sql.SQLException;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

/**
 * Holds functionality that falls under export-control regulations.
 * 
 * @author Mark Matthews
 * 
 * @version $Id: ExportControlled.java,v 1.1.2.1 2005/05/13 18:58:38 mmatthews
 *          Exp $
 */
public class ExportControlled {
	private static final String SQL_STATE_BAD_SSL_PARAMS = "08000";

	protected static boolean enabled() {
		// we may wish to un-static-ify this class
		// this static method call may be removed entirely by the compiler
		return true;
	}

	/**
	 * Converts the socket being used in the given MysqlIO to an SSLSocket by
	 * performing the SSL/TLS handshake.
	 * 
	 * @param mysqlIO
	 *            the MysqlIO instance containing the socket to convert to an
	 *            SSLSocket.
	 * 
	 * @throws CommunicationsException
	 *             if the handshake fails, or if this distribution of
	 *             Connector/J doesn't contain the SSL crytpo hooks needed to
	 *             perform the handshake.
	 */
	protected static void transformSocketToSSLSocket(MysqlIO mysqlIO)
			throws SQLException {
		javax.net.ssl.SSLSocketFactory sslFact = getSSLSocketFactoryDefaultOrConfigured(mysqlIO);

		try {
			mysqlIO.mysqlConnection = sslFact.createSocket(
					mysqlIO.mysqlConnection, mysqlIO.host, mysqlIO.port, true);

			// need to force TLSv1, or else JSSE tries to do a SSLv2 handshake
			// which MySQL doesn't understand
			((javax.net.ssl.SSLSocket) mysqlIO.mysqlConnection)
					.setEnabledProtocols(new String[] { "TLSv1" }); //$NON-NLS-1$
			((javax.net.ssl.SSLSocket) mysqlIO.mysqlConnection)
					.startHandshake();

			if (mysqlIO.connection.getUseUnbufferedInput()) {
				mysqlIO.mysqlInput = mysqlIO.mysqlConnection.getInputStream();
			} else {
				mysqlIO.mysqlInput = new BufferedInputStream(
						mysqlIO.mysqlConnection.getInputStream(), 16384);
			}

			mysqlIO.mysqlOutput = new BufferedOutputStream(
					mysqlIO.mysqlConnection.getOutputStream(), 16384);

			mysqlIO.mysqlOutput.flush();
		} catch (IOException ioEx) {
			throw SQLError.createCommunicationsException(mysqlIO.connection,
					mysqlIO.getLastPacketSentTimeMs(), mysqlIO.getLastPacketReceivedTimeMs(),
					ioEx, mysqlIO.getExceptionInterceptor());
		}
	}

	private ExportControlled() { /* prevent instantiation */
	}

	private static SSLSocketFactory getSSLSocketFactoryDefaultOrConfigured(
			MysqlIO mysqlIO) throws SQLException {
		String clientCertificateKeyStoreUrl = mysqlIO.connection
				.getClientCertificateKeyStoreUrl();
		String trustCertificateKeyStoreUrl = mysqlIO.connection
				.getTrustCertificateKeyStoreUrl();
		String clientCertificateKeyStoreType = mysqlIO.connection
				.getClientCertificateKeyStoreType();
		String clientCertificateKeyStorePassword = mysqlIO.connection
				.getClientCertificateKeyStorePassword();
		String trustCertificateKeyStoreType = mysqlIO.connection
				.getTrustCertificateKeyStoreType();
		String trustCertificateKeyStorePassword = mysqlIO.connection
				.getTrustCertificateKeyStorePassword();

		if (StringUtils.isNullOrEmpty(clientCertificateKeyStoreUrl)
				&& StringUtils.isNullOrEmpty(trustCertificateKeyStoreUrl)) {
			if (mysqlIO.connection.getVerifyServerCertificate()) {
				return (javax.net.ssl.SSLSocketFactory) javax.net.ssl.SSLSocketFactory
						.getDefault();
			}
		}

		TrustManagerFactory tmf = null;
		KeyManagerFactory kmf = null;

		try {
			tmf = TrustManagerFactory.getInstance(TrustManagerFactory
					.getDefaultAlgorithm());
			kmf = KeyManagerFactory.getInstance(KeyManagerFactory
					.getDefaultAlgorithm());
		} catch (NoSuchAlgorithmException nsae) {
			throw SQLError
					.createSQLException(
							"Default algorithm definitions for TrustManager and/or KeyManager are invalid.  Check java security properties file.",
							SQL_STATE_BAD_SSL_PARAMS, 0, false, mysqlIO.getExceptionInterceptor());
		}

		if (!StringUtils.isNullOrEmpty(clientCertificateKeyStoreUrl)) {
			InputStream ksIS = null;
			try {
				if (!StringUtils.isNullOrEmpty(clientCertificateKeyStoreType)) {
					KeyStore clientKeyStore = KeyStore
							.getInstance(clientCertificateKeyStoreType);
					URL ksURL = new URL(clientCertificateKeyStoreUrl);
					char[] password = (clientCertificateKeyStorePassword == null) ? new char[0]
							: clientCertificateKeyStorePassword.toCharArray();
					ksIS = ksURL.openStream();
					clientKeyStore.load(ksIS, password);
					kmf.init(clientKeyStore, password);
				}
			} catch (UnrecoverableKeyException uke) {
				throw SQLError
						.createSQLException(
								"Could not recover keys from client keystore.  Check password?",
								SQL_STATE_BAD_SSL_PARAMS, 0, false, mysqlIO.getExceptionInterceptor());
			} catch (NoSuchAlgorithmException nsae) {
				throw SQLError.createSQLException(
						"Unsupported keystore algorithm [" + nsae.getMessage()
								+ "]", SQL_STATE_BAD_SSL_PARAMS, 0, false, mysqlIO.getExceptionInterceptor());
			} catch (KeyStoreException kse) {
				throw SQLError.createSQLException(
						"Could not create KeyStore instance ["
								+ kse.getMessage() + "]", SQL_STATE_BAD_SSL_PARAMS, 0, false, mysqlIO.getExceptionInterceptor());
			} catch (CertificateException nsae) {
				throw SQLError.createSQLException("Could not load client"
						+ clientCertificateKeyStoreType + " keystore from "
						+ clientCertificateKeyStoreUrl, mysqlIO.getExceptionInterceptor());
			} catch (MalformedURLException mue) {
				throw SQLError.createSQLException(clientCertificateKeyStoreUrl
						+ " does not appear to be a valid URL.", SQL_STATE_BAD_SSL_PARAMS, 0,
						false, mysqlIO.getExceptionInterceptor());
			} catch (IOException ioe) {
				SQLException sqlEx = SQLError.createSQLException("Cannot open "
						+ clientCertificateKeyStoreUrl + " ["
						+ ioe.getMessage() + "]", SQL_STATE_BAD_SSL_PARAMS, 0, false, mysqlIO.getExceptionInterceptor());
				sqlEx.initCause(ioe);
				
				throw sqlEx;
			} finally {
				if (ksIS != null) {
					try {
						ksIS.close();
					} catch (IOException e) {
						// can't close input stream, but keystore can be properly initialized
						// so we shouldn't throw this exception
					}
				}
			}
		}

		if (!StringUtils.isNullOrEmpty(trustCertificateKeyStoreUrl)) {

			InputStream ksIS = null;
			try {
				if (!StringUtils.isNullOrEmpty(trustCertificateKeyStoreType)) {
					KeyStore trustKeyStore = KeyStore
							.getInstance(trustCertificateKeyStoreType);
					URL ksURL = new URL(trustCertificateKeyStoreUrl);
	
					char[] password = (trustCertificateKeyStorePassword == null) ? new char[0]
							: trustCertificateKeyStorePassword.toCharArray();
					ksIS = ksURL.openStream();
					trustKeyStore.load(ksIS, password);
					tmf.init(trustKeyStore);
				}
			} catch (NoSuchAlgorithmException nsae) {
				throw SQLError.createSQLException(
						"Unsupported keystore algorithm [" + nsae.getMessage()
								+ "]", SQL_STATE_BAD_SSL_PARAMS, 0, false, mysqlIO.getExceptionInterceptor());
			} catch (KeyStoreException kse) {
				throw SQLError.createSQLException(
						"Could not create KeyStore instance ["
								+ kse.getMessage() + "]", SQL_STATE_BAD_SSL_PARAMS, 0, false, mysqlIO.getExceptionInterceptor());
			} catch (CertificateException nsae) {
				throw SQLError.createSQLException("Could not load trust"
						+ trustCertificateKeyStoreType + " keystore from "
						+ trustCertificateKeyStoreUrl, SQL_STATE_BAD_SSL_PARAMS, 0, false, mysqlIO.getExceptionInterceptor());
			} catch (MalformedURLException mue) {
				throw SQLError.createSQLException(trustCertificateKeyStoreUrl
						+ " does not appear to be a valid URL.", SQL_STATE_BAD_SSL_PARAMS, 0,
						false, mysqlIO.getExceptionInterceptor());
			} catch (IOException ioe) {
				SQLException sqlEx = SQLError.createSQLException("Cannot open "
						+ trustCertificateKeyStoreUrl + " [" + ioe.getMessage()
						+ "]", SQL_STATE_BAD_SSL_PARAMS, 0, false, mysqlIO.getExceptionInterceptor());
				
				sqlEx.initCause(ioe);
				
				throw sqlEx;
			} finally {
				if (ksIS != null) {
					try {
						ksIS.close();
					} catch (IOException e) {
						// can't close input stream, but keystore can be properly initialized
						// so we shouldn't throw this exception
					}
				}
			}
		}

		SSLContext sslContext = null;

		try {
			sslContext = SSLContext.getInstance("TLS");
			sslContext.init(StringUtils.isNullOrEmpty(clientCertificateKeyStoreUrl) ?  null : kmf.getKeyManagers(), mysqlIO.connection
					.getVerifyServerCertificate() ? tmf.getTrustManagers()
					: new X509TrustManager[] { new X509TrustManager() {
						public void checkClientTrusted(X509Certificate[] chain,
								String authType) {
							// return without complaint
						}

						public void checkServerTrusted(X509Certificate[] chain,
								String authType) throws CertificateException {
							// return without complaint
						}

						public X509Certificate[] getAcceptedIssuers() {
							return null;
						}
					} }, null);

			return sslContext.getSocketFactory();
		} catch (NoSuchAlgorithmException nsae) {
			throw SQLError.createSQLException("TLS"
					+ " is not a valid SSL protocol.",
					SQL_STATE_BAD_SSL_PARAMS, 0, false, mysqlIO.getExceptionInterceptor());
		} catch (KeyManagementException kme) {
			throw SQLError.createSQLException("KeyManagementException: "
					+ kme.getMessage(), SQL_STATE_BAD_SSL_PARAMS, 0, false, mysqlIO.getExceptionInterceptor());
		}
	}
}