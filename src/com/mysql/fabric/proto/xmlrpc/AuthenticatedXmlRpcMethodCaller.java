/*
  Copyright (c) 2013, 2014, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.fabric.proto.xmlrpc;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.mysql.fabric.FabricCommunicationException;

/**
 * An XML-RPC method caller which wraps another caller with RFC-2617
 * authentication.
 */
public class AuthenticatedXmlRpcMethodCaller implements XmlRpcMethodCaller {
	private XmlRpcMethodCaller underlyingCaller;

	private String url;
	private String username;
	private String password;

	public AuthenticatedXmlRpcMethodCaller(XmlRpcMethodCaller underlyingCaller, String url, String username, String password) {
		this.underlyingCaller = underlyingCaller;
		this.url = url;
		this.username = username;
		this.password = password;
	}

	public void setHeader(String name, String value) {
		underlyingCaller.setHeader(name, value);
	}

	public void clearHeader(String name) {
		underlyingCaller.clearHeader(name);
	}

	public 	List<?> call(String methodName, Object args[])
		throws FabricCommunicationException {
		String authenticateHeader;

		try {
			authenticateHeader = DigestAuthentication.getChallengeHeader(this.url);
		} catch (IOException ex) {
			throw new FabricCommunicationException("Unable to obtain challenge header for authentication", ex);
		}

		Map<String, String> digestChallenge = DigestAuthentication.parseDigestChallenge(authenticateHeader);

		String authorizationHeader = DigestAuthentication
			.generateAuthorizationHeader(digestChallenge, this.username, this.password);

		this.underlyingCaller.setHeader("Authorization", authorizationHeader);

		return this.underlyingCaller.call(methodName, args);
	}
}
