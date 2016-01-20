/*
  Copyright (c) 2014, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.fabric.proto.xmlrpc;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * HTTP/1.1 Digest Authentication - RFC 2617
 */
public class DigestAuthentication {

    private static Random random = new Random();

    /**
     * Get the digest challenge header by connecting to the resource
     * with no credentials.
     */
    public static String getChallengeHeader(String url) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setDoOutput(true);
        conn.getOutputStream().close();
        try {
            conn.getInputStream().close();
        } catch (IOException ex) {
            if (401 == conn.getResponseCode()) {
                // we expect a 401-unauthorized response with the
                // WWW-Authenticate header to create the request with the
                // necessary auth data
                String hdr = conn.getHeaderField("WWW-Authenticate");
                if (hdr != null && !"".equals(hdr)) {
                    return hdr;
                }
            } else if (400 == conn.getResponseCode()) {
                // 400 usually means that auth is disabled on the Fabric node
                throw new IOException("Fabric returns status 400. If authentication is disabled on the Fabric node, "
                        + "omit the `fabricUsername' and `fabricPassword' properties from your connection.");
            } else {
                throw ex;
            }
        }
        return null;
    }

    /**
     * Calculate the request digest for algorithm=MD5.
     */
    public static String calculateMD5RequestDigest(String uri, String username, String password, String realm, String nonce, String nc, String cnonce,
            String qop) {
        String reqA1 = username + ":" + realm + ":" + password;
        // valid only for qop="auth"
        String reqA2 = "POST:" + uri;

        String hashA1 = checksumMD5(reqA1);
        String hashA2 = checksumMD5(reqA2);
        String requestDigest = digestMD5(hashA1, nonce + ":" + nc + ":" + cnonce + ":" + qop + ":" + hashA2);

        return requestDigest;
    }

    /**
     * MD5 version of the "H()" function from rfc2617.
     */
    private static String checksumMD5(String data) {
        MessageDigest md5 = null;
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException("Unable to create MD5 instance", ex);
        }
        // TODO encoding
        return hexEncode(md5.digest(data.getBytes()));
    }

    /**
     * MD5 version of the "KD()" function from rfc2617.
     */
    private static String digestMD5(String secret, String data) {
        return checksumMD5(secret + ":" + data);
    }

    /**
     * hex-encode a byte array
     */
    private static String hexEncode(byte data[]) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < data.length; ++i) {
            sb.append(String.format("%02x", data[i]));
        }
        return sb.toString();
    }

    /**
     * Serialize a parameter map into a digest response. This is used
     * as the "Authorization" header in the request. All parameters in
     * the supplied map will be added to the header.
     */
    public static String serializeDigestResponse(Map<String, String> paramMap) {
        StringBuilder sb = new StringBuilder("Digest ");

        boolean prefixComma = false;
        for (Map.Entry<String, String> entry : paramMap.entrySet()) {
            if (!prefixComma) {
                prefixComma = true;
            } else {
                sb.append(", ");
            }
            sb.append(entry.getKey());
            sb.append("=");
            sb.append(entry.getValue());
        }

        return sb.toString();
    }

    /**
     * Parse a digest challenge from the WWW-Authenticate header
     * return as the initial response during the authentication
     * exchange.
     */
    public static Map<String, String> parseDigestChallenge(String headerValue) {
        if (!headerValue.startsWith("Digest ")) {
            throw new IllegalArgumentException("Header is not a digest challenge");
        }

        String params = headerValue.substring(7);
        Map<String, String> paramMap = new HashMap<String, String>();
        for (String param : params.split(",\\s*")) {
            String pieces[] = param.split("=");
            paramMap.put(pieces[0], pieces[1].replaceAll("^\"(.*)\"$", "$1"));
        }
        return paramMap;
    }

    /**
     * Generate the cnonce value. This allows the client provide a
     * value used in the digest calculation. Same as Python. (no
     * motivation given for this algorithm)
     */
    @SuppressWarnings("deprecation")
    public static String generateCnonce(String nonce, String nc) {
        // Random string, keep it in basic printable ASCII range
        byte buf[] = new byte[8];
        random.nextBytes(buf);
        for (int i = 0; i < 8; ++i) {
            buf[i] = (byte) (0x20 + (buf[i] % 95));
        }

        String combo = String.format("%s:%s:%s:%s", nonce, nc, new Date().toGMTString(), new String(buf));
        MessageDigest sha1 = null;
        try {
            sha1 = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException("Unable to create SHA-1 instance", ex);
        }

        return hexEncode(sha1.digest(combo.getBytes()));
    }

    /**
     * Quote a parameter to be included in the header. Parameters with
     * embedded quotes will be rejected.
     */
    private static String quoteParam(String param) {
        if (param.contains("\"") || param.contains("'")) {
            throw new IllegalArgumentException("Invalid character in parameter");
        }
        return "\"" + param + "\"";
    }

    /**
     * Generate the Authorization header to make the authenticated
     * request.
     */
    public static String generateAuthorizationHeader(Map<String, String> digestChallenge, String username, String password) {
        String nonce = digestChallenge.get("nonce");
        String nc = "00000001";
        String cnonce = generateCnonce(nonce, nc);
        String qop = "auth";
        String uri = "/RPC2";
        String realm = digestChallenge.get("realm");
        String opaque = digestChallenge.get("opaque");

        String requestDigest = calculateMD5RequestDigest(uri, username, password, realm, nonce, nc, cnonce, qop);
        Map<String, String> digestResponseMap = new HashMap<String, String>();
        digestResponseMap.put("algorithm", "MD5");
        digestResponseMap.put("username", quoteParam(username));
        digestResponseMap.put("realm", quoteParam(realm));
        digestResponseMap.put("nonce", quoteParam(nonce));
        digestResponseMap.put("uri", quoteParam(uri));
        digestResponseMap.put("qop", qop);
        digestResponseMap.put("nc", nc);
        digestResponseMap.put("cnonce", quoteParam(cnonce));
        digestResponseMap.put("response", quoteParam(requestDigest));
        digestResponseMap.put("opaque", quoteParam(opaque));

        return serializeDigestResponse(digestResponseMap);
    }
}
