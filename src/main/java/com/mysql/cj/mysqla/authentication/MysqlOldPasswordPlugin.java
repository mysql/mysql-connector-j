/*
  Copyright (c) 2012, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.mysqla.authentication;

import java.io.UnsupportedEncodingException;
import java.util.List;

import com.mysql.cj.api.io.Protocol;
import com.mysql.cj.api.mysqla.authentication.AuthenticationPlugin;
import com.mysql.cj.api.mysqla.io.NativeProtocol.IntegerDataType;
import com.mysql.cj.api.mysqla.io.NativeProtocol.StringSelfDataType;
import com.mysql.cj.api.mysqla.io.PacketPayload;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.mysqla.io.Buffer;

/**
 * MySQL Native Old-Password Authentication Plugin
 */
public class MysqlOldPasswordPlugin implements AuthenticationPlugin {

    private Protocol protocol;
    private String password = null;

    @Override
    public void init(Protocol prot) {
        this.protocol = prot;
    }

    public void destroy() {
        this.password = null;
    }

    public String getProtocolPluginName() {
        return "mysql_old_password";
    }

    public boolean requiresConfidentiality() {
        return false;
    }

    public boolean isReusable() {
        return true;
    }

    public void setAuthenticationParameters(String user, String password) {
        this.password = password;
    }

    @Override
    public boolean nextAuthenticationStep(PacketPayload fromServer, List<PacketPayload> toServer) {
        toServer.clear();

        PacketPayload bresp = null;

        String pwd = this.password;

        if (fromServer == null || pwd == null || pwd.length() == 0) {
            bresp = new Buffer(new byte[0]);
        } else {
            bresp = new Buffer(StringUtils.getBytes(
                    newCrypt(pwd, fromServer.readString(StringSelfDataType.STRING_TERM, null).substring(0, 8), this.protocol.getPasswordCharacterEncoding())));

            bresp.setPosition(bresp.getPayloadLength());
            bresp.writeInteger(IntegerDataType.INT1, 0);
            bresp.setPosition(0);
        }
        toServer.add(bresp);

        return true;
    }

    // Right from Monty's code
    private static String newCrypt(String password, String seed, String encoding) {
        byte b;
        double d;

        if ((password == null) || (password.length() == 0)) {
            return password;
        }

        long[] pw = newHash(seed.getBytes());
        long[] msg = hashPre41Password(password, encoding);
        long max = 0x3fffffffL;
        long seed1 = (pw[0] ^ msg[0]) % max;
        long seed2 = (pw[1] ^ msg[1]) % max;
        char[] chars = new char[seed.length()];

        for (int i = 0; i < seed.length(); i++) {
            seed1 = ((seed1 * 3) + seed2) % max;
            seed2 = (seed1 + seed2 + 33) % max;
            d = (double) seed1 / (double) max;
            b = (byte) java.lang.Math.floor((d * 31) + 64);
            chars[i] = (char) b;
        }

        seed1 = ((seed1 * 3) + seed2) % max;
        seed2 = (seed1 + seed2 + 33) % max;
        d = (double) seed1 / (double) max;
        b = (byte) java.lang.Math.floor(d * 31);

        for (int i = 0; i < seed.length(); i++) {
            chars[i] ^= (char) b;
        }

        return new String(chars);
    }

    private static long[] hashPre41Password(String password, String encoding) {
        // remove white spaces and convert to bytes
        try {
            return newHash(password.replaceAll("\\s", "").getBytes(encoding));
        } catch (UnsupportedEncodingException e) {
            return new long[0];
        }
    }

    private static long[] newHash(byte[] password) {
        long nr = 1345345333L;
        long add = 7;
        long nr2 = 0x12345671L;
        long tmp;

        for (byte b : password) {
            tmp = 0xff & b;
            nr ^= ((((nr & 63) + add) * tmp) + (nr << 8));
            nr2 += ((nr2 << 8) ^ nr);
            add += tmp;
        }

        long[] result = new long[2];
        result[0] = nr & 0x7fffffffL;
        result[1] = nr2 & 0x7fffffffL;

        return result;
    }

}
