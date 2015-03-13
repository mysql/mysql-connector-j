/*
  Copyright (c) 2002, 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.core.authentication;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.mysql.cj.core.util.StringUtils;

/**
 * Methods for doing secure authentication with MySQL-4.1 and newer.
 */
public class Security {

    /**
     * Encrypt/Decrypt function used for password encryption in authentication
     * 
     * Simple XOR is used here but it is OK as we crypt random strings
     * 
     * @param from
     *            IN Data for encryption
     * @param to
     *            OUT Encrypt data to the buffer (may be the same)
     * @param scramble
     *            IN Scramble used for encryption
     * @param length
     *            IN Length of data to encrypt
     */
    public static void xorString(byte[] from, byte[] to, byte[] scramble, int length) {
        int pos = 0;
        int scrambleLength = scramble.length;

        while (pos < length) {
            to[pos] = (byte) (from[pos] ^ scramble[pos % scrambleLength]);
            pos++;
        }
    }

    // SERVER: public_seed=create_random_string()
    // send(public_seed)
    //
    // CLIENT: recv(public_seed)
    // hash_stage1=sha1("password")
    // hash_stage2=sha1(hash_stage1)
    // reply=xor(hash_stage1, sha1(public_seed,hash_stage2)
    //
    // // this three steps are done in scramble()
    //
    // send(reply)
    //
    //
    // SERVER: recv(reply)
    // hash_stage1=xor(reply, sha1(public_seed,hash_stage2))
    // candidate_hash2=sha1(hash_stage1)
    // check(candidate_hash2==hash_stage2)
    public static byte[] scramble411(String password, String seed, String passwordEncoding) throws NoSuchAlgorithmException, UnsupportedEncodingException {
        MessageDigest md = MessageDigest.getInstance("SHA-1");

        byte[] passwordHashStage1 = md.digest((passwordEncoding == null || passwordEncoding.length() == 0) ? StringUtils.getBytes(password) : StringUtils
                .getBytes(password, passwordEncoding));
        md.reset();

        byte[] passwordHashStage2 = md.digest(passwordHashStage1);
        md.reset();

        byte[] seedAsBytes = StringUtils.getBytes(seed, "ASCII"); // for debugging
        md.update(seedAsBytes);
        md.update(passwordHashStage2);

        byte[] toBeXord = md.digest();

        int numToXor = toBeXord.length;

        for (int i = 0; i < numToXor; i++) {
            toBeXord[i] = (byte) (toBeXord[i] ^ passwordHashStage1[i]);
        }

        return toBeXord;
    }

    /**
     * Prevent construction.
     */
    private Security() {
        super();
    }
}
