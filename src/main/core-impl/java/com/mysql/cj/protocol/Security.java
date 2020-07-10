/*
 * Copyright (c) 2002, 2020, Oracle and/or its affiliates.
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

package com.mysql.cj.protocol;

import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.mysql.cj.exceptions.AssertionFailedException;
import com.mysql.cj.util.StringUtils;

/**
 * Methods for doing secure authentication with MySQL-4.1 and newer.
 */
public class Security {

    private static int CACHING_SHA2_DIGEST_LENGTH = 32;

    /**
     * Encrypt/Decrypt function used for password encryption in authentication
     * 
     * Simple XOR is used here but it is OK as we encrypt random strings
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

    public static byte[] scramble411(String password, byte[] seed, String passwordEncoding) {
        byte[] passwordBytes = (passwordEncoding == null || passwordEncoding.length() == 0) ? StringUtils.getBytes(password)
                : StringUtils.getBytes(password, passwordEncoding);
        return scramble411(passwordBytes, seed);
    }

    /**
     * Hashing for MySQL-4.1 authentication. Algorithm is as follows (c.f. <i>sql/auth/password.c</i>):
     *
     * <pre>
     * SERVER: public_seed=create_random_string()
     * send(public_seed)
     *
     * CLIENT: recv(public_seed)
     * hash_stage1=sha1("password")
     * hash_stage2=sha1(hash_stage1)
     * reply=xor(hash_stage1, sha1(public_seed,hash_stage2))
     * send(reply)
     * </pre>
     * 
     * @param password
     *            password
     * @param seed
     *            seed
     * @return bytes
     */
    public static byte[] scramble411(byte[] password, byte[] seed) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException ex) {
            throw new AssertionFailedException(ex);
        }

        byte[] passwordHashStage1 = md.digest(password);
        md.reset();

        byte[] passwordHashStage2 = md.digest(passwordHashStage1);
        md.reset();

        md.update(seed);
        md.update(passwordHashStage2);

        byte[] toBeXord = md.digest();

        int numToXor = toBeXord.length;

        for (int i = 0; i < numToXor; i++) {
            toBeXord[i] = (byte) (toBeXord[i] ^ passwordHashStage1[i]);
        }

        return toBeXord;
    }

    /**
     * Scrambling for caching_sha2_password plugin.
     * 
     * <pre>
     * Scramble = XOR(SHA2(password), SHA2(SHA2(SHA2(password)), Nonce))
     * </pre>
     * 
     * @param password
     *            password
     * @param seed
     *            seed
     * @return bytes
     * 
     * @throws DigestException
     *             if an error occurs
     */
    public static byte[] scrambleCachingSha2(byte[] password, byte[] seed) throws DigestException {
        /*
         * Server does it in 4 steps (see sql/auth/sha2_password_common.cc Generate_scramble::scramble method):
         * 
         * SHA2(src) => digest_stage1
         * SHA2(digest_stage1) => digest_stage2
         * SHA2(digest_stage2, m_rnd) => scramble_stage1
         * XOR(digest_stage1, scramble_stage1) => scramble
         */
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException ex) {
            throw new AssertionFailedException(ex);
        }

        byte[] dig1 = new byte[CACHING_SHA2_DIGEST_LENGTH];
        byte[] dig2 = new byte[CACHING_SHA2_DIGEST_LENGTH];
        byte[] scramble1 = new byte[CACHING_SHA2_DIGEST_LENGTH];

        // SHA2(src) => digest_stage1
        md.update(password, 0, password.length);
        md.digest(dig1, 0, CACHING_SHA2_DIGEST_LENGTH);
        md.reset();

        // SHA2(digest_stage1) => digest_stage2
        md.update(dig1, 0, dig1.length);
        md.digest(dig2, 0, CACHING_SHA2_DIGEST_LENGTH);
        md.reset();

        // SHA2(digest_stage2, m_rnd) => scramble_stage1
        md.update(dig2, 0, dig1.length);
        md.update(seed, 0, seed.length);
        md.digest(scramble1, 0, CACHING_SHA2_DIGEST_LENGTH);

        // XOR(digest_stage1, scramble_stage1) => scramble
        byte[] mysqlScrambleBuff = new byte[CACHING_SHA2_DIGEST_LENGTH];
        xorString(dig1, mysqlScrambleBuff, scramble1, CACHING_SHA2_DIGEST_LENGTH);

        return mysqlScrambleBuff;
    }

    /**
     * Prevent construction.
     */
    private Security() {
        super();
    }
}
