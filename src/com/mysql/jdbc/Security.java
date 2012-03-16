/*
 Copyright (c) 2002, 2012, Oracle and/or its affiliates. All rights reserved.
 

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

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Methods for doing secure authentication with MySQL-4.1 and newer.
 * 
 * @author Mark Matthews
 * 
 * @version $Id$
 */
public class Security {
	private static final char PVERSION41_CHAR = '*';

	private static final int SHA1_HASH_SIZE = 20;

	/**
	 * Returns hex value for given char
	 */
	private static int charVal(char c) {
		return ((c >= '0') && (c <= '9')) ? (c - '0')
				: (((c >= 'A') && (c <= 'Z')) ? (c - 'A' + 10) : (c - 'a' + 10));
	}

	/*
	 * Convert password in salted form to binary string password and hash-salt
	 * For old password this involes one more hashing
	 * 
	 * SYNOPSIS get_hash_and_password() salt IN Salt to convert from pversion IN
	 * Password version to use hash OUT Store zero ended hash here bin_password
	 * OUT Store binary password here (no zero at the end)
	 * 
	 * RETURN 0 for pre 4.1 passwords !0 password version char for newer
	 * passwords
	 */

	/**
	 * Creates key from old password to decode scramble Used in 4.1
	 * authentication with passwords stored pre-4.1 hashing.
	 * 
	 * @param passwd
	 *            the password to create the key from
	 * 
	 * @return 20 byte generated key
	 * 
	 * @throws NoSuchAlgorithmException
	 *             if the message digest 'SHA-1' is not available.
	 */
	static byte[] createKeyFromOldPassword(String passwd)
			throws NoSuchAlgorithmException {
		/* At first hash password to the string stored in password */
		passwd = makeScrambledPassword(passwd);

		/* Now convert it to the salt form */
		int[] salt = getSaltFromPassword(passwd);

		/* Finally get hash and bin password from salt */
		return getBinaryPassword(salt, false);
	}

	/**
	 * DOCUMENT ME!
	 * 
	 * @param salt
	 *            DOCUMENT ME!
	 * @param usingNewPasswords
	 *            DOCUMENT ME!
	 * 
	 * @return DOCUMENT ME!
	 * 
	 * @throws NoSuchAlgorithmException
	 *             if the message digest 'SHA-1' is not available.
	 */
	static byte[] getBinaryPassword(int[] salt, boolean usingNewPasswords)
			throws NoSuchAlgorithmException {
		int val = 0;

		byte[] binaryPassword = new byte[SHA1_HASH_SIZE]; /*
															 * Binary password
															 * loop pointer
															 */

		if (usingNewPasswords) /* New password version assumed */{
			int pos = 0;

			for (int i = 0; i < 4; i++) /* Iterate over these elements */{
				val = salt[i];

				for (int t = 3; t >= 0; t--) {
					binaryPassword[pos++] = (byte) (val & 255);
					val >>= 8; /* Scroll 8 bits to get next part */
				}
			}

			return binaryPassword;
		}

		int offset = 0;

		for (int i = 0; i < 2; i++) /* Iterate over these elements */{
			val = salt[i];

			for (int t = 3; t >= 0; t--) {
				binaryPassword[t + offset] = (byte) (val % 256);
				val >>= 8; /* Scroll 8 bits to get next part */
			}

			offset += 4;
		}

		MessageDigest md = MessageDigest.getInstance("SHA-1"); //$NON-NLS-1$

		md.update(binaryPassword, 0, 8);

		return md.digest();
	}

	private static int[] getSaltFromPassword(String password) {
		int[] result = new int[6];

		if ((password == null) || (password.length() == 0)) {
			return result;
		}

		if (password.charAt(0) == PVERSION41_CHAR) {
			// new password
			String saltInHex = password.substring(1, 5);

			int val = 0;

			for (int i = 0; i < 4; i++) {
				val = (val << 4) + charVal(saltInHex.charAt(i));
			}

			return result;
		}

		int resultPos = 0;
		int pos = 0;
		int length = password.length();

		while (pos < length) {
			int val = 0;

			for (int i = 0; i < 8; i++) {
				val = (val << 4) + charVal(password.charAt(pos++));
			}

			result[resultPos++] = val;
		}

		return result;
	}

	private static String longToHex(long val) {
		String longHex = Long.toHexString(val);

		int length = longHex.length();

		if (length < 8) {
			int padding = 8 - length;
			StringBuffer buf = new StringBuffer();

			for (int i = 0; i < padding; i++) {
				buf.append("0"); //$NON-NLS-1$
			}

			buf.append(longHex);

			return buf.toString();
		}

		return longHex.substring(0, 8);
	}

	/**
	 * Creates password to be stored in user database from raw string.
	 * 
	 * Handles Pre-MySQL 4.1 passwords.
	 * 
	 * @param password
	 *            plaintext password
	 * 
	 * @return scrambled password
	 * 
	 * @throws NoSuchAlgorithmException
	 *             if the message digest 'SHA-1' is not available.
	 */
	static String makeScrambledPassword(String password)
			throws NoSuchAlgorithmException {
		long[] passwordHash = Util.newHash(password);
		StringBuffer scramble = new StringBuffer();

		scramble.append(longToHex(passwordHash[0]));
		scramble.append(longToHex(passwordHash[1]));

		return scramble.toString();
	}

	/**
	 * Encrypt/Decrypt function used for password encryption in authentication
	 * 
	 * Simple XOR is used here but it is OK as we crypt random strings
	 * 
	 * @param from
	 *            IN Data for encryption
	 * @param to
	 *            OUT Encrypt data to the buffer (may be the same)
	 * @param password
	 *            IN Password used for encryption (same length)
	 * @param length
	 *            IN Length of data to encrypt
	 */
	static void passwordCrypt(byte[] from, byte[] to, byte[] password,
			int length) {
		int pos = 0;

		while ((pos < from.length) && (pos < length)) {
			to[pos] = (byte) (from[pos] ^ password[pos]);
			pos++;
		}
	}

	/**
	 * Stage one password hashing, used in MySQL 4.1 password handling
	 * 
	 * @param password
	 *            plaintext password
	 * 
	 * @return stage one hash of password
	 * 
	 * @throws NoSuchAlgorithmException
	 *             if the message digest 'SHA-1' is not available.
	 */
	static byte[] passwordHashStage1(String password)
			throws NoSuchAlgorithmException {
		MessageDigest md = MessageDigest.getInstance("SHA-1"); //$NON-NLS-1$
		StringBuffer cleansedPassword = new StringBuffer();

		int passwordLength = password.length();

		for (int i = 0; i < passwordLength; i++) {
			char c = password.charAt(i);

			if ((c == ' ') || (c == '\t')) {
				continue; /* skip space in password */
			}

			cleansedPassword.append(c);
		}

		return md.digest(StringUtils.getBytes(cleansedPassword.toString()));
	}

	/**
	 * Stage two password hashing used in MySQL 4.1 password handling
	 * 
	 * @param hash
	 *            from passwordHashStage1
	 * @param salt
	 *            salt used for stage two hashing
	 * 
	 * @return result of stage two password hash
	 * 
	 * @throws NoSuchAlgorithmException
	 *             if the message digest 'SHA-1' is not available.
	 */
	static byte[] passwordHashStage2(byte[] hashedPassword, byte[] salt)
			throws NoSuchAlgorithmException {
		MessageDigest md = MessageDigest.getInstance("SHA-1"); //$NON-NLS-1$

		// hash 4 bytes of salt
		md.update(salt, 0, 4);

		md.update(hashedPassword, 0, SHA1_HASH_SIZE);

		return md.digest();
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
	public static byte[] scramble411(String password, String seed, Connection conn)
			throws NoSuchAlgorithmException, UnsupportedEncodingException {
		MessageDigest md = MessageDigest.getInstance("SHA-1"); //$NON-NLS-1$
		String passwordEncoding = conn.getPasswordCharacterEncoding();
		
		byte[] passwordHashStage1 = md
				.digest((passwordEncoding == null || passwordEncoding.length() == 0) ? 
						StringUtils.getBytes(password)
						: StringUtils.getBytes(password, passwordEncoding));
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
