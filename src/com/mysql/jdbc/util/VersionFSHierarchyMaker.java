/*
 Copyright  2005-2006 MySQL AB, 2008 Sun Microsystems
 All rights reserved. Use is subject to license terms.

  The MySQL Connector/J is licensed under the terms of the GPL,
  like most MySQL Connectors. There are special exceptions to the
  terms and conditions of the GPL as it is applied to this software,
  see the FLOSS License Exception available on mysql.com.

  This program is free software; you can redistribute it and/or
  modify it under the terms of the GNU General Public License as
  published by the Free Software Foundation; version 2 of the
  License.

  This program is distributed in the hope that it will be useful,  
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  02110-1301 USA
*/

package com.mysql.jdbc.util;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;

import com.mysql.jdbc.NonRegisteringDriver;

/**
 * Creates output directory structure for multi-jvm, multi-url
 * unit, regression and compliance tests.
 */
public class VersionFSHierarchyMaker {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			usage();
			System.exit(1);
		}
		
		String jdbcUrl = null;

		String jvmVersion = removeWhitespaceChars(System.getProperty("java.version"));
		String jvmVendor = removeWhitespaceChars(System.getProperty("java.vendor"));
		String osName = removeWhitespaceChars(System.getProperty("os.name"));
		String osArch = removeWhitespaceChars(System.getProperty("os.arch"));
		String osVersion = removeWhitespaceChars(System.getProperty("os.version"));
		
		jdbcUrl = System.getProperty("com.mysql.jdbc.testsuite.url");
		
		String mysqlVersion = "not-available";
		
		try {
			Connection conn = new NonRegisteringDriver().connect(jdbcUrl, null);
	
			ResultSet rs = conn.createStatement().executeQuery("SELECT VERSION()");
			rs.next();
			mysqlVersion = removeWhitespaceChars(rs.getString(1));
		} catch (Throwable t) {
			mysqlVersion = "no-server-running-on-" + removeWhitespaceChars(jdbcUrl);
		}

		String jvmSubdirName = jvmVendor + "-" + jvmVersion;
		String osSubdirName = osName + "-" + osArch + "-" + osVersion;
		
		File baseDir = new File(args[1]);
		File mysqlVersionDir = new File(baseDir, mysqlVersion);
		File osVersionDir = new File(mysqlVersionDir, osSubdirName);
		File jvmVersionDir = new File(osVersionDir, jvmSubdirName);
		
		jvmVersionDir.mkdirs();

		
		FileOutputStream pathOut = null;
		
		try {
			String propsOutputPath = args[2];
			pathOut = new FileOutputStream(propsOutputPath);
			String baseDirStr = baseDir.getAbsolutePath();
			String jvmVersionDirStr = jvmVersionDir.getAbsolutePath();
			
			if (jvmVersionDirStr.startsWith(baseDirStr)) {
				jvmVersionDirStr = jvmVersionDirStr.substring(baseDirStr.length() + 1);
			}

			pathOut.write(jvmVersionDirStr.getBytes());
		} finally {
			if (pathOut != null) {
				pathOut.flush();
				pathOut.close();
			}
		}
	}

	public static String removeWhitespaceChars(String input) {
		if (input == null) {
			return input;
		}
		
		int strLen = input.length();
		
		StringBuffer output = new StringBuffer(strLen);
		
		for (int i = 0; i < strLen; i++) {
			char c = input.charAt(i);
			if (!Character.isDigit(c) && !Character.isLetter(c)) {
				if (Character.isWhitespace(c)) {
					output.append("_");
				} else {
					output.append(".");
				}
			} else {
				output.append(c);
			}
		}
		
		return output.toString();
	}
	
	private static void usage() {
		System.err.println("Creates a fs hierarchy representing MySQL version, OS version and JVM version.");
		System.err.println("Stores the full path as 'outputDirectory' property in file 'directoryPropPath'");
		System.err.println();
		System.err.println("Usage: java VersionFSHierarchyMaker unit|compliance baseDirectory directoryPropPath");
	}
}
