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
package com.mysql.jdbc.util;

import java.io.File;
import java.io.IOException;

import java.util.Iterator;
import java.util.Locale;
import java.util.Properties;

import com.mysql.jdbc.StringUtils;

/**
 * Controls a MySQL server using Java RunTime methods
 * 
 * @version $Id: ServerController.java,v 1.1.2.1 2005/05/13 18:58:39 mmatthews
 *          Exp $
 * @author Mark Matthews
 */
public class ServerController {

	/**
	 * Where is the server installed?
	 */
	public static final String BASEDIR_KEY = "basedir";

	/**
	 * Where are the databases installed?
	 */
	public static final String DATADIR_KEY = "datadir";

	/**
	 * Where is the config file located?
	 */

	public static final String DEFAULTS_FILE_KEY = "defaults-file";

	/**
	 * What is the name of the executable to run?
	 */

	public static final String EXECUTABLE_NAME_KEY = "executable";

	/**
	 * What is the path to the mysql server executable (if not standard?)
	 */

	public static final String EXECUTABLE_PATH_KEY = "executablePath";

	/**
	 * The default executable to run
	 */

	/**
	 * The process representing the MySQL server
	 */
	private Process serverProcess = null;

	/**
	 * The list of properties for this server
	 */
	private Properties serverProps = null;

	/**
	 * The system properties
	 */
	private Properties systemProps = null;

	/**
	 * Creates a ServerController with the directory for the MySQL server.
	 * 
	 * The 'datadir' is set to the same directory.
	 * 
	 * @param baseDir
	 *            the base directory for the MySQL server.
	 */
	public ServerController(String baseDir) {
		setBaseDir(baseDir);
	}

	/**
	 * Creates a server controller for the MySQL server with the given basedir
	 * and datadir.
	 * 
	 * @param basedir
	 *            the basedir to use when starting MySQL.
	 * @param datadir
	 *            the datadir to use when starting MySQL.
	 */
	public ServerController(String basedir, String datadir) {
	}

	/**
	 * Sets the basedir to use when starting MySQL.
	 * 
	 * @param baseDir
	 *            the basedir to use when starting MySQL.
	 */
	public void setBaseDir(String baseDir) {
		getServerProps().setProperty(BASEDIR_KEY, baseDir);
	}

	/**
	 * Sets the data to use when starting MySQL.
	 * 
	 * @param dataDir
	 *            the basedir to use when starting MySQL.
	 */
	public void setDataDir(String dataDir) {
		getServerProps().setProperty(DATADIR_KEY, dataDir);
	}

	/**
	 * Starts the server, returning a java.lang.Process instance that represents
	 * the mysql server.
	 * 
	 * @return Process a java.lang.Process instance representing the mysql
	 *         server process.
	 * @throws IOException
	 *             if an error occurs while starting the mysql server.
	 */
	public Process start() throws IOException {
		if (this.serverProcess != null) {
			throw new IllegalArgumentException("Server already started");
		} else {
			this.serverProcess = Runtime.getRuntime().exec(getCommandLine());

			return this.serverProcess;
		}
	}

	/**
	 * Stops the server (if started)
	 * 
	 * @param forceIfNecessary
	 *            use forceStop if mysqladmin doesn't shut the server down
	 * 
	 * @throws IOException
	 *             if an error occurs while stopping the server
	 */
	public void stop(boolean forceIfNecessary) throws IOException {
		if (this.serverProcess != null) {

			String basedir = getServerProps().getProperty(BASEDIR_KEY);

			StringBuffer pathBuf = new StringBuffer(basedir);

			if (!basedir.endsWith(File.separator)) {
				pathBuf.append(File.separator);
			}

			String defaultsFilePath = getServerProps().getProperty(
					DEFAULTS_FILE_KEY);

			pathBuf.append("bin");
			pathBuf.append(File.separator);
			pathBuf.append("mysqladmin shutdown");

			System.out.println(pathBuf.toString());

			Process mysqladmin = Runtime.getRuntime().exec(pathBuf.toString());

			int exitStatus = -1;

			try {
				exitStatus = mysqladmin.waitFor();
			} catch (InterruptedException ie) {
				; // ignore
			}

			//
			// Terminate the process if mysqladmin couldn't
			// do it, and the user requested a force stop.
			//
			if (exitStatus != 0 && forceIfNecessary) {
				forceStop();
			}
		}
	}

	/**
	 * Forcefully terminates the server process (if started).
	 */
	public void forceStop() {
		if (this.serverProcess != null) {
			this.serverProcess.destroy();
			this.serverProcess = null;
		}
	}

	/**
	 * Returns the list of properties that will be used to start/control the
	 * server.
	 * 
	 * @return Properties the list of properties.
	 */
	public synchronized Properties getServerProps() {
		if (this.serverProps == null) {
			this.serverProps = new Properties();
		}

		return this.serverProps;
	}

	/**
	 * Returns the full commandline used to start the mysql server, including
	 * and arguments to be passed to the server process.
	 * 
	 * @return String the commandline used to start the mysql server.
	 */
	private String getCommandLine() {
		StringBuffer commandLine = new StringBuffer(getFullExecutablePath());
		commandLine.append(buildOptionalCommandLine());

		return commandLine.toString();
	}

	/**
	 * Returns the fully-qualifed path to the 'mysqld' executable
	 * 
	 * @return String the path to the server executable.
	 */
	private String getFullExecutablePath() {
		StringBuffer pathBuf = new StringBuffer();

		String optionalExecutablePath = getServerProps().getProperty(
				EXECUTABLE_PATH_KEY);

		if (optionalExecutablePath == null) {
			// build the path using the defaults
			String basedir = getServerProps().getProperty(BASEDIR_KEY);
			pathBuf.append(basedir);

			if (!basedir.endsWith(File.separator)) {
				pathBuf.append(File.separatorChar);
			}

			if (runningOnWindows()) {
				pathBuf.append("bin");
			} else {
				pathBuf.append("libexec");
			}

			pathBuf.append(File.separatorChar);
		} else {
			pathBuf.append(optionalExecutablePath);

			if (!optionalExecutablePath.endsWith(File.separator)) {
				pathBuf.append(File.separatorChar);
			}
		}

		String executableName = getServerProps().getProperty(
				EXECUTABLE_NAME_KEY, "mysqld");

		pathBuf.append(executableName);

		return pathBuf.toString();
	}

	/**
	 * Builds the list of command-line arguments that will be passed to the
	 * mysql server to be started.
	 * 
	 * @return String the list of command-line arguments.
	 */
	private String buildOptionalCommandLine() {
		StringBuffer commandLineBuf = new StringBuffer();

		if (this.serverProps != null) {

			for (Iterator iter = this.serverProps.keySet().iterator(); iter
					.hasNext();) {
				String key = (String) iter.next();
				String value = this.serverProps.getProperty(key);

				if (!isNonCommandLineArgument(key)) {
					if (value != null && value.length() > 0) {
						commandLineBuf.append(" \"");
						commandLineBuf.append("--");
						commandLineBuf.append(key);
						commandLineBuf.append("=");
						commandLineBuf.append(value);
						commandLineBuf.append("\"");
					} else {
						commandLineBuf.append(" --");
						commandLineBuf.append(key);
					}
				}
			}
		}

		return commandLineBuf.toString();
	}

	/**
	 * Returns true if the property does not belong as a command-line argument
	 * 
	 * @return boolean if the property should not be a command-line argument.
	 */
	private boolean isNonCommandLineArgument(String propName) {
		return propName.equals(EXECUTABLE_NAME_KEY)
				|| propName.equals(EXECUTABLE_PATH_KEY);
	}

	/**
	 * Lazily creates a list of system properties.
	 * 
	 * @return Properties the properties from System.getProperties()
	 */
	private synchronized Properties getSystemProperties() {
		if (this.systemProps == null) {
			this.systemProps = System.getProperties();
		}

		return this.systemProps;
	}

	/**
	 * Is this ServerController running on a Windows operating system?
	 * 
	 * @return boolean if this ServerController is running on Windows
	 */
	private boolean runningOnWindows() {
		return StringUtils.indexOfIgnoreCase(getSystemProperties().getProperty(
				"os.name"), "WINDOWS") != -1;
	}
}
