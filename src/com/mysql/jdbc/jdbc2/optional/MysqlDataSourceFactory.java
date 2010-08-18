/*
 Copyright (c) 2002, 2010, Oracle and/or its affiliates. All rights reserved.
 

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
package com.mysql.jdbc.jdbc2.optional;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;

import com.mysql.jdbc.NonRegisteringDriver;

/**
 * Factory class for MysqlDataSource objects
 * 
 * @author Mark Matthews
 */
public class MysqlDataSourceFactory implements ObjectFactory {
	/**
	 * The class name for a standard MySQL DataSource.
	 */
	protected final static String DATA_SOURCE_CLASS_NAME = "com.mysql.jdbc.jdbc2.optional.MysqlDataSource";

	/**
	 * The class name for a poolable MySQL DataSource.
	 */
	protected final static String POOL_DATA_SOURCE_CLASS_NAME = "com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource";

	/**
	 * The class name for a MysqlXADataSource
	 */
	 
	protected final static String XA_DATA_SOURCE_CLASS_NAME = "com.mysql.jdbc.jdbc2.optional.MysqlXADataSource";
	
	/**
	 * DOCUMENT ME!
	 * 
	 * @param refObj
	 *            DOCUMENT ME!
	 * @param nm
	 *            DOCUMENT ME!
	 * @param ctx
	 *            DOCUMENT ME!
	 * @param env
	 *            DOCUMENT ME!
	 * @return DOCUMENT ME!
	 * @throws Exception
	 *             DOCUMENT ME!
	 */
	public Object getObjectInstance(Object refObj, Name nm, Context ctx,
			Hashtable env) throws Exception {
		Reference ref = (Reference) refObj;
		String className = ref.getClassName();

		if ((className != null)
				&& (className.equals(DATA_SOURCE_CLASS_NAME) || className
						.equals(POOL_DATA_SOURCE_CLASS_NAME) ||
						className.equals(XA_DATA_SOURCE_CLASS_NAME))) {
			MysqlDataSource dataSource = null;

			try {
				dataSource = (MysqlDataSource) Class.forName(className)
						.newInstance();
			} catch (Exception ex) {
				throw new RuntimeException("Unable to create DataSource of "
						+ "class '" + className + "', reason: " + ex.toString());
			}

			int portNumber = 3306;

			String portNumberAsString = nullSafeRefAddrStringGet("port", ref);
			
			if (portNumberAsString != null) {
				portNumber = Integer.parseInt(portNumberAsString);
			}

			dataSource.setPort(portNumber);
			
			String user = nullSafeRefAddrStringGet(NonRegisteringDriver.USER_PROPERTY_KEY, ref);

			if (user != null) {
				dataSource.setUser(user);
			}

			String password = nullSafeRefAddrStringGet(NonRegisteringDriver.PASSWORD_PROPERTY_KEY, ref);

			if (password != null) {
				dataSource.setPassword(password);
			}

			String serverName = nullSafeRefAddrStringGet("serverName", ref);

			if (serverName != null) {
				dataSource.setServerName(serverName);
			}

			String databaseName = nullSafeRefAddrStringGet("databaseName", ref);

			if (databaseName != null) {
				dataSource.setDatabaseName(databaseName);
			}

			String explicitUrlAsString = nullSafeRefAddrStringGet("explicitUrl", ref);

			if (explicitUrlAsString != null) {
				if (Boolean.valueOf(explicitUrlAsString).booleanValue()) {
					dataSource.setUrl(nullSafeRefAddrStringGet("url", ref));
				}
			}

			dataSource.setPropertiesViaRef(ref);

			return dataSource;
		}

		// We can't create an instance of the reference
		return null;
	}
	
	private String nullSafeRefAddrStringGet(String referenceName, Reference ref) {
		RefAddr refAddr = ref.get(referenceName);
		
		String asString = refAddr != null ? (String)refAddr.getContent() : null;
		
		return asString;
	}
}