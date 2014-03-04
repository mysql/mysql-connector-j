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

package testsuite.simple;

import java.sql.SQLException;
import java.util.Properties;

import com.mysql.jdbc.ConnectionPropertiesTransform;
import com.mysql.jdbc.NonRegisteringDriver;

/**
 * Used for testing the ConnectionPropertiesTransform functionality.
 * 
 * @author Mark Matthews
 * 
 * @version $Id: SimpleTransformer.java,v 1.1.2.1 2005/05/13 18:58:37 mmatthews
 *          Exp $
 */
public class SimpleTransformer implements ConnectionPropertiesTransform {

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.mysql.jdbc.ConnectionPropertiesTransform#transformProperties(java.util.Properties)
	 */
	public Properties transformProperties(Properties props) throws SQLException {
		props
				.setProperty(NonRegisteringDriver.HOST_PROPERTY_KEY,
						"albequerque");

		return props;
	}

}
