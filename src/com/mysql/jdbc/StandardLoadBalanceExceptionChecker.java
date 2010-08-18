/*
  Copyright (c) 2010, Oracle and/or its affiliates. All rights reserved.

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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class StandardLoadBalanceExceptionChecker implements
		LoadBalanceExceptionChecker {
	
	private List sqlStateList;
	private List sqlExClassList;
	
	public boolean shouldExceptionTriggerFailover(SQLException ex) {
		String sqlState = ex.getSQLState();

		if (sqlState != null) {
			if (sqlState.startsWith("08")) {
				// connection error
				return true;
			}
			if(this.sqlStateList != null){
				// check against SQLState list
				for(Iterator i = sqlStateList.iterator(); i.hasNext(); ){
					if(sqlState.startsWith(i.next().toString())){
						return true;
					}
				}
			}
		}
		
		// always handle CommunicationException
		if(ex instanceof CommunicationsException){
			return true;
		}
		if(this.sqlExClassList != null){
			// check against configured class lists
			for(Iterator i = sqlExClassList.iterator(); i.hasNext(); ){
				if(((Class)i.next()).isInstance(ex)){
					return true;
				}
			}
		}
		// no matches
		return false;

	}

	public void destroy() {
		// TODO Auto-generated method stub

	}

	public void init(Connection conn, Properties props) throws SQLException {
		configureSQLStateList(props.getProperty("loadBalanceSQLStateFailover", null));
		configureSQLExceptionSubclassList(props.getProperty("loadBalanceSQLExceptionSubclassFailover", null));

	}
	
	private void configureSQLStateList(String sqlStates){
		if(sqlStates == null || "".equals(sqlStates)){
			return;
		}
		List states = StringUtils.split(sqlStates, ",", true);
		List newStates = new ArrayList();
		Iterator i = states.iterator();
		
		while(i.hasNext()){
			String state = i.next().toString();
			if(state.length() > 0){
				newStates.add(state);
			}
		}
		if(newStates.size() > 0){
			this.sqlStateList = newStates;
		}
		
	}
	private void configureSQLExceptionSubclassList(String sqlExClasses){
		if(sqlExClasses == null || "".equals(sqlExClasses)){
			return;
		}
		List classes = StringUtils.split(sqlExClasses, ",", true);
		List newClasses = new ArrayList();
		Iterator i = classes.iterator();
		
		while(i.hasNext()){
			String exClass = i.next().toString();
			try{
				Class c = Class.forName(exClass);
				newClasses.add(c);
			} catch (Exception e){ 
				// ignore and don't check, class doesn't exist
			}
		}
		if(newClasses.size() > 0){
			this.sqlExClassList = newClasses;
		}
		
	}

}
