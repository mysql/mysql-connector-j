/*
   Copyright (C) 2002 MySQL AB
   
      This program is free software; you can redistribute it and/or modify
      it under the terms of the GNU General Public License as published by
      the Free Software Foundation; either version 2 of the License, or
      (at your option) any later version.
   
      This program is distributed in the hope that it will be useful,
      but WITHOUT ANY WARRANTY; without even the implied warranty of
      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
      GNU General Public License for more details.
   
      You should have received a copy of the GNU General Public License
      along with this program; if not, write to the Free Software
      Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
      
 */
package com.mysql.jdbc.jdbc2.optional;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;


/**
 * Factory class for MysqlDataSource objects
 */
public class MysqlDataSourceFactory
    implements ObjectFactory {

    //~ Instance/static variables .............................................

    /**
     * The class name for a standard Mysql DataSource.
     */
    protected final String dataSourceClassName = "com.mysql.jdbc.jdbc2.optional.MysqlDataSource";
    protected final String poolDataSourceName = "com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource";

    //~ Methods ...............................................................

    /**
     * DOCUMENT ME!
     * 
     * @param refObj DOCUMENT ME!
     * @param nm DOCUMENT ME!
     * @param ctx DOCUMENT ME!
     * @param env DOCUMENT ME!
     * @return DOCUMENT ME! 
     * @throws Exception DOCUMENT ME!
     */
    public Object getObjectInstance(Object refObj, Name nm, Context ctx, 
                                    Hashtable env)
                             throws Exception {

        Reference ref = (Reference) refObj;
        String className = ref.getClassName();

        if (className != null
            && (className.equals(dataSourceClassName) || className.equals(
                                                                 poolDataSourceName))) {

            MysqlDataSource dataSource = null;

            try {
                dataSource = (MysqlDataSource) Class.forName(className).newInstance();
            } catch (Exception ex) {
                throw new RuntimeException("Unable to create DataSource of "
                                           + "class '" + className
                                           + "', reason: " + ex.toString());
            }

            int portNumber = 3306;
            portNumber = Integer.parseInt((String) ref.get("port").getContent());
            dataSource.setPort(portNumber);
            dataSource.setUser((String) ref.get("user").getContent());
            dataSource.setPassword((String) ref.get("password").getContent());
            dataSource.setServerName(
                    (String) ref.get("serverName").getContent());
            dataSource.setDatabaseName(
                    (String) ref.get("databaseName").getContent());

            return dataSource;
        } else { // We can't create an instance of the reference

            return null;
        }
    }
}