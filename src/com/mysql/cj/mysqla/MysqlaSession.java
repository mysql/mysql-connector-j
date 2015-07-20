/*
2  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.mysqla;

import java.io.IOException;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import com.mysql.cj.api.MysqlConnection;
import com.mysql.cj.api.Session;
import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.io.SocketConnection;
import com.mysql.cj.api.log.Log;
import com.mysql.cj.core.AbstractSession;
import com.mysql.cj.core.ConnectionString;
import com.mysql.cj.core.ConnectionString.HostInfo;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.ServerVersion;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exceptions.CJException;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.core.log.LogFactory;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.jdbc.util.TimeUtil;
import com.mysql.cj.mysqla.io.Buffer;
import com.mysql.cj.mysqla.io.MysqlaProtocol;
import com.mysql.cj.mysqla.io.MysqlaSocketConnection;

public class MysqlaSession extends AbstractSession implements Session {

    protected transient MysqlaProtocol protocol;

    /** The timezone of the server */
    private TimeZone serverTimezoneTZ = null;

    /** c.f. getDefaultTimeZone(). this value may be overridden during connection initialization */
    private TimeZone defaultTimeZone = TimeZone.getDefault();

    /** The max-rows setting for current session */
    private int sessionMaxRows = -1;

    private HostInfo hostInfo = null;

    private String hostToConnectTo = "localhost";
    private int portToConnectTo = 3306;

    public MysqlaSession(ConnectionString connectionString, String hostToConnectTo, int portToConnectTo, Properties info, String databaseToConnectTo,
            String url, PropertySet propSet) {
        this.propertySet = propSet;

        this.hostToConnectTo = hostToConnectTo;
        this.portToConnectTo = portToConnectTo;

        this.hostInfo = connectionString.getHostInfo(this.propertySet, this.hostToConnectTo, this.portToConnectTo, info);

        //
        // Normally, this code would be in initializeDriverProperties, but we need to do this as early as possible, so we can start logging to the 'correct'
        // place as early as possible...this.log points to 'NullLogger' for every connection at startup to avoid NPEs and the overhead of checking for NULL at
        // every logging call.
        //
        // We will reset this to the configured logger during properties initialization.
        //
        this.log = LogFactory.getLogger(getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_logger).getStringValue(),
                Log.LOGGER_INSTANCE_NAME, getExceptionInterceptor());

        if (ConnectionString.isHostPropertiesList(hostToConnectTo)) {
            Properties hostSpecificProps = ConnectionString.expandHostKeyValues(hostToConnectTo);

            Enumeration<?> propertyNames = hostSpecificProps.propertyNames();

            while (propertyNames.hasMoreElements()) {
                String propertyName = propertyNames.nextElement().toString();
                String propertyValue = hostSpecificProps.getProperty(propertyName);

                info.setProperty(propertyName, propertyValue);
            }
        } else {
        }

    }

    public void connect(MysqlConnection conn, ConnectionString connectionString, Properties mergedProps, String user, String password, String database,
            int loginTimeout) throws IOException {

        this.hostInfo = connectionString.getHostInfo(this.propertySet, this.hostToConnectTo, this.portToConnectTo, mergedProps);

        // reset max-rows to default value
        this.setSessionMaxRows(-1);

        // TODO do we need different types of physical connections?
        SocketConnection socketConnection = new MysqlaSocketConnection();
        socketConnection.connect(this.hostInfo.getHost(), this.hostInfo.getPort(), mergedProps, getPropertySet(), getExceptionInterceptor(), this.log,
                loginTimeout);

        // we use physical connection to create a -> protocol
        // this configuration places no knowledge of protocol or session on physical connection.
        // physical connection is responsible *only* for I/O streams
        this.protocol = MysqlaProtocol.getInstance(conn, socketConnection, this.propertySet, this.log);

        // use protocol to create a -> session
        // protocol is responsible for building a session and authenticating (using AuthenticationProvider) internally
        this.protocol.connect(user, password, database);

        // error messages are returned according to character_set_results which, at this point, is set from the response packet
        setErrorMessageEncoding(this.protocol.getAuthenticationProvider().getEncodingForHandshake());
    }

    public MysqlaProtocol getProtocol() {
        return this.protocol;
    }

    @Override
    public void changeUser(String userName, String password, String database) {
        // reset maxRows to default value
        this.sessionMaxRows = -1;

        this.protocol.changeUser(userName, password, database);
    }

    @Override
    public boolean characterSetNamesMatches(String mysqlEncodingName) {
        return this.protocol.getServerSession().characterSetNamesMatches(mysqlEncodingName);
    }

    @Override
    public String getServerVariable(String name) {
        return this.protocol.getServerSession().getServerVariable(name);
    }

    @Override
    public int getServerVariable(String variableName, int fallbackValue) {

        try {
            return Integer.valueOf(getServerVariable(variableName));
        } catch (NumberFormatException nfe) {
            getLog().logWarn(
                    Messages.getString("Connection.BadValueInServerVariables", new Object[] { variableName, getServerVariable(variableName), fallbackValue }));

        }
        return fallbackValue;
    }

    @Override
    public boolean inTransactionOnServer() {
        return this.protocol.getServerSession().inTransactionOnServer();
    }

    @Override
    public int getServerDefaultCollationIndex() {
        return this.protocol.getServerSession().getServerDefaultCollationIndex();
    }

    @Override
    public void setServerDefaultCollationIndex(int serverDefaultCollationIndex) {
        this.protocol.getServerSession().setServerDefaultCollationIndex(serverDefaultCollationIndex);
    }

    @Override
    public Map<String, String> getServerVariables() {
        return this.protocol.getServerSession().getServerVariables();
    }

    @Override
    public void setServerVariables(Map<String, String> serverVariables) {
        this.protocol.getServerSession().setServerVariables(serverVariables);
    }

    // TODO: we should examine the call flow here, we shouldn't have to know about the socket connection but this should be address in a wider scope.
    @Override
    public void abortInternal() {
        if (this.protocol != null) {
            try {
                this.protocol.getSocketConnection().forceClose();
            } catch (Throwable t) {
                // can't do anything about it, and we're forcibly aborting
            }
            this.protocol.releaseResources();
            this.protocol = null;
        }
        //this.isClosed = true;
    }

    @Override
    public void quit() {
        if (this.protocol != null) {
            try {
                this.protocol.quit();
            } catch (Exception e) {
            }

        }
    }

    @Override
    public void forceClose() {
        abortInternal();
    }

    @Override
    public ServerVersion getServerVersion() {
        return this.protocol.getServerSession().getServerVersion();
    }

    @Override
    public boolean versionMeetsMinimum(int major, int minor, int subminor) {
        return this.protocol.versionMeetsMinimum(major, minor, subminor);
    }

    public void enableMultiQueries() {
        Buffer buf = this.protocol.getSharedSendPacket();

        buf.writeByte((byte) MysqlaConstants.COM_SET_OPTION);
        buf.writeInt(0);
        this.protocol.sendCommand(MysqlaConstants.COM_SET_OPTION, null, buf, false, null, 0);
    }

    public void disableMultiQueries() {
        Buffer buf = this.protocol.getSharedSendPacket();

        buf.writeByte((byte) MysqlaConstants.COM_SET_OPTION);
        buf.writeInt(1);
        this.protocol.sendCommand(MysqlaConstants.COM_SET_OPTION, null, buf, false, null, 0);
    }

    @Override
    public long getThreadId() {
        return this.protocol.getServerSession().getCapabilities().getThreadId();
    }

    @Override
    public boolean isSetNeededForAutoCommitMode(boolean autoCommitFlag) {
        return this.protocol.getServerSession().isSetNeededForAutoCommitMode(autoCommitFlag,
                getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_elideSetAutoCommits).getValue());
    }

    /**
     * Configures the client's timezone if required.
     * 
     * @throws CJException
     *             if the timezone the server is configured to use can't be
     *             mapped to a Java timezone.
     */
    public void configureTimezone() {
        String configuredTimeZoneOnServer = getServerVariable("timezone");

        if (configuredTimeZoneOnServer == null) {
            configuredTimeZoneOnServer = getServerVariable("time_zone");

            if ("SYSTEM".equalsIgnoreCase(configuredTimeZoneOnServer)) {
                configuredTimeZoneOnServer = getServerVariable("system_time_zone");
            }
        }

        String canonicalTimezone = getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_serverTimezone).getValue();

        if (configuredTimeZoneOnServer != null) {
            // user can override this with driver properties, so don't detect if that's the case
            if (canonicalTimezone == null || StringUtils.isEmptyOrWhitespaceOnly(canonicalTimezone)) {
                try {
                    canonicalTimezone = TimeUtil.getCanonicalTimezone(configuredTimeZoneOnServer, getExceptionInterceptor());
                } catch (IllegalArgumentException iae) {
                    throw ExceptionFactory.createException(WrongArgumentException.class, iae.getMessage(), getExceptionInterceptor());
                }
            }
        }

        if (canonicalTimezone != null && canonicalTimezone.length() > 0) {
            this.serverTimezoneTZ = TimeZone.getTimeZone(canonicalTimezone);

            //
            // The Calendar class has the behavior of mapping unknown timezones to 'GMT' instead of throwing an exception, so we must check for this...
            //
            if (!canonicalTimezone.equalsIgnoreCase("GMT") && this.serverTimezoneTZ.getID().equals("GMT")) {
                throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("Connection.9", new Object[] { canonicalTimezone }),
                        getExceptionInterceptor());
            }
        }

        this.defaultTimeZone = this.serverTimezoneTZ;
    }

    public TimeZone getDefaultTimeZone() {
        return this.defaultTimeZone;
    }

    @Override
    public String getErrorMessageEncoding() {
        return this.protocol.getServerSession().getErrorMessageEncoding();
    }

    @Override
    public void setErrorMessageEncoding(String errorMessageEncoding) {
        this.protocol.getServerSession().setErrorMessageEncoding(errorMessageEncoding);
    }

    /**
     * Returns the server's character set
     * 
     * @return the server's character set.
     */
    public String getServerCharset() {
        return this.protocol.getServerSession().getServerDefaultCharset();
    }

    public int getMaxBytesPerChar(String javaCharsetName) {
        return this.protocol.getServerSession().getMaxBytesPerChar(javaCharsetName);
    }

    public int getMaxBytesPerChar(Integer charsetIndex, String javaCharsetName) {
        return this.protocol.getServerSession().getMaxBytesPerChar(charsetIndex, javaCharsetName);
    }

    public String getEncodingForIndex(int charsetIndex) {
        return this.protocol.getServerSession().getEncodingForIndex(charsetIndex);
    }

    public int getSessionMaxRows() {
        return this.sessionMaxRows;
    }

    public void setSessionMaxRows(int sessionMaxRows) {
        this.sessionMaxRows = sessionMaxRows;
    }

    public HostInfo getHostInfo() {
        return this.hostInfo;
    }

}
