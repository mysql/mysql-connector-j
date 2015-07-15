/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

package testsuite.mysqlx.internal;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.io.Protocol;
import com.mysql.cj.core.conf.DefaultPropertySet;
import com.mysql.cj.mysqla.io.MysqlaSocketConnection;
import com.mysql.cj.mysqlx.io.SyncMessageReader;
import com.mysql.cj.mysqlx.io.SyncMessageWriter;
import com.mysql.cj.mysqlx.io.MysqlxProtocol;

/**
 * Base class for tests of MySQL-X internal components.
 */
public class BaseInternalMysqlxTest {
    public Properties testProperties = new Properties();

    public BaseInternalMysqlxTest() throws Exception {
        InputStream propsFileStream = ClassLoader.getSystemResourceAsStream("test.mysqlx.properties");
        if (propsFileStream == null) {
            throw new Exception("Cannot load test.mysqlx.properties");
        }
        this.testProperties.load(propsFileStream);
    }

    public String getTestHost() {
        return this.testProperties.getProperty("com.mysql.mysqlx.testsuite.host");
    }

    public int getTestPort() {
        return Integer.valueOf(this.testProperties.getProperty("com.mysql.mysqlx.testsuite.port"));
    }

    public String getTestUser() {
        return this.testProperties.getProperty("com.mysql.mysqlx.testsuite.user");
    }

    public String getTestPassword() {
        return this.testProperties.getProperty("com.mysql.mysqlx.testsuite.password");
    }

    public String getTestDatabase() {
        return this.testProperties.getProperty("com.mysql.mysqlx.testsuite.database");
    }

    /**
     * Create a new {@link MysqlxProtocol} instance for testing.
     */
    public MysqlxProtocol getTestProtocol() {
        // TODO: we should share SocketConnection unless there comes a time where they need to diverge
        MysqlaSocketConnection socketConnection = new MysqlaSocketConnection();
        Properties socketFactoryProperties = new Properties();
        // TODO: customize this via props file?
        PropertySet propertySet = new DefaultPropertySet();
        socketConnection.connect(getTestHost(), getTestPort(), socketFactoryProperties, propertySet, null, null, 0);

        SyncMessageReader messageReader = new SyncMessageReader(socketConnection.getMysqlInput());
        SyncMessageWriter messageWriter = new SyncMessageWriter(socketConnection.getMysqlOutput());

        return new MysqlxProtocol(messageReader, messageWriter);
    }

    private MysqlxProtocol getAsyncTestProtocol() {
        try {
            AsynchronousSocketChannel sockChan = AsynchronousSocketChannel.open();

            Future<Void> connectPromise = sockChan.connect(new InetSocketAddress(getTestHost(), getTestPort()));
            connectPromise.get();

            //IMessageReader messageReader = new AsyncMessageReader(sockChan);
            throw new NullPointerException("");
        } catch (IOException | InterruptedException | ExecutionException ex) {
            throw new RuntimeException("unexpected", ex);
        }
    }

    /**
     * Create a new {@link MysqlxProtocol} that is part of an authenicated session.
     */
    public MysqlxProtocol getAuthenticatedTestProtocol() {
        MysqlxProtocol protocol;
        if (false) {
            protocol = getAsyncTestProtocol();
        } else {
            protocol = getTestProtocol();
        }

        protocol.sendSaslMysql41AuthStart();
        byte[] salt = protocol.readAuthenticateContinue();
        protocol.sendSaslMysql41AuthContinue(getTestUser(), getTestPassword(), salt, getTestDatabase());
        
        // not working for some reason
        //protocol.sendSaslAuthStart(getTestUser(), getTestPassword(), getTestDatabase());

        protocol.readAuthenticateOk();

        // TODO: remove this when server bug is fixed
        protocol.sendSqlStatement("use " + getTestDatabase());
        protocol.readStatementExecuteOk();

        return protocol;
    }
}
