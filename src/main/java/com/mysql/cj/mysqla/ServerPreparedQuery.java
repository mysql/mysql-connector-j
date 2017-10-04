/*
  Copyright (c) 2017, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FOSS License Exception
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
import java.io.InputStream;
import java.io.Reader;
import java.sql.SQLException;

import com.mysql.cj.api.PreparedQuery;
import com.mysql.cj.api.mysqla.io.NativeProtocol.IntegerDataType;
import com.mysql.cj.api.mysqla.io.NativeProtocol.StringLengthDataType;
import com.mysql.cj.api.mysqla.io.NativeProtocol.StringSelfDataType;
import com.mysql.cj.api.mysqla.io.PacketPayload;
import com.mysql.cj.api.mysqla.result.ColumnDefinition;
import com.mysql.cj.api.mysqla.result.Resultset;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.result.Field;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.mysqla.io.ColumnDefinitionFactory;

public class ServerPreparedQuery extends ClientPreparedQuery implements PreparedQuery {

    public static final int BLOB_STREAM_READ_BUF_SIZE = 8192;

    /** The ID that the server uses to identify this PreparedStatement */
    private long serverStatementId;

    /** Field-level metadata for parameters */
    private Field[] parameterFields;

    /** Field-level metadata for result sets. It's got from statement prepare. */
    private ColumnDefinition resultFields;

    public static ServerPreparedQuery getInstance(MysqlaSession sess, int statementId) {
        if (sess.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_autoGenerateTestcaseScript).getValue()) {
            return new ServerPreparedQueryTestcaseGenerator(sess, statementId);
        }
        return new ServerPreparedQuery(sess, statementId);
    }

    protected ServerPreparedQuery(MysqlaSession sess, int statementId) {
        super(sess, statementId);
    }

    /**
     * 
     * @param sql
     * @throws IOException
     */
    public void serverPrepare(String sql) throws IOException {
        this.session.checkClosed();

        synchronized (this.session) {
            //long begin = 0;

            this.isLoadDataQuery = StringUtils.startsWithIgnoreCaseAndWs(sql, "LOAD DATA");

            if (this.profileSQL) {
                // TODO
                // begin = System.currentTimeMillis();
            }

            String characterEncoding = null;
            String connectionEncoding = this.session.getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_characterEncoding).getValue();

            if (!this.isLoadDataQuery && (connectionEncoding != null)) {
                characterEncoding = connectionEncoding;
            }

            PacketPayload prepareResultPacket = this.session
                    .sendCommand(this.commandBuilder.buildComStmtPrepare(this.session.getSharedSendPacket(), sql, characterEncoding), false, 0);

            // 4.1.1 and newer use the first byte as an 'ok' or 'error' flag, so move the buffer pointer past it to start reading the statement id.
            prepareResultPacket.setPosition(1);

            this.serverStatementId = prepareResultPacket.readInteger(IntegerDataType.INT4);
            int fieldCount = (int) prepareResultPacket.readInteger(IntegerDataType.INT2);
            setParameterCount((int) prepareResultPacket.readInteger(IntegerDataType.INT2));

            this.queryBindings = new ServerPreparedQueryBindings(this.parameterCount);

            this.session.incrementNumberOfPrepares();

            // TODO
            //            if (this.profileSQL) {
            //                this.eventSink.consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_PREPARE, "", this.getCurrentCatalog(), this.connectionId,
            //                        this.query.getId(), -1, System.currentTimeMillis(), this.session.getCurrentTimeNanosOrMillis() - begin,
            //                        this.session.getQueryTimingUnits(), null, LogUtils.findCallingClassAndMethod(new Throwable()), truncateQueryToLog(sql)));
            //            }

            boolean checkEOF = !this.session.getServerSession().isEOFDeprecated();

            if (this.parameterCount > 0) {
                if (checkEOF) { // Skip the following EOF packet.
                    this.session.getProtocol().skipPacket();
                }

                this.parameterFields = this.session.getProtocol().read(ColumnDefinition.class, new ColumnDefinitionFactory(this.parameterCount, null))
                        .getFields();
            }

            // Read in the result set column information
            if (fieldCount > 0) {
                this.resultFields = this.session.getProtocol().read(ColumnDefinition.class, new ColumnDefinitionFactory(fieldCount, null));
            }
        }
    }

    /**
     * 
     * @param maxRowsToRetrieve
     * @param createStreamingResultSet
     * @param metadata
     * @return
     */
    public Resultset serverExecute(int maxRowsToRetrieve, boolean createStreamingResultSet, ColumnDefinition metadata) {
        // TODO
        return null;
    }

    @Override
    public void closeQuery() {
        this.queryBindings = null;
        this.parameterFields = null;
        this.resultFields = null;
        super.closeQuery();
    }

    public long getServerStatementId() {
        return this.serverStatementId;
    }

    public void setServerStatementId(long serverStatementId) {
        this.serverStatementId = serverStatementId;
    }

    public Field[] getParameterFields() {
        return this.parameterFields;
    }

    public void setParameterFields(Field[] parameterFields) {
        this.parameterFields = parameterFields;
    }

    public ColumnDefinition getResultFields() {
        return this.resultFields;
    }

    public void setResultFields(ColumnDefinition resultFields) {
        this.resultFields = resultFields;
    }

    public void storeStream(int parameterIndex, PacketPayload packet, InputStream inStream) {
        this.session.checkClosed();
        synchronized (this.session) {
            byte[] buf = new byte[BLOB_STREAM_READ_BUF_SIZE];

            int numRead = 0;

            try {
                int bytesInPacket = 0;
                int totalBytesRead = 0;
                int bytesReadAtLastSend = 0;
                int packetIsFullAt = this.session.getPropertySet().getMemorySizeReadableProperty(PropertyDefinitions.PNAME_blobSendChunkSize).getValue();

                packet.setPosition(0);
                packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_STMT_SEND_LONG_DATA);
                packet.writeInteger(IntegerDataType.INT4, this.serverStatementId);
                packet.writeInteger(IntegerDataType.INT2, parameterIndex);

                boolean readAny = false;

                while ((numRead = inStream.read(buf)) != -1) {

                    readAny = true;

                    packet.writeBytes(StringLengthDataType.STRING_FIXED, buf, 0, numRead);
                    bytesInPacket += numRead;
                    totalBytesRead += numRead;

                    if (bytesInPacket >= packetIsFullAt) {
                        bytesReadAtLastSend = totalBytesRead;

                        this.session.sendCommand(packet, true, 0);

                        bytesInPacket = 0;
                        packet.setPosition(0);
                        packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_STMT_SEND_LONG_DATA);
                        packet.writeInteger(IntegerDataType.INT4, this.serverStatementId);
                        packet.writeInteger(IntegerDataType.INT2, parameterIndex);
                    }
                }

                if (totalBytesRead != bytesReadAtLastSend) {
                    this.session.sendCommand(packet, true, 0);
                }

                if (!readAny) {
                    this.session.sendCommand(packet, true, 0);
                }
            } catch (IOException ioEx) {
                throw ExceptionFactory.createException(Messages.getString("ServerPreparedStatement.25") + ioEx.toString(), ioEx,
                        this.session.getExceptionInterceptor());
            } finally {
                if (this.autoClosePStmtStreams.getValue()) {
                    if (inStream != null) {
                        try {
                            inStream.close();
                        } catch (IOException ioEx) {
                            // ignore
                        }
                    }
                }
            }
        }
    }

    // TODO: Investigate using NIO to do this faster
    public void storeReader(int parameterIndex, PacketPayload packet, Reader inStream) {
        this.session.checkClosed();
        synchronized (this.session) {
            String forcedEncoding = this.session.getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_clobCharacterEncoding).getStringValue();

            String clobEncoding = (forcedEncoding == null
                    ? this.session.getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_characterEncoding).getValue() : forcedEncoding);

            int maxBytesChar = 2;

            if (clobEncoding != null) {
                if (!clobEncoding.equals("UTF-16")) {
                    maxBytesChar = this.session.getMaxBytesPerChar(clobEncoding);

                    if (maxBytesChar == 1) {
                        maxBytesChar = 2; // for safety
                    }
                } else {
                    maxBytesChar = 4;
                }
            }

            char[] buf = new char[ServerPreparedQuery.BLOB_STREAM_READ_BUF_SIZE / maxBytesChar];

            int numRead = 0;

            int bytesInPacket = 0;
            int totalBytesRead = 0;
            int bytesReadAtLastSend = 0;
            int packetIsFullAt = this.session.getPropertySet().getMemorySizeReadableProperty(PropertyDefinitions.PNAME_blobSendChunkSize).getValue();

            try {
                packet.setPosition(0);
                packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_STMT_SEND_LONG_DATA);
                packet.writeInteger(IntegerDataType.INT4, this.serverStatementId);
                packet.writeInteger(IntegerDataType.INT2, parameterIndex);

                boolean readAny = false;

                while ((numRead = inStream.read(buf)) != -1) {
                    readAny = true;

                    byte[] valueAsBytes = StringUtils.getBytes(buf, 0, numRead, clobEncoding);

                    packet.writeBytes(StringSelfDataType.STRING_EOF, valueAsBytes);

                    bytesInPacket += valueAsBytes.length;
                    totalBytesRead += valueAsBytes.length;

                    if (bytesInPacket >= packetIsFullAt) {
                        bytesReadAtLastSend = totalBytesRead;

                        this.session.sendCommand(packet, true, 0);

                        bytesInPacket = 0;
                        packet.setPosition(0);
                        packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_STMT_SEND_LONG_DATA);
                        packet.writeInteger(IntegerDataType.INT4, this.serverStatementId);
                        packet.writeInteger(IntegerDataType.INT2, parameterIndex);
                    }
                }

                if (totalBytesRead != bytesReadAtLastSend) {
                    this.session.sendCommand(packet, true, 0);
                }

                if (!readAny) {
                    this.session.sendCommand(packet, true, 0);
                }
            } catch (IOException ioEx) {
                throw ExceptionFactory.createException(Messages.getString("ServerPreparedStatement.24") + ioEx.toString(), ioEx,
                        this.session.getExceptionInterceptor());
            } finally {
                if (this.autoClosePStmtStreams.getValue()) {
                    if (inStream != null) {
                        try {
                            inStream.close();
                        } catch (IOException ioEx) {
                            // ignore
                        }
                    }
                }
            }
        }
    }

    /**
     * 
     * @param clearServerParameters
     * @return Whether or not the long parameters have been 'switched' back to normal parameters. We can not execute() if clearParameters() hasn't been called
     *         in this case.
     */
    public boolean clearParametersInternal(boolean clearServerParameters) {
        boolean hadLongData = false;

        if (this.queryBindings != null) {
            hadLongData = this.queryBindings.clearBindValues();
        }

        if (clearServerParameters && hadLongData) {
            serverResetStatement();
            return false;
        }

        return true;
    }

    public void serverResetStatement() {
        this.session.checkClosed();
        synchronized (this.session) {
            try {
                this.session.sendCommand(this.commandBuilder.buildComStmtReset(this.session.getSharedSendPacket(), this.serverStatementId), false, 0);
            } finally {
                this.session.clearInputStream();
            }
        }
    }

    /**
     * Computes the maximum parameter set size, and entire batch size given
     * the number of arguments in the batch.
     * 
     * @throws SQLException
     */
    @Override
    protected long[] computeMaxParameterSetSizeAndBatchSize(int numBatchedArgs) {

        long sizeOfEntireBatch = 1 + /* com_execute */+4 /* stmt id */ + 1 /* flags */ + 4 /* batch count padding */;
        long maxSizeOfParameterSet = 0;

        for (int i = 0; i < numBatchedArgs; i++) {
            ServerPreparedQueryBindValue[] paramArg = ((ServerPreparedQueryBindings) this.batchedArgs.get(i)).getBindValues();

            long sizeOfParameterSet = (this.parameterCount + 7) / 8; // for isNull
            sizeOfParameterSet += this.parameterCount * 2; // have to send types

            ServerPreparedQueryBindValue[] parameterBindings = this.queryBindings.getBindValues();
            for (int j = 0; j < parameterBindings.length; j++) {
                if (!paramArg[j].isNull()) {

                    long size = paramArg[j].getBoundLength();

                    if (paramArg[j].isLongData) {
                        if (size != -1) {
                            sizeOfParameterSet += size;
                        }
                    } else {
                        sizeOfParameterSet += size;
                    }
                }
            }

            sizeOfEntireBatch += sizeOfParameterSet;

            if (sizeOfParameterSet > maxSizeOfParameterSet) {
                maxSizeOfParameterSet = sizeOfParameterSet;
            }
        }

        return new long[] { maxSizeOfParameterSet, sizeOfEntireBatch };
    }
}
