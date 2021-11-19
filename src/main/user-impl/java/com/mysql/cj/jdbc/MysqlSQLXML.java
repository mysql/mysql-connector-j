/*
 * Copyright (c) 2002, 2021, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.mysql.cj.jdbc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.sql.SQLException;
import java.sql.SQLXML;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stax.StAXResult;
import javax.xml.transform.stax.StAXSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

import com.mysql.cj.Messages;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.jdbc.result.ResultSetInternalMethods;

public class MysqlSQLXML implements SQLXML {

    private XMLInputFactory inputFactory;

    private XMLOutputFactory outputFactory;

    private String stringRep;

    private ResultSetInternalMethods owningResultSet;

    private int columnIndexOfXml;

    private boolean fromResultSet;

    private boolean isClosed = false;

    private boolean workingWithResult;

    private DOMResult asDOMResult;

    private SAXResult asSAXResult;

    private SimpleSaxToReader saxToReaderConverter;

    private StringWriter asStringWriter;

    private ByteArrayOutputStream asByteArrayOutputStream;

    private ExceptionInterceptor exceptionInterceptor;

    public MysqlSQLXML(ResultSetInternalMethods owner, int index, ExceptionInterceptor exceptionInterceptor) {
        this.owningResultSet = owner;
        this.columnIndexOfXml = index;
        this.fromResultSet = true;
        this.exceptionInterceptor = exceptionInterceptor;
    }

    public MysqlSQLXML(ExceptionInterceptor exceptionInterceptor) {
        this.fromResultSet = false;
        this.exceptionInterceptor = exceptionInterceptor;
    }

    @Override
    public synchronized void free() throws SQLException {
        this.stringRep = null;
        this.asDOMResult = null;
        this.asSAXResult = null;
        this.inputFactory = null;
        this.outputFactory = null;
        this.owningResultSet = null;
        this.workingWithResult = false;
        this.isClosed = true;

    }

    @Override
    public synchronized String getString() throws SQLException {
        checkClosed();
        checkWorkingWithResult();

        if (this.fromResultSet) {
            return this.owningResultSet.getString(this.columnIndexOfXml);
        }

        return this.stringRep;
    }

    private synchronized void checkClosed() throws SQLException {
        if (this.isClosed) {
            throw SQLError.createSQLException(Messages.getString("MysqlSQLXML.0"), this.exceptionInterceptor);
        }
    }

    private synchronized void checkWorkingWithResult() throws SQLException {
        if (this.workingWithResult) {
            throw SQLError.createSQLException(Messages.getString("MysqlSQLXML.1"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
        }
    }

    @Override
    public synchronized void setString(String str) throws SQLException {
        checkClosed();
        checkWorkingWithResult();

        this.stringRep = str;
        this.fromResultSet = false;
    }

    public synchronized boolean isEmpty() throws SQLException {
        checkClosed();
        checkWorkingWithResult();

        if (!this.fromResultSet) {
            return this.stringRep == null || this.stringRep.length() == 0;
        }

        return false;
    }

    @Override
    public synchronized InputStream getBinaryStream() throws SQLException {
        checkClosed();
        checkWorkingWithResult();

        return this.owningResultSet.getBinaryStream(this.columnIndexOfXml);
    }

    @Override
    public synchronized Reader getCharacterStream() throws SQLException {
        checkClosed();
        checkWorkingWithResult();

        return this.owningResultSet.getCharacterStream(this.columnIndexOfXml);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Source> T getSource(Class<T> clazz) throws SQLException {
        checkClosed();
        checkWorkingWithResult();

        // Note that we try and use streams here wherever possible for the day that the server actually supports streaming from server -> client
        // (futureproofing)

        if (clazz == null || clazz.equals(SAXSource.class)) {

            try {
                XMLReader reader = XMLReaderFactory.createXMLReader();
                // According to https://cheatsheetseries.owasp.org/cheatsheets/XML_External_Entity_Prevention_Cheat_Sheet.html
                reader.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
                setFeature(reader, "http://apache.org/xml/features/disallow-doctype-decl", true);
                setFeature(reader, "http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
                setFeature(reader, "http://xml.org/sax/features/external-general-entities", false);
                setFeature(reader, "http://xml.org/sax/features/external-parameter-entities", false);

                return (T) new SAXSource(reader, this.fromResultSet ? new InputSource(this.owningResultSet.getCharacterStream(this.columnIndexOfXml))
                        : new InputSource(new StringReader(this.stringRep)));
            } catch (SAXException ex) {
                SQLException sqlEx = SQLError.createSQLException(ex.getMessage(), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, ex, this.exceptionInterceptor);
                throw sqlEx;
            }

        } else if (clazz.equals(DOMSource.class)) {
            try {
                DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
                builderFactory.setNamespaceAware(true);

                // According to https://cheatsheetseries.owasp.org/cheatsheets/XML_External_Entity_Prevention_Cheat_Sheet.html
                setFeature(builderFactory, XMLConstants.FEATURE_SECURE_PROCESSING, true);
                setFeature(builderFactory, "http://apache.org/xml/features/disallow-doctype-decl", true);
                setFeature(builderFactory, "http://xml.org/sax/features/external-general-entities", false);
                setFeature(builderFactory, "http://xml.org/sax/features/external-parameter-entities", false);
                setFeature(builderFactory, "http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
                builderFactory.setXIncludeAware(false);
                builderFactory.setExpandEntityReferences(false);

                builderFactory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");

                DocumentBuilder builder = builderFactory.newDocumentBuilder();

                return (T) new DOMSource(builder.parse(this.fromResultSet ? new InputSource(this.owningResultSet.getCharacterStream(this.columnIndexOfXml))
                        : new InputSource(new StringReader(this.stringRep))));
            } catch (Throwable t) {
                SQLException sqlEx = SQLError.createSQLException(t.getMessage(), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, t, this.exceptionInterceptor);
                throw sqlEx;
            }

        } else if (clazz.equals(StreamSource.class)) {
            return (T) new StreamSource(this.fromResultSet ? this.owningResultSet.getCharacterStream(this.columnIndexOfXml) : new StringReader(this.stringRep));

        } else if (clazz.equals(StAXSource.class)) {
            try {
                return (T) new StAXSource(this.inputFactory.createXMLStreamReader(
                        this.fromResultSet ? this.owningResultSet.getCharacterStream(this.columnIndexOfXml) : new StringReader(this.stringRep)));
            } catch (XMLStreamException ex) {
                SQLException sqlEx = SQLError.createSQLException(ex.getMessage(), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, ex, this.exceptionInterceptor);
                throw sqlEx;
            }
        } else {
            throw SQLError.createSQLException(Messages.getString("MysqlSQLXML.2", new Object[] { clazz.toString() }),
                    MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
        }
    }

    private static void setFeature(Object factory, String name, boolean value) {
        try {
            if (factory instanceof DocumentBuilderFactory) {
                ((DocumentBuilderFactory) factory).setFeature(name, value);
            } else if (factory instanceof XMLReader) {
                ((XMLReader) factory).setFeature(name, value);
            }
        } catch (Exception ignore) {
            // no-op
        }
    }

    @Override
    public synchronized OutputStream setBinaryStream() throws SQLException {
        checkClosed();
        checkWorkingWithResult();

        this.workingWithResult = true;

        return setBinaryStreamInternal();
    }

    private synchronized OutputStream setBinaryStreamInternal() throws SQLException {
        this.asByteArrayOutputStream = new ByteArrayOutputStream();

        return this.asByteArrayOutputStream;
    }

    @Override
    public synchronized Writer setCharacterStream() throws SQLException {
        checkClosed();
        checkWorkingWithResult();

        this.workingWithResult = true;

        return setCharacterStreamInternal();
    }

    private synchronized Writer setCharacterStreamInternal() throws SQLException {
        this.asStringWriter = new StringWriter();

        return this.asStringWriter;
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized <T extends Result> T setResult(Class<T> clazz) throws SQLException {
        checkClosed();
        checkWorkingWithResult();

        this.workingWithResult = true;
        this.asDOMResult = null;
        this.asSAXResult = null;
        this.saxToReaderConverter = null;
        this.stringRep = null;
        this.asStringWriter = null;
        this.asByteArrayOutputStream = null;

        if (clazz == null || clazz.equals(SAXResult.class)) {
            this.saxToReaderConverter = new SimpleSaxToReader();

            this.asSAXResult = new SAXResult(this.saxToReaderConverter);

            return (T) this.asSAXResult;
        } else if (clazz.equals(DOMResult.class)) {

            this.asDOMResult = new DOMResult();
            return (T) this.asDOMResult;

        } else if (clazz.equals(StreamResult.class)) {
            return (T) new StreamResult(setCharacterStreamInternal());
        } else if (clazz.equals(StAXResult.class)) {
            try {
                if (this.outputFactory == null) {
                    this.outputFactory = XMLOutputFactory.newInstance();
                }

                return (T) new StAXResult(this.outputFactory.createXMLEventWriter(setCharacterStreamInternal()));
            } catch (XMLStreamException ex) {
                SQLException sqlEx = SQLError.createSQLException(ex.getMessage(), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, ex, this.exceptionInterceptor);
                throw sqlEx;
            }
        } else {
            throw SQLError.createSQLException(Messages.getString("MysqlSQLXML.3", new Object[] { clazz.toString() }),
                    MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
        }
    }

    private Reader binaryInputStreamStreamToReader(ByteArrayOutputStream out) {

        try {
            // There's got to be an easier way to do this, but I don't feel like coding up Appendix F of the XML Spec myself, when there's a reusable way to do
            // it, and we can warn folks away from BINARY xml streams that have to be parsed to determine the character encoding :P

            String encoding = "UTF-8";

            try {
                ByteArrayInputStream bIn = new ByteArrayInputStream(out.toByteArray());
                XMLStreamReader reader = this.inputFactory.createXMLStreamReader(bIn);

                int eventType = 0;

                while ((eventType = reader.next()) != XMLStreamConstants.END_DOCUMENT) {
                    if (eventType == XMLStreamConstants.START_DOCUMENT) {
                        String possibleEncoding = reader.getEncoding();

                        if (possibleEncoding != null) {
                            encoding = possibleEncoding;
                        }

                        break;
                    }
                }
            } catch (Throwable t) {
                // ignore, dealt with later when the string can't be parsed into valid XML
            }

            return new StringReader(new String(out.toByteArray(), encoding));
        } catch (UnsupportedEncodingException badEnc) {
            throw new RuntimeException(badEnc);
        }
    }

    protected String readerToString(Reader reader) throws SQLException {
        StringBuilder buf = new StringBuilder();

        int charsRead = 0;

        char[] charBuf = new char[512];

        try {
            while ((charsRead = reader.read(charBuf)) != -1) {
                buf.append(charBuf, 0, charsRead);
            }
        } catch (IOException ioEx) {
            SQLException sqlEx = SQLError.createSQLException(ioEx.getMessage(), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, ioEx, this.exceptionInterceptor);
            throw sqlEx;
        }

        return buf.toString();
    }

    protected synchronized Reader serializeAsCharacterStream() throws SQLException {
        checkClosed();
        if (this.workingWithResult || this.owningResultSet == null) {
            // figure out what kind of result
            if (this.stringRep != null) {
                return new StringReader(this.stringRep);
            }

            if (this.asDOMResult != null) {
                return new StringReader(domSourceToString());
            }

            if (this.asStringWriter != null) { // stax result
                return new StringReader(this.asStringWriter.toString());
            }

            if (this.asSAXResult != null) {
                return this.saxToReaderConverter.toReader();
            }

            if (this.asByteArrayOutputStream != null) {
                return binaryInputStreamStreamToReader(this.asByteArrayOutputStream);
            }
        }

        return this.owningResultSet.getCharacterStream(this.columnIndexOfXml);
    }

    protected String domSourceToString() throws SQLException {
        try {
            DOMSource source = new DOMSource(this.asDOMResult.getNode());
            Transformer identity = TransformerFactory.newInstance().newTransformer();
            StringWriter stringOut = new StringWriter();
            Result result = new StreamResult(stringOut);
            identity.transform(source, result);

            return stringOut.toString();
        } catch (Throwable t) {
            SQLException sqlEx = SQLError.createSQLException(t.getMessage(), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, t, this.exceptionInterceptor);
            throw sqlEx;
        }
    }

    protected synchronized String serializeAsString() throws SQLException {
        checkClosed();
        if (this.workingWithResult) {
            // figure out what kind of result
            if (this.stringRep != null) {
                return this.stringRep;
            }

            if (this.asDOMResult != null) {
                return domSourceToString();
            }

            if (this.asStringWriter != null) { // stax result
                return this.asStringWriter.toString();
            }

            if (this.asSAXResult != null) {
                return readerToString(this.saxToReaderConverter.toReader());
            }

            if (this.asByteArrayOutputStream != null) {
                return readerToString(binaryInputStreamStreamToReader(this.asByteArrayOutputStream));
            }
        }

        return this.owningResultSet.getString(this.columnIndexOfXml);
    }

    /*
     * The SimpleSaxToReader class is an adaptation of the SAX "Writer"
     * example from the Apache XercesJ-2 Project. The license for this
     * code is as follows:
     * 
     * Licensed to the Apache Software Foundation (ASF) under one or more
     * contributor license agreements. See the NOTICE file distributed with
     * this work for additional information regarding copyright ownership.
     * The ASF licenses this file to You under the Apache License, Version 2.0
     * (the "License"); you may not use this file except in compliance with
     * the License. You may obtain a copy of the License at
     * 
     * http://www.apache.org/licenses/LICENSE-2.0
     * 
     * Unless required by applicable law or agreed to in writing, software
     * distributed under the License is distributed on an "AS IS" BASIS,
     * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     * See the License for the specific language governing permissions and
     * limitations under the License.
     */

    class SimpleSaxToReader extends DefaultHandler {
        StringBuilder buf = new StringBuilder();

        @Override
        public void startDocument() throws SAXException {
            this.buf.append("<?xml version='1.0' encoding='UTF-8'?>");
        }

        @Override
        public void endDocument() throws SAXException {
            // Do we need to override this?
        }

        @Override
        public void startElement(String namespaceURI, String sName, String qName, Attributes attrs) throws SAXException {

            this.buf.append("<");
            this.buf.append(qName);

            if (attrs != null) {
                for (int i = 0; i < attrs.getLength(); i++) {
                    this.buf.append(" ");
                    this.buf.append(attrs.getQName(i)).append("=\"");
                    escapeCharsForXml(attrs.getValue(i), true);
                    this.buf.append("\"");
                }
            }

            this.buf.append(">");
        }

        @Override
        public void characters(char buffer[], int offset, int len) throws SAXException {
            if (!this.inCDATA) {
                escapeCharsForXml(buffer, offset, len, false);
            } else {
                this.buf.append(buffer, offset, len);
            }
        }

        @Override
        public void ignorableWhitespace(char ch[], int start, int length) throws SAXException {
            characters(ch, start, length);
        }

        private boolean inCDATA = false;

        public void startCDATA() throws SAXException {
            this.buf.append("<![CDATA[");
            this.inCDATA = true;
        }

        public void endCDATA() throws SAXException {
            this.inCDATA = false;
            this.buf.append("]]>");
        }

        public void comment(char ch[], int start, int length) throws SAXException {
            // if (!fCanonical && fElementDepth > 0) {
            this.buf.append("<!--");
            for (int i = 0; i < length; ++i) {
                this.buf.append(ch[start + i]);
            }
            this.buf.append("-->");
            // }
        }

        Reader toReader() {
            return new StringReader(this.buf.toString());
        }

        private void escapeCharsForXml(String str, boolean isAttributeData) {
            if (str == null) {
                return;
            }

            int strLen = str.length();

            for (int i = 0; i < strLen; i++) {
                escapeCharsForXml(str.charAt(i), isAttributeData);
            }
        }

        private void escapeCharsForXml(char[] buffer, int offset, int len, boolean isAttributeData) {

            if (buffer == null) {
                return;
            }

            for (int i = 0; i < len; i++) {
                escapeCharsForXml(buffer[offset + i], isAttributeData);
            }
        }

        private void escapeCharsForXml(char c, boolean isAttributeData) {
            switch (c) {
                case '<':
                    this.buf.append("&lt;");
                    break;

                case '>':
                    this.buf.append("&gt;");
                    break;

                case '&':
                    this.buf.append("&amp;");
                    break;

                case '"':

                    if (!isAttributeData) {
                        this.buf.append("\"");
                    } else {
                        this.buf.append("&quot;");
                    }

                    break;

                case '\r':
                    this.buf.append("&#xD;");
                    break;

                default:

                    if (((c >= 0x01 && c <= 0x1F && c != 0x09 && c != 0x0A) || (c >= 0x7F && c <= 0x9F) || c == 0x2028)
                            || isAttributeData && (c == 0x09 || c == 0x0A)) {
                        this.buf.append("&#x");
                        this.buf.append(Integer.toHexString(c).toUpperCase());
                        this.buf.append(";");
                    } else {
                        this.buf.append(c);
                    }
            }
        }
    }
}
