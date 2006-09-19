package com.mysql.jdbc;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.FactoryConfigurationError;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stax.StAXResult;
import javax.xml.transform.stax.StAXSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import com.mysql.jdbc.exceptions.JDBC40NotYetImplementedException;

public class MysqlSQLXML implements SQLXML, WriterWatcher, OutputStreamWatcher {

	private XMLInputFactory inputFactory;

	private XMLOutputFactory outputFactory;

	private String stringRep;

	private ResultSet owningResultSet;

	private int columnIndexOfXml;

	private boolean fromResultSet;

	private boolean isClosed = false;

	private boolean workingWithResult;

	private DOMResult asDOMResult;

	protected MysqlSQLXML(ResultSet owner, int index) {
		this.owningResultSet = owner;
		this.columnIndexOfXml = index;
		this.fromResultSet = true;
	}

	protected MysqlSQLXML() {
		this.fromResultSet = false;
	}

	public synchronized void free() throws SQLException {
		this.inputFactory = null;
		this.outputFactory = null;
		this.owningResultSet = null;
		this.workingWithResult = false;
		this.isClosed = true;
		
	}

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
			throw SQLError.createSQLException("SQLXMLInstance has been free()d");
		}
	}

	private synchronized void checkWorkingWithResult() throws SQLException {
		if (this.workingWithResult) {
			throw SQLError.createSQLException("Can't perform requested operation after getResult() has been called to write XML data",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
		}
	}

	/**
	 * Sets the XML value designated by this SQLXML instance to the given String
	 * representation. The format of this String is defined by
	 * org.xml.sax.InputSource, where the characters in the stream represent the
	 * unicode code points for XML according to section 2 and appendix B of the
	 * XML 1.0 specification. Although an encoding declaration other than
	 * unicode may be present, the encoding of the String is unicode. The
	 * behavior of this method is the same as ResultSet.updateString() when the
	 * designated column of the ResultSet has a type java.sql.Types of SQLXML.
	 * <p>
	 * The SQL XML object becomes not writeable when this method is called and
	 * may also become not readable depending on implementation.
	 * 
	 * @param value
	 *            the XML value
	 * @throws SQLException
	 *             if there is an error processing the XML value. The getCause()
	 *             method of the exception may provide a more detailed
	 *             exception, for example, if the stream does not contain valid
	 *             characters. An exception is thrown if the state is not
	 *             writable.
	 * @exception SQLFeatureNotSupportedException
	 *                if the JDBC driver does not support this method
	 * @since 1.6
	 */

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

	public synchronized InputStream getBinaryStream() throws SQLException {
		checkClosed();
		checkWorkingWithResult();
		
		return this.owningResultSet.getBinaryStream(this.columnIndexOfXml);
	}

	/**
	 * Retrieves the XML value designated by this SQLXML instance as a
	 * java.io.Reader object. The format of this stream is defined by
	 * org.xml.sax.InputSource, where the characters in the stream represent the
	 * unicode code points for XML according to section 2 and appendix B of the
	 * XML 1.0 specification. Although an encoding declaration other than
	 * unicode may be present, the encoding of the stream is unicode. The
	 * behavior of this method is the same as ResultSet.getCharacterStream()
	 * when the designated column of the ResultSet has a type java.sql.Types of
	 * SQLXML.
	 * <p>
	 * The SQL XML object becomes not readable when this method is called and
	 * may also become not writable depending on implementation.
	 * 
	 * @return a stream containing the XML data.
	 * @throws SQLException
	 *             if there is an error processing the XML value. The getCause()
	 *             method of the exception may provide a more detailed
	 *             exception, for example, if the stream does not contain valid
	 *             characters. An exception is thrown if the state is not
	 *             readable.
	 * @exception SQLFeatureNotSupportedException
	 *                if the JDBC driver does not support this method
	 * @since 1.6
	 */
	public synchronized Reader getCharacterStream() throws SQLException {
		checkClosed();
		checkWorkingWithResult();
		
		return this.owningResultSet.getCharacterStream(this.columnIndexOfXml);
	}

	/**
	 * Returns a Source for reading the XML value designated by this SQLXML
	 * instance. Sources are used as inputs to XML parsers and XSLT
	 * transformers.
	 * <p>
	 * Sources for XML parsers will have namespace processing on by default. The
	 * systemID of the Source is implementation dependent.
	 * <p>
	 * The SQL XML object becomes not readable when this method is called and
	 * may also become not writable depending on implementation.
	 * <p>
	 * Note that SAX is a callback architecture, so a returned SAXSource should
	 * then be set with a content handler that will receive the SAX events from
	 * parsing. The content handler will receive callbacks based on the contents
	 * of the XML.
	 * 
	 * <pre>
	 * SAXSource saxSource = sqlxml.getSource(SAXSource.class);
	 * XMLReader xmlReader = saxSource.getXMLReader();
	 * xmlReader.setContentHandler(myHandler);
	 * xmlReader.parse(saxSource.getInputSource());
	 * </pre>
	 * 
	 * @param sourceClass
	 *            The class of the source, or null. If the class is null, a
	 *            vendor specifc Source implementation will be returned. The
	 *            following classes are supported at a minimum:
	 *            
	 *            (MySQL returns a SAXSource if sourceClass == null)
	 * 
	 * <pre>
	 *    javax.xml.transform.dom.DOMSource - returns a DOMSource
	 *    javax.xml.transform.sax.SAXSource - returns a SAXSource
	 *    javax.xml.transform.stax.StAXSource - returns a StAXSource
	 *    javax.xml.transform.stream.StreamSource - returns a StreamSource
	 * </pre>
	 * 
	 * @return a Source for reading the XML value.
	 * @throws SQLException
	 *             if there is an error processing the XML value or if this
	 *             feature is not supported. The getCause() method of the
	 *             exception may provide a more detailed exception, for example,
	 *             if an XML parser exception occurs. An exception is thrown if
	 *             the state is not readable.
	 * @exception SQLFeatureNotSupportedException
	 *                if the JDBC driver does not support this method
	 * @since 1.6
	 */
	public synchronized Source getSource(Class clazz) throws SQLException {
		checkClosed();
		checkWorkingWithResult();
		
		// Note that we try and use streams here wherever possible
		// for the day that the server actually supports streaming
		// from server -> client (futureproofing)

		if (clazz == null || clazz.equals(SAXSource.class)) {
			
			InputSource inputSource = null;
			
			if (this.fromResultSet) {
				inputSource = new InputSource(
						this.owningResultSet
						.getCharacterStream(this.columnIndexOfXml));
			} else {
				inputSource = new InputSource(new StringReader(this.stringRep));
			}
			
			return new SAXSource(inputSource);
		} else if (clazz.equals(DOMSource.class)) {
			try {
				DocumentBuilderFactory builderFactory = DocumentBuilderFactory
						.newInstance();
				builderFactory.setNamespaceAware(true);
				DocumentBuilder builder = builderFactory.newDocumentBuilder();

				InputSource inputSource = null;
				
				if (this.fromResultSet) {
					inputSource = new InputSource(
							this.owningResultSet
							.getCharacterStream(this.columnIndexOfXml));
				} else {
					inputSource = new InputSource(new StringReader(this.stringRep));
				}

				return new DOMSource(builder.parse(inputSource));
			} catch (Throwable t) {
				// FIXME - need to use factory method and set cause here

				throw new SQLException(t);
			}

		} else if (clazz.equals(StreamSource.class)) {
			Reader reader = null;
			
			if (this.fromResultSet) {
				reader = this.owningResultSet
							.getCharacterStream(this.columnIndexOfXml);
			} else {
				reader = new StringReader(this.stringRep);
			}
			
			return new StreamSource(reader);
		} else if (clazz.equals(StAXSource.class)) {
			try {
				Reader reader = null;
				
				if (this.fromResultSet) {
					reader = this.owningResultSet
								.getCharacterStream(this.columnIndexOfXml);
				} else {
					reader = new StringReader(this.stringRep);
				}
				
				return new StAXSource(this.inputFactory
						.createXMLStreamReader(reader));
			} catch (XMLStreamException ex) {
				SQLException sqlEx = SQLError.createSQLException(ex
						.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
				// perhaps setCause here w/ ex?
				throw sqlEx;
			}
		} else {
			throw SQLError.createSQLException("XML Source of type \""
					+ clazz.toString() + "\" Not supported.",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
		}
	}

	/**
	 * Retrieves a stream that can be used to write the XML value that this
	 * SQLXML instance represents. The stream begins at position 0. The bytes of
	 * the stream are interpreted according to appendix F of the XML 1.0
	 * specification The behavior of this method is the same as
	 * ResultSet.updateBinaryStream() when the designated column of the
	 * ResultSet has a type java.sql.Types of SQLXML.
	 * <p>
	 * The SQL XML object becomes not writeable when this method is called and
	 * may also become not readable depending on implementation.
	 * 
	 * @return a stream to which data can be written.
	 * @throws SQLException
	 *             if there is an error processing the XML value. An exception
	 *             is thrown if the state is not writable.
	 * @exception SQLFeatureNotSupportedException
	 *                if the JDBC driver does not support this method
	 * @since 1.6
	 */
	public synchronized OutputStream setBinaryStream() throws SQLException {
		checkClosed();
		checkWorkingWithResult();
		
		this.workingWithResult = true;
		
		return setBinaryStreamInternal();
	}

	private OutputStream setBinaryStreamInternal() throws SQLException {
		WatchableOutputStream bytesOut = new WatchableOutputStream();
		bytesOut.setWatcher(this);

		return bytesOut;
	}
	
	/**
	 * Retrieves a stream to be used to write the XML value that this SQLXML
	 * instance represents. The format of this stream is defined by
	 * org.xml.sax.InputSource, where the characters in the stream represent the
	 * unicode code points for XML according to section 2 and appendix B of the
	 * XML 1.0 specification. Although an encoding declaration other than
	 * unicode may be present, the encoding of the stream is unicode. The
	 * behavior of this method is the same as ResultSet.updateCharacterStream()
	 * when the designated column of the ResultSet has a type java.sql.Types of
	 * SQLXML.
	 * <p>
	 * The SQL XML object becomes not writeable when this method is called and
	 * may also become not readable depending on implementation.
	 * 
	 * @return a stream to which data can be written.
	 * @throws SQLException
	 *             if there is an error processing the XML value. The getCause()
	 *             method of the exception may provide a more detailed
	 *             exception, for example, if the stream does not contain valid
	 *             characters. An exception is thrown if the state is not
	 *             writable.
	 * @exception SQLFeatureNotSupportedException
	 *                if the JDBC driver does not support this method
	 * @since 1.6
	 */
	public synchronized Writer setCharacterStream() throws SQLException {
		checkClosed();
		checkWorkingWithResult();
		
		this.workingWithResult = true;
		
		return setCharacterStreamInternal();
	}

	private Writer setCharacterStreamInternal() throws SQLException {
		WatchableWriter writer = new WatchableWriter();
		writer.setWatcher(this);

		return writer;
	}
	
	/**
	 * Returns a Result for setting the XML value designated by this SQLXML
	 * instance.
	 * <p>
	 * The systemID of the Result is implementation dependent.
	 * <p>
	 * The SQL XML object becomes not writeable when this method is called and
	 * may also become not readable depending on implementation.
	 * <p>
	 * Note that SAX is a callback architecture and the returned SAXResult has a
	 * content handler assigned that will receive the SAX events based on the
	 * contents of the XML. Call the content handler with the contents of the
	 * XML document to assign the values.
	 * 
	 * <pre>
	 * SAXResult saxResult = sqlxml.setResult(SAXResult.class);
	 * ContentHandler contentHandler = saxResult.getXMLReader().getContentHandler();
	 * contentHandler.startDocument();
	 * // set the XML elements and attributes into the result
	 * contentHandler.endDocument();
	 * </pre>
	 * 
	 * @param resultClass
	 *            The class of the result, or null. If resultClass is null, a
	 *            vendor specific Result implementation will be returned. The
	 *            following classes are supported at a minimum:
	 * 
	 * <pre>
	 *    javax.xml.transform.dom.DOMResult - returns a DOMResult
	 *    javax.xml.transform.sax.SAXResult - returns a SAXResult
	 *    javax.xml.transform.stax.StAXResult - returns a StAXResult
	 *    javax.xml.transform.stream.StreamResult - returns a StreamResult
	 * </pre>
	 * 
	 * @return Returns a Result for setting the XML value.
	 * @throws SQLException
	 *             if there is an error processing the XML value or if this
	 *             feature is not supported. The getCause() method of the
	 *             exception may provide a more detailed exception, for example,
	 *             if an XML parser exception occurs. An exception is thrown if
	 *             the state is not writable.
	 * @exception SQLFeatureNotSupportedException
	 *                if the JDBC driver does not support this method
	 * @since 1.6
	 */
	public synchronized Result setResult(Class clazz) throws SQLException {
		checkClosed();
		checkWorkingWithResult();
		
		this.workingWithResult = true;
		this.asDOMResult = null;
		this.stringRep = null;
		
		if (clazz == null || clazz.equals(SAXResult.class)) {
			
			// TODO: Need to flesh this out
			return new SAXResult(null);
		} else if (clazz.equals(DOMResult.class)) {
			
			this.asDOMResult = new DOMResult();
			return this.asDOMResult;

		} else if (clazz.equals(StreamResult.class)) {
			return new StreamResult(setCharacterStreamInternal());
		} else if (clazz.equals(StAXResult.class)) {
			try {
				return new StAXResult(this.outputFactory.createXMLEventWriter(
						setCharacterStreamInternal()));
			} catch (XMLStreamException ex) {
				SQLException sqlEx = SQLError.createSQLException(ex
						.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
				// perhaps setCause here w/ ex?
				throw sqlEx;
			}
		} else {
			throw SQLError.createSQLException("XML Result of type \""
					+ clazz.toString() + "\" Not supported.",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
		}
	}

	/**
	 * @see com.mysql.jdbc.WriterWatcher#writerClosed(char[])
	 */
	public void writerClosed(WatchableWriter out) {
		try {
			setString(out.toString());
		} catch (SQLException sqlEx) {
			// never actually thrown in our impl
		}
	}

	public void streamClosed(WatchableOutputStream out) {
		
		try {
			// There's got to be an easier way to do this, but
			// I don't feel like coding up Appendix F of the XML Spec
			// myself, when there's a reusable way to do it, and we
			// can warn folks away from BINARY xml streams that have
			// to be parsed to determine the character encoding :P
			
			String encoding = "UTF-8";

			try {
				ByteArrayInputStream bIn = new ByteArrayInputStream(out.toByteArray());
				XMLStreamReader reader = this.inputFactory.createXMLStreamReader(bIn);

				int eventType = 0;

				while ((eventType = reader.next()) != XMLStreamReader.END_DOCUMENT) {
					if (eventType == XMLStreamReader.START_DOCUMENT) {
						String possibleEncoding = reader.getEncoding();

						if (possibleEncoding != null) {
							encoding = possibleEncoding;
						}

						break;
					}
				}
			} catch (Throwable t) {
				// ignore, dealt with later when the string can't be parsed
				// into valid XML
			}

			setString(new String(out.toByteArray(), encoding));
		} catch (UnsupportedEncodingException badEnc) {
			throw new RuntimeException(badEnc);
		} catch (SQLException sqlEx) {
			// never actually thrown in our impl
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
				// serialize from DOM
			}
		}
		
		return getString(); 
	}
}
