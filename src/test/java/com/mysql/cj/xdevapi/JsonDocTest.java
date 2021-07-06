/*
 * Copyright (c) 2015, 2021, Oracle and/or its affiliates.
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

package com.mysql.cj.xdevapi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.StringReader;
import java.math.BigDecimal;
import java.util.concurrent.Callable;

import org.junit.jupiter.api.Test;

import com.mysql.cj.exceptions.WrongArgumentException;

/**
 * DbDoc tests.
 */
public class JsonDocTest {

    @Test
    public void testEscaping() throws Exception {

        String testStr = "\"\\\"\\\\\\\u002F\\b\\f\\n\\r\\t\"";
        JsonString val = JsonParser.parseString(new StringReader(testStr));
        assertEquals(8, val.getString().length());
        assertEquals('\"', val.getString().charAt(0));
        assertEquals('\\', val.getString().charAt(1));
        assertEquals('/', val.getString().charAt(2));
        assertEquals('\u0008', val.getString().charAt(3));
        assertEquals('\u000C', val.getString().charAt(4));
        assertEquals('\n', val.getString().charAt(5));
        assertEquals('\r', val.getString().charAt(6));
        assertEquals('\t', val.getString().charAt(7));
        assertEquals(testStr, val.toString());

        assertThrows(WrongArgumentException.class, "Unknown escape sequence '\\\\q'.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseString(new StringReader("\"\\q\""));
                return null;
            }
        });
    }

    @Test
    public void bracketAsValue() throws Exception {
        // Bug MYSQLCONNJ-572
        DbDoc d = JsonParser.parseDoc(new StringReader("{\"x\":\"}\",\"y\":1}"));
        assertEquals("}", ((JsonString) d.get("x")).getString());
        assertEquals(new Integer(1), ((JsonNumber) d.get("y")).getInteger());
    }

    @Test
    public void testParseString() throws Exception {

        // ignore whitespaces
        JsonString val = JsonParser.parseString(new StringReader(" \\n\\r \" qq \" "));
        assertEquals(" qq ", val.getString());

        // don't ignore other symbols before opening quotation mark
        assertThrows(WrongArgumentException.class, "Attempt to add character '\\\\' to unopened string.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseString(new StringReader("\\\\ \" "));
                return null;
            }
        });

        assertThrows(WrongArgumentException.class, "Attempt to add character 'f' to unopened string.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseString(new StringReader(" f \" "));
                return null;
            }
        });

        // check quotation marks
        assertThrows(WrongArgumentException.class, "Missed closing '\"'.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseString(new StringReader("\""));
                return null;
            }
        });

        val = JsonParser.parseString(new StringReader("\"\""));
        assertEquals("", val.getString());

        val = JsonParser.parseString(new StringReader(""));
        assertNull(val);

        val = JsonParser.parseString(new StringReader(" \\t\\r\\n"));
        assertNull(val);
    }

    @Test
    public void testParseNumber() throws Exception {

        JsonNumber val;

        // ignore whitespaces
        val = JsonParser.parseNumber(new StringReader(" \n\r  -1.2E-12  "));
        assertEquals(new BigDecimal("-1.2E-12"), val.getBigDecimal());

        val = JsonParser.parseNumber(new StringReader(" \n\r  1.2e12  "));
        assertEquals(new BigDecimal("1.2E12"), val.getBigDecimal());

        assertThrows(WrongArgumentException.class, "Invalid whitespace character 'k'.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseNumber(new StringReader("-1.2E-12k  "));
                return null;
            }
        });

        // '-' position
        assertThrows(WrongArgumentException.class, "Wrong '-' position after '-1.2E-1'.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseNumber(new StringReader("-1.2E-1-2  "));
                return null;
            }
        });

        // exponent position
        assertThrows(WrongArgumentException.class, "Wrong 'E' position after '-12.'.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseNumber(new StringReader("-12.E-12  "));
                return null;
            }
        });

        // dot position
        assertThrows(WrongArgumentException.class, "Wrong '.' occurrence after '1.2', it is allowed only once per number.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseNumber(new StringReader("1.2.0E-12  "));
                return null;
            }
        });

        assertThrows(WrongArgumentException.class, "Wrong '.' occurrence after '1.20E', it is allowed only once per number.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseNumber(new StringReader("1.20E.12  "));
                return null;
            }
        });

        assertThrows(WrongArgumentException.class, "'.' is not allowed in the exponent.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseNumber(new StringReader("10E.12  "));
                return null;
            }
        });

        assertThrows(WrongArgumentException.class, "'.' is not allowed in the exponent.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseNumber(new StringReader("10E1.2  "));
                return null;
            }
        });

        // '+' position
        assertThrows(WrongArgumentException.class, "Invalid whitespace character '\u002E'.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseNumber(new StringReader("+10E12  "));
                return null;
            }
        });

        assertThrows(WrongArgumentException.class, "Wrong '\u002E' position after '1'.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseNumber(new StringReader("1+0E12  "));
                return null;
            }
        });

        assertThrows(WrongArgumentException.class, "Wrong '\u002E' position after '10'.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseNumber(new StringReader("10+E12  "));
                return null;
            }
        });

        assertThrows(WrongArgumentException.class, "Wrong '\u002E' position after '10E1'.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseNumber(new StringReader("10E1+2  "));
                return null;
            }
        });

        // closing chars
        val = JsonParser.parseNumber(new StringReader("10E+12  "));
        assertEquals(new BigDecimal("10E+12"), val.getBigDecimal());

        val = JsonParser.parseNumber(new StringReader("10E+12,"));
        assertEquals(new BigDecimal("10E+12"), val.getBigDecimal());

        val = JsonParser.parseNumber(new StringReader("10E+12]"));
        assertEquals(new BigDecimal("10E+12"), val.getBigDecimal());

        val = JsonParser.parseNumber(new StringReader("10E+12}"));
        assertEquals(new BigDecimal("10E+12"), val.getBigDecimal());

        // base part length
        val = JsonParser.parseNumber(new StringReader("-1234567890.5E+12"));
        assertEquals(new BigDecimal("-1234567890.5E+12"), val.getBigDecimal());

        val = JsonParser.parseNumber(new StringReader("1234567890.5E+12"));
        assertEquals(new BigDecimal("1234567890.5E+12"), val.getBigDecimal());

        val = JsonParser.parseNumber(new StringReader("-1234567890.5"));
        assertEquals(new BigDecimal("-1234567890.5"), val.getBigDecimal());

        val = JsonParser.parseNumber(new StringReader("1234567890.5"));
        assertEquals(new BigDecimal("1234567890.5"), val.getBigDecimal());

        val = JsonParser.parseNumber(new StringReader("-1234567890E+12"));
        assertEquals(new BigDecimal("-1234567890E+12"), val.getBigDecimal());

        val = JsonParser.parseNumber(new StringReader("1234567890E+12"));
        assertEquals(new BigDecimal("1234567890E+12"), val.getBigDecimal());

        val = JsonParser.parseNumber(new StringReader("-1234567890"));
        assertEquals(new BigDecimal("-1234567890"), val.getBigDecimal());

        val = JsonParser.parseNumber(new StringReader("1234567890"));
        assertEquals(new BigDecimal("1234567890"), val.getBigDecimal());

        val = JsonParser.parseNumber(new StringReader("-12345678901.5E+12"));
        assertEquals(new BigDecimal("-12345678901.5E+12"), val.getBigDecimal());

        val = JsonParser.parseNumber(new StringReader("12345678901.5E+12"));
        assertEquals(new BigDecimal("12345678901.5E+12"), val.getBigDecimal());

        val = JsonParser.parseNumber(new StringReader("-12345678901.5"));
        assertEquals(new BigDecimal("-12345678901.5"), val.getBigDecimal());

        val = JsonParser.parseNumber(new StringReader("12345678901.5"));
        assertEquals(new BigDecimal("12345678901.5"), val.getBigDecimal());

        val = JsonParser.parseNumber(new StringReader("-12345678901E+12"));
        assertEquals(new BigDecimal("-12345678901E+12"), val.getBigDecimal());

        val = JsonParser.parseNumber(new StringReader("12345678901E+12"));
        assertEquals(new BigDecimal("12345678901E+12"), val.getBigDecimal());

        val = JsonParser.parseNumber(new StringReader("-12345678901"));
        assertEquals(new BigDecimal("-12345678901"), val.getBigDecimal());

        val = JsonParser.parseNumber(new StringReader("12345678901"));
        assertEquals(new BigDecimal("12345678901"), val.getBigDecimal());

        val = JsonParser.parseNumber(new StringReader("-1546300800000"));
        assertEquals(new BigDecimal("-1546300800000"), val.getBigDecimal());

        val = JsonParser.parseNumber(new StringReader("1546300800000"));
        assertEquals(new BigDecimal("1546300800000"), val.getBigDecimal());

        // integer
        val = JsonParser.parseNumber(new StringReader("12345"));
        assertEquals(12345, val.getInteger().intValue());

        // empty value
        assertThrows(WrongArgumentException.class, "No valid value was found.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseNumber(new StringReader(""));
                return null;
            }
        });
    }

    @Test
    public void testParseTrueLiteral() throws Exception {
        JsonLiteral val;

        val = JsonParser.parseLiteral(new StringReader("true"));
        assertEquals("true", val.toString());

        // ignore whitespaces
        val = JsonParser.parseLiteral(new StringReader(" \n\r  true  "));
        assertEquals("true", val.toString());

        val = JsonParser.parseLiteral(new StringReader(" \n\r  true,  "));
        assertEquals("true", val.toString());

        val = JsonParser.parseLiteral(new StringReader(" \n\r  true}  "));
        assertEquals("true", val.toString());

        val = JsonParser.parseLiteral(new StringReader(" \n\r  true]  "));
        assertEquals("true", val.toString());

        assertThrows(WrongArgumentException.class, "Invalid whitespace character 'q'.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseLiteral(new StringReader(" q  true  "));
                return null;
            }
        });

        // incomplete literal
        assertThrows(WrongArgumentException.class, "Wrong literal 't'.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseLiteral(new StringReader("t"));
                return null;
            }
        });

        // empty value
        assertThrows(WrongArgumentException.class, "No valid value was found.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseLiteral(new StringReader(""));
                return null;
            }
        });
        assertThrows(WrongArgumentException.class, "No valid value was found.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseLiteral(new StringReader(" \r "));
                return null;
            }
        });
        assertThrows(WrongArgumentException.class, "Invalid whitespace character '}'.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseLiteral(new StringReader("}"));
                return null;
            }
        });
    }

    @Test
    public void testParseFalseLiteral() throws Exception {
        JsonLiteral val;

        val = JsonParser.parseLiteral(new StringReader("false"));
        assertEquals("false", val.toString());

        // ignore whitespaces
        val = JsonParser.parseLiteral(new StringReader(" \n\r  false  "));
        assertEquals("false", val.toString());

        val = JsonParser.parseLiteral(new StringReader(" \n\r  false,  "));
        assertEquals("false", val.toString());

        val = JsonParser.parseLiteral(new StringReader(" \n\r  false}  "));
        assertEquals("false", val.toString());

        val = JsonParser.parseLiteral(new StringReader(" \n\r  false]  "));
        assertEquals("false", val.toString());

        assertThrows(WrongArgumentException.class, "Invalid whitespace character 'q'.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseLiteral(new StringReader(" q  false  "));
                return null;
            }
        });

        // incomplete literal
        assertThrows(WrongArgumentException.class, "Wrong literal 'f'.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseLiteral(new StringReader("f"));
                return null;
            }
        });

        // empty value
        assertThrows(WrongArgumentException.class, "No valid value was found.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseLiteral(new StringReader(""));
                return null;
            }
        });
        assertThrows(WrongArgumentException.class, "No valid value was found.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseLiteral(new StringReader(" \r "));
                return null;
            }
        });
        assertThrows(WrongArgumentException.class, "Invalid whitespace character '}'.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseLiteral(new StringReader("}"));
                return null;
            }
        });
    }

    @Test
    public void testParseNullLiteral() throws Exception {
        JsonLiteral val;

        val = JsonParser.parseLiteral(new StringReader("null"));
        assertEquals("null", val.toString());

        // ignore whitespaces
        val = JsonParser.parseLiteral(new StringReader(" \n\r  null  "));
        assertEquals("null", val.toString());

        val = JsonParser.parseLiteral(new StringReader(" \n\r  null,  "));
        assertEquals("null", val.toString());

        val = JsonParser.parseLiteral(new StringReader(" \n\r  null}  "));
        assertEquals("null", val.toString());

        val = JsonParser.parseLiteral(new StringReader(" \n\r  null]  "));
        assertEquals("null", val.toString());

        assertThrows(WrongArgumentException.class, "Invalid whitespace character 'q'.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseLiteral(new StringReader(" q  true  "));
                return null;
            }
        });

        // incomplete literal
        assertThrows(WrongArgumentException.class, "Wrong literal 'n'.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseLiteral(new StringReader("n"));
                return null;
            }
        });

        // empty value
        assertThrows(WrongArgumentException.class, "No valid value was found.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseLiteral(new StringReader(""));
                return null;
            }
        });
        assertThrows(WrongArgumentException.class, "No valid value was found.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseLiteral(new StringReader(" \r "));
                return null;
            }
        });
        assertThrows(WrongArgumentException.class, "Invalid whitespace character '}'.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseLiteral(new StringReader("}"));
                return null;
            }
        });
    }

    @Test
    public void testParseArray() throws Exception {
        JsonArray val;

        val = JsonParser.parseArray(new StringReader("[\"arr.val1\", 123, true, false, null, {\"k1\" : \"v1\"}, [1,2,3]]"));
        assertEquals(7, val.size());
        assertEquals(JsonString.class, val.get(0).getClass());
        assertEquals("\"arr.val1\"", val.get(0).toString());
        assertEquals(JsonNumber.class, val.get(1).getClass());
        assertEquals("123", val.get(1).toString());
        assertEquals(JsonLiteral.TRUE.getClass(), val.get(2).getClass());
        assertEquals("true", val.get(2).toString());
        assertEquals(JsonLiteral.FALSE.getClass(), val.get(3).getClass());
        assertEquals("false", val.get(3).toString());
        assertEquals(JsonLiteral.NULL.getClass(), val.get(4).getClass());
        assertEquals("null", val.get(4).toString());
        assertTrue(DbDoc.class.isAssignableFrom(val.get(5).getClass()));
        assertEquals("{\"k1\":\"v1\"}", val.get(5).toString());
        assertEquals("{\n\"k1\" : \"v1\"\n}", val.get(5).toFormattedString());
        assertEquals(JsonArray.class, val.get(6).getClass());
        assertEquals("[1,2,3]", val.get(6).toString());
        assertEquals("[1, 2, 3]", val.get(6).toFormattedString());

        // ignore whitespaces
        val = JsonParser.parseArray(new StringReader(" \r\n  [ \r\n 1, \n 2 \r\n  ]  \r\n "));
        assertEquals("[1,2]", val.toString());
        assertEquals("[1, 2]", val.toFormattedString());

        // wrong spaces
        assertThrows(WrongArgumentException.class, "Invalid whitespace character 'a'.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseArray(new StringReader(" a  [ b 1 c, d 2 e  ]  x "));
                return null;
            }
        });
        assertThrows(WrongArgumentException.class, "Invalid whitespace character 'b'.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseArray(new StringReader("[ b 1 c, d 2 e  ]  x "));
                return null;
            }
        });
        assertThrows(WrongArgumentException.class, "Invalid whitespace character 'c'.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseArray(new StringReader("[1 c, d 2 e  ]  x "));
                return null;
            }
        });
        assertThrows(WrongArgumentException.class, "Invalid whitespace character 'd'.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseArray(new StringReader("[1, d 2 e  ]  x "));
                return null;
            }
        });

        val = JsonParser.parseArray(new StringReader("    [   1 ,  2   ]  x "));
        assertEquals("[1,2]", val.toString());
        assertEquals("[1, 2]", val.toFormattedString());

        // check brackets
        assertThrows(WrongArgumentException.class, "Missed closing ']'.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseArray(new StringReader("[123"));
                return null;
            }
        });

        // empty array
        val = JsonParser.parseArray(new StringReader("[]"));
        assertEquals("[]", val.toString());
    }

    @Test
    public void testParseDoc() throws Exception {

        DbDoc doc;

        // empty doc
        doc = JsonParser.parseDoc(new StringReader("{}"));
        assertEquals(0, doc.size());
        doc = JsonParser.parseDoc(new StringReader(" \r\n { \r \n} \r \n"));
        assertEquals(0, doc.size());

        doc = JsonParser.parseDoc(new StringReader("{\"x\":22}"));

        // check brackets
        assertThrows(WrongArgumentException.class, "Missed closing '}'.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseDoc(new StringReader("{"));
                return null;
            }
        });
        assertThrows(WrongArgumentException.class, "No valid JSON document was found.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseDoc(new StringReader("}"));
                return null;
            }
        });

        // key without value
        assertThrows(WrongArgumentException.class, "Colon is missed after key 'key1'.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseDoc(new StringReader("{\"key1\"}"));
                return null;
            }
        });

        assertThrows(WrongArgumentException.class, "Invalid value was found after key 'key1'.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseDoc(new StringReader("{\"key1\" : }"));
                return null;
            }
        });

        // invalid whitespace
        assertThrows(WrongArgumentException.class, "Invalid whitespace character 'a'.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseDoc(new StringReader(" a {\"key1\" x : \"value1\"}"));
                return null;
            }
        });
        assertThrows(WrongArgumentException.class, "Attempt to add character 'a' to unopened string.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseDoc(new StringReader("  {a\"key1\" : \"value1\"}"));
                return null;
            }
        });

        assertThrows(WrongArgumentException.class, "Invalid whitespace character 'x'.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseDoc(new StringReader("{\"key1\" x : \"value1\"}"));
                return null;
            }
        });

        assertThrows(WrongArgumentException.class, "Invalid value was found after key 'key1'.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseDoc(new StringReader("{\"key1\" : x \"value1\"}"));
                return null;
            }
        });

        assertThrows(WrongArgumentException.class, "Invalid whitespace character 'x'.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseDoc(new StringReader("{\"key1\" : \"value1\"x}"));
                return null;
            }
        });

        assertThrows(WrongArgumentException.class, "Invalid whitespace character ']'.", new Callable<Void>() {
            public Void call() throws Exception {
                JsonParser.parseDoc("{\"_id\":\"1004\",\"F1\": ] }");
                return null;
            }
        });

        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"\" : \"val0\", ");
        sb.append("\"key1\" : \"val1\", ");
        sb.append("\"key2\" : -1.2E-12, ");
        sb.append("\"key3\" : {\"in.key1\" :   true, \"in.key2\" : 3.1415}, ");
        sb.append("\"key4\" :  false, ");
        sb.append("\"key5\" : [\"arr.val1\", null], ");
        sb.append("\"key6\" :  true, ");
        sb.append("\"key7\" :  null ");
        sb.append("}");

        doc = JsonParser.parseDoc(new StringReader(sb.toString()));

        assertEquals(8, doc.size());
        assertEquals(JsonString.class, doc.get("").getClass());
        assertEquals("\"val0\"", doc.get("").toString());
        assertEquals(JsonString.class, doc.get("key1").getClass());
        assertEquals("\"val1\"", doc.get("key1").toString());
        assertEquals(JsonNumber.class, doc.get("key2").getClass());
        assertEquals("-1.2E-12", doc.get("key2").toString());
        assertTrue(DbDoc.class.isAssignableFrom(doc.get("key3").getClass()));
        assertEquals("{\"in.key1\":true,\"in.key2\":3.1415}", doc.get("key3").toString());
        assertEquals("{\n\"in.key1\" : true,\n\"in.key2\" : 3.1415\n}", doc.get("key3").toFormattedString());
        assertEquals(JsonLiteral.FALSE.getClass(), doc.get("key4").getClass());
        assertEquals("false", doc.get("key4").toString());
        assertEquals(JsonArray.class, doc.get("key5").getClass());
        assertEquals("[\"arr.val1\",null]", doc.get("key5").toString());
        assertEquals("[\"arr.val1\", null]", doc.get("key5").toFormattedString());
        assertEquals(JsonLiteral.TRUE.getClass(), doc.get("key6").getClass());
        assertEquals("true", doc.get("key6").toString());
        assertEquals(JsonLiteral.NULL.getClass(), doc.get("key7").getClass());
        assertEquals("null", doc.get("key7").toString());

        assertEquals("{\"\":\"val0\",\"key1\":\"val1\",\"key2\":-1.2E-12,\"key3\":{\"in.key1\":true,\"in.key2\":3.1415},"
                + "\"key4\":false,\"key5\":[\"arr.val1\",null],\"key6\":true,\"key7\":null}", doc.toString());

        assertEquals("{\n\"\" : \"val0\",\n\"key1\" : \"val1\",\n\"key2\" : -1.2E-12,\n\"key3\" : {\n\"in.key1\" : true,\n\"in.key2\" : 3.1415\n},\n"
                + "\"key4\" : false,\n\"key5\" : [\"arr.val1\", null],\n\"key6\" : true,\n\"key7\" : null\n}", doc.toFormattedString());

        // Number at the end
        doc = JsonParser.parseDoc(new StringReader("{\"x\" : 2}"));
        assertEquals(JsonNumber.class, doc.get("x").getClass());
        assertEquals("2", doc.get("x").toString());

        // Literals at the end
        doc = JsonParser.parseDoc(new StringReader("{\"x\" : true}"));
        assertEquals(JsonLiteral.TRUE.getClass(), doc.get("x").getClass());
        assertEquals("true", doc.get("x").toString());

        doc = JsonParser.parseDoc(new StringReader("{\"x\" : false}"));
        assertEquals(JsonLiteral.FALSE.getClass(), doc.get("x").getClass());
        assertEquals("false", doc.get("x").toString());

        doc = JsonParser.parseDoc(new StringReader("{\"x\" : null}"));
        assertEquals(JsonLiteral.NULL.getClass(), doc.get("x").getClass());
        assertEquals("null", doc.get("x").toString());

        // Array at the end
        doc = JsonParser.parseDoc(new StringReader("{\"x\" : [1,2]}"));
        assertEquals(JsonArray.class, doc.get("x").getClass());
        assertEquals("[1,2]", doc.get("x").toString());
        assertEquals("[1, 2]", doc.get("x").toFormattedString());

        // DbDoc at the end
        doc = JsonParser.parseDoc(new StringReader("{\"x\" : {\"y\" : true}}"));
        assertTrue(DbDoc.class.isAssignableFrom(doc.get("x").getClass()));
        assertEquals("{\"y\":true}", doc.get("x").toString());
        assertEquals("{\n\"y\" : true\n}", doc.get("x").toFormattedString());
    }

    @Test
    public void testToJsonString() {

        DbDoc doc = new DbDocImpl().add("field1", new JsonString().setValue("value 1")).add("field2", new JsonNumber().setValue("12345.44E22"))
                .add("field3", JsonLiteral.TRUE).add("field4", JsonLiteral.FALSE).add("field5", JsonLiteral.NULL)
                .add("field6",
                        new DbDocImpl().add("inner field 1", new JsonString().setValue("inner value 1")).add("inner field 2", new JsonNumber().setValue("2"))
                                .add("inner field 3", JsonLiteral.TRUE).add("inner field 4", JsonLiteral.FALSE).add("inner field 5", JsonLiteral.NULL)
                                .add("inner field 6", new JsonArray()).add("inner field 7", new DbDocImpl()))
                .add("field7", new JsonArray().addValue(new JsonString().setValue("arr1")).addValue(new JsonNumber().setValue("3")).addValue(JsonLiteral.TRUE)
                        .addValue(JsonLiteral.FALSE).addValue(JsonLiteral.NULL).addValue(new JsonArray()).addValue(new DbDocImpl()));

        assertEquals("{\"field1\":\"value 1\",\"field2\":1.234544E+26,\"field3\":true,\"field4\":false,\"field5\":null,"
                + "\"field6\":{\"inner field 1\":\"inner value 1\",\"inner field 2\":2,\"inner field 3\":true,"
                + "\"inner field 4\":false,\"inner field 5\":null,\"inner field 6\":[],\"inner field 7\":{}},"
                + "\"field7\":[\"arr1\",3,true,false,null,[],{}]}", doc.toString());

        assertEquals("{\n\"field1\" : \"value 1\",\n\"field2\" : 1.234544E+26,\n\"field3\" : true,\n\"field4\" : false,\n\"field5\" : null,\n"
                + "\"field6\" : {\n\"inner field 1\" : \"inner value 1\",\n\"inner field 2\" : 2,\n\"inner field 3\" : true,\n"
                + "\"inner field 4\" : false,\n\"inner field 5\" : null,\n\"inner field 6\" : [],\n\"inner field 7\" : {}\n},\n"
                + "\"field7\" : [\"arr1\", 3, true, false, null, [], {}]\n}", doc.toFormattedString());
    }

    @Test
    public void testJsonNumberAtEnd() throws Exception {
        JsonParser.parseDoc(new StringReader("{\"x\":2}"));
    }

    protected static <EX extends Throwable> EX assertThrows(Class<EX> throwable, Callable<?> testRoutine) {
        try {
            testRoutine.call();
        } catch (Throwable t) {
            assertTrue(throwable.isAssignableFrom(t.getClass()),
                    "Expected exception of type '" + throwable.getName() + "' but instead a exception of type '" + t.getClass().getName() + "' was thrown.");
            return throwable.cast(t);
        }
        fail("Expected exception of type '" + throwable.getName() + "'.");

        // never reaches here
        return null;
    }

    protected static <EX extends Throwable> EX assertThrows(Class<EX> throwable, String msgMatchesRegex, Callable<?> testRoutine) {
        try {
            testRoutine.call();
        } catch (Throwable t) {
            assertTrue(throwable.isAssignableFrom(t.getClass()),
                    "Expected exception of type '" + throwable.getName() + "' but instead a exception of type '" + t.getClass().getName() + "' was thrown.");
            assertTrue(t.getMessage().matches(msgMatchesRegex), "The error message [" + t.getMessage() + "] was expected to match [" + msgMatchesRegex + "].");
            return throwable.cast(t);
        }
        fail("Expected exception of type '" + throwable.getName() + "'.");

        // never reaches here
        return null;
    }
}
