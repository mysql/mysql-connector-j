
package com.mysql.cj.core;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class MessagesTest {

    @Test
    public void testLocalizedErrorMessages() throws Exception {
        Exception ex = new Exception();
        assertEquals("Cannot load connection class because of underlying exception: " + ex.toString() + ".",
                Messages.getString("NonRegisteringDriver.17", new Object[] { ex.toString() }));
        assertEquals("Unable to create properties transform instance 'Test' due to underlying exception: " + ex.toString(),
                Messages.getString("NonRegisteringDriver.38", new Object[] { "Test", ex.toString() }));
        assertEquals("Can't find configuration template named 'Test'", Messages.getString("NonRegisteringDriver.39", new Object[] { "Test" }));
        assertEquals("Unable to load configuration template 'Test' due to underlying IOException",
                Messages.getString("NonRegisteringDriver.40", new Object[] { "Test" }));

        assertEquals("Unsupported character encoding 'Test'", Messages.getString("ByteArrayBuffer.1", new Object[] { "Test" }));
        assertEquals("Unsupported character encoding 'Test'", Messages.getString("Field.12", new Object[] { "Test" }));
        assertEquals("Unsupported character encoding 'Test'", Messages.getString("PreparedStatement.32", new Object[] { "Test" }));
        assertEquals("Unsupported character encoding 'Test'", Messages.getString("StringUtils.0", new Object[] { "Test" }));
    }
}
