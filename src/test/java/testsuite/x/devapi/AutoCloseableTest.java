package testsuite.x.devapi;

import com.mysql.cj.xdevapi.Session;
import com.mysql.cj.xdevapi.SessionImpl;
import com.mysql.cj.xdevapi.SessionFactory;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class AutoCloseableTest {
    @Test
    public void testAutoCloseableSession() throws Exception {
        final SessionFactory testSessionFactory = new SessionFactory();
        Session session = null;
        try (Session s = new MockSession()) {
            session = s;
            assertTrue(s.isOpen());
        }
        assertNotNull(session);
        assertFalse(session.isOpen());
    }

    private static class MockSession extends SessionImpl {
        boolean closed = false;

        public MockSession() {
        }

        @Override
        public boolean isOpen() {
            return !closed;
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
