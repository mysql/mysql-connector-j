/*
  Copyright (c) 2013, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.jdbc;

import java.lang.ref.Reference;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.mysql.jdbc.NonRegisteringDriver.ConnectionPhantomReference;

/**
 * This class implements a thread that is responsible for closing abandoned MySQL connections, i.e., connections that are not explicitly closed.
 * There is only one instance of this class and there is a single thread to do this task. This thread's executor is statically referenced in this same class.
 */
public class AbandonedConnectionCleanupThread implements Runnable {
    private static final ExecutorService cleanupThreadExcecutorService;
    static Thread threadRef = null;

    static {
        cleanupThreadExcecutorService = Executors.newSingleThreadExecutor(new ThreadFactory() {
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "Abandoned connection cleanup thread");
                t.setDaemon(true);
                // Tie the thread's context ClassLoader to the ClassLoader that loaded the class instead of inheriting the context ClassLoader from the current
                // thread, which would happen by default.
                // Application servers may use this information if they attempt to shutdown this thread. By leaving the default context ClassLoader this thread
                // could end up being shut down even when it is shared by other applications and, being it statically initialized, thus, never restarted again.
                t.setContextClassLoader(AbandonedConnectionCleanupThread.class.getClassLoader());
                return threadRef = t;
            }
        });
        cleanupThreadExcecutorService.execute(new AbandonedConnectionCleanupThread());
    }

    private AbandonedConnectionCleanupThread() {
    }

    public void run() {
        for (;;) {
            try {
                checkContextClassLoaders();
                Reference<? extends ConnectionImpl> ref = NonRegisteringDriver.refQueue.remove(5000);
                if (ref != null) {
                    try {
                        ((ConnectionPhantomReference) ref).cleanup();
                    } finally {
                        NonRegisteringDriver.connectionPhantomRefs.remove(ref);
                    }
                }

            } catch (InterruptedException e) {
                threadRef = null;
                return;

            } catch (Exception ex) {
                // Nowhere to really log this.
            }
        }
    }

    /**
     * Checks if the thread's context ClassLoader is active. This is usually true but some application managers implement a life-cycle mechanism in their
     * ClassLoaders that is linked to the corresponding application's life-cycle. As such, a stopped/ended application will have a ClassLoader unable to load
     * anything and, eventually, they throw an exception when trying to do so. When this happens, this thread has no point in being alive anymore.
     */
    private void checkContextClassLoaders() {
        try {
            threadRef.getContextClassLoader().getResource("");
        } catch (Throwable e) {
            // Shutdown no matter what.
            uncheckedShutdown();
        }
    }

    /**
     * Checks if the context ClassLoaders from this and the caller thread are the same.
     * 
     * @return true if both threads share the same context ClassLoader, false otherwise
     */
    private static boolean consistentClassLoaders() {
        ClassLoader callerCtxClassLoader = Thread.currentThread().getContextClassLoader();
        ClassLoader threadCtxClassLoader = threadRef.getContextClassLoader();
        return callerCtxClassLoader != null && threadCtxClassLoader != null && callerCtxClassLoader == threadCtxClassLoader;
    }

    /**
     * Performs a checked shutdown, i.e., the context ClassLoaders from this and the caller thread are checked for consistency prior to performing the shutdown
     * operation.
     */
    public static void checkedShutdown() {
        shutdown(true);
    }

    /**
     * Performs an unchecked shutdown, i.e., the shutdown is performed independently of the context ClassLoaders from the involved threads.
     */
    public static void uncheckedShutdown() {
        shutdown(false);
    }

    /**
     * Shuts down this thread either checking or not the context ClassLoaders from the involved threads.
     * 
     * @param checked
     *            does a checked shutdown if true, unchecked otherwise
     */
    private static void shutdown(boolean checked) {
        if (checked && !consistentClassLoaders()) {
            // This thread can't be shutdown from the current thread's context ClassLoader. Doing so would most probably prevent from restarting this thread
            // later on. An unchecked shutdown can still be done if needed by calling shutdown(false).
            return;
        }
        cleanupThreadExcecutorService.shutdownNow();
    }

    /**
     * Shuts down this thread.
     * 
     * @deprecated use {@link #checkedShutdown()} instead.
     */
    @Deprecated
    public static void shutdown() {
        checkedShutdown();
    }
}
