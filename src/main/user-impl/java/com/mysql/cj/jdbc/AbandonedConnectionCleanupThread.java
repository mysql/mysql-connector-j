/*
 * Copyright (c) 2013, 2023, Oracle and/or its affiliates.
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

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.mysql.cj.MysqlConnection;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.protocol.NetworkResources;

/**
 * This class implements a thread that is responsible for closing abandoned MySQL connections, i.e., connections that are not explicitly closed.
 * There is only one instance of this class and there is a single thread to do this task. This thread's executor is statically referenced in this same class.
 */
public class AbandonedConnectionCleanupThread implements Runnable {

    private static final Set<ConnectionFinalizerPhantomReference> connectionFinalizerPhantomRefs = ConcurrentHashMap.newKeySet();
    private static final ReferenceQueue<MysqlConnection> referenceQueue = new ReferenceQueue<>();

    private static final ExecutorService cleanupThreadExecutorService;
    private static Thread threadRef = null;
    private static Lock threadRefLock = new ReentrantLock();
    private static boolean abandonedConnectionCleanupDisabled = Boolean.getBoolean(PropertyDefinitions.SYSP_disableAbandonedConnectionCleanup);

    static {
        if (abandonedConnectionCleanupDisabled) {
            cleanupThreadExecutorService = null;
        } else {
            cleanupThreadExecutorService = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "mysql-cj-abandoned-connection-cleanup");
                t.setDaemon(true);
                // Tie the thread's context ClassLoader to the ClassLoader that loaded the class instead of inheriting the context ClassLoader from the current
                // thread, which would happen by default.
                // Application servers may use this information if they attempt to shutdown this thread. By leaving the default context ClassLoader this thread
                // could end up being shut down even when it is shared by other applications and, being it statically initialized, thus, never restarted again.

                ClassLoader classLoader = AbandonedConnectionCleanupThread.class.getClassLoader();
                if (classLoader == null) {
                    // This class was loaded by the Bootstrap ClassLoader, so let's tie the thread's context ClassLoader to the System ClassLoader instead.
                    classLoader = ClassLoader.getSystemClassLoader();
                }

                t.setContextClassLoader(classLoader);
                return threadRef = t;
            });
            cleanupThreadExecutorService.execute(new AbandonedConnectionCleanupThread());
        }
    }

    private AbandonedConnectionCleanupThread() {
    }

    @Override
    public void run() {
        for (;;) {
            try {
                checkThreadContextClassLoader();
                Reference<? extends MysqlConnection> reference = referenceQueue.remove(5000);
                if (reference != null) {
                    finalizeResource((ConnectionFinalizerPhantomReference) reference);
                }
            } catch (InterruptedException e) {
                threadRefLock.lock();
                try {
                    threadRef = null;

                    // Finalize remaining references.
                    Reference<? extends MysqlConnection> reference;
                    while ((reference = referenceQueue.poll()) != null) {
                        finalizeResource((ConnectionFinalizerPhantomReference) reference);
                    }
                    connectionFinalizerPhantomRefs.clear();
                } finally {
                    threadRefLock.unlock();
                }
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
    private void checkThreadContextClassLoader() {
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
        threadRefLock.lock();
        try {
            if (threadRef == null) {
                return false;
            }
            ClassLoader callerCtxClassLoader = Thread.currentThread().getContextClassLoader();
            ClassLoader threadCtxClassLoader = threadRef.getContextClassLoader();
            return callerCtxClassLoader != null && threadCtxClassLoader != null && callerCtxClassLoader == threadCtxClassLoader;
        } finally {
            threadRefLock.unlock();
        }
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
        if (cleanupThreadExecutorService != null) {
            cleanupThreadExecutorService.shutdownNow();
        }
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
     * Returns true if the working thread is alive. It is alive if it was initialized successfully and wasn't shutdown yet.
     *
     * @return true if the working thread is alive; false otherwise.
     */
    public static boolean isAlive() {
        threadRefLock.lock();
        try {
            return threadRef != null && threadRef.isAlive();
        } finally {
            threadRefLock.unlock();
        }
    }

    /**
     * Tracks the finalization of a {@link MysqlConnection} object and keeps a reference to its {@link NetworkResources} so that they can be later released.
     *
     * @param conn
     *            the Connection object to track for finalization
     * @param io
     *            the network resources to close on the connection finalization
     */
    protected static void trackConnection(MysqlConnection conn, NetworkResources io) {
        if (abandonedConnectionCleanupDisabled) {
            return;
        }
        threadRefLock.lock();
        try {
            if (isAlive()) {
                ConnectionFinalizerPhantomReference reference = new ConnectionFinalizerPhantomReference(conn, io, referenceQueue);
                connectionFinalizerPhantomRefs.add(reference);
            }
        } finally {
            threadRefLock.unlock();
        }
    }

    /**
     * Release resources from the given {@link ConnectionFinalizerPhantomReference} and remove it from the references set.
     *
     * @param reference
     *            the {@link ConnectionFinalizerPhantomReference} to finalize.
     */
    private static void finalizeResource(ConnectionFinalizerPhantomReference reference) {
        try {
            reference.finalizeResources();
            reference.clear();
        } finally {
            connectionFinalizerPhantomRefs.remove(reference);
        }
    }

    /**
     * {@link PhantomReference} subclass to track {@link MysqlConnection} objects finalization.
     * This class holds a reference to the Connection's {@link NetworkResources} so they it can be later closed.
     */
    private static class ConnectionFinalizerPhantomReference extends PhantomReference<MysqlConnection> {

        private NetworkResources networkResources;

        ConnectionFinalizerPhantomReference(MysqlConnection conn, NetworkResources networkResources, ReferenceQueue<? super MysqlConnection> refQueue) {
            super(conn, refQueue);
            this.networkResources = networkResources;
        }

        void finalizeResources() {
            if (this.networkResources != null) {
                try {
                    this.networkResources.forceClose();
                } finally {
                    this.networkResources = null;
                }
            }
        }

    }

}
