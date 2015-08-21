/*
  Copyright (c) 2013, 2014, Oracle and/or its affiliates. All rights reserved.

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

import com.mysql.jdbc.NonRegisteringDriver.ConnectionPhantomReference;

public class AbandonedConnectionCleanupThread extends Thread {
    private static boolean running = true;
    private static Thread threadRef = null;

    public AbandonedConnectionCleanupThread() {
        super("Abandoned connection cleanup thread");
    }

    @Override
    public void run() {
        threadRef = this;
        while (running) {
            try {
                Reference<? extends ConnectionImpl> ref = NonRegisteringDriver.refQueue.remove(100);
                if (ref != null) {
                    try {
                        ((ConnectionPhantomReference) ref).cleanup();
                    } finally {
                        NonRegisteringDriver.connectionPhantomRefs.remove(ref);
                    }
                }

            } catch (Exception ex) {
                // no where to really log this if we're static
            }
        }
    }

    public static void shutdown() throws InterruptedException {
        running = false;
        if (threadRef != null) {
            threadRef.interrupt();
            threadRef.join();
            threadRef = null;
        }
    }

}
