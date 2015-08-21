/*
  Copyright (c) 2002, 2014, Oracle and/or its affiliates. All rights reserved.

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

package testsuite.simple;

import com.mysql.jdbc.log.StandardLogger;
import com.mysql.jdbc.profiler.ProfilerEvent;

public class TestBug57662Logger extends StandardLogger {

    public boolean hasNegativeDurations = false;

    public TestBug57662Logger(String name) {
        super(name, false);
    }

    @Override
    protected void logInternal(int level, Object msg, Throwable exception) {
        if (!this.hasNegativeDurations && msg instanceof ProfilerEvent) {
            this.hasNegativeDurations = ((ProfilerEvent) msg).getEventDuration() < 0;
        }
        super.logInternal(level, msg, exception);
    }
}
