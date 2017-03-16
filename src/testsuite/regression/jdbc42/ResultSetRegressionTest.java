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

package testsuite.regression.jdbc42;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;

import testsuite.BaseTestCase;

public class ResultSetRegressionTest extends BaseTestCase {
    public ResultSetRegressionTest(String name) {
        super(name);
    }
    
    /**
     * Tests fix for Bug#84189 - Allow null when extracting java.time.* classes from ResultSet.
     */
    public void testBug84189() throws Exception {
        createTable("testBug84189", "(d DATE NULL, t TIME NULL, dt DATETIME NULL, ts TIMESTAMP NULL, ot VARCHAR(100), odt VARCHAR(100))");
        this.stmt.execute(
                "INSERT INTO testBug84189 VALUES ('2017-01-01', '10:20:30', '2017-01-01 10:20:30', '2017-01-01 10:20:30', '10:20:30+04:00', '2017-01-01T10:20:30+04:00')");
        this.stmt.execute("INSERT INTO testBug84189 VALUES (NULL, NULL, NULL, NULL, NULL, NULL)");

        this.rs = this.stmt.executeQuery("SELECT * FROM testBug84189");
        assertTrue(this.rs.next());
        assertEquals(LocalDate.of(2017, 1, 1), this.rs.getObject(1, LocalDate.class));
        assertEquals(LocalTime.of(10, 20, 30), this.rs.getObject(2, LocalTime.class));
        assertEquals(LocalDateTime.of(2017, 1, 1, 10, 20, 30), this.rs.getObject(3, LocalDateTime.class));
        assertEquals(LocalDateTime.of(2017, 1, 1, 10, 20, 30), this.rs.getObject(4, LocalDateTime.class));
        assertEquals(OffsetTime.of(10, 20, 30, 0, ZoneOffset.ofHours(4)), this.rs.getObject(5, OffsetTime.class));
        assertEquals(OffsetDateTime.of(2017, 01, 01, 10, 20, 30, 0, ZoneOffset.ofHours(4)), this.rs.getObject(6, OffsetDateTime.class));

        assertEquals(LocalDate.class, this.rs.getObject(1, LocalDate.class).getClass());
        assertEquals(LocalTime.class, this.rs.getObject(2, LocalTime.class).getClass());
        assertEquals(LocalDateTime.class, this.rs.getObject(3, LocalDateTime.class).getClass());
        assertEquals(LocalDateTime.class, this.rs.getObject(4, LocalDateTime.class).getClass());
        assertEquals(OffsetTime.class, this.rs.getObject(5, OffsetTime.class).getClass());
        assertEquals(OffsetDateTime.class, this.rs.getObject(6, OffsetDateTime.class).getClass());

        assertTrue(this.rs.next());
        assertNull(this.rs.getObject(1, LocalDate.class));
        assertNull(this.rs.getObject(2, LocalTime.class));
        assertNull(this.rs.getObject(3, LocalDateTime.class));
        assertNull(this.rs.getObject(4, LocalDateTime.class));
        assertNull(this.rs.getObject(5, OffsetTime.class));
        assertNull(this.rs.getObject(6, OffsetDateTime.class));

        assertFalse(this.rs.next());
    }
}
