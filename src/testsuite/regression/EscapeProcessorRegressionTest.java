/*
 Copyright (c) 2005, 2010, Oracle and/or its affiliates. All rights reserved.
 

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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
package testsuite.regression;

import testsuite.BaseTestCase;

/**
 * Tests regressions w/ the Escape Processor code.
 * 
 * @version $Id:$
 * 
 */
public class EscapeProcessorRegressionTest extends BaseTestCase {

	public EscapeProcessorRegressionTest(String name) {
		super(name);
	}

	/**
	 * Tests fix for BUG#11797 - Escape tokenizer doesn't respect stacked single
	 * quotes for escapes.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug11797() throws Exception {
		assertEquals(
				"select 'ESCAPED BY ''\\'' ON {tbl_name | * | *.* | db_name.*}'",
				this.conn
						.nativeSQL("select 'ESCAPED BY ''\\'' ON {tbl_name | * | *.* | db_name.*}'"));
	}

	/**
	 * Tests fix for BUG#11498 - Escape processor didn't honor strings
	 * demarcated with double quotes.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug11498() throws Exception {
		assertEquals(
				"replace into t1 (id, f1, f4) VALUES(1,\"\",\"tko { zna gdje se sakrio\"),(2,\"a\",\"sedmi { kontinentio\"),(3,\"a\",\"a } cigov si ti?\")",
				this.conn
						.nativeSQL("replace into t1 (id, f1, f4) VALUES(1,\"\",\"tko { zna gdje se sakrio\"),(2,\"a\",\"sedmi { kontinentio\"),(3,\"a\",\"a } cigov si ti?\")"));

	}

	/**
	 * Tests fix for BUG#14909 - escape processor replaces quote character in
	 * quoted string with string delimiter.
	 * 
	 * @throws Exception
	 */
	public void testBug14909() throws Exception {
		assertEquals("select '{\"','}'", this.conn
				.nativeSQL("select '{\"','}'"));
	}
	
	/**
	 * Tests fix for BUG#25399 - EscapeProcessor gets confused by multiple backslashes
	 * 
	 * @throws Exception if the test fails.
	 */
	public void testBug25399() throws Exception {
		assertEquals("\\' {d}",
				getSingleValueWithQuery("SELECT '\\\\\\' {d}'"));
	}
}
