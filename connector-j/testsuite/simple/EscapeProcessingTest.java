/*
   Copyright (C) 2002 MySQL AB
   
      This program is free software; you can redistribute it and/or modify
      it under the terms of the GNU General Public License as published by
      the Free Software Foundation; either version 2 of the License, or
      (at your option) any later version.
   
      This program is distributed in the hope that it will be useful,
      but WITHOUT ANY WARRANTY; without even the implied warranty of
      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
      GNU General Public License for more details.
   
      You should have received a copy of the GNU General Public License
      along with this program; if not, write to the Free Software
      Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
      
 */
 
package testsuite.simple;

import testsuite.BaseTestCase;

/**
 * Tests escape processing
 * 
 * @author Mark Matthews
 */
public class EscapeProcessingTest extends BaseTestCase {

	/**
	 * Constructor for EscapeProcessingTest.
	 * @param name
	 */
	public EscapeProcessingTest(String name) {
		super(name);
	}
    
    public void testEscapeProcessing() throws Exception {
      
        String exSql = "select {fn dayname ({fn abs({fn now()})})},   -- Today    \n" +
        "           {d '1997-05-24'},  -- a date                    \n" +
        "           {t '10:30:29' },  -- a time                     \n" +
        "           {ts '1997-05-24 10:30:29.123'}, -- a timestamp  \n" +
        "          '{string data with { or } will not be altered'   \n" +
        "--  Also note that you can safely include { and } in comments";
        
        String escapedSql = conn.nativeSQL(exSql);
        
        System.out.println(escapedSql);
    }

}
