/*
 * MM JDBC Drivers for MySQL
 *
 * $Id$
 *
 * Copyright (C) 1998 Mark Matthews <mmatthew@worldserver.com>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the
 * Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA  02111-1307, USA.
 *
 * See the COPYING file located in the top-level-directory of
 * the archive of this library for complete text of license.
 */

/**
 * EscapeProcessor performs all escape code processing as outlined
 * in the JDBC spec by JavaSoft.
 *
 * @author Mark Matthews <mmatthew@worldserver.com>
 * @version $Id$
 */

package org.gjt.mm.mysql;

import java.util.StringTokenizer;
import java.util.Vector;
import java.util.Stack;

class EscapeProcessor
{
  /**
   * Escape process one string
   *
   * @param SQL the SQL to escape process.
   * @return the SQL after it has been escape processed.
   */

  public synchronized String escapeSQL(String SQL) throws java.sql.SQLException
  {
    boolean replaceEscapeSequence = false;
    String EscapeSequence = null;
    StringBuffer NewSQL = new StringBuffer();

    if (SQL == null) {
      return null;
    }

    /*
     * Short circuit this code if we don't have a matching pair of
     * "{}". - Suggested by Ryan Gustafason
     */

    int begin_brace = SQL.indexOf("{");
    int next_end_brace = (begin_brace == -1) ? -1 : SQL.indexOf("}", begin_brace);

    if (next_end_brace == -1) {
      return SQL;
    }

    EscapeTokenizer ET = new EscapeTokenizer(SQL);

    while (ET.hasMoreTokens()) {
      String Token = ET.nextToken();
      
      if (Token.startsWith("{")) { // It's an escape code
        if (!Token.endsWith("}")) {
          throw new java.sql.SQLException("Not a valid escape sequence: " + Token);
        }

        /*
         * Process the escape code
         */

        if (Token.toLowerCase().startsWith("{escape")) {
          try {
            StringTokenizer ST = new StringTokenizer(Token, " '");
            ST.nextToken(); // eat the "escape" token
            EscapeSequence = ST.nextToken();
            if (EscapeSequence.length() < 3) {
              throw new java.sql.SQLException("Syntax error for escape sequence '" + Token + "'", "42000");
            }
            EscapeSequence = EscapeSequence.substring( 1, EscapeSequence.length() - 1);

            replaceEscapeSequence = true;
          }
          catch (java.util.NoSuchElementException E) {
            throw new java.sql.SQLException("Syntax error for escape sequence '" + Token + "'", "42000");
          }
        }
        else if (Token.toLowerCase().startsWith("{fn")) {

          // just pass functions right to the DB
          int start_pos = Token.indexOf("fn ") + 3;
          int end_pos = Token.length() -1; // no }

          NewSQL.append(Token.substring(start_pos, end_pos));
        }
        else if (Token.toLowerCase().startsWith("{d")) {
          int start_pos = Token.indexOf("'") + 1;
      
          int end_pos = Token.lastIndexOf("'"); // no }
          
          if (start_pos == -1 || end_pos == -1) {
            throw new java.sql.SQLException("Syntax error for DATE escape sequence '" + Token + "'", "42000");
          }

          String Argument = Token.substring(start_pos, end_pos);
                
          try {
            StringTokenizer ST = new StringTokenizer(Argument, " -");

            String YYYY = ST.nextToken();
            String MM   = ST.nextToken();
            String DD   = ST.nextToken();

            String DateString = "'" + YYYY + "-" + MM + "-" + DD + "'";

            NewSQL.append(DateString);
          }
          catch (java.util.NoSuchElementException E) {
            throw new java.sql.SQLException("Syntax error for DATE escape sequence '" + Argument + "'", "42000");
          }
        }
        else if (Token.toLowerCase().startsWith("{ts")) {  
          int start_pos = Token.indexOf("'") + 1;
      
          int end_pos = Token.lastIndexOf("'"); // no }
          
          if (start_pos == -1 || end_pos == -1) {
            throw new java.sql.SQLException("Syntax error for TIMESTAMP escape sequence '" + Token + "'", "42000");
          }
          
          String Argument = Token.substring(start_pos, end_pos);
          
          try {
            StringTokenizer ST = new StringTokenizer(Argument, " .-:");

            String YYYY = ST.nextToken();
            String MM   = ST.nextToken();
            String DD   = ST.nextToken();
            String HH   = ST.nextToken();
            String Mm   = ST.nextToken();
            String SS   = ST.nextToken();

            /*
             * For now, we get the fractional seconds
             * part, but we don't use it, as MySQL doesn't
             * support it in it's TIMESTAMP data type
             */

            String F = "";

            if (ST.hasMoreTokens()) {
              F = ST.nextToken();
            }

            /*
             * Use the full format because number format
             * will not work for "between" clauses.
             *
             * Ref. Mysql Docs
             *
             * You can specify DATETIME, DATE and TIMESTAMP values
             * using any of a common set of formats:
             *
             * As a string in either 'YYYY-MM-DD HH:MM:SS' or
             * 'YY-MM-DD HH:MM:SS' format.
             *
             * Thanks to Craig Longman for pointing out this bug
             */
            NewSQL.append("'").append(YYYY).append("-").append(MM).append("-").append(DD).append(" ").append(HH).append(":").append(Mm).append(":").append(SS).append("'");
          }
          catch (java.util.NoSuchElementException E) {
            throw new java.sql.SQLException("Syntax error for TIMESTAMP escape sequence '" + Argument + "'", "42000");
          }
        }
        else if (Token.toLowerCase().startsWith("{t")) {

          int start_pos = Token.indexOf("'") + 1;
      
          int end_pos = Token.lastIndexOf("'"); // no }
          
          if (start_pos == -1 || end_pos == -1) {
            throw new java.sql.SQLException("Syntax error for TIME escape sequence '" + Token + "'", "42000");
          }

          String Argument = Token.substring(start_pos, end_pos);
          
          try {
            StringTokenizer ST = new StringTokenizer(Argument, " :");

            String HH   = ST.nextToken();
            String MM   = ST.nextToken();
            String SS   = ST.nextToken();

            String TimeString = "'" + HH + ":" + MM + ":" +SS + "'";

            NewSQL.append(TimeString);
          }
          catch (java.util.NoSuchElementException E) {
            throw new java.sql.SQLException("Syntax error for escape sequence '" + Argument + "'", "42000");
          }
        }
        else if (Token.toLowerCase().startsWith("{call") ||
        Token.toLowerCase().startsWith("{? = call")) {
          throw new java.sql.SQLException("Stored procedures not supported: " + Token, "S1C00");
        }
        else if (Token.toLowerCase().startsWith("{oj")) {
          // MySQL already handles this escape sequence
          // because of ODBC. Cool.


          NewSQL.append(Token);
        }

      }
      else {
        NewSQL.append(Token); // it's just part of the query
      }
    }

    String EscapedSQL = NewSQL.toString();

    if (replaceEscapeSequence) {
      String CurrentSQL = EscapedSQL;
      while (CurrentSQL.indexOf(EscapeSequence) != -1) {
        int escapePos = CurrentSQL.indexOf(EscapeSequence);
        String LHS = CurrentSQL.substring(0, escapePos);
        String RHS = CurrentSQL.substring(escapePos + 1, CurrentSQL.length());
        CurrentSQL = LHS + "\\" + RHS;
      }
      EscapedSQL = CurrentSQL;
    }

    return EscapedSQL;
  }
}
