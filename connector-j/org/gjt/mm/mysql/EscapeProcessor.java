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

class EscapeProcessor
{
  /**
   * Escape process one string
   *
   * @param SQL the SQL to escape process.
   * @return the SQL after it has been escape processed.
   */

  public synchronized String escapeSQL(String sql) throws java.sql.SQLException
  {
    boolean replaceEscapeSequence = false;
    String escapeSequence = null;
    StringBuffer newSql = new StringBuffer();

    if (sql == null) {
      return null;
    }

    /*
     * Short circuit this code if we don't have a matching pair of
     * "{}". - Suggested by Ryan Gustafason
     */

    int begin_brace = sql.indexOf("{");
    int next_end_brace = (begin_brace == -1) ? -1 : sql.indexOf("}", begin_brace);

    if (next_end_brace == -1) {
      return sql;
    }

    EscapeTokenizer escapeTokenizer = new EscapeTokenizer(sql);

    while (escapeTokenizer.hasMoreTokens()) {
      String token = escapeTokenizer.nextToken();
      
      if (token.startsWith("{")) { // It's an escape code
        if (!token.endsWith("}")) {
          throw new java.sql.SQLException("Not a valid escape sequence: " + token);
        }

        /*
         * Process the escape code
         */

        if (token.toLowerCase().startsWith("{escape")) {
          try {
            StringTokenizer st = new StringTokenizer(token, " '");
            st.nextToken(); // eat the "escape" token
            escapeSequence = st.nextToken();
            if (escapeSequence.length() < 3) {
              throw new java.sql.SQLException("Syntax error for escape sequence '" + token + "'", "42000");
            }
            escapeSequence = escapeSequence.substring( 1, escapeSequence.length() - 1);

            replaceEscapeSequence = true;
          }
          catch (java.util.NoSuchElementException e) {
            throw new java.sql.SQLException("Syntax error for escape sequence '" + token + "'", "42000");
          }
        }
        else if (token.toLowerCase().startsWith("{fn")) {

          // just pass functions right to the DB
          int startPos = token.indexOf("fn ") + 3;
          int endPos = token.length() -1; // no }

          newSql.append(token.substring(startPos, endPos));
        }
        else if (token.toLowerCase().startsWith("{d")) {
          int startPos = token.indexOf("'") + 1;
      
          int endPos = token.lastIndexOf("'"); // no }
          
          if (startPos == -1 || endPos == -1) {
            throw new java.sql.SQLException("Syntax error for DATE escape sequence '" + token + "'", "42000");
          }

          String argument = token.substring(startPos, endPos);
                
          try {
            StringTokenizer st = new StringTokenizer(argument, " -");

            String year4 = st.nextToken();
            String month2   = st.nextToken();
            String day2   = st.nextToken();

            String dateString = "'" + year4 + "-" + month2 + "-" + day2 + "'";

            newSql.append(dateString);
          }
          catch (java.util.NoSuchElementException e) {
            throw new java.sql.SQLException("Syntax error for DATE escape sequence '" + argument + "'", "42000");
          }
        }
        else if (token.toLowerCase().startsWith("{ts")) {  
          int startPos = token.indexOf("'") + 1;
      
          int endPos = token.lastIndexOf("'"); // no }
          
          if (startPos == -1 || endPos == -1) {
            throw new java.sql.SQLException("Syntax error for TIMESTAMP escape sequence '" + token + "'", "42000");
          }
          
          String argument = token.substring(startPos, endPos);
          
          try {
            StringTokenizer st = new StringTokenizer(argument, " .-:");

            String year4 = st.nextToken();
            String month2   = st.nextToken();
            String day2   = st.nextToken();
            String hour   = st.nextToken();
            String minute   = st.nextToken();
            String second   = st.nextToken();

            /*
             * For now, we get the fractional seconds
             * part, but we don't use it, as MySQL doesn't
             * support it in it's TIMESTAMP data type
             */

            String fractionalSecond = "";

            if (st.hasMoreTokens()) {
              fractionalSecond = st.nextToken();
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
            newSql.append("'").append(year4).append("-").append(month2).append("-").append(day2).append(" ").append(hour).append(":").append(minute).append(":").append(second).append("'");
          }
          catch (java.util.NoSuchElementException e) {
            throw new java.sql.SQLException("Syntax error for TIMESTAMP escape sequence '" + argument + "'", "42000");
          }
        }
        else if (token.toLowerCase().startsWith("{t")) {

          int startPos = token.indexOf("'") + 1;
      
          int endPos = token.lastIndexOf("'"); // no }
          
          if (startPos == -1 || endPos == -1) {
            throw new java.sql.SQLException("Syntax error for TIME escape sequence '" + token + "'", "42000");
          }

          String argument = token.substring(startPos, endPos);
          
          try {
            StringTokenizer st = new StringTokenizer(argument, " :");

            String hour   = st.nextToken();
            String minute   = st.nextToken();
            String second   = st.nextToken();

            String timeString = "'" + hour + ":" + minute + ":" +second + "'";

            newSql.append(timeString);
          }
          catch (java.util.NoSuchElementException e) {
            throw new java.sql.SQLException("Syntax error for escape sequence '" + argument + "'", "42000");
          }
        }
        else if (token.toLowerCase().startsWith("{call") ||
        token.toLowerCase().startsWith("{? = call")) {
          throw new java.sql.SQLException("Stored procedures not supported: " + token, "S1C00");
        }
        else if (token.toLowerCase().startsWith("{oj")) {
          // MySQL already handles this escape sequence
          // because of ODBC. Cool.


          newSql.append(token);
        }

      }
      else {
        newSql.append(token); // it's just part of the query
      }
    }

    String escapedSql = newSql.toString();

    if (replaceEscapeSequence) {
      String currentSql = escapedSql;
      
      while (currentSql.indexOf(escapeSequence) != -1) {
        int escapePos = currentSql.indexOf(escapeSequence);
        String LHS = currentSql.substring(0, escapePos);
        String RHS = currentSql.substring(escapePos + 1, currentSql.length());
        currentSql = LHS + "\\" + RHS;
      }
      
      escapedSql = currentSql;
    }

    return escapedSql;
  }
}
