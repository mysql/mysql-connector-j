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
 
package com.mysql.jdbc;

import java.sql.SQLException;

/** 
 * This interface abstracts away how row data is accessed by
 * the result set. It is meant to allow a static implementation
 * (Current version), and a streaming one.
 */
public interface RowData {

    //~ Instance/static variables .............................................

    /** 
     * What's returned for the size of a result set
     * when its size can not be determined.
     */
    public static final int RESULT_SET_SIZE_UNKNOWN = -1;

    //~ Methods ...............................................................

    /** Returns true if another row exsists.*/
    boolean hasNext()
                    throws SQLException;

    /** Returns the next row.*/
    byte[][] next()
                  throws SQLException;

    /** Returns if iteration has not occured yet.*/
    boolean isBeforeFirst()
                          throws SQLException;

    /** Returns true if we got the last element.*/
    boolean isAfterLast()
                        throws SQLException;

    /** Moves to before first.*/
    void beforeFirst()
                     throws SQLException;

    /** Moves to after last.*/
    void afterLast()
                   throws SQLException;

    /** Moves to before last so next el is the last el.*/
    void beforeLast()
                    throws SQLException;

    /**
     * Are we on the first row of the result set?
     */
    boolean isFirst()
                    throws SQLException;

    /**
     * Are we on the last row of the result set?
     */
    boolean isLast()
                   throws SQLException;

    /** We're done.*/
    void close()
               throws SQLException;

    /**
     * Returns the current position in the result set as 
     * a row number.
     */
    int getCurrentRowNumber()
                            throws SQLException;

    /**
     * Moves the current position in the result set to 
     * the given row number.
     */
    void setCurrentRow(int rowNumber)
                       throws SQLException;

    /**
     * Moves the current position relative 'rows' from
     * the current position.
     */
    void moveRowRelative(int rows)
                         throws SQLException;

    /** 
     * Returns true if the result set is dynamic.
     *  
     * This means that move back and move forward won't work
     * because we do not hold on to the records.
     */
    boolean isDynamic()
                      throws SQLException;

    /** Only works on non dynamic result sets.*/
    int size()
             throws SQLException;

    /** Has no records.*/
    boolean isEmpty()
                    throws SQLException;

    /** Only works on non dynamic result sets.*/
    byte[][] getAt(int index)
                   throws SQLException;

    /**
     * Adds a row to this row data.
     */
    void addRow(byte[][] row)
                throws SQLException;

    /**
     * Removes the row at the given index.
     */
    void removeRow(int index)
                   throws SQLException;
}