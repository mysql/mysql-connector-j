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
 * Allows streaming of MySQL data.
 * 
 * @author dgan
 * @version $Id$
 */

public class RowDataDynamic
    implements RowData {

    //~ Instance/static variables .............................................

    private MysqlIO io;
    private byte[][] nextRow;
    private int columnCount;
    private int index = -1;
    private boolean isAtEnd = false;
    private boolean isAfterEnd = false;

    //~ Constructors ..........................................................

    /**
     * Creates a new RowDataDynamic object.
     * 
     * @param io DOCUMENT ME!
     * @param colCount DOCUMENT ME!
     * @throws SQLException DOCUMENT ME!
     */
    public RowDataDynamic(MysqlIO io, int colCount)
                   throws SQLException {
        this.io = io;
        this.columnCount = colCount;
        nextRecord();
    }

    //~ Methods ...............................................................

    private void nextRecord()
                     throws SQLException {

        try {

            if (!isAtEnd) {
                nextRow = io.nextRow((int) columnCount);

                if (nextRow == null) {
                    isAtEnd = true;
                }
            } else {
                isAfterEnd = true;
            }
        } catch (Exception e) {
            throw new SQLException("Error trying to fetch row:"
                                   + e.getMessage(), "S1000");
        }
    }

    private boolean streamerClosed = false;

    /** 
     * Returns true if another row exsists.
     *
     * @return true if more rows
     * @throws SQLException if a database error occurs
     */
    public boolean hasNext()
                    throws SQLException {

        boolean hasNext = (nextRow != null);

        if (!hasNext && !streamerClosed) {
            io.closeStreamer(this);
            streamerClosed = true;
        }

        return hasNext;
    }

    /** 
     * Returns the next row.
     *
     * @return the next row value
     * @throws SQLException if a database error occurs
     */
    public byte[][] next()
                  throws SQLException {
        index++;

        byte[][] ret = nextRow;
        nextRecord();

        return ret;
    }

    private void notSupported()
                       throws SQLException {
        throw new OperationNotSupportedException();
    }

    /** 
     * Returns if iteration has not occured yet.
     *
     * @return true if before first row
     * @throws SQLException if a database error occurs
     */
    public boolean isBeforeFirst()
                          throws SQLException {

        return index < 0;
    }

    /** 
     * Returns true if we got the last element.
     *
     * @return true if after last row 
     * @throws SQLException if a database error occurs
     */
    public boolean isAfterLast()
                        throws SQLException {

        return isAfterEnd;
    }

    /** 
     * Moves to before first.
     *
     * @throws SQLException if a database error occurs
     */
    public void beforeFirst()
                     throws SQLException {
        notSupported();
    }

    /** 
     * Moves to after last.
     *
     * @throws SQLException if a database error occurs
     */
    public void afterLast()
                   throws SQLException {
        notSupported();
    }

    /** 
     * Moves to before last so next el is the last el.
     *
     * @throws SQLException if a database error occurs
     */
    public void beforeLast()
                    throws SQLException {
        notSupported();
    }

    /**
     * Are we on the first row of the result set?
     * 
     * @return true if on first row
     * @throws SQLException if a database error occurs
     */
    public boolean isFirst()
                    throws SQLException {
        notSupported();

        return false;
    }

    /**
     * Are we on the last row of the result set?
     * 
     * @return true if on last row
     * @throws SQLException if a database error occurs
     */
    public boolean isLast()
                   throws SQLException {
        notSupported();

        return false;
    }

    /** 
     * We're done.
     *
     * @throws SQLException if a database error occurs
     */
    public void close()
               throws SQLException {

        //drain the rest of the records.
        while (this.hasNext()) {
            this.next();
        }
    }

    /**
     * Returns the current position in the result set as
     * a row number.
     * 
     * @return the current row number
     * @throws SQLException if a database error occurs
     */
    public int getCurrentRowNumber()
                            throws SQLException {
        notSupported();

        return -1;
    }

   /**
     * Moves the current position in the result set to
     * the given row number.
     * 
     * @param rowNumber row to move to
     * @throws SQLException if a database error occurs
     */
    public void setCurrentRow(int rowNumber)
                       throws SQLException {
        notSupported();
    }

    /**
     * Moves the current position relative 'rows' from
     * the current position.
     *
     * @param rows the relative number of rows to move
     * @throws SQLException if a database error occurs
     */
    public void moveRowRelative(int rows)
                         throws SQLException {
        notSupported();
    }

    /**
     * Returns true if the result set is dynamic.
     *
     * This means that move back and move forward won't work
     * because we do not hold on to the records.
     * 
     * @return true if this result set is streaming from the server
     */
    public boolean isDynamic() {

        return true;
    }

    /** 
     * Only works on non dynamic result sets.
     *
     * @return the size of this row data
     */
    public int size() {

        return RESULT_SET_SIZE_UNKNOWN;
    }

    /** 
     * Has no records.
     * 
     * @return true if no records
     * @throws SQLException if a database error occurs
     */
    public boolean isEmpty()
                    throws SQLException {
        notSupported();

        return false;
    }

    /** 
     * Only works on non dynamic result sets.
     *
     * @param index row number to get at
     * @return row data at index
     * @throws SQLException if a database error occurs
     */
    public byte[][] getAt(int index)
                   throws SQLException {
        notSupported();

        return null;
    }

    /**
     * Adds a row to this row data.
     *
     * @param row the row to add
     * @throws SQLException if a database error occurs
     */
    public void addRow(byte[][] row)
                throws SQLException {
        notSupported();
    }

    /**
     * Removes the row at the given index.
     *
     * @param index the row to move to
     * @throws SQLException if a database error occurs
     */
    public void removeRow(int index)
                   throws SQLException {
        notSupported();
    }

    //~ Inner classes .........................................................

    class OperationNotSupportedException
        extends SQLException {
        OperationNotSupportedException() {
            super("Operation not supported for streaming result sets", "S1009");
        }
    }
}