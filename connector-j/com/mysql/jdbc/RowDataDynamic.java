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


/** Allows streaming of MySQL data.*/
public class RowDataDynamic
    implements RowData
{

    private MysqlIO io;
    private byte[][] nextRow;
    private int columnCount;
    private int index = -1;
    private boolean isAtEnd = false;
    private boolean isAfterEnd = false;

    /**
     * Creates a new RowDataDynamic object.
     * 
     * @param io DOCUMENT ME!
     * @param colCount DOCUMENT ME!
     * @throws SQLException DOCUMENT ME!
     */
    public RowDataDynamic(MysqlIO io, int colCount)
                   throws SQLException
    {
        this.io = io;
        this.columnCount = colCount;
        nextRecord();
    }

    private void nextRecord()
                     throws SQLException
    {

        try
        {

            if (!isAtEnd)
            {
                nextRow = io.nextRow((int)columnCount);

                if (nextRow == null)
                    isAtEnd = true;
            }
            else
            {
                isAfterEnd = true;
            }
        }
        catch (Exception e)
        {
            throw new SQLException("Error trying to fetch row:" + 
                                   e.getMessage(), "S1000");
        }
    }

    /** Returns true if another row exsists.*/
    public boolean hasNext()
                    throws SQLException
    {

        boolean hasNext = (nextRow != null);

        if (!hasNext)
        {
            io.closeStreamer(this);
        }

        return hasNext;
    }

    /** Returns the next row.*/
    public byte[][] next()
                  throws SQLException
    {
        index++;

        byte[][] ret = nextRow;
        nextRecord();

        return ret;
    }

    private void notSupported()
                       throws SQLException
    {
        throw new OperationNotSupportedException();
    }

    /** Returns if iteration has not occured yet.*/
    public boolean isBeforeFirst()
                          throws SQLException
    {

        return index < 0;
    }

    /** Returns true if we got the last element.*/
    public boolean isAfterLast()
                        throws SQLException
    {

        return isAfterEnd;
    }

    /** Moves to before first.*/
    public void beforeFirst()
                     throws SQLException
    {
        notSupported();
    }

    /** Moves to after last.*/
    public void afterLast()
                   throws SQLException
    {
        notSupported();
    }

    /** Moves to before last so next el is the last el.*/
    public void beforeLast()
                    throws SQLException
    {
        notSupported();
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     * @throws SQLException DOCUMENT ME!
     */
    public boolean isFirst()
                    throws SQLException
    {
        notSupported();

        return false;
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     * @throws SQLException DOCUMENT ME!
     */
    public boolean isLast()
                   throws SQLException
    {
        notSupported();

        return false;
    }

    /** We're done.*/
    public void close()
               throws SQLException
    {

        //drain the rest of the records.
        while (this.hasNext())
        {
            this.next();
        }

        io.closeStreamer(this);
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     * @throws SQLException DOCUMENT ME!
     */
    public int getCurrentRowNumber()
                            throws SQLException
    {
        notSupported();

        return -1;
    }

    /**
     * DOCUMENT ME!
     * 
     * @param index DOCUMENT ME!
     * @throws SQLException DOCUMENT ME!
     */
    public void setCurrentRow(int index)
                       throws SQLException
    {
        notSupported();
    }

    /**
     * DOCUMENT ME!
     * 
     * @param rows DOCUMENT ME!
     * @throws SQLException DOCUMENT ME!
     */
    public void moveRowRelative(int rows)
                         throws SQLException
    {
        notSupported();
    }

    /** Returns true if the result set is dynamic.
   *  This means that move back and move forward won't work
   *  because we do not hold on to the records.
   */
    public boolean isDynamic()
    {

        return true;
    }

    /** Only works on non dynamic result sets.*/
    public int size()
    {
        return RESULT_SET_SIZE_UNKNOWN;
    }

    /** Has no records.*/
    public boolean isEmpty()
                    throws SQLException
    {
        notSupported();

        return false;
    }

    /** Only works on non dynamic result sets.*/
    public byte[][] getAt(int index)
                   throws SQLException
    {
        notSupported();

        return null;
    }

    /**
     * DOCUMENT ME!
     * 
     * @param row DOCUMENT ME!
     * @throws SQLException DOCUMENT ME!
     */
    public void addRow(byte[][] row)
                throws SQLException
    {
        notSupported();
    }

    /**
     * DOCUMENT ME!
     * 
     * @param index DOCUMENT ME!
     * @throws SQLException DOCUMENT ME!
     */
    public void removeRow(int index)
                   throws SQLException
    {
        notSupported();
    }

    class OperationNotSupportedException
        extends SQLException
    {
        OperationNotSupportedException()
        {
            super("Operation not supported for streaming result sets", "S1009");
        }
    }
}