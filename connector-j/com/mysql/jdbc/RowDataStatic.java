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

import java.util.ArrayList;


public class RowDataStatic
    implements RowData
{

    private ArrayList rows;
    private int index;

    /**
     * Creates a new RowDataStatic object.
     * 
     * @param rows DOCUMENT ME!
     */
    public RowDataStatic(ArrayList rows)
    {
        this.index = -1;
        this.rows = rows;
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public boolean hasNext()
    {

        boolean hasMore = (this.index + 1) < rows.size();

        return hasMore;
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public byte[][] next()
    {
        this.index++;

        if (this.index < rows.size())
        {

            return (byte[][])rows.get(this.index);
        }
        else
        {

            return null;
        }
    }

    /**
     * DOCUMENT ME!
     */
    public void close()
    {
    }

    /** Returns if iteration has not occured yet.*/
    public boolean isBeforeFirst()
    {

        return this.index == -1;
    }

    /** Returns true if we got the last element.*/
    public boolean isAfterLast()
    {

        return this.index >= rows.size();
    }

    /** Moves to before first.*/
    public void beforeFirst()
    {
        this.index = -1;
    }

    /**
     * DOCUMENT ME!
     */
    public void beforeLast()
    {
        this.index = rows.size() - 2;
    }

    /** Moves to after last.*/
    public void afterLast()
    {
        this.index = rows.size();
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public boolean isFirst()
    {

        return this.index == 0;
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public boolean isLast()
    {

        return (this.index == rows.size() - 1);
    }

    /**
     * DOCUMENT ME!
     * 
     * @param atIndex DOCUMENT ME!
     * @return DOCUMENT ME! 
     */
    public byte[][] getAt(int atIndex)
    {

        if (atIndex < 0 || atIndex > rows.size())
        {

            return null;
        }
        else
        {

            return (byte[][])rows.get(atIndex);
        }
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public int getCurrentRowNumber()
    {

        return this.index;
    }

    /**
     * DOCUMENT ME!
     * 
     * @param newIndex DOCUMENT ME!
     */
    public void setCurrentRow(int newIndex)
    {
        this.index = newIndex;
    }

    /**
     * DOCUMENT ME!
     * 
     * @param rows DOCUMENT ME!
     */
    public void moveRowRelative(int rows)
    {
        this.index += rows;
    }

    /**
     * DOCUMENT ME!
     * 
     * @param row DOCUMENT ME!
     */
    public void addRow(byte[][] row)
    {
        rows.add(row);
    }

    /**
     * DOCUMENT ME!
     * 
     * @param atIndex DOCUMENT ME!
     */
    public void removeRow(int atIndex)
    {
        rows.remove(atIndex);
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public boolean isEmpty()
    {

        return rows.size() == 0;
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public int size()
    {

        return rows.size();
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public boolean isDynamic()
    {

        return false;
    }
}