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

    public RowDataStatic(ArrayList rows)
    {
    	this.index = -1;
        this.rows = rows;
    }

    public boolean hasNext()
    {
		boolean hasMore = (this.index + 1) < rows.size();
		
        return hasMore;
    }

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

    public void beforeLast()
    {
        this.index = rows.size() - 2;
    }

    /** Moves to after last.*/
    public void afterLast()
    {
        this.index = rows.size();
    }

    public boolean isFirst()
    {

        return this.index == 0;
    }

    public boolean isLast()
    {

        return (this.index == rows.size() - 1);
    }

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

    public int getCurrentRowNumber()
    {

        return this.index;
    }

    public void setCurrentRow(int newIndex)
    {
        this.index = newIndex;
    }

    public void moveRowRelative(int rows)
    {
        this.index += rows;
    }

    public void addRow(byte[][] row)
    {
        rows.add(row);
    }

    public void removeRow(int atIndex)
    {
        rows.remove(atIndex);
    }

    public boolean isEmpty()
    {

        return rows.size() == 0;
    }

    public int size()
    {

        return rows.size();
    }

    public boolean isDynamic()
    {

        return false;
    }
}