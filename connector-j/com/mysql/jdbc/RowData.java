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
public interface RowData
{

    //~ Instance/static variables .............................................

    /** 
     * What's returned for the size of a result set
     * when its size can not be determined.
     */
    public int RESULT_SET_SIZE_UNKNOWN = -1;

    //~ Methods ...............................................................

    /** Returns true if another row exsists.*/
    public boolean hasNext()
                    throws SQLException;

    /** Returns the next row.*/
    public byte[][] next()
                  throws SQLException;

    /** Returns if iteration has not occured yet.*/
    public boolean isBeforeFirst()
                          throws SQLException;

    /** Returns true if we got the last element.*/
    public boolean isAfterLast()
                        throws SQLException;

    /** Moves to before first.*/
    public void beforeFirst()
                     throws SQLException;

    /** Moves to after last.*/
    public void afterLast()
                   throws SQLException;

    /** Moves to before last so next el is the last el.*/
    public void beforeLast()
                    throws SQLException;

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     * @throws SQLException DOCUMENT ME!
     */
    public boolean isFirst()
                    throws SQLException;

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     * @throws SQLException DOCUMENT ME!
     */
    public boolean isLast()
                   throws SQLException;

    /** We're done.*/
    public void close()
               throws SQLException;

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     * @throws SQLException DOCUMENT ME!
     */
    public int getCurrentRowNumber()
                            throws SQLException;

    /**
     * DOCUMENT ME!
     * 
     * @param index DOCUMENT ME!
     * @throws SQLException DOCUMENT ME!
     */
    public void setCurrentRow(int index)
                       throws SQLException;

    /**
     * DOCUMENT ME!
     * 
     * @param rows DOCUMENT ME!
     * @throws SQLException DOCUMENT ME!
     */
    public void moveRowRelative(int rows)
                         throws SQLException;

    /** 
     * Returns true if the result set is dynamic.
        *  
        * This means that move back and move forward won't work
     * because we do not hold on to the records.
     */
    public boolean isDynamic()
                      throws SQLException;

    /** Only works on non dynamic result sets.*/
    public int size()
             throws SQLException;

    /** Has no records.*/
    public boolean isEmpty()
                    throws SQLException;

    /** Only works on non dynamic result sets.*/
    public byte[][] getAt(int index)
                   throws SQLException;

    /**
     * DOCUMENT ME!
     * 
     * @param row DOCUMENT ME!
     * @throws SQLException DOCUMENT ME!
     */
    public void addRow(byte[][] row)
                throws SQLException;

    /**
     * DOCUMENT ME!
     * 
     * @param index DOCUMENT ME!
     * @throws SQLException DOCUMENT ME!
     */
    public void removeRow(int index)
                   throws SQLException;
}