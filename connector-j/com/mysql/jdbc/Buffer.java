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
package com.mysql.jdbc;

import java.io.*;

import java.net.*;

import java.sql.*;

import java.util.*;


/**
 * Buffer contains code to read and write packets from/to the MySQL server.
 *
 * @author Mark Matthews <mmatthew@worldserver.com>
 * @version $Id$
 */
class Buffer
{
    public byte[] _buf;
    int _bufLength = 0;
    public int _pos = 0;
    int _sendLength = 0;
    final static int NO_LENGTH_LIMIT = -1;
    int _maxLength = NO_LENGTH_LIMIT;
    static long NULL_LENGTH = -1;

    Buffer(byte[] buf)
    {
        this._buf = buf;
        _bufLength = buf.length;
    }

    Buffer(int size, int max_packet_size)
    {
        _buf = new byte[size];
        _bufLength = _buf.length;
        _pos = MysqlIO.HEADER_LENGTH;
        _maxLength = max_packet_size;
    }

    Buffer(int size)
    {
        this(size, NO_LENGTH_LIMIT);
    }

    final void setBytes(byte[] buf)
    {
        _sendLength = _bufLength;
        System.arraycopy(buf, 0, this._buf, 0, _bufLength);
    }

    final byte readByte()
    {
        return _buf[_pos++];
    }


    // 2000-06-05 Changed
    final int readInt()
    {
        byte[] b = _buf; // a little bit optimization

        return (b[_pos++] & 0xff) | 
               ((b[_pos++] & 0xff) << 8);
    }


    // 2000-06-05 Changed
    final int readLongInt()
    {
        byte[] b = _buf;

        return (b[_pos++] & 0xff) | 
               ((b[_pos++] & 0xff) << 8) | 
               ((b[_pos++] & 0xff) << 16);
    }


    // 2000-06-05 Fixed
    final long readLong()
    {
        byte[] b = _buf;

        return (b[_pos++] & 0xff) | 
               ((b[_pos++] & 0xff) << 8) | 
               ((b[_pos++] & 0xff) << 16) | 
               ((b[_pos++] & 0xff) << 24);
    }


    // 2000-06-05 Fixed
    final long readLongLong()
    {
        byte[] b = _buf;

        return (long)(b[_pos++] & 0xff) | 
               ((long)(b[_pos++] & 0xff) << 8) | 
               ((long)(b[_pos++] & 0xff) << 16) | 
               ((long)(b[_pos++] & 0xff) << 24) | 
               ((long)(b[_pos++] & 0xff) << 32) | 
               ((long)(b[_pos++] & 0xff) << 40) | 
               ((long)(b[_pos++] & 0xff) << 48) | 
               ((long)(b[_pos++] & 0xff) << 56);
    }


    // Read n bytes depending
    final int readnBytes()
    {
        int sw = _buf[_pos++] & 0xff;

        switch (sw)
        {
            case 1:
                return _buf[_pos++] & 0xff;

            case 2:
                return this.readInt();

            case 3:
                return this.readLongInt();

            case 4:
                return (int)this.readLong();

            default:
                return 255;
        }
    }

    final long readLength()
    {
        int sw = _buf[_pos++] & 0xff;

        switch (sw)
        {
            case 251:
                return (long)0;

            case 252:
                return (long)readInt();

            case 253:
                return (long)readLongInt();

            case 254:
                return (long)readLong();

            default:
                return (long)sw;
        }
    }


    // For MySQL servers > 3.22.5
    final long newReadLength()
    {
        int sw = _buf[_pos++] & 0xff;

        switch (sw)
        {
            case 251:
                return (long)0;

            case 252:
                return (long)readInt();

            case 253:
                return (long)readLongInt();

            case 254: // changed for 64 bit lengths
                return (long)readLongLong();

            default:
                return (long)sw;
        }
    }

    final long readFieldLength()
    {
        int sw = _buf[_pos++] & 0xff;

        switch (sw)
        {
            case 251:
                return NULL_LENGTH;

            case 252:
                return (long)readInt();

            case 253:
                return (long)readLongInt();

            case 254:
                return (long)readLong();

            default:
                return (long)sw;
        }
    }


    // Read null-terminated string (native)
    final byte[] readByteArray()
    {
        return getNullTerminatedBytes();
    }


    // Read given-length string (native)
    final byte[] readLenByteArray()
    {
        long len = this.readFieldLength();

        if (len == NULL_LENGTH)
        {
            return null;
        }

        if (len == 0)
        {
            return new byte[0];
        }

        return getBytes((int)len);
    }


    //
    // Read a null-terminated string
    //
    // To avoid alloc'ing a new byte array, we
    // do this by hand, rather than calling getNullTerminatedBytes()
    //
    final String readString()
    {
        int i = _pos;
        int len = 0;

        while (_buf[i] != 0 && i < _bufLength)
        {
            len++;
            i++;
        }

        String S = new String(_buf, _pos, len);
        _pos += (len + 1); // update cursor

        return S;
    }


    //
    // Read given-length string
    //
    // To avoid alloc'ing a byte array that will
    // quickly be thrown away, we do this by
    // hand instead of calling getBytes()
    //
    final String readLenString()
    {
        long len = this.readFieldLength();

        if (len == NULL_LENGTH)
        {
            return null;
        }

        if (len == 0)
        {
            return "";
        }

        String S = new String(_buf, _pos, (int)len);
        _pos += len; // update cursor

        return S;
    }


    //
    // Read a given-length array of bytes
    //
    final byte[] getBytes(int len)
    {
        byte[] b = new byte[len];
        System.arraycopy(_buf, _pos, b, 0, len);
        _pos += len; // update cursor

        return b;
    }


    //
    // Read a null-terminated array of bytes
    //
    final byte[] getNullTerminatedBytes()
    {
        int i = _pos;
        int len = 0;

        while (_buf[i] != 0 && i < _bufLength)
        {
            len++;
            i++;
        }

        byte[] b = new byte[len];
        System.arraycopy(_buf, _pos, b, 0, len);
        _pos += (len + 1); // update cursor

        return b;
    }


    // 2000-06-05 Changed
    final boolean isLastDataPacket()
    {

        // return ((buf_length <= 2) && (ub(buf[0]) == 254));
        return ((_bufLength <= 2) && 
               ((_buf[0] & 0xff) == 254));
    }

    final void clear()
    {
        _pos = MysqlIO.HEADER_LENGTH;
    }

    final void writeByte(byte b)
    {
        _buf[_pos++] = b;
    }


    // 2000-06-05 Changed
    final void writeInt(int i)
    {
        byte[] b = _buf;
        b[_pos++] = (byte)(i & 0xff);
        b[_pos++] = (byte)(i >>> 8);
    }


    // 2000-06-05 Changed
    final void writeLongInt(int i)
    {
        byte[] b = _buf;
        b[_pos++] = (byte)(i & 0xff);
        b[_pos++] = (byte)(i >>> 8);
        b[_pos++] = (byte)(i >>> 16);
    }


    // 2000-06-05 Changed
    final void writeLong(long i)
    {
        byte[] b = _buf;
        b[_pos++] = (byte)(i & 0xff);
        b[_pos++] = (byte)(i >>> 8);
        b[_pos++] = (byte)(i >>> 16);
        b[_pos++] = (byte)(i >>> 24);
    }


    // Write null-terminated string
    final void writeString(String S)
    {
        writeStringNoNull(S);
        _buf[_pos++] = 0;
    }


    // Write string, with no termination
    final void writeStringNoNull(String S)
    {
        int len = S.length();
        ensureCapacity(len);

        for (int i = 0; i < len; i++)
        {
            _buf[_pos++] = (byte)S.charAt(i);
        }
    }


    // Write a String using the specified character
    // encoding
    final void writeStringNoNull(String S, String Encoding)
                          throws java.io.UnsupportedEncodingException
    {
        byte[] b = S.getBytes(Encoding);
        int len = b.length;
        ensureCapacity(len);
        System.arraycopy(b, 0, _buf, _pos, len);
        _pos += len;
    }


    // Write a byte array
    final void writeBytesNoNull(byte[] Bytes)
    {
        int len = Bytes.length;
        ensureCapacity(len);
        System.arraycopy(Bytes, 0, _buf, _pos, len);
        _pos += len;
    }

    final void ensureCapacity(int additional_data)
    {
        if ((_pos + additional_data) > _bufLength)
        {
            int new_length = (int)(_bufLength * 1.25);

            if (new_length < (_bufLength + additional_data))
            {
                new_length = _bufLength + 
                             (int)(additional_data * 1.25);
            }

            if (_maxLength != NO_LENGTH_LIMIT && 
                (new_length > _maxLength))
            {
                throw new IllegalArgumentException(
                        "Packet is larger than max_allowed_packet from server configuration of " + 
                        _maxLength + " bytes");
            }

            byte[] NewBytes = new byte[new_length];
            System.arraycopy(_buf, 0, NewBytes, 0, _buf.length);
            _buf = NewBytes;
            _bufLength = _buf.length;
        }
    }

    final void dump()
    {
        int p = 0;
        int rows = _bufLength / 8;

        for (int i = 0; i < rows; i++)
        {
            int ptemp = p;

            for (int j = 0; j < 8; j++)
            {
                String HexVal = Integer.toHexString((int)_buf[ptemp]);

                if (HexVal.length() == 1)
                {
                    HexVal = "0" + HexVal;
                }

                System.out.print(HexVal + " ");
                ptemp++;
            }

            System.out.print("    ");

            for (int j = 0; j < 8; j++)
            {
                if (_buf[p] > 32 && _buf[p] < 127)
                {
                    System.out.print((char)_buf[p] + " ");
                }
                else
                {
                    System.out.print(". ");
                }

                p++;
            }

            System.out.println();
        }

        int n = 0;

        for (int i = p; i < _bufLength; i++)
        {
            String HexVal = Integer.toHexString((int)_buf[i]);

            if (HexVal.length() == 1)
            {
                HexVal = "0" + HexVal;
            }

            System.out.print(HexVal + " ");
            n++;
        }

        for (int i = n; i < 8; i++)
        {
            System.out.print("   ");
        }

        System.out.print("    ");

        for (int i = p; i < _bufLength; i++)
        {
            if (_buf[i] > 32 && _buf[i] < 127)
            {
                System.out.print((char)_buf[i] + " ");
            }
            else
            {
                System.out.print(". ");
            }
        }

        System.out.println();
    }
}