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

import java.io.UnsupportedEncodingException;

import java.sql.SQLException;


/**
 * Buffer contains code to read and write packets from/to the MySQL server.
 * 
 * @version $Id$
 * @author Mark Matthews
 */
class Buffer {

    //~ Instance/static variables .............................................

    static final int NO_LENGTH_LIMIT = -1;
    static final long NULL_LENGTH = -1;
    private byte[] byteBuffer;
    private boolean wasMultiPacket = false;
    private int bufLength = 0;
    private int maxLength = NO_LENGTH_LIMIT;
    private int position = 0;
    private int sendLength = 0;

    //~ Constructors ..........................................................

    Buffer(byte[] buf) {
        this.byteBuffer = buf;
        setBufLength(buf.length);
    }

    Buffer(int size, int maxPacketSize) {
        this.byteBuffer = new byte[size];
        setBufLength(this.byteBuffer.length);
        this.position = MysqlIO.HEADER_LENGTH;
        setMaxLength(maxPacketSize);
    }

    Buffer(int size) {
        this(size, NO_LENGTH_LIMIT);
    }

    //~ Methods ...............................................................

    /**
     * Sets the array of bytes to use as a buffer to read from.
     * 
     * @param byteBuffer the array of bytes to use as a buffer
     */
    public void setByteBuffer(byte[] byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    /**
     * Returns the array of bytes this Buffer is using to read from.
     * 
     * @return byte array being read from
     */
    public byte[] getByteBuffer() {

        return this.byteBuffer;
    }

    /**
     * DOCUMENT ME!
     * 
     * @param position DOCUMENT ME!
     */
    public void setPosition(int position) {
        this.position = position;
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME!
     */
    public int getPosition() {

        return this.position;
    }

    /**
     * DOCUMENT ME!
     * 
     * @param flag DOCUMENT ME!
     */
    public void setWasMultiPacket(boolean flag) {
        wasMultiPacket = flag;
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public int fastSkipLenString() {

        long len = this.readFieldLength();
        
        position += len;

        return (int)len; // this is safe, as this is only
                          // used when unpacking fields, and
                          // field names can't be > Integer.MAX_VALUE
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public boolean wasMultiPacket() {

        return wasMultiPacket;
    }

    protected final byte[] getBufferSource() {

        return byteBuffer;
    }

    final byte[] getBytes(int len) {

        byte[] b = new byte[len];
        System.arraycopy(this.byteBuffer, this.position, b, 0, len);
        this.position += len; // update cursor

        return b;
    }

    // 2000-06-05 Changed
    final boolean isLastDataPacket() {

        return ((getBufLength() < 9) && ((this.byteBuffer[0] & 0xff) == 254));
    }

    final void clear() {
        this.position = MysqlIO.HEADER_LENGTH;
    }

    final void dump() {

        int p = 0;
        int rows = getBufLength() / 8;

        for (int i = 0; i < rows; i++) {

            int ptemp = p;

            for (int j = 0; j < 8; j++) {

                String hexVal = Integer.toHexString(
                                        (int) this.byteBuffer[ptemp] & 0xff);

                if (hexVal.length() == 1) {
                    hexVal = "0" + hexVal;
                }

                System.out.print(hexVal + " ");
                ptemp++;
            }

            System.out.print("    ");

            for (int j = 0; j < 8; j++) {

                if ((this.byteBuffer[p] > 32) && (this.byteBuffer[p] < 127)) {
                    System.out.print((char) this.byteBuffer[p] + " ");
                } else {
                    System.out.print(". ");
                }

                p++;
            }

            System.out.println();
        }

        int n = 0;

        for (int i = p; i < getBufLength(); i++) {

            String hexVal = Integer.toHexString((int) this.byteBuffer[i]);

            if (hexVal.length() == 1) {
                hexVal = "0" + hexVal;
            }

            System.out.print(hexVal + " ");
            n++;
        }

        for (int i = n; i < 8; i++) {
            System.out.print("   ");
        }

        System.out.print("    ");

        for (int i = p; i < getBufLength(); i++) {

            if ((this.byteBuffer[i] > 32) && (this.byteBuffer[i] < 127)) {
                System.out.print((char) this.byteBuffer[i] + " ");
            } else {
                System.out.print(". ");
            }
        }

        System.out.println();
    }

    final void ensureCapacity(int additionalData)
                       throws SQLException {

        if ((this.position + additionalData) > getBufLength()) {

            int newLength = (int) (getBufLength() * 1.25);

            if (newLength < (getBufLength() + additionalData)) {
                newLength = getBufLength() + (int) (additionalData * 1.25);
            }

            if ((getMaxLength() != NO_LENGTH_LIMIT)
                && (newLength > getMaxLength())) {
                throw new PacketTooBigException(newLength, getMaxLength());
            }

            byte[] newBytes = new byte[newLength];
            System.arraycopy(this.byteBuffer, 0, newBytes, 0, 
                             this.byteBuffer.length);
            this.byteBuffer = newBytes;
            setBufLength(this.byteBuffer.length);
        }
    }

    final long newReadLength() {

        int sw = this.byteBuffer[this.position++] & 0xff;

        switch (sw) {

            case 251:
                return (long) 0;

            case 252:
                return (long) readInt();

            case 253:
                return (long) readLongInt();

            case 254: // changed for 64 bit lengths
                return (long) readLongLong();

            default:
                return (long) sw;
        }
    }

    final byte readByte() {

        return this.byteBuffer[this.position++];
    }

    final long readFieldLength() {

        int sw = this.byteBuffer[this.position++] & 0xff;

        switch (sw) {

            case 251:
                return NULL_LENGTH;

            case 252:
                return (long) readInt();

            case 253:
                return (long) readLongInt();

            case 254:
                return (long) readLong();

            default:
                return (long) sw;
        }
    }

    // 2000-06-05 Changed
    final int readInt() {

        byte[] b = this.byteBuffer; // a little bit optimization

        return (b[this.position++] & 0xff)
               | ((b[this.position++] & 0xff) << 8);
    }

    final byte[] readLenByteArray(int offset) {

        long len = this.readFieldLength();

        if (len == NULL_LENGTH) {

            return null;
        }

        if (len == 0) {

            return new byte[0];
        }

        this.position += offset;

        return getBytes((int) len);
    }

    final long readLength() {

        int sw = this.byteBuffer[this.position++] & 0xff;

        switch (sw) {

            case 251:
                return (long) 0;

            case 252:
                return (long) readInt();

            case 253:
                return (long) readLongInt();

            case 254:
                return (long) readLong();

            default:
                return (long) sw;
        }
    }

    // 2000-06-05 Fixed
    final long readLong() {

        byte[] b = this.byteBuffer;

        return (b[this.position++] & 0xff)
               | ((b[this.position++] & 0xff) << 8)
               | ((b[this.position++] & 0xff) << 16)
               | ((b[this.position++] & 0xff) << 24);
    }

    // 2000-06-05 Changed
    final int readLongInt() {

        byte[] b = this.byteBuffer;

        return (b[this.position++] & 0xff)
               | ((b[this.position++] & 0xff) << 8)
               | ((b[this.position++] & 0xff) << 16);
    }

    // 2000-06-05 Fixed
    final long readLongLong() {

        byte[] b = this.byteBuffer;

        return (long) (b[this.position++] & 0xff)
               | ((long) (b[this.position++] & 0xff) << 8)
               | ((long) (b[this.position++] & 0xff) << 16)
               | ((long) (b[this.position++] & 0xff) << 24)
               | ((long) (b[this.position++] & 0xff) << 32)
               | ((long) (b[this.position++] & 0xff) << 40)
               | ((long) (b[this.position++] & 0xff) << 48)
               | ((long) (b[this.position++] & 0xff) << 56);
    }

    //
    // Read a null-terminated string
    //
    // To avoid alloc'ing a new byte array, we
    // do this by hand, rather than calling getNullTerminatedBytes()
    //
    final String readString() {

        int i = this.position;
        int len = 0;

        while ((this.byteBuffer[i] != 0) && (i < getBufLength())) {
            len++;
            i++;
        }

        String s = new String(this.byteBuffer, this.position, len);
        this.position += (len + 1); // update cursor

        return s;
    }

    final int readnBytes() {

        int sw = this.byteBuffer[this.position++] & 0xff;

        switch (sw) {

            case 1:
                return this.byteBuffer[this.position++] & 0xff;

            case 2:
                return this.readInt();

            case 3:
                return this.readLongInt();

            case 4:
                return (int) this.readLong();

            default:
                return 255;
        }
    }

    final void writeByte(byte b) {
        this.byteBuffer[this.position++] = b;
    }

    // Write a byte array
    final void writeBytesNoNull(byte[] bytes)
                         throws SQLException {

        int len = bytes.length;
        ensureCapacity(len);
        System.arraycopy(bytes, 0, this.byteBuffer, this.position, len);
        this.position += len;
    }

    // Write a byte array with the given offset and length
    final void writeBytesNoNull(byte[] bytes, int offset, int length)
                         throws SQLException {
        ensureCapacity(length);
        System.arraycopy(bytes, offset, this.byteBuffer, this.position, length);
        this.position += length;
    }

    final void writeDouble(double d) {

        long l = Double.doubleToLongBits(d);
        writeLongLong(l);
    }

    final void writeFloat(float f) {

        int i = Float.floatToIntBits(f);
        byte[] b = this.byteBuffer;
        b[this.position++] = (byte) (i & 0xff);
        b[this.position++] = (byte) (i >>> 8);
        b[this.position++] = (byte) (i >>> 16);
        b[this.position++] = (byte) (i >>> 24);
    }

    // 2000-06-05 Changed
    final void writeInt(int i) {

        byte[] b = this.byteBuffer;
        b[this.position++] = (byte) (i & 0xff);
        b[this.position++] = (byte) (i >>> 8);
    }

    // 2000-06-05 Changed
    final void writeLong(long i) {

        byte[] b = this.byteBuffer;
        b[this.position++] = (byte) (i & 0xff);
        b[this.position++] = (byte) (i >>> 8);
        b[this.position++] = (byte) (i >>> 16);
        b[this.position++] = (byte) (i >>> 24);
    }

    // 2000-06-05 Changed
    final void writeLongInt(int i) {

        byte[] b = this.byteBuffer;
        b[this.position++] = (byte) (i & 0xff);
        b[this.position++] = (byte) (i >>> 8);
        b[this.position++] = (byte) (i >>> 16);
    }

    final void writeLongLong(long i) {

        byte[] b = this.byteBuffer;
        b[this.position++] = (byte) (i & 0xff);
        b[this.position++] = (byte) (i >>> 8);
        b[this.position++] = (byte) (i >>> 16);
        b[this.position++] = (byte) (i >>> 24);
        b[this.position++] = (byte) (i >>> 32);
        b[this.position++] = (byte) (i >>> 40);
        b[this.position++] = (byte) (i >>> 48);
        b[this.position++] = (byte) (i >>> 56);
    }

    // Write null-terminated string
    final void writeString(String s)
                    throws SQLException {
        writeStringNoNull(s);
        this.byteBuffer[this.position++] = 0;
    }

    // Write string, with no termination
    final void writeStringNoNull(String s)
                          throws SQLException {

        int len = s.length();
        ensureCapacity(len);
        System.arraycopy(s.getBytes(), 0, byteBuffer, position, len);
        position += len;

        //         for (int i = 0; i < len; i++)
        //         {
        //             this.byteBuffer[this.position++] = (byte)s.charAt(i);
        //         }
    }

    // Write a String using the specified character
    // encoding
    final void writeStringNoNull(String s, String encoding)
                          throws UnsupportedEncodingException, SQLException {

        byte[] b = null;
        SingleByteCharsetConverter converter = SingleByteCharsetConverter.getInstance(
                                                       encoding);

        if (converter != null) {
            b = converter.toBytes(s);
        } else {
            b = StringUtils.getBytes(s, encoding);
        }

        int len = b.length;
        ensureCapacity(len);
        System.arraycopy(b, 0, this.byteBuffer, this.position, len);
        this.position += len;
    }

    void setBufLength(int bufLength) {
        this.bufLength = bufLength;
    }

    int getBufLength() {

        return bufLength;
    }

    void setMaxLength(int maxLength) {
        this.maxLength = maxLength;
    }

    int getMaxLength() {

        return maxLength;
    }

    void setSendLength(int sendLength) {
        this.sendLength = sendLength;
    }
}