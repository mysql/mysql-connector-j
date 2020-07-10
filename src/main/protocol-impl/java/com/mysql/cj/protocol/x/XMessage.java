/*
 * Copyright (c) 2018, 2020, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.mysql.cj.protocol.x;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.OneofDescriptor;
import com.google.protobuf.Parser;
import com.google.protobuf.UnknownFieldSet;
import com.mysql.cj.protocol.Message;

public class XMessage implements Message, com.google.protobuf.Message {

    private com.google.protobuf.Message message;
    private List<Notice> notices = null;

    public XMessage(com.google.protobuf.Message mess) {
        this.message = mess;
    }

    public com.google.protobuf.Message getMessage() {
        return this.message;
    }

    @Override
    public byte[] getByteBuffer() {
        return this.message.toByteArray();
    }

    @Override
    public int getPosition() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getSerializedSize() {
        return this.message.getSerializedSize();
    }

    @Override
    public byte[] toByteArray() {
        return this.message.toByteArray();
    }

    @Override
    public ByteString toByteString() {
        return this.message.toByteString();
    }

    @Override
    public void writeDelimitedTo(OutputStream arg0) throws IOException {
        this.message.writeDelimitedTo(arg0);
    }

    @Override
    public void writeTo(CodedOutputStream arg0) throws IOException {
        this.message.writeTo(arg0);
    }

    @Override
    public void writeTo(OutputStream arg0) throws IOException {
        this.message.writeTo(arg0);
    }

    @Override
    public boolean isInitialized() {
        return this.message.isInitialized();
    }

    @Override
    public List<String> findInitializationErrors() {
        return this.message.findInitializationErrors();
    }

    @Override
    public Map<FieldDescriptor, Object> getAllFields() {
        return this.message.getAllFields();
    }

    @Override
    public com.google.protobuf.Message getDefaultInstanceForType() {
        return this.message.getDefaultInstanceForType();
    }

    @Override
    public Descriptor getDescriptorForType() {
        return this.message.getDescriptorForType();
    }

    @Override
    public Object getField(FieldDescriptor arg0) {
        return this.message.getField(arg0);
    }

    @Override
    public String getInitializationErrorString() {
        return this.message.getInitializationErrorString();
    }

    @Override
    public FieldDescriptor getOneofFieldDescriptor(OneofDescriptor arg0) {
        return this.message.getOneofFieldDescriptor(arg0);
    }

    @Override
    public Object getRepeatedField(FieldDescriptor arg0, int arg1) {
        return this.message.getRepeatedField(arg0, arg1);
    }

    @Override
    public int getRepeatedFieldCount(FieldDescriptor arg0) {
        return this.message.getRepeatedFieldCount(arg0);
    }

    @Override
    public UnknownFieldSet getUnknownFields() {
        return this.message.getUnknownFields();
    }

    @Override
    public boolean hasField(FieldDescriptor arg0) {
        return this.message.hasField(arg0);
    }

    @Override
    public boolean hasOneof(OneofDescriptor arg0) {
        return this.message.hasOneof(arg0);
    }

    @Override
    public Parser<? extends com.google.protobuf.Message> getParserForType() {
        return this.message.getParserForType();
    }

    @Override
    public Builder newBuilderForType() {
        return this.message.newBuilderForType();
    }

    @Override
    public Builder toBuilder() {
        return this.message.toBuilder();
    }

    public List<Notice> getNotices() {
        return this.notices;
    }

    public XMessage addNotices(List<Notice> n) {
        if (n != null) {
            if (this.notices == null) {
                this.notices = new ArrayList<>();
            }
            this.notices.addAll(n);
        }
        return this;
    }

}
