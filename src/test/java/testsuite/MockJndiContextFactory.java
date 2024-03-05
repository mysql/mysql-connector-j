/*
 * Copyright (c) 2020, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package testsuite;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;

import javax.naming.Binding;
import javax.naming.Context;
import javax.naming.InvalidNameException;
import javax.naming.Name;
import javax.naming.NameClassPair;
import javax.naming.NameParser;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;

/**
 * The following classes implement a mock JDNI provider that holds serialized objects in memory.
 */
public class MockJndiContextFactory implements InitialContextFactory {

    @Override
    public Context getInitialContext(Hashtable<?, ?> environment) throws NamingException {
        return new MockJndiContext();
    }

    public static class MockJndiContext implements Context {

        private HashMap<String, byte[]> data = new HashMap<>();

        @Override
        public Object lookup(Name name) throws NamingException {
            return lookup(name.get(0));
        }

        @Override
        public Object lookup(String name) throws NamingException {
            if (!this.data.containsKey(name)) {
                throw new NamingException("Key not found");
            }
            try {
                ObjectInputStream objIn = new ObjectInputStream(new ByteArrayInputStream(this.data.get(name)));
                return objIn.readObject();
            } catch (IOException | ClassNotFoundException e) {
                NamingException namingEx = new NamingException();
                namingEx.initCause(e);
                throw namingEx;
            }
        }

        @Override
        public void bind(Name name, Object obj) throws NamingException {
            bind(name.get(0), obj);
        }

        @Override
        public void bind(String name, Object obj) throws NamingException {
            ByteArrayOutputStream objBytes = new ByteArrayOutputStream();
            ObjectOutputStream objOut;
            try {
                objOut = new ObjectOutputStream(objBytes);
                objOut.writeObject(obj);
            } catch (IOException e) {
                NamingException namingEx = new NamingException();
                namingEx.initCause(e);
                throw namingEx;
            }
            this.data.put(name, objBytes.toByteArray());
        }

        @Override
        public void rebind(Name name, Object obj) throws NamingException {
            bind(name, obj);
        }

        @Override
        public void rebind(String name, Object obj) throws NamingException {
            bind(name, obj);
        }

        @Override
        public void unbind(Name name) throws NamingException {
            unbind(name.get(0));
        }

        @Override
        public void unbind(String name) throws NamingException {
            this.data.remove(name);
        }

        @Override
        public void rename(Name oldName, Name newName) throws NamingException {
        }

        @Override
        public void rename(String oldName, String newName) throws NamingException {
        }

        @Override
        public NamingEnumeration<NameClassPair> list(Name name) throws NamingException {
            return null;
        }

        @Override
        public NamingEnumeration<NameClassPair> list(String name) throws NamingException {
            return null;
        }

        @Override
        public NamingEnumeration<Binding> listBindings(Name name) throws NamingException {
            return null;
        }

        @Override
        public NamingEnumeration<Binding> listBindings(String name) throws NamingException {
            return null;
        }

        @Override
        public void destroySubcontext(Name name) throws NamingException {
        }

        @Override
        public void destroySubcontext(String name) throws NamingException {
        }

        @Override
        public Context createSubcontext(Name name) throws NamingException {
            return null;
        }

        @Override
        public Context createSubcontext(String name) throws NamingException {
            return null;
        }

        @Override
        public Object lookupLink(Name name) throws NamingException {
            return null;
        }

        @Override
        public Object lookupLink(String name) throws NamingException {
            return null;
        }

        @Override
        public NameParser getNameParser(Name name) throws NamingException {
            return new MockJndiNameParser();
        }

        @Override
        public NameParser getNameParser(String name) throws NamingException {
            return new MockJndiNameParser();
        }

        @Override
        public Name composeName(Name name, Name prefix) throws NamingException {
            return null;
        }

        @Override
        public String composeName(String name, String prefix) throws NamingException {
            return null;
        }

        @Override
        public Object addToEnvironment(String propName, Object propVal) throws NamingException {
            return null;
        }

        @Override
        public Object removeFromEnvironment(String propName) throws NamingException {
            return null;
        }

        @Override
        public Hashtable<?, ?> getEnvironment() throws NamingException {
            return null;
        }

        @Override
        public void close() throws NamingException {
            this.data.clear();
        }

        @Override
        public String getNameInNamespace() throws NamingException {
            return null;
        }

    }

    public static class MockJndiNameParser implements NameParser {

        @Override
        public Name parse(String name) throws NamingException {
            Name myName = new MockJndiName();
            myName.add(name);
            return myName;
        }

    }

    public static class MockJndiName implements Name {

        private static final long serialVersionUID = 1L;

        private String data = "";

        @Override
        public int compareTo(Object obj) {
            return 0;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public Enumeration<String> getAll() {
            return null;
        }

        @Override
        public String get(int posn) {
            return this.data;
        }

        @Override
        public Name getPrefix(int posn) {
            return this;
        }

        @Override
        public Name getSuffix(int posn) {
            return this;
        }

        @Override
        public boolean startsWith(Name n) {
            return false;
        }

        @Override
        public boolean endsWith(Name n) {
            return false;
        }

        @Override
        public Name addAll(Name suffix) throws InvalidNameException {
            return null;
        }

        @Override
        public Name addAll(int posn, Name n) throws InvalidNameException {
            return null;
        }

        @Override
        public Name add(String comp) throws InvalidNameException {
            this.data = comp;
            return this;
        }

        @Override
        public Name add(int posn, String comp) throws InvalidNameException {
            return add(comp);
        }

        @Override
        public Object remove(int posn) throws InvalidNameException {
            this.data = "";
            return null;
        }

        @Override
        public Object clone() {
            return this;
        }

    }

}
