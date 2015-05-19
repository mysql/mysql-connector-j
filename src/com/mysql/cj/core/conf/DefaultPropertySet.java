/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
  <http://www.mysql.com/about/legal/licensing/foss-exception.html>.

  This program is free software; you can redistribute it and/or modify it under the terms
  of the GNU General Public License as published by the Free Software Foundation; version 2
  of the License.

  This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
  See the GNU General Public License for more details.

  You should have received a copy of the GNU General Public License along with this
  program; if not, write to the Free Software Foundation, Inc., 51 Franklin St, Fifth
  Floor, Boston, MA 02110-1301  USA

 */

package com.mysql.cj.core.conf;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.mysql.cj.api.conf.ModifiableProperty;
import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.conf.ReadableProperty;
import com.mysql.cj.api.conf.RuntimeProperty;

public class DefaultPropertySet implements PropertySet, Serializable {

    private static final long serialVersionUID = -5156024634430650528L;

    private final Map<String, RuntimeProperty<?>> PROPERTY_NAME_TO_RUNTIME_PROPERTY = new HashMap<String, RuntimeProperty<?>>();

    protected boolean useUnicodeAsBoolean = true;

    protected String characterEncodingAsString = null;

    public DefaultPropertySet() {

        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_paranoid));

        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_passwordCharacterEncoding));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_serverRSAPublicKeyFile));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_allowPublicKeyRetrieval));

        // SSL Options

        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_clientCertificateKeyStoreUrl));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_trustCertificateKeyStoreUrl));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_clientCertificateKeyStoreType));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_clientCertificateKeyStorePassword));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_trustCertificateKeyStoreType));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_trustCertificateKeyStorePassword));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_verifyServerCertificate));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_enabledSSLCipherSuites));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useUnbufferedInput));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_profilerEventHandler));
    }

    @Override
    public void addProperty(RuntimeProperty<?> prop) {
        this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.put(prop.getPropertyDefinition().getName(), prop);
        this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.put(prop.getPropertyDefinition().getAlias(), prop);
    }

    @Override
    public void removeProperty(String name) {
        RuntimeProperty<?> prop = this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.remove(name);
        if (prop != null) {
            if (name.equals(prop.getPropertyDefinition().getName())) {
                this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.remove(prop.getPropertyDefinition().getAlias());
            } else {
                this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.remove(prop.getPropertyDefinition().getName());
            }
        }
    }

    @Override
    public ReadableProperty<?> getReadableProperty(String name) {
        // TODO check property type
        return (ReadableProperty<?>) this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.get(name);
    }

    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    public <T> ReadableProperty<T> getReadableProperty(String name, boolean s) {
        // TODO check property type
        return (ReadableProperty<T>) this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.get(name);
    }

    @Override
    public ReadableProperty<Boolean> getBooleanReadableProperty(String name) {
        // TODO check property type
        return (ReadableProperty<Boolean>) this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.get(name);
    }

    @Override
    public ReadableProperty<Integer> getIntegerReadableProperty(String name) {
        // TODO check property type
        return (ReadableProperty<Integer>) this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.get(name);
    }

    @Override
    public ReadableProperty<Long> getLongReadableProperty(String name) {
        // TODO check property type
        return (ReadableProperty<Long>) this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.get(name);
    }

    @Override
    public ReadableProperty<Integer> getMemorySizeReadableProperty(String name) {
        // TODO check property type
        return (ReadableProperty<Integer>) this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.get(name);
    }

    @Override
    public ReadableProperty<String> getStringReadableProperty(String name) {
        // TODO check property type
        return (ReadableProperty<String>) this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.get(name);
    }

    @Override
    public ModifiableProperty<?> getModifiableProperty(String name) {
        // TODO check property type
        return (ModifiableProperty<?>) this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.get(name);
    }

    @Override
    public ModifiableProperty<Boolean> getBooleanModifiableProperty(String name) {
        // TODO check property type
        return (ModifiableProperty<Boolean>) this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.get(name);
    }

    @Override
    public ModifiableProperty<Integer> getIntegerModifiableProperty(String name) {
        // TODO check property type
        return (ModifiableProperty<Integer>) this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.get(name);
    }

    @Override
    public ModifiableProperty<Long> getLongModifiableProperty(String name) {
        // TODO check property type
        return (ModifiableProperty<Long>) this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.get(name);
    }

    @Override
    public ModifiableProperty<Integer> getMemorySizeModifiableProperty(String name) {
        // TODO check property type
        return (ModifiableProperty<Integer>) this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.get(name);
    }

    @Override
    public ModifiableProperty<String> getStringModifiableProperty(String name) {
        // TODO check property type
        return (ModifiableProperty<String>) this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.get(name);
    }

}
