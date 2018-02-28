/*
 * Copyright (c) 2015, 2018, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.conf;

import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.util.StringUtils;

public class MemorySizePropertyDefinition extends IntegerPropertyDefinition {

    private static final long serialVersionUID = -6878680905514177949L;

    public MemorySizePropertyDefinition(String name, String alias, int defaultValue, boolean isRuntimeModifiable, String description, String sinceVersion,
            String category, int orderInCategory) {
        super(name, alias, defaultValue, isRuntimeModifiable, description, sinceVersion, category, orderInCategory);
    }

    public MemorySizePropertyDefinition(String name, String alias, int defaultValue, boolean isRuntimeModifiable, String description, String sinceVersion,
            String category, int orderInCategory, int lowerBound, int upperBound) {
        super(name, alias, defaultValue, isRuntimeModifiable, description, sinceVersion, category, orderInCategory, lowerBound, upperBound);
    }

    @Override
    public Integer parseObject(String value, ExceptionInterceptor exceptionInterceptor) {
        this.multiplier = 1;

        if (value.endsWith("k") || value.endsWith("K") || value.endsWith("kb") || value.endsWith("Kb") || value.endsWith("kB") || value.endsWith("KB")) {
            this.multiplier = 1024;
            int indexOfK = StringUtils.indexOfIgnoreCase(value, "k");
            value = value.substring(0, indexOfK);
        } else if (value.endsWith("m") || value.endsWith("M") || value.endsWith("mb") || value.endsWith("Mb") || value.endsWith("mB") || value.endsWith("MB")) {
            this.multiplier = 1024 * 1024;
            int indexOfM = StringUtils.indexOfIgnoreCase(value, "m");
            value = value.substring(0, indexOfM);
        } else if (value.endsWith("g") || value.endsWith("G") || value.endsWith("gb") || value.endsWith("Gb") || value.endsWith("gB") || value.endsWith("GB")) {
            this.multiplier = 1024 * 1024 * 1024;
            int indexOfG = StringUtils.indexOfIgnoreCase(value, "g");
            value = value.substring(0, indexOfG);
        }

        return super.parseObject(value, exceptionInterceptor);
    }

    /**
     * Creates instance of ReadableMemorySizeProperty or ModifiableMemorySizeProperty depending on isRuntimeModifiable() result.
     * 
     * @return
     */
    @Override
    public RuntimeProperty<Integer> createRuntimeProperty() {
        return isRuntimeModifiable() ? new ModifiableMemorySizeProperty(this) : new ReadableMemorySizeProperty(this);
    }

}
