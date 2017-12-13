/*
  Copyright (c) 2015, 2017, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FOSS License Exception
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

import java.util.Arrays;

import com.mysql.cj.api.conf.RuntimeProperty;
import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.util.StringUtils;

public class BooleanPropertyDefinition extends AbstractPropertyDefinition<Boolean> {

    private static final long serialVersionUID = -7288366734350231540L;

    public enum AllowableValues {
        TRUE(true), FALSE(false), YES(true), NO(false);

        private boolean asBoolean;

        private AllowableValues(boolean booleanValue) {
            this.asBoolean = booleanValue;
        }

        public boolean asBoolean() {
            return this.asBoolean;
        }
    }

    public BooleanPropertyDefinition(String name, String alias, Boolean defaultValue, boolean isRuntimeModifiable, String description, String sinceVersion,
            String category, int orderInCategory) {
        super(name, alias, defaultValue, isRuntimeModifiable, description, sinceVersion, category, orderInCategory);
    }

    @Override
    public String[] getAllowableValues() {
        return Arrays.stream(AllowableValues.values()).map(AllowableValues::toString).toArray(String[]::new);
    }

    @Override
    public Boolean parseObject(String value, ExceptionInterceptor exceptionInterceptor) {
        try {
            return AllowableValues.valueOf(value.toUpperCase()).asBoolean();
        } catch (Exception e) {
            throw ExceptionFactory.createException(
                    Messages.getString("PropertyDefinition.1",
                            new Object[] { getName(), StringUtils.stringArrayToString(getAllowableValues(), "'", "', '", "' or '", "'"), value }),
                    e, exceptionInterceptor);
        }
    }

    /**
     * Creates instance of ReadableBooleanProperty or ModifiableBooleanProperty depending on isRuntimeModifiable() result.
     * 
     * @return
     */
    @Override
    public RuntimeProperty<Boolean> createRuntimeProperty() {
        return isRuntimeModifiable() ? new ModifiableBooleanProperty(this) : new ReadableBooleanProperty(this);
    }

}
