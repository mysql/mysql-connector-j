/*
 * Copyright (c) 2002, 2023, Oracle and/or its affiliates.
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

package documentation;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import com.mysql.cj.conf.PropertyDefinition;
import com.mysql.cj.conf.PropertyDefinitions;

/**
 * Creates docbook table of connection properties from ConnectionProperties class.
 */
public class PropertiesDocGenerator {

    public static void main(String[] args) {
        System.out.println(exposeAsXml());
    }

    static class XmlMap {

        protected Map<Integer, Map<String, PropertyDefinition<?>>> ordered = new TreeMap<>();
        protected Map<String, PropertyDefinition<?>> alpha = new TreeMap<>();

    }

    /**
     * Returns a description of the connection properties as an XML document.
     *
     * @return the connection properties as an XML document.
     */
    public static String exposeAsXml() {
        StringBuilder xmlBuf = new StringBuilder();
        xmlBuf.append("<ConnectionProperties>");

        int numCategories = PropertyDefinitions.PROPERTY_CATEGORIES.length;

        Map<String, XmlMap> propertyListByCategory = new HashMap<>();

        for (int i = 0; i < numCategories; i++) {
            propertyListByCategory.put(PropertyDefinitions.PROPERTY_CATEGORIES[i], new XmlMap());
        }

        for (PropertyDefinition<?> pdef : PropertyDefinitions.PROPERTY_KEY_TO_PROPERTY_DEFINITION.values()) {
            XmlMap sortMaps = propertyListByCategory.get(pdef.getCategory());
            int orderInCategory = pdef.getOrder();

            if (orderInCategory == Integer.MIN_VALUE) {
                sortMaps.alpha.put(pdef.getName(), pdef);
            } else {
                Integer order = Integer.valueOf(orderInCategory);
                Map<String, PropertyDefinition<?>> orderMap = sortMaps.ordered.get(order);

                if (orderMap == null) {
                    orderMap = new TreeMap<>();
                    sortMaps.ordered.put(order, orderMap);
                }

                orderMap.put(pdef.getName(), pdef);
            }
        }

        for (int j = 0; j < numCategories; j++) {
            XmlMap sortMaps = propertyListByCategory.get(PropertyDefinitions.PROPERTY_CATEGORIES[j]);

            xmlBuf.append("\n <PropertyCategory name=\"");
            xmlBuf.append(PropertyDefinitions.PROPERTY_CATEGORIES[j]);
            xmlBuf.append("\">");

            for (Map<String, PropertyDefinition<?>> orderedEl : sortMaps.ordered.values()) {
                for (PropertyDefinition<?> pdef : orderedEl.values()) {
                    xmlBuf.append("\n  <Property name=\"");
                    xmlBuf.append(pdef.getName());

                    xmlBuf.append("\" default=\"");
                    if (pdef.getDefaultValue() != null) {
                        xmlBuf.append(pdef.getDefaultValue());
                    }
                    xmlBuf.append("\" sortOrder=\"");
                    xmlBuf.append(pdef.getOrder());
                    xmlBuf.append("\" since=\"");
                    xmlBuf.append(pdef.getSinceVersion());
                    xmlBuf.append("\">\n");
                    xmlBuf.append("    ");
                    String escapedDescription = pdef.getDescription();
                    escapedDescription = escapedDescription.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");

                    xmlBuf.append(escapedDescription);
                    xmlBuf.append("\n  </Property>");
                }
            }

            for (PropertyDefinition<?> pdef : sortMaps.alpha.values()) {
                xmlBuf.append("\n  <Property name=\"");
                xmlBuf.append(pdef.getName());

                xmlBuf.append("\" default=\"");
                if (pdef.getDefaultValue() != null) {
                    xmlBuf.append(pdef.getDefaultValue());
                }

                xmlBuf.append("\" sortOrder=\"alpha\" since=\"");
                xmlBuf.append(pdef.getSinceVersion());
                xmlBuf.append("\">\n");
                xmlBuf.append("    ");
                xmlBuf.append(pdef.getDescription());
                xmlBuf.append("\n  </Property>");
            }

            xmlBuf.append("\n </PropertyCategory>");
        }

        xmlBuf.append("\n</ConnectionProperties>");

        return xmlBuf.toString();
    }

}
