/*
  Copyright (c) 2002, 2014, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.jdbc;

import java.text.MessageFormat;
import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

/**
 * Support for localized messages.
 */
public class Messages {

    private static final String BUNDLE_NAME = "com.mysql.jdbc.LocalizedErrorMessages";

    private static final ResourceBundle RESOURCE_BUNDLE;

    static {
        ResourceBundle temp = null;

        //
        // Overly-pedantic here, some appserver and JVM combos don't deal well with the no-args version, others don't deal well with the three-arg version, so
        // we need to try both :(
        //

        try {
            temp = ResourceBundle.getBundle(BUNDLE_NAME, Locale.getDefault(), Messages.class.getClassLoader());
        } catch (Throwable t) {
            try {
                temp = ResourceBundle.getBundle(BUNDLE_NAME);
            } catch (Throwable t2) {
                RuntimeException rt = new RuntimeException("Can't load resource bundle due to underlying exception " + t.toString());
                rt.initCause(t2);

                throw rt;
            }
        } finally {
            RESOURCE_BUNDLE = temp;
        }
    }

    /**
     * Returns the localized message for the given message key
     * 
     * @param key
     *            the message key
     * @return The localized message for the key
     */
    public static String getString(String key) {
        if (RESOURCE_BUNDLE == null) {
            throw new RuntimeException("Localized messages from resource bundle '" + BUNDLE_NAME + "' not loaded during initialization of driver.");
        }

        try {
            if (key == null) {
                throw new IllegalArgumentException("Message key can not be null");
            }

            String message = RESOURCE_BUNDLE.getString(key);

            if (message == null) {
                message = "Missing error message for key '" + key + "'";
            }

            return message;
        } catch (MissingResourceException e) {
            return '!' + key + '!';
        }
    }

    public static String getString(String key, Object[] args) {
        return MessageFormat.format(getString(key), args);
    }

    /**
     * Dis-allow construction ...
     */
    private Messages() {
    }
}
