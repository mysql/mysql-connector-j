/*
 * Copyright (c) 2015, 2020, Oracle and/or its affiliates.
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

package com.mysql.cj;

/**
 * A server version.
 */
public class ServerVersion implements Comparable<ServerVersion> {
    private String completeVersion;
    private Integer major;
    private Integer minor;
    private Integer subminor;

    public ServerVersion(String completeVersion, int major, int minor, int subminor) {
        this.completeVersion = completeVersion;
        this.major = major;
        this.minor = minor;
        this.subminor = subminor;
    }

    public ServerVersion(int major, int minor, int subminor) {
        this(null, major, minor, subminor);
    }

    public int getMajor() {
        return this.major;
    }

    public int getMinor() {
        return this.minor;
    }

    public int getSubminor() {
        return this.subminor;
    }

    /**
     * A string representation of this version. If this version was parsed from, or provided with, a "complete" string which may contain more than just the
     * version number, this string is returned verbatim. Otherwise, a string representation of the version numbers is given.
     * 
     * @return string version representation
     */
    @Override
    public String toString() {
        if (this.completeVersion != null) {
            return this.completeVersion;
        }
        return String.format("%d.%d.%d", this.major, this.minor, this.subminor);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || !ServerVersion.class.isAssignableFrom(obj.getClass())) {
            return false;
        }
        ServerVersion another = (ServerVersion) obj;
        if (this.getMajor() != another.getMajor() || this.getMinor() != another.getMinor() || this.getSubminor() != another.getSubminor()) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 23;
        hash += 19 * hash + this.major;
        hash += 19 * hash + this.minor;
        hash += 19 * hash + this.subminor;
        return hash;
    }

    public int compareTo(ServerVersion other) {
        int c;
        if ((c = this.major.compareTo(other.getMajor())) != 0) {
            return c;
        } else if ((c = this.minor.compareTo(other.getMinor())) != 0) {
            return c;
        }
        return this.subminor.compareTo(other.getSubminor());
    }

    /**
     * Does this version meet the minimum specified by `min'?
     *
     * @param min
     *            The minimum version to compare against.
     * @return true if version meets the minimum specified by `min'
     */
    public boolean meetsMinimum(ServerVersion min) {
        return compareTo(min) >= 0;
    }

    /**
     * Parse the server version into major/minor/subminor.
     * 
     * @param versionString
     *            string version representation
     * @return {@link ServerVersion}
     */
    public static ServerVersion parseVersion(final String versionString) {
        int point = versionString.indexOf('.');

        if (point != -1) {
            try {
                int serverMajorVersion = Integer.parseInt(versionString.substring(0, point));

                String remaining = versionString.substring(point + 1, versionString.length());
                point = remaining.indexOf('.');

                if (point != -1) {
                    int serverMinorVersion = Integer.parseInt(remaining.substring(0, point));

                    remaining = remaining.substring(point + 1, remaining.length());

                    int pos = 0;

                    while (pos < remaining.length()) {
                        if ((remaining.charAt(pos) < '0') || (remaining.charAt(pos) > '9')) {
                            break;
                        }

                        pos++;
                    }

                    int serverSubminorVersion = Integer.parseInt(remaining.substring(0, pos));

                    return new ServerVersion(versionString, serverMajorVersion, serverMinorVersion, serverSubminorVersion);
                }
            } catch (NumberFormatException NFE1) {
            }
        }

        // can't parse the server version
        return new ServerVersion(0, 0, 0);
    }
}
