/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.core;

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
     */
    @Override
    public String toString() {
        if (this.completeVersion != null) {
            return this.completeVersion;
        }
        return String.format("%d.%d.%d", this.major, this.minor, this.subminor);
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
     */
    public boolean meetsMinimum(ServerVersion min) {
        return compareTo(min) >= 0;
    }

    /**
     * Parse the server version into major/minor/subminor.
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
