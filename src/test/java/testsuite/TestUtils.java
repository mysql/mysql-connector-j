/*
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.
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

package testsuite;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

/**
 * Utility functions to use in tests.
 */

public class TestUtils {

    /**
     * Percent-encode all occurrence of the the percent sign (%) in the given string.
     *
     * @param strToEncode
     *            the string to encode
     * @return the encoded string
     */
    public static String encodePercent(String strToEncode) {
        return strToEncode.replaceAll("%", "%25");
    }

    /**
     * Get all IPv6 addresses defined in local network adapters.
     *
     * @return a list of {@link Inet6Address}s
     */
    public static List<Inet6Address> getIpv6List() {
        List<Inet6Address> addresses = new ArrayList<>();
        try {
            for (Enumeration<NetworkInterface> nis = NetworkInterface.getNetworkInterfaces(); nis.hasMoreElements();) {
                NetworkInterface ni = nis.nextElement();
                for (Enumeration<InetAddress> ias2 = ni.getInetAddresses(); ias2.hasMoreElements();) {
                    InetAddress ia = ias2.nextElement();
                    if (ia instanceof Inet6Address) {
                        addresses.add((Inet6Address) ia);
                    }
                }
            }
        } catch (SocketException e) {
            // Failed to get the network interfaces. Return an empty list.
        }
        return addresses;
    }

    /**
     * Get all IPv6 addresses of the given host.
     *
     * @return a list of {@link Inet6Address}s
     */
    public static List<Inet6Address> getIpv6List(String hostname) {
        List<Inet6Address> addresses = new ArrayList<>();
        try {
            InetAddress[] allAddresses = InetAddress.getAllByName(hostname);
            for (InetAddress address : allAddresses) {
                if (address instanceof Inet6Address) {
                    addresses.add((Inet6Address) address);
                }
                System.out.println(address.getHostAddress());
            }
        } catch (UnknownHostException e) {
            // Failed to get the network interfaces. Return an empty list.
        }
        return addresses;
    }

    /**
     * Checks if there is a server socket listening in the given host and port.
     *
     * @param hostName
     *            the host where to look for the server socket
     * @param port
     *            the expected port the server is listening
     * @return true if there is a server socket listening in the given address and port, false otherwise
     */
    public static boolean serverListening(String hostName, int port) {
        try {
            return serverListening(InetAddress.getByName(hostName), port);
        } catch (UnknownHostException e) {
            return false;
        }
    }

    /**
     * Checks if there is a server socket listening in the given address and port.
     *
     * @param addr
     *            the address where to look for the server socket
     * @param port
     *            the expected port the server is listening
     * @return true if there is a server socket listening in the given address and port, false otherwise
     */
    public static boolean serverListening(InetAddress addr, int port) {
        Socket s = null;
        try {
            s = new Socket(addr, port);
            return true;
        } catch (Exception e) {
            return false;
        } finally {
            if (s != null) {
                try {
                    s.close();
                } catch (Exception e) {
                }
            }
        }
    }

}
