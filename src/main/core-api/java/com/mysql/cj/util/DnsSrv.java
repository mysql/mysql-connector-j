/*
 * Copyright (c) 2019, 2020, Oracle and/or its affiliates.
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

package com.mysql.cj.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

public class DnsSrv {
    public static class SrvRecord implements Comparable<SrvRecord> {
        private final int priority;
        private final int weight;
        private final int port;
        private final String target;

        public static SrvRecord buildFrom(String srvLine) {
            String[] srvParts = srvLine.split("\\s+");
            if (srvParts.length == 4) {
                int priority = Integer.parseInt(srvParts[0]);
                int weight = Integer.parseInt(srvParts[1]);
                int port = Integer.parseInt(srvParts[2]);
                String target = srvParts[3].replaceFirst("\\.$", "");
                return new SrvRecord(priority, weight, port, target);
            }
            return null;
        }

        public SrvRecord(int priority, int weight, int port, String target) {
            this.priority = priority;
            this.weight = weight;
            this.port = port;
            this.target = target;
        }

        public int getPriority() {
            return this.priority;
        }

        public int getWeight() {
            return this.weight;
        }

        public int getPort() {
            return this.port;
        }

        public String getTarget() {
            return this.target;
        }

        @Override
        public String toString() {
            return String.format("{\"Priority\": %d, \"Weight\": %d, \"Port\": %d, \"Target\": \"%s\"}", getPriority(), getWeight(), getPort(), getTarget());
        }

        @Override
        public int compareTo(SrvRecord o) {
            int priorityDiff = getPriority() - o.getPriority();
            return priorityDiff == 0 ? getWeight() - o.getWeight() : priorityDiff;
        }
    }

    public static List<SrvRecord> lookupSrvRecords(String serviceName) throws NamingException {
        List<SrvRecord> srvRecords = new ArrayList<>();

        Properties environment = new Properties();
        environment.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.dns.DnsContextFactory");
        DirContext context = new InitialDirContext(environment);
        Attributes dnsEntries = context.getAttributes(serviceName, new String[] { "SRV" });
        if (dnsEntries != null) {
            Attribute hosts = dnsEntries.get("SRV");
            if (hosts != null) {
                for (int i = 0; i < hosts.size(); i++) {
                    srvRecords.add(SrvRecord.buildFrom((String) hosts.get(i)));
                }
            }
        }

        return sortSrvRecords(srvRecords);
    }

    /**
     * Sort a list of DNS SRV records according to the sorting rules described in rfc2782.
     * 
     * @param srvRecords
     *            the list of {@link SrvRecord}s to sort.
     * @return
     *         a new list of sorted {@link SrvRecord}s.
     */
    public static List<SrvRecord> sortSrvRecords(List<SrvRecord> srvRecords) {
        // Sort srv records by their natural order, i.e., first by priority then by weight.
        List<SrvRecord> srvRecordsSortedNatural = srvRecords.stream().sorted().collect(Collectors.toList());

        // Sort records with equal priority by weight using a categorical distribution.
        Random random = new Random(System.nanoTime());
        List<SrvRecord> srvRecordsSortedRfc2782 = new ArrayList<>();

        List<Integer> priorities = srvRecordsSortedNatural.stream().map(SrvRecord::getPriority).distinct().collect(Collectors.toList());
        for (Integer priority : priorities) {
            List<SrvRecord> srvRecordsSamePriority = srvRecordsSortedNatural.stream().filter(s -> s.getPriority() == priority).collect(Collectors.toList());
            while (srvRecordsSamePriority.size() > 1) {
                int recCount = srvRecordsSamePriority.size();
                int sumOfWeights = 0;
                int[] weights = new int[recCount];
                for (int i = 0; i < recCount; i++) {
                    sumOfWeights += srvRecordsSamePriority.get(i).getWeight();
                    weights[i] = sumOfWeights;
                }
                int selection = random.nextInt(sumOfWeights + 1);
                int pos = 0;
                for (; pos < recCount && weights[pos] < selection; pos++) {
                }
                srvRecordsSortedRfc2782.add(srvRecordsSamePriority.remove(pos));
            }
            srvRecordsSortedRfc2782.add(srvRecordsSamePriority.get(0));
        }

        return srvRecordsSortedRfc2782;
    }
}
