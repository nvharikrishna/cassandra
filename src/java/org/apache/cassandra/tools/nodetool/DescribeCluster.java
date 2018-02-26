/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.tools.nodetool;

import static java.lang.String.format;
import io.airlift.airline.Command;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;

import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "describecluster", description = "Print the name, snitch, partitioner and schema version of a cluster")
public class DescribeCluster extends NodeToolCmd
{
    private boolean withPort = false;

    @Override
    public void execute(NodeProbe probe)
    {
        // display cluster name, snitch and partitioner
        System.out.println("Cluster Information:");
        System.out.println("\tName: " + probe.getClusterName());

        System.out.println("\tLive nodes: " + probe.getLiveNodes(false).size());
        System.out.println("\tJoining nodes: " + probe.getLiveNodes(withPort));
        System.out.println("\tMoving nodes: " + probe.getMovingNodes(withPort));
        System.out.println("\tLeaving nodes: " + probe.getLeavingNodes(withPort));
        System.out.println("\tUnrechable nodes:" + probe.getUnreachableNodes(withPort));

        printKeyspacesInformation(probe);
        printEndpointsVersionInfo(probe);
        printDataCenterInfo(probe);

        String snitch = probe.getEndpointSnitchInfoProxy().getSnitchName();
        boolean dynamicSnitchEnabled = false;
        if (snitch.equals(DynamicEndpointSnitch.class.getName()))
        {
            snitch = probe.getDynamicEndpointSnitchInfoProxy().getSubsnitchClassName();
            dynamicSnitchEnabled = true;
        }
        System.out.println("\tSnitch: " + snitch);
        System.out.println("\tDynamicEndPointSnitch: " + (dynamicSnitchEnabled ? "enabled" : "disabled"));
        System.out.println("\tPartitioner: " + probe.getPartitioner());

        // display schema version for each node
        System.out.println("\tSchema versions:");
        Map<String, List<String>> schemaVersions = withPort ? probe.getSpProxy().getSchemaVersionsWithPort() : probe.getSpProxy().getSchemaVersions();
        for (String version : schemaVersions.keySet())
        {
            System.out.println(format("\t\t%s: %s%n", version, schemaVersions.get(version)));
        }
    }

    private void printKeyspacesInformation(NodeProbe probe)
    {
        Map<String, Integer> keyspaceReplicationFactors = probe.getKeyspacesWithReplicationFactor();
        System.out.println("\tKeyspaces: ");
        for (Entry<String, Integer> keyspaceRf : keyspaceReplicationFactors.entrySet())
            System.out.println("\t\t" + keyspaceRf.getKey() + " RF: " + keyspaceRf.getValue());
    }

    private void printEndpointsVersionInfo(NodeProbe probe)
    {
        Map<String, String> endpointsWithReleaseVersion = probe.getEndpointsWithReleaseVersion();
        Map<String, ArrayList<String>> endpointsByVersion = new HashMap<>();
        for (Entry<String, String> endpointVersion : endpointsWithReleaseVersion.entrySet())
        {
            if (endpointsByVersion.containsKey(endpointVersion.getValue()))
            {
                endpointsByVersion.get(endpointVersion.getValue()).add(endpointVersion.getKey());
            }
            else
            {
                ArrayList<String> endpoints = new ArrayList<String>();
                endpoints.add(endpointVersion.getKey());
                endpointsByVersion.put(endpointVersion.getValue(), endpoints);
            }
        }

        System.out.println("\tRelease versions:");
        for (Entry<String, ArrayList<String>> entry : endpointsByVersion.entrySet())
            System.out.println("\t\t" + entry.getKey() + ": " + entry.getValue());
    }

    private void printDataCenterInfo(NodeProbe probe)
    {
        Map<String, String> tokensToEndpoints = probe.getTokenToEndpointMap(withPort);
        Map<String, Float> ownerships = probe.getOwnershipWithPort();

        boolean resolveIp = false;
        SortedMap<String, SetHostStatWithPort> dcs = NodeTool.getOwnershipByDcWithPort(probe, resolveIp, tokensToEndpoints, ownerships);

        List<String> liveNodes = probe.getLiveNodes(withPort);
        List<String> unreachableNodes = probe.getUnreachableNodes(withPort);

        System.out.println("\tDatacenters:");
        for (Entry<String, SetHostStatWithPort> dc : dcs.entrySet())
        {
            int up = 0, down = 0, unknown = 0;

            Set<String> dcEndpoints = new HashSet<>();
            for (HostStatWithPort hostStatus : dc.getValue())
                dcEndpoints.add(hostStatus.endpoint.getHostAddress(withPort));

            for (String endpoint : dcEndpoints)
            {
                if (liveNodes.contains(endpoint)) up++;
                else if (unreachableNodes.contains(endpoint)) down++;
                else unknown++;
            }
            System.out.println("\t\t" + String.format("%s Up# %d Down# %d Unknown# %d", dc.getKey(), up, down, unknown));
        }
    }
}
