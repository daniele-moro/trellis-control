/*
 * Copyright 2015-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.onosproject.segmentrouting.grouphandler;

//import com.google.common.collect.ImmutableMap;

import org.onlab.packet.MacAddress;
import org.onlab.packet.MplsLabel;
import org.onlab.util.KryoNamespace;
import org.onosproject.core.ApplicationId;
import org.onosproject.net.Link;
import org.onosproject.net.Path;
import org.onosproject.net.DeviceId;
import org.onosproject.net.PortNumber;
//import org.onosproject.net.ConnectPoint;
import org.onosproject.net.flow.DefaultTrafficSelector;
import org.onosproject.net.flow.DefaultTrafficTreatment;
import org.onosproject.net.flow.TrafficSelector;
import org.onosproject.net.flow.TrafficTreatment;
import org.onosproject.net.flowobjective.DefaultNextTreatment;
import org.onosproject.net.flowobjective.DefaultNextObjective;
import org.onosproject.net.flowobjective.FlowObjectiveService;
import org.onosproject.net.flowobjective.NextObjective;
import org.onosproject.net.link.LinkService;
import org.onosproject.net.topology.Topology;
import org.onosproject.net.topology.TopologyService;
import org.onosproject.segmentrouting.DefaultRoutingHandler;
import org.onosproject.segmentrouting.SegmentRoutingManager;
import org.onosproject.segmentrouting.config.DeviceConfigNotFoundException;
import org.onosproject.segmentrouting.config.DeviceProperties;
import org.onosproject.segmentrouting.DeviceConfiguration;
import org.onosproject.segmentrouting.storekey.DestinationSetNextObjectiveStoreKey;
import org.slf4j.Logger;

import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
//import java.util.ArrayList;
import java.util.Set;
import java.util.Collections;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.onlab.util.Tools.groupedThreads;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Default WCMP group handler creation module. This component creates a set of
 * WCMP groups for every neighbor that this device is connected to based on
 * whether the current device is an edge device or a transit device.
 */
public class DefaultWCMPHandler {
    private static final Logger log = getLogger(DefaultWCMPHandler.class);

    private static final long VERIFY_INTERVAL = 30; // secs

    protected final DeviceId deviceId;
    protected final ApplicationId appId;
    protected final DeviceProperties deviceConfig;
    protected final List<Integer> allSegmentIds;
    protected int ipv4NodeSegmentId = -1;
    protected int ipv6NodeSegmentId = -1;
    protected boolean isEdgeRouter = false;
    protected MacAddress nodeMacAddr = null;
    protected LinkService linkService;
    protected FlowObjectiveService flowObjectiveService;
    private DeviceConfiguration config;
    private DefaultGroupHandler grHandler;
    private SegmentRoutingManager srManager;
    private TopologyService tpService;

    private ScheduledExecutorService executorService
            = newScheduledThreadPool(1, groupedThreads("weightCorrector", "WeightC-%d", log));

    protected KryoNamespace.Builder kryo = new KryoNamespace.Builder()
            .register(URI.class).register(HashSet.class)
            .register(DeviceId.class).register(PortNumber.class)
            .register(DestinationSet.class).register(PolicyGroupIdentifier.class)
            .register(PolicyGroupParams.class)
            .register(GroupBucketIdentifier.class)
            .register(GroupBucketIdentifier.BucketOutputType.class);


    protected DefaultWCMPHandler(DeviceId deviceId, ApplicationId appId,
                                  DeviceProperties config,
                                  LinkService linkService,
                                  FlowObjectiveService flowObjService,
                                  SegmentRoutingManager srManager,
                                  DefaultGroupHandler grHandler,
                                  TopologyService tpService) {
        this.deviceId = checkNotNull(deviceId);
        this.appId = checkNotNull(appId);
        this.deviceConfig = checkNotNull(config);
        this.linkService = checkNotNull(linkService);
        this.allSegmentIds = checkNotNull(config.getAllDeviceSegmentIds());
        try {
            this.ipv4NodeSegmentId = config.getIPv4SegmentId(deviceId);
            this.ipv6NodeSegmentId = config.getIPv6SegmentId(deviceId);
            this.isEdgeRouter = config.isEdgeDevice(deviceId);
            this.nodeMacAddr = checkNotNull(config.getDeviceMac(deviceId));
        } catch (DeviceConfigNotFoundException e) {
            log.warn(e.getMessage()
                    + " Skipping value assignment in DefaultGroupHandler");
        }
        this.flowObjectiveService = flowObjService;
        this.srManager = srManager;
        this.grHandler = grHandler;
        this.tpService = tpService;
        executorService.scheduleWithFixedDelay(new DefaultWCMPHandler.NextHopWeightCalculator(), 5,
                VERIFY_INTERVAL,
                TimeUnit.SECONDS);
    }

    /**
     * Gracefully shuts down a wcmpHandler. Typically called when the handler is
     * no longer needed.
     */
    public void shutdown() {
        executorService.shutdown();
    }

    /**
     * Creates a wcmp handler object.
     *
     * @param deviceId device identifier
     * @param appId application identifier
     * @param config interface to retrieve the device properties
     * @param linkService link service object
     * @param flowObjService flow objective service object
     * @param srManager segment routing manager
     * @param grHandler group handler
     * @param tpService topology service
     * @throws DeviceConfigNotFoundException if the device configuration is not found
     * @return default wcmp handler type
     */
    public static DefaultWCMPHandler createWCMPHandler(
            DeviceId deviceId,
            ApplicationId appId,
            DeviceProperties config,
            LinkService linkService,
            FlowObjectiveService flowObjService,
            SegmentRoutingManager srManager,
            DefaultGroupHandler grHandler,
            TopologyService tpService)
            throws DeviceConfigNotFoundException {
        return new DefaultWCMPHandler(deviceId, appId, config,
                linkService,
                flowObjService,
                srManager,
                grHandler,
                tpService);
    }

    /**
     * Performs weight calculation for all hash groups in this device.
     * Checks RouteHandler to ensure that routing is stable before computing.
     * The goal is to find weights for the next hop members. first we need to
     * find the outgoing link capacity of next hops.
     */
    protected final class NextHopWeightCalculator implements Runnable {
        Integer nextId;

        NextHopWeightCalculator() {
            this.nextId = null;
        }

        @Override
        public void run() {
            DefaultRoutingHandler rh = srManager.getRoutingHandler();
            if (rh == null || !rh.isRoutingStable() || !rh.shouldProgram(deviceId)) {
                return;
            }

            rh.acquireRoutingLock();
            // get list of nexthop neighbors for a destination:
            try {
                log.warn("WCMPdebug running weight calculator for dev: {}", deviceId);
                Set<DestinationSetNextObjectiveStoreKey> dsKeySet = grHandler.dsNextObjStore.entrySet()
                        .stream()
                        .filter(entry -> entry.getKey().deviceId().equals(deviceId))
                        // Filter out PW transit groups or include them if MPLS ECMP is supported
                        .filter(entry -> !entry.getKey().destinationSet().notBos() ||
                                (entry.getKey().destinationSet().notBos() && srManager.getMplsEcmp()))
                        // Filter out simple SWAP groups or include them if MPLS ECMP is supported
                        .filter(entry -> !entry.getKey().destinationSet().swap() ||
                                (entry.getKey().destinationSet().swap() && srManager.getMplsEcmp()))
                        .map(entry -> entry.getKey())
                        .collect(Collectors.toSet());
                for (DestinationSetNextObjectiveStoreKey dsKey : dsKeySet) {
                    NextNeighbors next = grHandler.dsNextObjStore.get(dsKey);
                    if (next == null) {
                        continue;
                    }
                    int nid = next.nextId();
                    if (nextId != null && nextId != nid) {
                        continue;
                    }
                    TrafficSelector.Builder metabuilder = DefaultTrafficSelector.builder();
                    metabuilder.matchVlanId(srManager.getDefaultInternalVlan());
                    NextObjective.Builder nextObjBuilder = DefaultNextObjective.builder()
                            .withId(nid)
                            .withType(NextObjective.Type.HASHED)
                            .withMeta(metabuilder.build())
                            .fromApp(appId);

                    next.dstNextHops().forEach((dstDev, nextHops) -> {

                        HashMap<DeviceId, Integer> intermediateWeightMap = new HashMap<>(); // Stores the weight of nh
                        HashMap<PortNumber, Integer> finalPortWeightMap;  // Stores the final weights of ports

                        if (nextHops.contains(dstDev)) {
                            //Destination is the nexthop so its weight is 1
                            intermediateWeightMap.put(dstDev, 1);
                        } else {
                            HashMap<DeviceId, Set<Link>> nextHopEgLinkMap = new HashMap<>(); //links from nh to Dst
                            HashMap<DeviceId, Long> nextHopLinkSpeedMap = new HashMap<>(); //Throughput from nh to Dst
                            // Find nextHopEgLinkMap through the paths calculated for each dstDev:
                            Topology topology = tpService.currentTopology();
                            for (Path path: tpService.getPaths(topology, deviceId, dstDev)) {
                                for (Link link : path.links()) {
                                    if (nextHops.contains(link.src().deviceId())) {
                                        //seeing a link egressing from the nh (next hop is an intermediate switch)
                                        Set<Link> soFarLinks = nextHopEgLinkMap.get(link.src().deviceId());
                                        if (soFarLinks == null) {
                                            soFarLinks = new HashSet<Link>();
                                        }
                                        soFarLinks.add(link);
                                        nextHopEgLinkMap.put(link.src().deviceId(), soFarLinks);
                                    }
                                }
                            }
                            //populate nextHopLinkSpeedMap for a leaf to leaf path):
                            nextHopEgLinkMap.forEach((neighbor, egLinksSet) -> {
                                Long intermediateWeight = 0L;
                                for (Link link: egLinksSet) {
                                    intermediateWeight = intermediateWeight +
                                            srManager.deviceService.getPort(neighbor, link.src().port()).portSpeed();
                                }
                                nextHopLinkSpeedMap.put(neighbor, intermediateWeight);
                            });
                            intermediateWeightMap =
                                    computeIntermediateWeights(nextHopLinkSpeedMap);
                        }

                        //Calculate the final weights based on the intermediate weights and interfaces port speed
                        HashMap<DeviceId, HashMap<PortNumber, Long>> neighborPortSpeedMap = new HashMap<>();
                        nextHops.forEach(neighbor -> {
                            MacAddress neighborMac;
                            try {
                                neighborMac = deviceConfig.getDeviceMac(neighbor);
                            } catch (DeviceConfigNotFoundException e) {
                                log.warn(e.getMessage() + " Aborting neighbor"
                                        + neighbor);
                                return;
                            }
                            HashMap<PortNumber, Long> innerPortSpeedMap = new HashMap<>();
                            grHandler.devicePortMap.get(neighbor).forEach(port -> {
                                innerPortSpeedMap.put(port, srManager.deviceService.getPort(deviceId, port)
                                        .portSpeed());
                            });
                            neighborPortSpeedMap.put(neighbor, innerPortSpeedMap);
                        });

                        finalPortWeightMap =
                                computeFinalWeights(intermediateWeightMap, neighborPortSpeedMap);
                        log.warn("WCMPdebug10 finalPortWeightMap for dev: {}, dstDev: {}, finalPortWeightMap:{}",
                                deviceId, dstDev, finalPortWeightMap);

                        //build the next objective with the new weight
                        int edgeLabel = dsKey.destinationSet().getEdgeLabel(dstDev);
                        nextHops.forEach(neighbor -> {
                            MacAddress neighborMac;
                            try {
                                neighborMac = deviceConfig.getDeviceMac(neighbor);
                            } catch (DeviceConfigNotFoundException e) {
                                log.warn(e.getMessage() + " Aborting neighbor"
                                        + neighbor);
                                return;
                            }
                            grHandler.devicePortMap.get(neighbor).forEach(port -> {
                                log.trace("verify in device {} nextId {}: bucket with"
                                                + " port/label {}/{} to dst {} via {}",
                                        deviceId, nid, port, edgeLabel,
                                        dstDev, neighbor);
                                //ToDo: can I use variable default weight from onos here?
                                int weight = 1;
                                if (finalPortWeightMap.containsKey(port)) {
                                    weight = finalPortWeightMap.get(port);
                                }
                                nextObjBuilder
                                        .addTreatment(DefaultNextTreatment.of(treatmentBuilder(port,
                                                neighborMac,
                                                dsKey.destinationSet().swap(),
                                                edgeLabel,
                                                grHandler.popVlanInHashGroup(dsKey.destinationSet())), weight));
                            });
                        });
                    });

                    NextObjective nextObjective = nextObjBuilder.modify();
                    flowObjectiveService.next(deviceId, nextObjective);
                }
            } finally {
                rh.releaseRoutingLock();
            }

        }

        TrafficTreatment treatmentBuilder(PortNumber outport, MacAddress dstMac,
                                          boolean swap, int edgeLabel, boolean popVlan) {
            TrafficTreatment.Builder tBuilder =
                    DefaultTrafficTreatment.builder();
            tBuilder.setOutput(outport)
                    .setEthDst(dstMac)
                    .setEthSrc(nodeMacAddr);

            if (popVlan) {
                tBuilder.popVlan();
            }

            if (edgeLabel != DestinationSet.NO_EDGE_LABEL) {
                if (swap) {
                    // swap label case
                    tBuilder.setMpls(MplsLabel.mplsLabel(edgeLabel));
                } else {
                    // ecmp with label push case
                    tBuilder.pushMpls()
                            .copyTtlOut()
                            .setMpls(MplsLabel.mplsLabel(edgeLabel));
                }
            }
            return tBuilder.build();
        }

        HashMap<PortNumber, Integer> computeFinalWeights(HashMap intermediateWeightMap, HashMap neighborPortSpeedMap) {

            HashMap<PortNumber, Integer> portWeightMap = new HashMap<>();
            HashMap<PortNumber, Double> tempMap = new HashMap<>();

            intermediateWeightMap.forEach((neighbor, neighborWeight) -> {
                HashMap<PortNumber, Long> portSpeedInnerMap =
                        (HashMap<PortNumber, Long>) neighborPortSpeedMap.get(neighbor);

                double sumSpeed = portSpeedInnerMap.values().stream().reduce(0L, Long::sum).doubleValue();
                portSpeedInnerMap.forEach((k, v) -> {
                    tempMap.put(k, ((Integer) neighborWeight).doubleValue() * v / sumSpeed);
                });
            });
            tempMap.forEach((k, v) -> {
                portWeightMap.put(k, (int) Math.round(v / Collections.min(tempMap.values())));
            });
            return portWeightMap;
        }

        HashMap<DeviceId, Integer> computeIntermediateWeights(HashMap nextHopLinkSpeedMap) {

            HashMap<DeviceId, Integer> intermediateWeights = new HashMap<>();
            long min = (long) Collections.min(nextHopLinkSpeedMap.values());
            nextHopLinkSpeedMap.forEach((k, v) -> {
                intermediateWeights.put(
                        (DeviceId) k, (int) Math.round(((Long) v).doubleValue() / (double) min));
            });
            return intermediateWeights;
        }
    }
}

