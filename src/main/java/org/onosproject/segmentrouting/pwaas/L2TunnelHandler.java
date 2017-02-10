/*
 * Copyright 2016-present Open Networking Laboratory
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

package org.onosproject.segmentrouting.pwaas;

import org.onosproject.net.config.NetworkConfigEvent;
import org.onosproject.segmentrouting.SegmentRoutingManager;
import org.onosproject.segmentrouting.config.PwaasConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles pwaas related events.
 */
public class L2TunnelHandler {
    private static final Logger log = LoggerFactory.getLogger(L2TunnelHandler.class);
    private static final String CONFIG_NOT_FOUND = "Pwaas config not found";
    private static final String NOT_MASTER = "Not master controller";
    private final SegmentRoutingManager srManager;

    public L2TunnelHandler(SegmentRoutingManager srManager) {
        this.srManager = srManager;
    }

    /**
     * Processes Pwaas Config added event.
     *
     * @param event network config added event
     */
    public void processPwaasConfigAdded(NetworkConfigEvent event) {
        log.info("Processing Pwaas CONFIG_ADDED");
        PwaasConfig config = (PwaasConfig) event.config().get();
        config.getPwIds().forEach(pwId -> {
            log.info("{}", config.getPwDescription(pwId));
        });
    }

    /**
     * Processes Pwaas Config updated event.
     *
     * @param event network config updated event
     */
    public void processPwaasConfigUpdated(NetworkConfigEvent event) {
        log.info("Processing Pwaas CONFIG_UPDATED");
        PwaasConfig config = (PwaasConfig) event.config().get();
        config.getPwIds().forEach(pwId -> {
            log.info("{}", config.getPwDescription(pwId));
        });
    }

    /**
     * Processes Pwaas Config removed event.
     *
     * @param event network config removed event
     */
    public void processPwaasConfigRemoved(NetworkConfigEvent event) {
        log.info("Processing Pwaas CONFIG_REMOVED");
        PwaasConfig config = (PwaasConfig) event.config().get();
        config.getPwIds().forEach(pwId -> {
            log.info("{}", config.getPwDescription(pwId));
        });
    }
}
