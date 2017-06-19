/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.usergrid.persistence.queue.impl;

import org.safehaus.guicyfig.Default;
import org.safehaus.guicyfig.FigSingleton;
import org.safehaus.guicyfig.GuicyFig;
import org.safehaus.guicyfig.Key;

import java.io.Serializable;


@FigSingleton
public interface CassQueueFig extends GuicyFig, Serializable {

    String CASS_QUEUE_MAX_REGIONS = "cassqueue.max.regions";
    String CASS_QUEUE_REGION_LIST = "usergrid.queue.regionList";
    String CASS_QUEUE_LOCAL_REGION = "usergrid.queue.region";
    String CASS_QUEUE_MULTIREGION = "usergrid.queue.multiregion";
    String CASS_QUEUE_ALL_REGIONS_LIST = "usergrid.cass.queue.allRegionsList";
    String CASS_QUEUE_MARKING_TTL_SECS = "usergrid.cass.queue.marking.ttl.secs";

    /** Cassandra Queue max number of regions, used to create table */
    @Key(CASS_QUEUE_MAX_REGIONS)
    @Default("100")
    int getMaxRegions();

    @Key(CASS_QUEUE_REGION_LIST)
    @Default("us-east-1")
    String getRegionList();

    @Key(CASS_QUEUE_LOCAL_REGION)
    @Default("us-east-1")
    String getLocalRegion();

    @Key(CASS_QUEUE_MULTIREGION)
    @Default("false")
    boolean isMultiregion();

    @Key(CASS_QUEUE_ALL_REGIONS_LIST)
    @Default("us-east-1")
    String getAllRegionsList();

    @Key(CASS_QUEUE_MARKING_TTL_SECS)
    @Default("30")
    int getMarkingTTL();
}
