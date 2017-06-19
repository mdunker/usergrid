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

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Using;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import org.apache.usergrid.persistence.core.CassandraFig;
import org.apache.usergrid.persistence.core.astyanax.MultiTenantColumnFamilyDefinition;
import org.apache.usergrid.persistence.core.datastax.CQLUtils;
import org.apache.usergrid.persistence.core.datastax.TableDefinition;
import org.apache.usergrid.persistence.core.datastax.impl.TableDefinitionImpl;
import org.apache.usergrid.persistence.core.migration.schema.Migration;
import org.apache.usergrid.persistence.queue.LegacyQueueManager;
import org.apache.usergrid.persistence.queue.LegacyQueueMessage;
import org.apache.usergrid.persistence.queue.LegacyQueueScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CassQueueManagerImpl implements LegacyQueueManager, Migration {

    private static final Logger logger = LoggerFactory.getLogger( CassQueueManagerImpl.class );

    private final LegacyQueueScope scope;
    private final CassandraFig cassandraConfig;
    private final CassQueueFig cassQueueConfig;
    private final Session session;

    private static final JsonFactory JSON_FACTORY = new JsonFactory();
    private static final ObjectMapper mapper = new ObjectMapper( JSON_FACTORY );

    private final String queueName;
    private final boolean isMultiregion;
    private final String localRegion;
    private final int localRegionNum;
    private final Set<Integer> clusterRegionNums = new HashSet<>();
    private final int markingTTL;


    private static final String CASS_QUEUE_TABLE = CQLUtils.quote("Cass_Index_Queue");
    private static final String CASS_QUEUE_NAME_COLUMN = "queue_name";
    private static final String CASS_REQUEST_ID_COLUMN = "request_id";
    private static final Collection<String> CASS_QUEUE_PARTITION_KEYS = Collections.singletonList(CASS_QUEUE_NAME_COLUMN);
    private static final Collection<String> CASS_QUEUE_COLUMN_KEYS = Collections.singletonList(CASS_REQUEST_ID_COLUMN);
    // CASS_QUEUE_COLUMNS set in getTables()
    private static final Map<String, String> CASS_QUEUE_CLUSTERING_ORDER =
        new HashMap<String, String>() {{
            put( "request_id", "ASC");
        }};

    @Inject
    public CassQueueManagerImpl(@Assisted LegacyQueueScope scope, CassandraFig cassandraConfig, CassQueueFig cassQueueConfig,
                                Session session) {
        this.scope = scope;
        this.cassandraConfig = cassandraConfig;
        this.cassQueueConfig = cassQueueConfig;
        this.session = session;

        this.queueName = scope.getName();
        this.isMultiregion = cassQueueConfig.isMultiregion();
        this.localRegion = cassQueueConfig.getLocalRegion();
        String[] clusterRegionsList = cassQueueConfig.getRegionList().split(",");
        String[] allRegionsList = cassQueueConfig.getAllRegionsList().split(",");
        Map<String,Integer> allRegionsMap = new HashMap<>();
        for (int i = 0; i < allRegionsList.length; i++) {
            allRegionsMap.put(allRegionsList[i],i+1);
        }
        this.localRegionNum = allRegionsMap.getOrDefault(this.localRegion, -1);
        if (localRegionNum < 0) {
            // this is a problem -- local region should show up in all regions list
        }
        if (scope.getRegionImplementation() == LegacyQueueScope.RegionImplementation.ALL) {
            for (String clusterRegion : clusterRegionsList) {
                int regionNum = allRegionsMap.getOrDefault(clusterRegion, -1);
                if (regionNum < 0) {
                    // this is a problem -- cluster regions should show up in all regions list
                    throw new RuntimeException("Region " + clusterRegion + " not in all regions list");
                }
                this.clusterRegionNums.add(regionNum);
            }
        } else {
            // only local region
            this.clusterRegionNums.add(localRegionNum);
        }
        this.markingTTL = cassQueueConfig.getMarkingTTL();
    }

    // Due to scheme, getMessages may return fewer messages than the number requested.
    @Override
    public List<LegacyQueueMessage> getMessages(int limit, Class klass) {

        Using timeToLive = QueryBuilder.ttl(markingTTL);

        // mark limit # of messages
        session.

        return null;
    }

    @Override
    public long getQueueDepth() {
        return 0;
    }

    @Override
    public void commitMessage(LegacyQueueMessage queueMessage) {

    }

    @Override
    public void commitMessages(List<LegacyQueueMessage> queueMessages) {

    }

    @Override
    public void sendMessages(List bodies) throws IOException {

    }

    @Override
    public List<LegacyQueueMessage> sendQueueMessages(List<LegacyQueueMessage> queueMessages) throws IOException {
        return null;
    }


    @Override
    public <T extends Serializable> void sendMessageToLocalRegion(T body) throws IOException {

    }

    @Override
    public <T extends Serializable> void sendMessageToAllRegions(T body) throws IOException {

    }

    @Override
    public void deleteQueue() {
        // N/A for cass queue
    }

    @Override
    public void clearQueueNameCache() {
        // N/A for cass queue
    }

    @Override
    public Collection<MultiTenantColumnFamilyDefinition> getColumnFamilies() {
        // This here only until all traces of Astyanax are removed.
        return Collections.emptyList();
    }

    @Override
    public Collection<TableDefinition> getTables() {
        final int maxRegions = cassQueueConfig.getMaxRegions();
        final Map<String, DataType.Name> cassQueueColumns = new HashMap<>();
        final Set<String> cassQueueIndexColumns = new HashSet<>();
        cassQueueColumns.put(CASS_QUEUE_NAME_COLUMN, DataType.Name.TEXT);
        cassQueueColumns.put(CASS_REQUEST_ID_COLUMN, DataType.Name.TIMEUUID);
        cassQueueColumns.put("application_id", DataType.Name.UUID);
        cassQueueColumns.put("entity_id", DataType.Name.UUID);
        cassQueueColumns.put("entity_version_id", DataType.Name.TIMEUUID);
        //cassQueueColumns.put("map_key", DataType.Name.UUID);
        cassQueueColumns.put("origin_region", DataType.Name.INT);
        for (int i = 1; i <= maxRegions; i++) {
            String columnName = "region" + i;
            cassQueueColumns.put(columnName, DataType.Name.TEXT);
            cassQueueIndexColumns.add(columnName);
        }

        TableDefinition cassQueue = new TableDefinitionImpl(
            cassandraConfig.getApplicationKeyspace(),
            CASS_QUEUE_TABLE,
            CASS_QUEUE_PARTITION_KEYS,
            CASS_QUEUE_COLUMN_KEYS,
            cassQueueColumns,
            TableDefinitionImpl.CacheOption.NONE,
            CASS_QUEUE_CLUSTERING_ORDER,
            cassQueueIndexColumns
        );

        return Collections.singletonList(cassQueue);
    }
}
