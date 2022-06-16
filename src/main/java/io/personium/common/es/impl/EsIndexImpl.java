/**
 * Personium
 * Copyright 2014-2021 Personium Project Authors
 * - FUJITSU LIMITED
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
package io.personium.common.es.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import co.elastic.clients.elasticsearch._types.Refresh;
import io.personium.common.es.EsBulkRequest;
import io.personium.common.es.EsIndex;
import io.personium.common.es.response.EsClientException;
import io.personium.common.es.response.PersoniumBulkResponse;
import io.personium.common.es.response.PersoniumMultiSearchResponse;
import io.personium.common.es.response.PersoniumSearchResponse;
import io.personium.common.es.response.impl.PersoniumBulkResponseImpl;
import io.personium.common.es.response.impl.PersoniumMultiSearchResponseImpl;
import io.personium.common.es.response.impl.PersoniumSearchResponseImpl;

/**
 * Class for index operations.
 */
public class EsIndexImpl implements EsIndex {

    private InternalEsClient esClient;

    // category - type - mappings.
    static Map<String, Map<String, ObjectNode>> mappingConfigs = null;

    String indexName;
    String category;

    public EsIndexImpl(final String indexName,
            final String category,
            int times,
            int interval,
            InternalEsClient client) {
        this.indexName = indexName;
        this.category = category;
        this.esClient = client;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return this.indexName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getCategory() {
        return this.category;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void create() {
        if (mappingConfigs == null) {
            loadMappingConfigs();
        }
        Map<String, ObjectNode> mappings = mappingConfigs.get(this.category);
        if (mappings == null) {
            throw new EsClientException("NO MAPPINGS DEFINED for " + this.category + this.indexName);
        }

        // load index config
        ObjectNode settingJson = readJsonResource("es/indexSettings.json");
        // static settings are moved to resource file except analyzer lang.
        // indexSettings.put("analysis.analyzer.default.type", "cjk");
        // dynamic
        String numberOfShards = System.getProperty("io.personium.es.index.numberOfShards");
        if (numberOfShards != null) {
            settingJson.put("number_of_shards", numberOfShards);
        }
        String numberOfReplicas = System.getProperty("io.personium.es.index.numberOfReplicas");
        if (numberOfReplicas != null) {
            settingJson.put("number_of_replicas", numberOfReplicas);
        }
        String maxResultWindow = System.getProperty("io.personium.es.index.maxResultWindow");
        if (maxResultWindow != null) {
            settingJson.put("max_result_window", maxResultWindow);
        }
        String maxThreadCount = System.getProperty("io.personium.es.index.merge.scheduler.maxThreadCount");
        if (maxThreadCount != null) {
            settingJson.put("index.merge.scheduler.max_thread_count", maxThreadCount);
        }

        try {
            esClient.syncCreateIndex(this.indexName, mappings, settingJson);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete() {
        try {
            esClient.syncDeleteIndex(this.indexName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public PersoniumSearchResponse search(String routingId, Map<String, Object> query) {
        try {
            var response = esClient.syncSearch(this.indexName, routingId, query);
            return PersoniumSearchResponseImpl.getInstance(response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public PersoniumMultiSearchResponse multiSearch(String routingId, List<Map<String, Object>> queryList) {
        try {
            var response = esClient.syncMultiSearch(this.indexName, routingId, queryList);
            return PersoniumMultiSearchResponseImpl.getInstance(response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteByQuery(String routingId, Map<String, Object> query) {
        try {
            esClient.deleteByQuery(this.indexName, query, true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // Confirm that all items have been deleted
        PersoniumSearchResponse searchRes = this.search(routingId, query);
        long failedCount = searchRes.getHits().getAllPages();
        if (failedCount != 0) {
            throw new EsClientException.EsDeleteByQueryException(failedCount);
        }
    }

    @Override
    public PersoniumBulkResponse bulkRequest(String routingId, List<EsBulkRequest> datas, boolean isWriteLog) {
        try {
            var response = esClient.bulkRequest(this.indexName, routingId, datas, isWriteLog, Refresh.True);
            return PersoniumBulkResponseImpl.getInstance(response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Void updateSettings(String index, Map<String, String> settings) {
        try {
            esClient.updateIndexSettings(index, settings);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    @Override
    public void updateSettings(Map<String, String> settings) {
        this.updateSettings(this.indexName, settings);
    }

    /**
     * Read mapping data from resources.
     */
    static synchronized void loadMappingConfigs() {
        if (mappingConfigs != null) {
            return;
        }
        mappingConfigs = new HashMap<String, Map<String, ObjectNode>>();

        // admin category
        var adminMap = new HashMap<String, ObjectNode>();
        adminMap.put("Domain", readJsonResource("es/mapping/domain.json"));
        adminMap.put("Cell", readJsonResource("es/mapping/cell.json"));

        // user category
        var userMap = new HashMap<String, ObjectNode>();
        userMap.put("link", readJsonResource("es/mapping/link.json"));
        userMap.put("Account", readJsonResource("es/mapping/account.json"));
        userMap.put("Box", readJsonResource("es/mapping/box.json"));
        userMap.put("Role", readJsonResource("es/mapping/role.json"));
        userMap.put("Relation", readJsonResource("es/mapping/relation.json"));
        userMap.put("SentMessage", readJsonResource("es/mapping/sentMessage.json"));
        userMap.put("ReceivedMessage", readJsonResource("es/mapping/receivedMessage.json"));
        userMap.put("EntityType", readJsonResource("es/mapping/entityType.json"));
        userMap.put("AssociationEnd", readJsonResource("es/mapping/associationEnd.json"));
        userMap.put("Property", readJsonResource("es/mapping/property.json"));
        userMap.put("ComplexType", readJsonResource("es/mapping/complexType.json"));
        userMap.put("ComplexTypeProperty", readJsonResource("es/mapping/complexTypeProperty.json"));
        userMap.put("ExtCell", readJsonResource("es/mapping/extCell.json"));
        userMap.put("ExtRole", readJsonResource("es/mapping/extRole.json"));
        userMap.put("dav", readJsonResource("es/mapping/dav.json"));
        userMap.put("UserData", readJsonResource("es/mapping/userdata.json"));
        userMap.put("Rule", readJsonResource("es/mapping/rule.json"));

        mappingConfigs.put(EsIndex.CATEGORY_AD, adminMap);
        mappingConfigs.put(EsIndex.CATEGORY_USR, userMap);
    }

    /**
     * Read JSON resource and return as ObjectNode.
     * @param resPath resource path
     * @return ObjectNode
     */
    private static ObjectNode readJsonResource(final String resPath) {
        ObjectMapper mapper = new ObjectMapper();
        try (InputStream is = EsIndexImpl.class.getClassLoader().getResourceAsStream(resPath)) {
            return mapper.readTree(is).deepCopy();
        } catch (IOException e) {
            throw new RuntimeException("exception while reading " + resPath, e);
        }
    }
}
