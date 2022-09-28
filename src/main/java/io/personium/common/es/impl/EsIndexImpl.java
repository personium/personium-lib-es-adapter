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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.Refresh;
import io.personium.common.es.EsBulkRequest;
import io.personium.common.es.EsIndex;
import io.personium.common.es.EsMappingConfig;
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
    private EsMappingConfig mappingConfig;

    String indexName;
    String category;

    /**
     * .
     * @param indexName .
     * @param category .
     * @param times .
     * @param interval .
     * @param client .
     * @param mappingConfig .
     */
    public EsIndexImpl(final String indexName,
            final String category,
            int times,
            int interval,
            InternalEsClient client,
            EsMappingConfig mappingConfig) {
        this.indexName = indexName;
        this.category = category;
        this.esClient = client;
        this.mappingConfig = mappingConfig;
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

        Map<String, ObjectNode> mapping = mappingConfig.getMapping();
        if (mapping == null) {
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
            esClient.asyncCreateIndex(this.indexName, mapping, settingJson).get();
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            // Elasticsearch throws ElasticsearchException and TransportException in ExecutionException
            Throwable cause = e.getCause();
            if (cause instanceof ElasticsearchException) {
                throw EsClientException.convertException((ElasticsearchException) cause);
            }
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
            var response = esClient.asyncSearch(this.indexName, routingId, query);
            return PersoniumSearchResponseImpl.getInstance(response.get());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            // Elasticsearch throws ElasticsearchException and TransportException in ExecutionException
            Throwable cause = e.getCause();
            if (cause instanceof ElasticsearchException) {
                throw EsClientException.convertException((ElasticsearchException) cause);
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public PersoniumMultiSearchResponse multiSearch(String routingId, List<Map<String, Object>> queryList) {
        try {
            var response = esClient.asyncMultiSearch(this.indexName, routingId, queryList).get();
            return PersoniumMultiSearchResponseImpl.getInstance(response);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            // Elasticsearch throws ElasticsearchException and TransportException in ExecutionException
            Throwable cause = e.getCause();
            if (cause instanceof ElasticsearchException) {
                throw EsClientException.convertException((ElasticsearchException) cause);
            }
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
        } catch (ElasticsearchException e) {
            throw EsClientException.convertException(e);
        }
        return null;
    }

    @Override
    public void updateSettings(Map<String, String> settings) {
        this.updateSettings(this.indexName, settings);
    }

    /**
     * Read JSON resource and return as ObjectNode.
     * @param resPath resource path
     * @return ObjectNode
     */
    private static ObjectNode readJsonResource(final String resPath) {
        ObjectMapper mapper = new ObjectMapper();
        try (var is = EsMappingFromResources.class.getClassLoader().getResourceAsStream(resPath)) {
            return mapper.readTree(is).deepCopy();
        } catch (IOException e) {
            throw new RuntimeException("exception while reading " + resPath, e);
        }
    }
}
