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

import java.io.Closeable;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.OpType;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.cluster.HealthResponse;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.DeleteByQueryResponse;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.MsearchResponse;
import co.elastic.clients.elasticsearch.core.ScrollResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.msearch.RequestItem;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.DeleteIndexResponse;
import co.elastic.clients.elasticsearch.indices.PutMappingResponse;
import co.elastic.clients.elasticsearch.indices.RecoveryResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.personium.common.es.EsBulkRequest;
import io.personium.common.es.EsClient.Event;
import io.personium.common.es.EsClient.EventHandler;
import io.personium.common.es.EsRequestLogInfo;
import io.personium.common.es.response.EsClientException;
import io.personium.common.es.response.EsClientException.EsMultiSearchQueryParseException;
import jakarta.json.Json;

/**
 * ElasticSearchのアクセサクラス.
 */
public class InternalEsClient implements Closeable {
    static Logger log = LoggerFactory.getLogger(InternalEsClient.class);

    private RestClient restClient;

    private RestClientTransport restClientTransport;

    private ElasticsearchClient esClient;

    private ElasticsearchAsyncClient esAsyncClient;

    private boolean routingFlag;

    /**
     * Default constructor.
     */
    protected InternalEsClient() {
    }

    /**
     * constructor.
     * @param hosts elasticsearch hosts
     */
    protected InternalEsClient(String hosts) {
        routingFlag = true;
        List<HttpHost> httpHosts = parseConfigAndInitializeHostsList(hosts);
        prepareClient(httpHosts.toArray(new HttpHost[httpHosts.size()]));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        closeConnection();
    }

    /**
     * クラスタ名、接続先情報を指定してEsClientのインスタンスを返す.
     * @param hosts elasticsearch hosts
     * @return EsClientのインスタンス
     */
    public static InternalEsClient getInstance(String hosts) {
        return new InternalEsClient(hosts);
    }

    /**
     * Parse hosts string.
     * @param hostNames hosts string ( `hostname1:port1, hostname2:port2` )
     * @return List of HttpHost.
     */
    private List<HttpHost> parseConfigAndInitializeHostsList(String hostNames) {
        return Arrays.asList(hostNames.split(",")).stream().map(host -> {
            String hostWithoutSpaces = host.replaceAll("\\s+", "");
            return HttpHost.create(hostWithoutSpaces);
        }).collect(Collectors.toList());
    }

    /**
     * ESとのコネクションを一度明示的に閉じる.
     */
    public void closeConnection() {
        try {
            if (this.restClientTransport != null) {
                this.restClientTransport.close();
            }
        } catch (IOException e) {
            log.info("Exception in closing restClientTransport", e);
        } finally {
            this.restClientTransport = null;
        }

        try {
            if (this.restClient != null) {
                this.restClient.close();
            }
        } catch (IOException e) {
            log.info("Exception in closing restClient", e);
        } finally {
            this.restClientTransport = null;
        }

        this.esClient = null;
        this.esAsyncClient = null;
    }

    /**
     * Preparing esClient for communicating with elasticsearch.
     * @param hosts elasticsearch host
     */
    private void prepareClient(HttpHost... hosts) {
        if (esClient != null) {
            return;
        }

        this.restClient = RestClient.builder(hosts).build();
        this.restClientTransport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        this.esClient = new ElasticsearchClient(restClientTransport);
        this.esAsyncClient = new ElasticsearchAsyncClient(restClientTransport);
    }

    static Map<Event, EventHandler> eventHandlerMap = new HashMap<Event, EventHandler>();

    /**
     * Eventハンドラの登録.
     * @param ev イベントの種類
     * @param handler ハンドラ
     */
    public static void setEventHandler(Event ev, EventHandler handler) {
        eventHandlerMap.put(ev, handler);
    }

    void fireEvent(Event ev, final Object... params) {
        this.fireEvent(ev, null, params);
    }

    void fireEvent(Event ev, EsRequestLogInfo logInfo, final Object... params) {
        EventHandler handler = eventHandlerMap.get(ev);
        if (handler != null) {
            handler.handleEvent(logInfo, params);
        }
    }

    /**
     * Get cluster status.
     * @return cluster status map.
     * @throws IOException exception while calling health api.
     */
    public Map<String, Object> checkHealth() throws IOException {
        HealthResponse clusterHealth = esClient.cluster().health();
        HashMap<String, Object> map = new HashMap<String, Object>();
        map.put("cluster_name", clusterHealth.clusterName());
        map.put("status", clusterHealth.status().name());
        map.put("timed_out", clusterHealth.timedOut());
        map.put("number_of_nodes", clusterHealth.numberOfNodes());
        map.put("number_of_data_nodes", clusterHealth.numberOfDataNodes());
        map.put("active_primary_shards", clusterHealth.activePrimaryShards());
        map.put("active_shards", clusterHealth.activeShards());
        map.put("relocating_shards", clusterHealth.relocatingShards());
        map.put("initializing_shards", clusterHealth.initializingShards());
        map.put("unassigned_shards", clusterHealth.unassignedShards());
        return map;
    }

    /**
     * Create Index.
     * @param index Name of index.
     * @param mappings Map of type mappings.
     * @param settingJson Json of index settings.
     * @return response.
     * @throws IOException exception while calling api.
     */
    public CompletableFuture<List<CreateIndexResponse>> asyncCreateIndex(String index,
            Map<String, ObjectNode> mappings,
            ObjectNode settingJson) throws IOException {
        this.fireEvent(Event.creatingIndex, index);
        ObjectMapper mapper = new ObjectMapper();
        var requests = new ArrayList<CompletableFuture<CreateIndexResponse>>();
        for (String type : mappings.keySet()) {
            JsonNode mappingJson = null;
            if (mappings.get(type).has("_doc")) {
                // for mapping for prior version Elasticsearch
                mappingJson = mappings.get(type).get("_doc");
            } else {
                mappingJson = mappings.get(type);
            }
            try (StringReader indexSr = new StringReader(mapper.writeValueAsString(settingJson));
                StringReader sr = new StringReader(mapper.writeValueAsString(mappingJson))) {
                // var indexSrParser = Json.createParser(indexSr);
                requests.add(esAsyncClient.indices()
                        .create(cir -> cir
                            .index(makeIndex(index, type))
                            // https://github.com/elastic/elasticsearch-java/issues/297
                            //.settings(iset -> iset.withJson(indexSrParser, esClient._jsonpMapper()))
                            .settings(iset -> iset.withJson(indexSr))
                            .mappings(mtb -> mtb.withJson(sr))
                        ));
            }
        }
        return (CompletableFuture<List<CreateIndexResponse>>) CompletableFuture
            .allOf(requests.toArray(new CompletableFuture[requests.size()]))
            .thenApply(ignored -> {
                return requests.stream().map(request -> request.join()).toList();
            });

    }

    /**
     * インデックスを削除する.
     * @param index インデックス名
     * @return 非同期応答
     * @throws IOException exception while calling API.
     */
    public DeleteIndexResponse syncDeleteIndex(String index) throws IOException {
        return esClient.indices().delete(dir -> dir.index(makeIndex(index, null)));
    }

    /**
     * インデックスの設定を更新する.
     * @param index インデックス名
     * @param settings 更新するインデックス設定
     * @throws IOException exception while calling API.
     */
    public void updateIndexSettings(String index, Map<String, String> settings) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        try (StringReader sr = new StringReader(mapper.writeValueAsString(settings))) {
            esClient.indices().putSettings(psr -> psr
                .index(makeIndex(index, null))
                .settings(is -> is.withJson(sr))
            );
        }
    }

    /**
     * Mapping定義を取得する.
     * @param index インデックス名
     * @param type タイプ名
     * @return Mapping定義
     * @throws IOException exception while calling API.
     */
    public TypeMapping getMapping(String index, String type) throws IOException {
        return esClient.indices().getMapping(gmr -> gmr.index(makeIndex(index, type))).get(makeIndex(index, type))
                .mappings();
    }

    /**
     * Mapping定義を更新する.
     * @param index インデックス名
     * @param type タイプ名
     * @param mappings マッピング情報
     * @return 非同期応答
     * @throws IOException exception while calling API.
     */
    public PutMappingResponse putMapping(String index, String type, Map<String, Object> mappings) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        try (StringReader sr = new StringReader(mapper.writeValueAsString(mappings))) {
            return esClient.indices()
                    .putMapping(pmr -> pmr.index(makeIndex(index, type)).type(makeType(type)).withJson(sr));
        }
    }

    /**
     * インデックスステータスを取得する.
     * @return 非同期応答
     * @throws IOException exception while calling recovery api.
     */
    public RecoveryResponse syncGetIndicesStatus() throws IOException {
        return esClient.indices().recovery();
    }

    /**
     * Get document asynchronously.
     * @param index インデックス名
     * @param type タイプ名
     * @param id ドキュメントのID
     * @param routingId routingId
     * @param realtime リアルタイムモードなら真
     * @return 非同期応答
     * @throws IOException IO exception while calling API.
     * @throws ElasticsearchException ES exception while calling API.
     */
    public CompletableFuture<GetResponse<ObjectNode>> asyncGet(String index,
            String type,
            String id,
            String routingId,
            boolean realtime) throws IOException, ElasticsearchException {
        return asyncGet(index, type, id, routingId, realtime, -1);
    }

    /**
     * Asynchronous gettings documents with specified version.
     * @param index インデックス名
     * @param type タイプ名
     * @param id ドキュメントのID
     * @param routingId routingId
     * @param realtime リアルタイムモードなら真
     * @param version version
     * @return 非同期応答
     */
    public CompletableFuture<GetResponse<ObjectNode>> asyncGet(String index,
            String type,
            String id,
            String routingId,
            boolean realtime,
            long version) {
        var ret = esAsyncClient.get(gr -> {
            var getRequest = gr.index(makeIndex(index, type)).type(makeType(type)).id(id).realtime(realtime);
            if (routingFlag) {
                getRequest = getRequest.routing(routingId);
            }
            if (version != -1) {
                getRequest = getRequest.version(version);
            }
            return getRequest;
        }, ObjectNode.class);
        this.fireEvent(Event.afterRequest, index, type, id, null, "Get");
        return ret;
    }

    /**
     * 非同期でドキュメントを検索.
     * @param index インデックス名
     * @param type タイプ名
     * @param routingId routingId
     * @param query クエリ情報
     * @return 非同期応答
     */
    public CompletableFuture<SearchResponse<ObjectNode>> asyncSearch(String index,
            String type,
            String routingId,
            Map<String, Object> query) {
        var result = esAsyncClient.search(sreq -> {
            var builder = sreq.index(makeIndex(index, type)).type(makeType(type));
            if (query != null) {
                try (var sr = new StringReader(queryMapToJSON(query, type))) {
                    builder.withJson(sr);
                }
            }
            if (routingFlag) {
                builder.routing(routingId);
            }
            return builder;
        }, ObjectNode.class);
        this.fireEvent(Event.afterRequest, index, type, null, JSONObject.toJSONString(query), "Search");
        return result;
    }

    /**
     * Search douments.
     * @param index インデックス名
     * @param routingId routingId
     * @param query クエリ情報
     * @return async response
     * @throws IOException IO exception while calling API.
     */
    public CompletableFuture<SearchResponse<ObjectNode>> asyncSearch(String index,
            String routingId,
            Map<String, Object> query) {
        return this.asyncSearch(index, null, routingId, query);
    }


    /**
     * Search documents from multiple indices asynchronously.
     * @param index インデックス名
     * @param routingId routingId
     * @param queryList マルチ検索用のクエリ情報リスト
     * @return Asynchronous response.
     * @throws IOException IO exception while calling API.
     */
    public CompletableFuture<MsearchResponse<ObjectNode>> asyncMultiSearch(String index,
            String routingId,
            List<Map<String, Object>> queryList) throws IOException {
        return this.asyncMultiSearch(index, null, routingId, queryList);
    }

    /**
     * Search documents from multiple indices asynchronously.
     * @param index インデックス名
     * @param type タイプ名
     * @param routingId routingId
     * @param queryList マルチ検索用のクエリ情報リスト
     * @return 非同期応答
     * @throws IOException IO exception while calling API.
     */
    public CompletableFuture<MsearchResponse<ObjectNode>> asyncMultiSearch(String index,
            String type,
            String routingId,
            List<Map<String, Object>> queryList) throws IOException {
        if (queryList == null || queryList.size() == 0) {
            throw new EsMultiSearchQueryParseException();
        }

        List<RequestItem> listRequestItems = new ArrayList<RequestItem>();

        for (var query : queryList) {
            var riBuilder = new RequestItem.Builder();
            riBuilder = riBuilder.header(mh -> {
                var mheader = mh.index(makeIndex(index, type));
                if (routingFlag) {
                    return mheader.routing(routingId);
                }
                return mheader;
            }).body(mb -> {
                var mbody = mb;
                if (query != null) {
                    String queryJson = queryMapToJSON(query, null);
                    try (StringReader sr = new StringReader(queryJson)) {
                        var parser = Json.createParser(sr);
                        mbody = mbody.withJson(parser, esClient._jsonpMapper());
                    }
                }
                return mbody;
            });
            listRequestItems.add(riBuilder.build());
        }

        var response = esAsyncClient.msearch(mr -> mr.searches(listRequestItems), ObjectNode.class);

        this.fireEvent(Event.afterRequest, index, type, null, JSONArray.toJSONString(queryList), "MultiSearch");
        return response;
    }

    private static final String SCROLL_SEARCH_KEEP_ALIVE_TIME = "5m";

    /**
     * ScrollSearch with query.
     * @param index インデックス名
     * @param type タイプ名
     * @param query 検索クエリ
     * @return SearchResponse.
     * @throws IOException IO exception while calling API.
     */
    public SearchResponse<ObjectNode> scrollSearch(String index, String type, Map<String, Object> query)
            throws IOException {
        var builder = new SearchRequest.Builder().index(makeIndex(index, type))
                .scroll(t -> t.time(SCROLL_SEARCH_KEEP_ALIVE_TIME));
        if (type != null) {
            builder = builder.type(makeType(type));
        }
        if (query != null) {
            try (StringReader sr = new StringReader(queryMapToJSON(query, null))) {
                builder = builder.withJson(sr);
            }
        }
        return esClient.search(builder.build(), ObjectNode.class);
    }

    /**
     * Continue scroll search with specified scrollId.
     * @param scrollId スクロールID
     * @return SearchResponse.
     * @throws IOException IO exception while calling API.
     */
    public ScrollResponse<ObjectNode> scrollSearch(String scrollId) throws IOException {
        return esClient.scroll(sr -> sr.scrollId(scrollId), ObjectNode.class);
    }

    /**
     * Search documents in all types in an index asynchronously.
     * @param index インデックス名
     * @param query クエリ情報
     * @return Asynchronous response.
     * @throws IOException IO exception while calling API.
     */
    public CompletableFuture<SearchResponse<ObjectNode>> indexSearch(String index,
            Map<String, Object> query) throws IOException {
        SearchRequest.Builder builder = new SearchRequest.Builder().index(makeIndex(index, null));
        if (query != null) {
            try (StringReader sr = new StringReader(queryMapToJSON(query, null))) {
                builder = builder.withJson(sr);
            }
        }

        var response = esAsyncClient.search(builder.build(), ObjectNode.class);
        this.fireEvent(Event.afterRequest, index, null, null, JSONObject.toJSONString(query), "Search");
        return response;
    }

    /**
     * Index a document asynchronously.
     * @param index インデックス名
     * @param type タイプ名
     * @param id ドキュメントのid
     * @param routingId routingId
     * @param data データ
     * @param opType 操作タイプ
     * @param seqNoPrimaryTerm SeqNoPrimaryTerm for optimistic lock.
     * @return IndexResponse
     * @throws IOException IO exception while calling API.
     */
    public CompletableFuture<IndexResponse> asyncIndex(String index,
            String type,
            String id,
            String routingId,
            Map<String, Object> data,
            OpType opType,
            SeqNoPrimaryTerm seqNoPrimaryTerm) {

        var response = esAsyncClient.index(ir -> {
            var indexReq = ir
                    .index(makeIndex(index, type))
                    .type(makeType(type))
                    .id(id)
                    .opType(opType)
                    .refresh(Refresh.True)
                    .document(data);
            if (routingFlag) {
                indexReq = indexReq.routing(routingId);
            }
            if (seqNoPrimaryTerm != null) {
                indexReq = indexReq.ifSeqNo(seqNoPrimaryTerm.seqNo).ifPrimaryTerm(seqNoPrimaryTerm.primaryTerm);
            }
            return indexReq;
        });

        EsRequestLogInfo logInfo = new EsRequestLogInfo(index,
            type, id, routingId, data, opType.toString(), seqNoPrimaryTerm);
        this.fireEvent(Event.afterCreate, logInfo);

        return response;
    }

    /**
     * Delete a document asynchronously.
     * @param index インデックス名
     * @param type タイプ名
     * @param id Document id to delete
     * @param routingId routingId
     * @param version The version of the document to delete
     * @return DeleteResponse.
     * @throws IOException IO exception while calling API.
     */
    public CompletableFuture<DeleteResponse> asyncDelete(String index,
            String type, String id,
            String routingId,
            long version) throws IOException {
        var response = esAsyncClient.delete(dr -> {
            var deleteReq = dr
                .index(makeIndex(index, type))
                .type(makeType(type))
                .id(id)
                .refresh(Refresh.True);
            if (routingFlag) {
                deleteReq = deleteReq.routing(routingId);
            }
            if (version > -1) {
                deleteReq = deleteReq.version(version);
            }
            return deleteReq;
        });

        this.fireEvent(Event.afterRequest, index, type, id, null, "Delete");
        return response;
    }

    /**
     * バルクでドキュメントを登録/更新/削除.
     * @param index インデックス名
     * @param routingId routingId
     * @param datas バルクドキュメント
     * @param isWriteLog リクエスト情報のログ出力有無
     * @param refresh refresh flag.
     * @return BulkResponse
     * @throws IOException IO exception while calling API.
     */
    @SuppressWarnings("unchecked")
    public BulkResponse bulkRequest(String index,
        String routingId,
        List<EsBulkRequest> datas,
        boolean isWriteLog,
        Refresh refresh) throws IOException {

        List<BulkOperation> lOperations = new ArrayList<BulkOperation>();
        List<Map<String, Object>> bulkList = new ArrayList<Map<String, Object>>();

        for (EsBulkRequest data : datas) {
            if (EsBulkRequest.BulkRequestType.DELETE == data.getRequestType()) {
                lOperations.add(createDeleteOperation(index, routingId, data));
            } else {
                lOperations.add(createIndexOperation(index, routingId, data));
            }

            JSONObject logData = new JSONObject();
            logData.put("reqType", data.getRequestType().toString());
            logData.put("type", data.getType());
            logData.put("id", data.getId());
            logData.put("source", data.getSource());
            bulkList.add(logData);
            log.debug("BulkItemRequest:" + logData.toJSONString());
        }
        Map<String, Object> debug = new HashMap<String, Object>();
        debug.put("bulk", bulkList);

        var result = esClient.bulk(br -> br.operations(lOperations).refresh(refresh));
        if (result.errors()) {
            for (var item: result.items()) {
                if (item.error() != null) {
                    log.debug("BulkItemReponse:" + ":" + item.operationType() + ":" + item.id() + ":" + item.index()
                    + "/" + item.type() + "/" /*+ item.getItemId()*/ + ":" + item.error().reason());
                }
            }
        }
        if (isWriteLog) {
            this.fireEvent(Event.afterRequest, index, "none", "none", debug, "bulkRequest");
        }
        return result;
    }

    /**
    * バルクリクエストのINDEXリクエストを作成する.
    * @param index インデックス名
    * @param routingId ルーティングID
    * @param data バルクドキュメント情報
    * @return 作成したINDEXリクエスト
    */
    private BulkOperation createDeleteOperation(String index, String routingId, EsBulkRequest data) {
        BulkOperation.Builder builder = new BulkOperation.Builder();
        return builder.delete(dob -> {
            var deleteBuilder = dob
                .index(makeIndex(index, data.getType()))
                .id(data.getId());
            if (routingFlag) {
                deleteBuilder = deleteBuilder.routing(routingId);
            }
            return deleteBuilder;
        }).build();
    }

    /**
    * バルクリクエストのDELETEリクエストを作成する.
    * @param index インデックス名
    * @param routingId ルーティングID
    * @param data バルクドキュメント情報
    * @return 作成したDELETEリクエスト
    */
    private BulkOperation createIndexOperation(String index, String routingId, EsBulkRequest data) {
        BulkOperation.Builder builder = new BulkOperation.Builder();
        return builder.index(ib -> {
            var indexBuilder = ib
                .index(makeIndex(index, data.getType()))
                .id(data.getId())
                .document(makeData(data.getSource(), data.getType()));
            if (routingFlag) {
                indexBuilder = indexBuilder.routing(routingId);
            }
            return indexBuilder;
        }).build();
    }

    /**
     * ルーティングIDに関係なくバルクでドキュメントを登録.
     * @param index インデックス名
     * @param bulkMap バルクドキュメント
     * @return ES応答
     */
    // public PersoniumBulkResponse asyncBulkCreate(
    // String index, Map<String, List<EsBulkRequest>> bulkMap) {
    // BulkRequestBuilder bulkRequest = esTransportClient.prepareBulk();
    // // ルーティングIDごとにバルク登録を行うと効率が悪いため、引数で渡されたEsBulkRequestは全て一括登録する。
    // // また、バルク登録後にactionGet()すると同期実行となるため、ここでは実行しない。
    // // このため、execute()のレスポンスを返却し、呼び出し側でactionGet()してからレスポンスチェック、リフレッシュすること。
    // for (Entry<String, List<EsBulkRequest>> ents : bulkMap.entrySet()) {
    // for (EsBulkRequest data : ents.getValue()) {
    // IndexRequestBuilder req = esTransportClient
    // .prepareIndex(makeIndex(index, data.getType()), makeType(data.getType()), data.getId())
    // .setSource(makeData(data.getSource(), data.getType()));
    // if (routingFlag) {
    // req = req.setRouting(ents.getKey());
    // }
    // bulkRequest.add(req);
    // }
    // }
    // PersoniumBulkResponse response = PersoniumBulkResponseImpl.getInstance(bulkRequest.execute().actionGet());
    // return response;
    // }

    /**
     * 指定されたクエリを使用してデータの削除を行う.
     * @param index 削除対象のインデックス
     * @param deleteQuery 削除対象を指定するクエリ
     * @param refresh refresh flag.
     * @return DeleteByQueryResponse.
     * @throws IOException IO exception while calling API.
     */
    public DeleteByQueryResponse deleteByQuery(String index,
        Map<String, Object> deleteQuery,
        boolean refresh) throws IOException {
        String queryJson = queryMapToJSON(deleteQuery, null);
        try (var sr = new StringReader(queryJson)) {
            return esClient.deleteByQuery(dqb -> dqb
                .index(makeIndex(index, null))
                .withJson(sr)
                .refresh(refresh)
            );
        }
    }

    // /**
    //  * 引数で指定されたインデックスに対してrefreshする.
    //  * @param index インデックス名
    //  * @return レスポンス
    //  */
    // public PersoniumRefreshResponse refresh(String index) {
    // RefreshResponse response = esTransportClient.admin().indices()
    // .refresh(new RefreshRequest(makeIndex(index, null))).actionGet();
    // return PersoniumRefreshResponseImpl.getInstance(response);
    // }

    // /**
    // * flushを行う.
    // * @param index flush対象のindex名
    // * @return 非同期応答
    // */
    // public ActionFuture<FlushResponse> flushTransLog(String index) {
    // ActionFuture<FlushResponse> ret = esTransportClient.admin().indices()
    // .flush(new FlushRequest(makeIndex(index, null)));
    // this.fireEvent(Event.afterRequest, index, null, null, null, "Flush");
    // return ret;
    // }

    /**
     * ES2 -> ES6 非互換吸収のためにメソッド追加.
     */
    private static final String UNIQE_TYPE = "_doc";

    /**
     * Get index and return index.
     * @param index index
     * @param type type
     * @return Elasticsearch index
     */
    public static String makeIndex(String index, String type) {
        StringBuffer sb = new StringBuffer();
        sb.append(index).append(".");
        if (type == null) {
            sb.append("*");
        } else {
            sb.append(type.toLowerCase());
        }
        return sb.toString();
    }

    /**
     * Get type and return it.
     * @param type type
     * @return Elasticsearch type
     */
    public static String makeType(String type) { // NOPMD - Methods for incompatible absorption
        return UNIQE_TYPE;
    }

    private static Map<String, Object> makeData(Map<String, Object> data, String type) {
        Map<String, Object> newData = deepClone(1, data, type);
        newData.put("type", type);
        return newData;
    }

    @SuppressWarnings("unchecked") // CHECKSTYLE IGNORE
    private static String queryMapToJSON(Map<String, Object> map, String type) { // CHECKSTYLE IGNORE
        if (log.isDebugEnabled()) {
            log.debug("\n--- Before ---\n" + toJSON(map, false));
        }
        // Convert start
        Map<String, Object> cloneMap = deepClone(1, map, type);
        Map<String, Object> newMap = new HashMap<String, Object>();
        // version
        Object version = getNestedMapObject(cloneMap, new String[] { "version" }, 0);
        if (version != null) {
            newMap.put("version", version);
        }
        // size
        Object size = getNestedMapObject(cloneMap, new String[] { "size" }, 0);
        if (size != null) {
            newMap.put("size", size);
        }
        // from
        Object from = getNestedMapObject(cloneMap, new String[] { "from" }, 0);
        if (from != null) {
            newMap.put("from", from);
        }
        // sort
        Object sort = getNestedMapObject(cloneMap, new String[] { "sort" }, 0);
        if (sort != null) {
            newMap.put("sort", sort);
        }
        // _source
        Object source = getNestedMapObject(cloneMap, new String[] { "_source" }, 0);
        if (source != null) {
            if (source instanceof List) {
                ((List) source).add(0, "type");
                var newSource = new HashMap<String, Object>();
                newSource.put("includes", source);
                newSource.put("excludes", List.of());
                source = newSource;
            }
            newMap.put("_source", source);
        }
        // query
        Map<String, Object> query = new HashMap<String, Object>();
        newMap.put("query", query);
        Map<String, Object> queryBool = new HashMap<String, Object>();
        query.put("bool", queryBool);
        Map<String, Object> queryBoolFilter = new HashMap<String, Object>();
        queryBool.put("filter", queryBoolFilter);
        Map<String, Object> queryBoolFilterBool = new HashMap<String, Object>();
        queryBoolFilter.put("bool", queryBoolFilterBool);
        List<Map<String, Object>> queryBoolMust = new ArrayList<Map<String, Object>>();
        queryBool.put("must", queryBoolMust);
        List<Map<String, Object>> queryBoolMustnot = new ArrayList<Map<String, Object>>();
        queryBool.put("must_not", queryBoolMustnot);
        List<Map<String, Object>> queryBoolShould = new ArrayList<Map<String, Object>>();
        queryBool.put("should", queryBoolShould);
        List<Map<String, Object>> queryBoolFilterBoolMust = new ArrayList<Map<String, Object>>();
        queryBoolFilterBool.put("must", queryBoolFilterBoolMust);
        List<Map<String, Object>> queryBoolFilterBoolMustnot = new ArrayList<Map<String, Object>>();
        queryBoolFilterBool.put("must_not", queryBoolFilterBoolMustnot);
        List<Map<String, Object>> queryBoolFilterBoolShould = new ArrayList<Map<String, Object>>();
        queryBoolFilterBool.put("should", queryBoolFilterBoolShould);
        /////
        // -query/filterd/filter
        List<Map<String, Object>> queryXmust = (List<Map<String, Object>>) getNestedMapObject(cloneMap,
                new String[] { "query", "filtered", "filter", "bool", "must" }, 0);
        List<Map<String, Object>> queryXmustnot = (List<Map<String, Object>>) getNestedMapObject(cloneMap,
                new String[] { "query", "filtered", "filter", "bool", "must_not" }, 0);
        List<Map<String, Object>> queryXshould = (List<Map<String, Object>>) getNestedMapObject(cloneMap,
                new String[] { "query", "filtered", "filter", "bool", "should" }, 0);
        if (queryXmust != null) {
            for (Map<String, Object> tmap : queryXmust) {
                queryBoolFilterBoolMust.add(tmap);
            }
        }
        if (queryXmustnot != null) {
            for (Map<String, Object> tmap : queryXmustnot) {
                queryBoolFilterBoolMustnot.add(tmap);
            }
        }
        if (queryXshould != null) {
            for (Map<String, Object> tmap : queryXshould) {
                queryBoolFilterBoolShould.add(tmap);
            }
        }
        // -query/filterd/filter other pattern
        if (queryBoolFilterBoolMust.isEmpty() && queryBoolFilterBoolMustnot.isEmpty()
                && queryBoolFilterBoolShould.isEmpty()) {
            // other eregular pattern
            List<Map<String, Object>> queryXfilters = (List<Map<String, Object>>) getNestedMapObject(cloneMap,
                    new String[] { "query", "filtered", "filter", "and", "filters" }, 0);
            if (queryXfilters != null) {
                for (Map<String, Object> tmap : queryXfilters) {
                    queryBoolFilterBoolMust.add(tmap);
                }
            } else {
                Map<String, Object> queryXfilter = (Map<String, Object>) getNestedMapObject(cloneMap,
                        new String[] { "query", "filtered", "filter" }, 0);
                if (queryXfilter != null) {
                    queryBoolFilterBoolMust.add(queryXfilter);
                }
            }
        }
        // -query/filterd/query
        Object queryFilteredQuery = getNestedMapObject(cloneMap, new String[] { "query", "filtered", "query" }, 0);
        if (queryFilteredQuery != null) {
            if (queryFilteredQuery instanceof List) {
                queryBoolMust.addAll((List<Map<String, Object>>) queryFilteredQuery);
            } else if (queryFilteredQuery instanceof Map) {
                queryBoolMust.add((Map<String, Object>) queryFilteredQuery);
            }
        }
        /////
        // -filter/ids
        Map<String, Object> filterIds = (Map<String, Object>) getNestedMapObject(cloneMap,
                new String[] { "filter", "ids" }, 0);
        if (filterIds != null) {
            Map<String, Object> ids = new HashMap<String, Object>();
            ids.put("ids", filterIds);
            queryBoolFilterBoolMust.add(ids);
        }
        // -filter
        Map<String, Object> filter = (Map<String, Object>) getNestedMapObject(cloneMap, new String[] { "filter" }, 0);
        if (filter != null) {
            // -query
            List<Map<String, Object>> filterXquerys = new ArrayList<Map<String, Object>>();
            parseQueryMap(filterXquerys, filter);
            if (!filterXquerys.isEmpty()) {
                Map<String, Object> queryMap = new HashMap<String, Object>();
                queryBoolMust.add(queryMap);
                Map<String, Object> queryBoolMustBool = new HashMap<String, Object>();
                queryMap.put("bool", queryBoolMustBool);
                List<Map<String, Object>> queryBoolMustBoolMust = new ArrayList<Map<String, Object>>();
                queryBoolMustBool.put("must", queryBoolMustBoolMust);
                for (Map<String, Object> filterXquery : filterXquerys) {
                    if (filterXquery instanceof List) {
                        queryBoolMustBoolMust.addAll((List<Map<String, Object>>) filterXquery);
                    } else if (filterXquery instanceof Map) {
                        queryBoolMustBoolMust.add((Map<String, Object>) filterXquery);
                    }
                }
                removeNestedMapObject(filter, "query");
            }
            // -filter
            Map<String, Object> filerXfilter = parseMap(filter);
            List<Map<String, Object>> filterBoolMust = (List<Map<String, Object>>) getNestedMapObject(filerXfilter,
                    new String[] { "bool", "must" }, 0);
            if (filterBoolMust != null) {
                queryBoolFilterBoolMust.add(filerXfilter);
            }
            List<Map<String, Object>> filterBoolShould = (List<Map<String, Object>>) getNestedMapObject(filerXfilter,
                    new String[] { "bool", "should" }, 0);
            if (filterBoolShould != null) {
                queryBoolFilterBoolShould.add(filerXfilter);
            }

            // for version 7
            if (!queryBoolFilterBoolShould.isEmpty() && !queryBoolFilterBoolMust.isEmpty()) {
                queryBoolFilterBool.put("minimum_should_match", 1);
            }
        }
        /////
        removeNestedMapObject(newMap, "ignore_unmapped");
        removeNestedMapObject(newMap, "_cache");
        if (queryBoolMust.isEmpty()) {
            queryBool.remove("must");
        }
        if (queryBoolMustnot.isEmpty()) {
            queryBool.remove("must_not");
        }
        if (queryBoolShould.isEmpty()) {
            queryBool.remove("should");
        }
        if (queryBoolFilterBoolMust.isEmpty()) {
            queryBoolFilterBool.remove("must");
        }
        if (queryBoolFilterBoolMustnot.isEmpty()) {
            queryBoolFilterBool.remove("must_not");
        }
        if (queryBoolFilterBoolShould.isEmpty()) {
            queryBoolFilterBool.remove("should");
        }

        // for version 7
        if (!queryBoolShould.isEmpty() && (!queryBoolMust.isEmpty() || !queryBoolFilter.isEmpty())) {
            queryBool.put("minimum_should_match", 1);
        }
        String jsonstr = toJSON(newMap, true);
        // Convert end
        if (log.isDebugEnabled()) {
            log.debug("\n--- After ---\n" + jsonstr);
        }

        return jsonstr;
    }

    private static Map<String, Object> parseMap(Map<String, Object> map) {
        if (map.get("and") != null) {
            Object value = map.get("and");
            Map<String, Object> and = new HashMap<String, Object>();
            Map<String, Object> bool = new HashMap<String, Object>();
            and.put("bool", bool);
            List<Map<String, Object>> boolMust = new ArrayList<Map<String, Object>>();
            bool.put("must", boolMust);
            parseAndOrNotMap(boolMust, value);
            return and;
        }
        if (map.get("or") != null) {
            Object value = map.get("or");
            Map<String, Object> or = new HashMap<String, Object>();
            Map<String, Object> bool = new HashMap<String, Object>();
            or.put("bool", bool);
            List<Map<String, Object>> boolShould = new ArrayList<Map<String, Object>>();
            bool.put("should", boolShould);
            parseAndOrNotMap(boolShould, value);
            return or;
        }
        if (map.get("not") != null) {
            Object value = map.get("not");
            Map<String, Object> not = new HashMap<String, Object>();
            Map<String, Object> bool = new HashMap<String, Object>();
            not.put("bool", bool);
            List<Map<String, Object>> boolMustnot = new ArrayList<Map<String, Object>>();
            bool.put("must_not", boolMustnot);
            parseAndOrNotMap(boolMustnot, value);
            return not;
        }
        if (map.get("missing") != null) {
            Map<String, Object> missing = new HashMap<String, Object>();
            Map<String, Object> bool = new HashMap<String, Object>();
            missing.put("bool", bool);
            List<Map<String, Object>> boolMustnot = new ArrayList<Map<String, Object>>();
            bool.put("must_not", boolMustnot);
            Map<String, Object> exists = new HashMap<String, Object>();
            boolMustnot.add(exists);
            exists.put("exists", map.get("missing"));
            return missing;
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    private static void parseAndOrNotMap(List<Map<String, Object>> listmap, Object value) {
        if (value instanceof List) {
            List<Map<String, Object>> lmap = parseListMap((List<Map<String, Object>>) value);
            if (!lmap.isEmpty()) {
                listmap.addAll(lmap);
            }
        } else if (value instanceof Map) {
            Map<String, Object> nestedMap = (Map<String, Object>) value;
            if (nestedMap.get("filters") != null) {
                Object filtersValue = nestedMap.get("filters");
                if (filtersValue instanceof List) {
                    List<Map<String, Object>> lmap = parseListMap((List<Map<String, Object>>) filtersValue);
                    if (!lmap.isEmpty()) {
                        listmap.addAll(lmap);
                    }
                } else if (filtersValue instanceof Map) {
                    Map<String, Object> map = parseMap((Map<String, Object>) filtersValue);
                    if (!map.isEmpty()) {
                        listmap.add(map);
                    }
                }
            } else if (nestedMap.get("filter") != null) {
                Object filterValue = nestedMap.get("filter");
                if (filterValue instanceof List) {
                    List<Map<String, Object>> lmap = parseListMap((List<Map<String, Object>>) filterValue);
                    if (!lmap.isEmpty()) {
                        listmap.addAll(lmap);
                    }
                } else if (filterValue instanceof Map) {
                    Map<String, Object> map = parseMap((Map<String, Object>) filterValue);
                    if (!map.isEmpty()) {
                        listmap.add(map);
                    }
                }
            } else {
                Map<String, Object> map = parseMap((Map<String, Object>) value);
                if (!map.isEmpty()) {
                    listmap.add(map);
                }
            }
        }
    }

    private static List<Map<String, Object>> parseListMap(List<Map<String, Object>> listmap) {
        List<Map<String, Object>> rlistmap = new ArrayList<Map<String, Object>>();
        for (Map<String, Object> emap : listmap) {
            if (emap instanceof Map) {
                Map<String, Object> rmap = parseMap(emap);
                if (!rmap.isEmpty()) {
                    rlistmap.add(rmap);
                }
            }
        }
        return rlistmap;
    }

    @SuppressWarnings("unchecked")
    private static void parseQueryMap(List<Map<String, Object>> listQueryMap, Map<String, Object> map) {
        for (Entry<String, Object> entry : map.entrySet()) {
            if (entry.getKey().equals("query")) {
                Map<String, Object> queryMap = (Map<String, Object>) entry.getValue();
                String type = (String) getNestedMapObject(queryMap, "type");
                if (type != null) {
                    Map<String, Object> match = (Map<String, Object>) getNestedMapObject(queryMap,
                            new String[] { "match" }, 0);
                    removeNestedMapObject(match, "type");
                    removeNestedMapObject(match, "operator");
                    removeNestedMapObject(queryMap, "match");
                    queryMap.put("match_" + type, match);
                }
                listQueryMap.add(queryMap);
                return;
            }
            if (entry.getValue() instanceof List) {
                for (Object cmap : (List<Map<String, Object>>) entry.getValue()) {
                    if (cmap instanceof Map) {
                        parseQueryMap(listQueryMap, (Map<String, Object>) cmap);
                    }
                }
            } else if (entry.getValue() instanceof Map) {
                parseQueryMap(listQueryMap, (Map<String, Object>) entry.getValue());
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void removeNestedMapObject(Map<String, Object> map, String key) {
        Map<String, Object> queryClone = new HashMap<String, Object>(map);
        for (Entry<String, Object> entry : queryClone.entrySet()) {
            if (entry.getKey().equals(key)) {
                map.remove(entry.getKey());
                continue;
            }
            if (entry.getValue() instanceof List) {
                for (Object cmap : (List<Map<String, Object>>) entry.getValue()) {
                    if (cmap instanceof Map) {
                        removeNestedMapObject((Map<String, Object>) cmap, key);
                    }
                }
            } else if (entry.getValue() instanceof Map) {
                removeNestedMapObject((Map<String, Object>) entry.getValue(), key);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static Object getNestedMapObject(Map<String, Object> map, String key) {
        Map<String, Object> queryClone = new HashMap<String, Object>(map);
        for (Entry<String, Object> entry : queryClone.entrySet()) {
            if (entry.getKey().equals(key)) {
                return entry.getValue();
            }
            if (entry.getValue() instanceof List) {
                for (Object cmap : (List<Map<String, Object>>) entry.getValue()) {
                    if (cmap instanceof Map) {
                        Object cobj = getNestedMapObject((Map<String, Object>) cmap, key);
                        if (cobj != null) {
                            return cobj;
                        }
                    }
                }
            } else if (entry.getValue() instanceof Map) {
                Object cobj = getNestedMapObject((Map<String, Object>) entry.getValue(), key);
                if (cobj != null) {
                    return cobj;
                }
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static Object getNestedMapObject(Map<String, Object> map, String[] keys, int index) {
        Object obj = map.get(keys[index]);
        if (obj != null) {
            if (index == keys.length - 1) {
                return obj;
            }
            if (obj instanceof Map) {
                ++index;
                Object cobj = getNestedMapObject((Map<String, Object>) obj, keys, index);
                if (cobj != null) {
                    return cobj;
                }
            } else if (obj instanceof List) {
                ++index;
                for (Map<String, Object> cmap : (List<Map<String, Object>>) obj) {
                    Object cobj = getNestedMapObject(cmap, keys, index);
                    if (cobj != null) {
                        return cobj;
                    }
                }
            }
        }
        return null;
    }

    private static String toJSON(Map<String, Object> map, boolean shaping) {
        String json = "{}";
        try {
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            if (shaping) {
                mapper.setSerializationInclusion(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL);
            }
            mapper.enable(com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT);
            json = mapper.writeValueAsString(map);
        } catch (JsonProcessingException ex) {
            throw new EsClientException("Map To JSON Error.", ex);
        }
        return json;
    }

    /**
     * deep clone.
     * @param direction direction
     * @param map map
     * @param type type
     * @return deep clone map
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> deepClone(int direction, Map<String, Object> map, String type) {
        try {
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            String json = mapper.writeValueAsString(map);
            json = replaceSource(direction, json, type);
            Map<String, Object> newMap = mapper.readValue(json, Map.class);
            return newMap;
        } catch (IOException ex) {
            throw new EsClientException("InternalEsClient.deepClone Error.", ex);
        }
    }

    /**
     * replace source.
     * @param direction direction
     * @param json json
     * @param type type
     * @return replace source string
     */
    public static String replaceSource(int direction, String json, String type) {
        if (direction > 0) {
            switch (direction) {
            case 1:
                json = json.replaceAll("\"_type\":", "\"type\":");
                json = json.replaceAll("\"_all\":", "\"alldata\":");
                break;
            case 2:
                break;
            default:
                break;
            }
        }
        return json;
    }
}
