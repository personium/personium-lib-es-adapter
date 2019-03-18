/**
 * personium.io
 * Copyright 2014 FUJITSU LIMITED
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.function.Function;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.recovery.RecoveryAction;
import org.elasticsearch.action.admin.indices.recovery.RecoveryRequestBuilder;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.personium.common.es.EsBulkRequest;
import io.personium.common.es.EsClient.Event;
import io.personium.common.es.EsClient.EventHandler;
import io.personium.common.es.EsRequestLogInfo;
import io.personium.common.es.response.EsClientException;
import io.personium.common.es.response.EsClientException.EsMultiSearchQueryParseException;
import io.personium.common.es.response.PersoniumBulkResponse;
import io.personium.common.es.response.PersoniumRefreshResponse;
import io.personium.common.es.response.impl.PersoniumBulkResponseImpl;
import io.personium.common.es.response.impl.PersoniumRefreshResponseImpl;

/**
 * ElasticSearchのアクセサクラス.
 */
public class InternalEsClient {
    static Logger log = LoggerFactory.getLogger(InternalEsClient.class);

    private static final int DEFAULT_ES_PORT = 9300;

    private TransportClient esTransportClient;
    private boolean routingFlag;

    /**
     * デフォルトコンストラクタ.
     */
    protected InternalEsClient() {
    }

    /**
     * コンストラクタ.
     * @param cluster クラスタ名
     * @param hosts ホスト
     */
    protected InternalEsClient(String cluster, String hosts) {
        routingFlag = true;
        prepareClient(cluster, hosts);
    }

    /**
     * クラスタ名、接続先情報を指定してEsClientのインスタンスを返す.
     * 既に生成されているインスタンスは破棄する
     * @param cluster クラスタ名
     * @param hosts 接続先情報
     * @return EsClientのインスタンス
     */
    public static InternalEsClient getInstance(String cluster, String hosts) {
        return new InternalEsClient(cluster, hosts);
    }

    /**
     * ESとのコネクションを一度明示的に閉じる.
     */
    public void closeConnection() {
        if (esTransportClient == null) {
            return;
        }
        esTransportClient.close();
        esTransportClient = null;
    }

    private void prepareClient(String clusterName, String hostNames) {
        if (esTransportClient != null) {
            return;
        }

        if (clusterName == null || hostNames == null) {
            return;
        }

        Settings st = Settings.builder()
                .put("cluster.name", clusterName)
                //.put("client.transport.sniff", true)
                .build();
        List<DiscoveryNode> connectedNodes = null;
        esTransportClient = new PreBuiltTransportClient(st);
        List<EsHost> hostList = parseConfigAndInitializeHostsList(hostNames);
        for (EsHost host : hostList) {

            try {
                esTransportClient.addTransportAddress(
                        new TransportAddress(InetAddress.getByName(host.getName()), host.getPort()));
                connectedNodes = esTransportClient.connectedNodes();
            } catch (UnknownHostException ex) {
                throw new EsClientException("Datastore Connection Error.", ex);
            }
        }
        if (connectedNodes.isEmpty()) {
            throw new EsClientException("Datastore Connection Error.");
        }
        loggingConnectedNode(connectedNodes);
    }

    private List<EsHost> parseConfigAndInitializeHostsList(String hostNames) {
        List<EsHost> hostList = new ArrayList<EsHost>();
        StringTokenizer tokenizer = new StringTokenizer(hostNames, ",");
        while (tokenizer.hasMoreTokens()) {
            String host = tokenizer.nextToken();
            hostList.add(createEsHost(host));
        }
        return hostList;
    }

    private EsHost createEsHost(String host) {
        EsHost hostInfo = null;
        if (hasPortNumber(host)) {
            int index = host.indexOf(":");
            hostInfo = new EsHost(host.substring(0, index), Integer.parseInt(host.substring(index + 1)));
        } else {
            hostInfo = new EsHost(host, DEFAULT_ES_PORT);
        }
        return hostInfo;
    }

    private boolean hasPortNumber(String host) {
        return host.indexOf(":") > 0;
    }

    private void loggingConnectedNode(List<DiscoveryNode> list) {
        DiscoveryNode node = list.get(0);
        this.fireEvent(Event.connected, node.getAddress().toString());
    }

    /**
     * elasticsearchのノード情報（ホスト名、ポート番号）を保持するコンテナクラス.
     */
    private static class EsHost {
        private String name;
        private int port;

        EsHost(String name, int port) {
            this.name = name;
            this.port = port;
        }

        public String getName() {
            return name;
        }

        public int getPort() {
            return port;
        }
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
     * Clusterの状態取得.
     * @return 状態Map
     */
    public Map<String, Object> checkHealth() {
        ClusterHealthResponse clusterHealth;
        clusterHealth = esTransportClient.admin().cluster().health(new ClusterHealthRequest()).actionGet();
        HashMap<String, Object> map = new HashMap<String, Object>();
        map.put("cluster_name", clusterHealth.getClusterName());
        map.put("status", clusterHealth.getStatus().name());
        map.put("timed_out", clusterHealth.isTimedOut());
        map.put("number_of_nodes", clusterHealth.getNumberOfNodes());
        map.put("number_of_data_nodes", clusterHealth.getNumberOfDataNodes());
        map.put("active_primary_shards", clusterHealth.getActivePrimaryShards());
        map.put("active_shards", clusterHealth.getActiveShards());
        map.put("relocating_shards", clusterHealth.getRelocatingShards());
        map.put("initializing_shards", clusterHealth.getInitializingShards());
        map.put("unassigned_shards", clusterHealth.getUnassignedShards());
        return map;
    }

    /**
     * インデックスを作成する.
     * @param index インデックス名
     * @param mappings マッピング情報
     * @return 非同期応答
     */
    public ActionFuture<CreateIndexResponse> createIndex(String index, Map<String, JSONObject> mappings) {
        this.fireEvent(Event.creatingIndex, index);
        CreateIndexRequestBuilder cirb =
                new CreateIndexRequestBuilder(esTransportClient.admin().indices(), CreateIndexAction.INSTANCE, index);

        // index setting parameters
        Settings.Builder indexSettings = Settings.builder();
        //  static
        indexSettings.put("analysis.analyzer.default.type", "cjk");
        indexSettings.put("index.mapping.total_fields.limit", "10000");
        indexSettings.put("index.refresh_interval", "-1");
        //  dynamic
        indexSettings.put("index.number_of_shards", System.getProperty("io.personium.es.index.numberOfShards", "10"));
        indexSettings.put("index.number_of_replicas",
                System.getProperty("io.personium.es.index.numberOfReplicas", "0"));
        indexSettings.put("index.max_result_window",
                System.getProperty("io.personium.es.index.maxResultWindow", "110000"));
        String maxThreadCount = System.getProperty("io.personium.es.index.merge.scheduler.maxThreadCount");
        if (maxThreadCount != null) {
            indexSettings.put("index.merge.scheduler.max_thread_count", maxThreadCount);
        }

        cirb.setSettings(indexSettings);
        if (mappings != null) {
            for (Map.Entry<String, JSONObject> ent : mappings.entrySet()) {
                cirb = cirb.addMapping(ent.getKey(), ent.getValue());
            }
        }
        return cirb.execute();
    }

    /**
     * インデックスを削除する.
     * @param index インデックス名
     * @return 非同期応答
     */
    public ActionFuture<AcknowledgedResponse> deleteIndex(String index) {
        DeleteIndexRequest dir = new DeleteIndexRequest(index);
        return esTransportClient.admin().indices().delete(dir);
    }

    /**
     * インデックスの設定を更新する.
     * @param index インデックス名
     * @param settings 更新するインデックス設定
     * @return Void
     */
    public Void updateIndexSettings(String index, Map<String, String> settings) {

        Function<String, String> keyFunction = new Function<String, String>() {
            public String apply(String t) {
                return t;
            }
        };
        Settings settingsForUpdate = Settings.builder()
                .putProperties(settings, keyFunction)
                .build();
        esTransportClient.admin().indices().prepareUpdateSettings(index)
                .setSettings(settingsForUpdate)
                .execute()
                .actionGet();
        return null;
    }

    /**
     * Mapping定義を取得する.
     * @param index インデックス名
     * @param type タイプ名
     * @return Mapping定義
     */
    public MappingMetaData getMapping(String index, String type) {
        ClusterState cs = esTransportClient.admin().cluster().prepareState().
                setIndices(index).execute().actionGet().getState();
        return cs.getMetaData().index(index).mapping(makeType(type));
    }

    /**
     * Mapping定義を更新する.
     * @param index インデックス名
     * @param type タイプ名
     * @param mappings マッピング情報
     * @return 非同期応答
     */
    public ActionFuture<AcknowledgedResponse> putMapping(String index,
            String type,
            Map<String, Object> mappings) {
        PutMappingRequestBuilder builder = new PutMappingRequestBuilder(esTransportClient.admin().indices(),
                PutMappingAction.INSTANCE)
                        .setIndices(index)
                        .setType(makeType(type))
                        .setSource(mappings);
        return builder.execute();
    }

    /**
     * インデックスステータスを取得する.
     * @return 非同期応答
     */
    public ActionFuture<RecoveryResponse> indicesStatus() {
        RecoveryRequestBuilder cirb =
                new RecoveryRequestBuilder(esTransportClient.admin().indices(), RecoveryAction.INSTANCE);
        return cirb.execute();
    }

    /**
     * 非同期でドキュメントを取得.
     * @param index インデックス名
     * @param type タイプ名
     * @param id ドキュメントのID
     * @param routingId routingId
     * @param realtime リアルタイムモードなら真
     * @return 非同期応答
     */
    public ActionFuture<GetResponse> asyncGet(String index, String type, String id, String routingId,
            boolean realtime) {
        GetRequest req = new GetRequest(index, makeType(type), id);

        if (routingFlag) {
            req = req.routing(routingId);
        }

        req.realtime(realtime);
        ActionFuture<GetResponse> ret = esTransportClient.get(req);
        this.fireEvent(Event.afterRequest, index, type, id, null, "Get");
        return ret;
    }

    /**
     * 非同期でドキュメントを検索.
     * @param index インデックス名
     * @param type タイプ名
     * @param routingId routingId
     * @param builder クエリ情報
     * @return 非同期応答
     */
    public ActionFuture<SearchResponse> asyncSearch(
            String index,
            String type,
            String routingId,
            SearchSourceBuilder builder) {
        // TODO type
        SearchRequest req = new SearchRequest(index).types(makeType(type)).searchType(SearchType.DEFAULT)
                .source(builder);
        if (routingFlag) {
            req = req.routing(routingId);
        }
        ActionFuture<SearchResponse> ret = esTransportClient.search(req);
        this.fireEvent(Event.afterRequest, index, type, null, builder.toString(), "Search");
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
    public ActionFuture<SearchResponse> asyncSearch(
            String index,
            String type,
            String routingId,
            Map<String, Object> query) {
        SearchRequest req = new SearchRequest(index).types(makeType(type)).searchType(SearchType.DEFAULT);
        if (query != null) {
            req.source(makeSearchSourceBuilder(query, type));
        }
        if (routingFlag) {
            req = req.routing(routingId);
        }
        ActionFuture<SearchResponse> ret = esTransportClient.search(req);
        this.fireEvent(Event.afterRequest, index, type, null, JSONObject.toJSONString(query), "Search");
        return ret;
    }

    /**
     * 非同期でドキュメントを検索. <br />
     * Queryの指定方法をMapで直接記述せずにQueryBuilderにするため、非推奨とする.
     * @param index インデックス名
     * @param routingId routingId
     * @param query クエリ情報
     * @return 非同期応答
     */
    public ActionFuture<SearchResponse> asyncSearch(
            String index,
            String routingId,
            Map<String, Object> query) {
        SearchRequest req = new SearchRequest(index).searchType(SearchType.DEFAULT);
        if (query != null) {
            req.source(makeSearchSourceBuilder(query, null));
        }
        if (routingFlag) {
            req = req.routing(routingId);
        }
        ActionFuture<SearchResponse> ret = esTransportClient.search(req);
        this.fireEvent(Event.afterRequest, index, null, null, JSONObject.toJSONString(query), "Search");
        return ret;
    }

    /**
     * 非同期でドキュメントを検索.
     * @param index インデックス名
     * @param routingId routingId
     * @param query クエリ情報
     * @return 非同期応答
     * @throws IOException
     */
    public ActionFuture<SearchResponse> asyncSearch(
            String index,
            String routingId,
            QueryBuilder query) {
        SearchRequest req = new SearchRequest(index).searchType(SearchType.DEFAULT);

        String queryString = "null";
        if (query != null) {
            req.source(new SearchSourceBuilder().query(query));
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            OutputStreamStreamOutput osso = new OutputStreamStreamOutput(baos);
            try {
                query.writeTo(osso);
                queryString = baos.toString("UTF-8");
            } catch (IOException ex) {
                throw new EsClientException("query to string error.", ex);
            }
        }
        if (routingFlag) {
            req = req.routing(routingId);
        }
        ActionFuture<SearchResponse> ret = esTransportClient.search(req);
        this.fireEvent(Event.afterRequest, index, null, null, queryString, "Search");
        return ret;
    }

    /**
     * 非同期でインデックスに対してドキュメントをマルチ検索.
     * 存在しないインデックスに対して本メソッドを使用すると、TransportSerializationExceptionがスローされるので注意すること
     * @param index インデックス名
     * @param routingId routingId
     * @param queryList マルチ検索用のクエリ情報リスト
     * @return 非同期応答
     */
    public ActionFuture<MultiSearchResponse> asyncMultiSearch(
            String index,
            String routingId,
            List<Map<String, Object>> queryList) {
        return this.asyncMultiSearch(index, null, routingId, queryList);
    }

    /**
     * 非同期でドキュメントをマルチ検索.
     * 存在しないインデックスに対して本メソッドを使用すると、TransportSerializationExceptionがスローされるので注意すること
     * @param index インデックス名
     * @param type タイプ名
     * @param routingId routingId
     * @param queryList マルチ検索用のクエリ情報リスト
     * @return 非同期応答
     */
    public ActionFuture<MultiSearchResponse> asyncMultiSearch(
            String index,
            String type,
            String routingId,
            List<Map<String, Object>> queryList) {
        MultiSearchRequest mrequest = new MultiSearchRequest();
        if (queryList == null || queryList.size() == 0) {
            throw new EsMultiSearchQueryParseException();
        }
        for (Map<String, Object> query : queryList) {
            SearchRequest req = new SearchRequest(index).searchType(SearchType.DEFAULT);
            if (type != null) {
                req.types(makeType(type));
            }
            // クエリ指定なしの場合はタイプに対する全件検索を行う
            if (query != null) {
                req.source(makeSearchSourceBuilder(query, type));
            }
            if (routingFlag) {
                req = req.routing(routingId);
            }
            mrequest.add(req);
        }

        ActionFuture<MultiSearchResponse> ret = esTransportClient.multiSearch(mrequest);
        this.fireEvent(Event.afterRequest, index, type, null, JSONArray.toJSONString(queryList), "MultiSearch");
        return ret;
    }

    private static final int SCROLL_SEARCH_KEEP_ALIVE_TIME = 1000 * 60 * 5;

    /**
     * クエリを指定してスクロールサーチを実行する.
     * @param index インデックス名
     * @param type タイプ名
     * @param query 検索クエリ
     * @return 非同期応答
     */
    public ActionFuture<SearchResponse> asyncScrollSearch(String index, String type, Map<String, Object> query) {
        SearchRequest req = new SearchRequest(index)
                .searchType(SearchType.QUERY_THEN_FETCH)
                .scroll(new TimeValue(SCROLL_SEARCH_KEEP_ALIVE_TIME));
        if (type != null) {
            req.types(makeType(type));
        }
        if (query != null) {
            req.source(makeSearchSourceBuilder(query, type));
        }

        ActionFuture<SearchResponse> ret = esTransportClient.search(req);
        return ret;
    }

    /**
     * スクロールIDを指定してスクロールサーチを継続する.
     * @param scrollId スクロールID
     * @return 非同期応答
     */
    public ActionFuture<SearchResponse> asyncScrollSearch(String scrollId) {
        ActionFuture<SearchResponse> ret = esTransportClient.prepareSearchScroll(scrollId)
                .setScroll(new TimeValue(SCROLL_SEARCH_KEEP_ALIVE_TIME))
                .execute();
        return ret;
    }

    /**
     * 非同期でドキュメントを検索.
     * @param index インデックス名
     * @param query クエリ情報
     * @return 非同期応答
     */
    public ActionFuture<SearchResponse> asyncSearch(String index, Map<String, Object> query) {
        SearchRequest req = new SearchRequest(index).searchType(SearchType.DEFAULT);
        if (query != null) {
            req.source(makeSearchSourceBuilder(query, null));
        }
        ActionFuture<SearchResponse> ret = esTransportClient.search(req);
        this.fireEvent(Event.afterRequest, index, null, null, JSONObject.toJSONString(query), "Search");
        return ret;
    }

    /**
     * 非同期でドキュメントを登録する.
     * @param index インデックス名
     * @param type タイプ名
     * @param id ドキュメントのid
     * @param routingId routingId
     * @param data データ
     * @param opType 操作タイプ
     * @param version version番号
     * @return 非同期応答
     */
    public ActionFuture<IndexResponse> asyncIndex(String index,
            String type,
            String id,
            String routingId,
            Map<String, Object> data,
            OpType opType,
            long version) {
        IndexRequestBuilder req = esTransportClient.prepareIndex(index, makeType(type), id)
                .setSource(makeData(data, type))
                .setOpType(opType)
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE);
        if (routingFlag) {
            req = req.setRouting(routingId);
        }
        if (version > -1) {
            req.setVersion(version);
        }

        ActionFuture<IndexResponse> ret = req.execute();
        EsRequestLogInfo logInfo = new EsRequestLogInfo(index, type, id, routingId, data, opType.toString(),
                version);
        this.fireEvent(Event.afterCreate, logInfo);

        return ret;
    }

    /**
     * 非同期でversionつきでdocumentを削除します.
     * @param index インデックス名
     * @param type タイプ名
     * @param id Document id to delete
     * @param routingId routingId
     * @param version The version of the document to delete
     * @return 非同期応答
     */
    public ActionFuture<DeleteResponse> asyncDelete(String index, String type,
            String id, String routingId, long version) {
        DeleteRequestBuilder req = esTransportClient.prepareDelete(index, makeType(type), id)
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE);
        if (routingFlag) {
            req = req.setRouting(routingId);
        }
        if (version > -1) {
            req.setVersion(version);
        }
        ActionFuture<DeleteResponse> ret = req.execute();
        this.fireEvent(Event.afterRequest, index, type, id, null, "Delete");
        return ret;
    }

    /**
     * バルクでドキュメントを登録/更新/削除.
     * @param index インデックス名
     * @param routingId routingId
     * @param datas バルクドキュメント
     * @param isWriteLog リクエスト情報のログ出力有無
     * @return ES応答
     */
    @SuppressWarnings("unchecked")
    public BulkResponse bulkRequest(String index, String routingId, List<EsBulkRequest> datas, boolean isWriteLog) {
        BulkRequestBuilder bulkRequest = esTransportClient.prepareBulk();
        List<Map<String, Object>> bulkList = new ArrayList<Map<String, Object>>();
        for (EsBulkRequest data : datas) {

            if (EsBulkRequest.BulkRequestType.DELETE == data.getRequestType()) {
                bulkRequest.add(createDeleteRequest(index, routingId, data));
            } else {
                bulkRequest.add(createIndexRequest(index, routingId, data));
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

        BulkResponse ret = bulkRequest.setRefreshPolicy(RefreshPolicy.IMMEDIATE).execute().actionGet();
        if (ret.hasFailures()) {
            for (BulkItemResponse item : ret.getItems()) {
                if (item.isFailed()) {
                    log.debug("BulkItemReponse:" + ":" + item.getOpType() + ":" + item.getId() + ":" + item.getIndex()
                            + "/" + item.getType() + "/" + item.getItemId() + ":" + item.getFailureMessage());
                }
            }
        }
        if (isWriteLog) {
            this.fireEvent(Event.afterRequest, index, "none", "none", debug, "bulkRequest");
        }
        return ret;
    }

    /**
     * バルクリクエストのINDEXリクエストを作成する.
     * @param index インデックス名
     * @param routingId ルーティングID
     * @param data バルクドキュメント情報
     * @return 作成したINDEXリクエスト
     */
    private IndexRequestBuilder createIndexRequest(String index, String routingId, EsBulkRequest data) {
        IndexRequestBuilder request = esTransportClient.prepareIndex(index, makeType(data.getType()),
                data.getId()).setSource(makeData(data.getSource(), data.getType()));
        if (routingFlag) {
            request = request.setRouting(routingId);
        }
        return request;
    }

    /**
     * バルクリクエストのDELETEリクエストを作成する.
     * @param index インデックス名
     * @param routingId ルーティングID
     * @param data バルクドキュメント情報
     * @return 作成したDELETEリクエスト
     */
    private DeleteRequestBuilder createDeleteRequest(String index, String routingId, EsBulkRequest data) {
        DeleteRequestBuilder request = esTransportClient.prepareDelete(index, makeType(data.getType()), data.getId());
        if (routingFlag) {
            request = request.setRouting(routingId);
        }
        return request;
    }

    /**
     * ルーティングIDに関係なくバルクでドキュメントを登録.
     * @param index インデックス名
     * @param bulkMap バルクドキュメント
     * @return ES応答
     */
    public PersoniumBulkResponse asyncBulkCreate(
            String index, Map<String, List<EsBulkRequest>> bulkMap) {
        BulkRequestBuilder bulkRequest = esTransportClient.prepareBulk();
        // ルーティングIDごとにバルク登録を行うと効率が悪いため、引数で渡されたEsBulkRequestは全て一括登録する。
        // また、バルク登録後にactionGet()すると同期実行となるため、ここでは実行しない。
        // このため、execute()のレスポンスを返却し、呼び出し側でactionGet()してからレスポンスチェック、リフレッシュすること。
        for (Entry<String, List<EsBulkRequest>> ents : bulkMap.entrySet()) {
            for (EsBulkRequest data : ents.getValue()) {
                IndexRequestBuilder req = esTransportClient.prepareIndex(index, makeType(data.getType()),
                        data.getId()).setSource(makeData(data.getSource(), data.getType()));
                if (routingFlag) {
                    req = req.setRouting(ents.getKey());
                }
                bulkRequest.add(req);
            }
        }
        PersoniumBulkResponse response = PersoniumBulkResponseImpl.getInstance(bulkRequest.execute().actionGet());
        return response;
    }

    /**
     * 引数で指定されたインデックスに対してrefreshする.
     * @param index インデックス名
     * @return レスポンス
     */
    public PersoniumRefreshResponse refresh(String index) {
        RefreshResponse response = esTransportClient.admin().indices()
                .refresh(new RefreshRequest(index)).actionGet();
        return PersoniumRefreshResponseImpl.getInstance(response);
    }

    /**
     * 指定されたクエリを使用してデータの削除を行う.
     * @param index 削除対象のインデックス
     * @param deleteQuery 削除対象を指定するクエリ
     * @return ES応答
     */
    public BulkByScrollResponse deleteByQuery(String index, Map<String, Object> deleteQuery) {
        DeleteByQueryRequestBuilder builder = DeleteByQueryAction.INSTANCE.newRequestBuilder(esTransportClient);
        builder.source(index);
        builder.filter(makeQueryBuilder(deleteQuery, null));
        BulkByScrollResponse response = builder.execute().actionGet();
        refresh(index);
        return response;
    }

    /**
     * flushを行う.
     * @param index flush対象のindex名
     * @return 非同期応答
     */
    public ActionFuture<FlushResponse> flushTransLog(String index) {
        ActionFuture<FlushResponse> ret = esTransportClient.admin().indices().flush(new FlushRequest(index));
        this.fireEvent(Event.afterRequest, index, null, null, null, "Flush");
        return ret;
    }

    /**
     * ES2 -> ES6 非互換吸収のためにメソッド追加。
     */
    private static final String UNIQE_TYPE = "_doc";

    public static String makeType(String type) { // NOPMD - Methods for incompatible absorption
        return UNIQE_TYPE;
    }

    private static Map<String, Object> makeData(Map<String, Object> data, String type) {
        Map<String, Object> newData = deepClone(1, data, type);
        newData.put("type", type);
        return newData;
    }

    private static SearchSourceBuilder makeSearchSourceBuilder(Map<String, Object> map, String type) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(
                new NamedXContentRegistry(searchModule.getNamedXContents()), null, queryMapToJSON(map, type))) {
            searchSourceBuilder.parseXContent(parser);
        } catch (IOException ex) {
            throw new EsClientException("SearchBuilder Make Error.", ex);
        }

        return searchSourceBuilder;
    }

    private static QueryBuilder makeQueryBuilder(Map<String, Object> map, String type) {
        log.debug("#DeleteByQuerye");
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        Map<String, Object> queryMap = null;
        try {
            queryMap = mapper.readValue(queryMapToJSON(map, type), Map.class);
        } catch (IOException ex) {
            throw new EsClientException("QueryBuilder Make Error.", ex);
        }
        Map<String, Object> queryQuery = (Map<String, Object>) getNestedMapObject(queryMap, new String[] {"query"}, 0);
        QueryBuilder queryBuilder = QueryBuilders.wrapperQuery(JSONObject.toJSONString(queryQuery));
        return queryBuilder;
    }

    private static String queryMapToJSON(Map<String, Object> map, String type) { // CHECKSTYLE IGNORE
        if (log.isDebugEnabled()) {
            log.debug("\n--- Before ---\n" + toJSON(map, false));
        }
        // Convert start
        Map<String, Object> cloneMap = deepClone(1, map, type);
        Map<String, Object> newMap = new HashMap<String, Object>();
        //version
        Object version = getNestedMapObject(cloneMap, new String[] {"version"}, 0);
        if (version != null) {
            newMap.put("version", version);
        }
        // size
        Object size = getNestedMapObject(cloneMap, new String[] {"size"}, 0);
        if (size != null) {
            newMap.put("size", size);
        }
        // from
        Object from = getNestedMapObject(cloneMap, new String[] {"from"}, 0);
        if (from != null) {
            newMap.put("from", from);
        }
        // sort
        Object sort = getNestedMapObject(cloneMap, new String[] {"sort"}, 0);
        if (sort != null) {
            newMap.put("sort", sort);
        }
        // _source
        Object source = getNestedMapObject(cloneMap, new String[] {"_source"}, 0);
        if (source != null) {
            if (source instanceof List) {
                ((List) source).add(0, "type");
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
                new String[] {"query", "filtered", "filter", "bool", "must"}, 0);
        List<Map<String, Object>> queryXmustnot = (List<Map<String, Object>>) getNestedMapObject(cloneMap,
                new String[] {"query", "filtered", "filter", "bool", "must_not"}, 0);
        List<Map<String, Object>> queryXshould = (List<Map<String, Object>>) getNestedMapObject(cloneMap,
                new String[] {"query", "filtered", "filter", "bool", "should"}, 0);
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
                    new String[] {"query", "filtered", "filter", "and", "filters"}, 0);
            if (queryXfilters != null) {
                for (Map<String, Object> tmap : queryXfilters) {
                    queryBoolFilterBoolMust.add(tmap);
                }
            } else {
                Map<String, Object> queryXfilter = (Map<String, Object>) getNestedMapObject(cloneMap,
                        new String[] {"query", "filtered", "filter"}, 0);
                if (queryXfilter != null) {
                    queryBoolFilterBoolMust.add(queryXfilter);
                }
            }
        }
        // -query/filterd/query
        Object queryFilteredQuery = getNestedMapObject(cloneMap, new String[] {"query", "filtered", "query"}, 0);
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
                new String[] {"filter", "ids"}, 0);
        if (filterIds != null) {
            Map<String, Object> ids = new HashMap<String, Object>();
            ids.put("ids", filterIds);
            queryBoolFilterBoolMust.add(ids);
        }
        // -filter
        Map<String, Object> filter = (Map<String, Object>) getNestedMapObject(cloneMap, new String[] {"filter"}, 0);
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
                    new String[] {"bool", "must"}, 0);
            if (filterBoolMust != null) {
                queryBoolFilterBoolMust.add(filerXfilter);
            }
            List<Map<String, Object>> filterBoolShould = (List<Map<String, Object>>) getNestedMapObject(
                    filerXfilter, new String[] {"bool", "should"}, 0);
            if (filterBoolShould != null) {
                queryBoolFilterBoolShould.add(filerXfilter);
            }
        }
        /////
        // type
        if (type != null) {
            Map<String, Object> queryBoolFilterBoolMustTerm = new HashMap<String, Object>();
            queryBoolFilterBoolMust.add(0, queryBoolFilterBoolMustTerm);
            Map<String, Object> queryBoolFilterBoolMustTermType = new HashMap<String, Object>();
            queryBoolFilterBoolMustTerm.put("term", queryBoolFilterBoolMustTermType);
            queryBoolFilterBoolMustTermType.put("type", type);
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

    private static void parseQueryMap(List<Map<String, Object>> listQueryMap, Map<String, Object> map) {
        for (Entry<String, Object> entry : map.entrySet()) {
            if (entry.getKey().equals("query")) {
                Map<String, Object> queryMap = (Map<String, Object>) entry.getValue();
                String type = (String) getNestedMapObject(queryMap, "type");
                if (type != null) {
                    Map<String, Object> match = (Map<String, Object>) getNestedMapObject(queryMap,
                            new String[] {"match"}, 0);
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
                //mapper.setSerializationInclusion(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY);
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
                if (type != null) {
                    if (type.equals("EntityType")) {
                        json = json.replaceAll("\"l\":", "\"lo\":");
                    }
                    if (type.equals("UserData")) {
                        json = json.replaceAll("\"h\":", "\"ho\":");
                    }
                }
                json = json.replaceAll("\"_type\":", "\"type\":");
                json = json.replaceAll("\"_all\":", "\"alldata\":");
                break;
            case 2:
                if (type != null) {
                    if (type.equals("EntityType")) {
                        json = json.replaceAll("\"lo\":", "\"l\":");
                    }
                    if (type.equals("UserData")) {
                        json = json.replaceAll("\"ho\":", "\"h\":");
                    }
                }
                break;
            default:
                break;
            }
        }
        return json;
    }
}
