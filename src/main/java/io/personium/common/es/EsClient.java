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
package io.personium.common.es;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import io.personium.common.es.impl.EsIndexImpl;
import io.personium.common.es.impl.EsMappingAdmin;
import io.personium.common.es.impl.EsMappingUser;
import io.personium.common.es.impl.EsTypeImpl;
import io.personium.common.es.impl.InternalEsClient;
import io.personium.common.es.response.EsClientException;
import io.personium.common.es.response.PersoniumIndicesStatusResponse;
import io.personium.common.es.response.PersoniumSearchResponse;
import io.personium.common.es.response.impl.PersoniumIndicesStatusResponseImpl;
import io.personium.common.es.response.impl.PersoniumScrollResponse;
import io.personium.common.es.response.impl.PersoniumSearchResponseImpl;
import io.personium.common.es.util.IndexNameEncoder;


/**
 * ElasticSearchのアクセサクラス.
 */
public class EsClient {
    private InternalEsClient internalClient;
    private EsMappingConfig esMappingAdmin;
    private EsMappingConfig esMappingUser;

    /**
     * Default constructor.
     * @param hosts target elasticsearch hosts
     */
    public EsClient(String hosts) {
        this(hosts, new EsMappingAdmin(), new EsMappingUser());
    }

    /**
     * Constructor with specified mappingConfig.
     * @param hosts target elasticsearch hosts
     * @param mappingAdmin .
     * @param mappingUser .
     */
    public EsClient(String hosts, EsMappingConfig mappingAdmin, EsMappingConfig mappingUser) {
        internalClient = InternalEsClient.getInstance(hosts);
        esMappingAdmin = mappingAdmin;
        esMappingUser = mappingUser;
    }

    /**
     * ESとのコネクションを一度明示的に閉じる.
     */
    public void closeConnection() {
        internalClient.closeConnection();
    }

    /**
     * EsClientEventの種類.
     */
    public enum Event {
        /** ESへの接続. */
        connected,
        /** ESへのリクエスト後. */
        afterRequest,
        /** インデックスを作成. */
        creatingIndex,
        /** ESへの登録リクエスト後(Bodyなしでの出力用). */
        afterCreateNonBody,
        /** ESへの登録リクエスト後. */
        afterCreate
    }

    /**
     * Eventハンドラ.
     */
    public interface EventHandler {
        /**
         * Eventハンドラ.
         * @param logInfo ログ出力情報
         * @param params パラメタ
         */
        void handleEvent(EsRequestLogInfo logInfo, Object... params);
    }

    static Map<Event, EventHandler> eventHandlerMap = new HashMap<Event, EventHandler>();

    /**
     * Eventハンドラの登録.
     * @param ev イベントの種類
     * @param handler ハンドラ
     */
    public static void setEventHandler(Event ev, EventHandler handler) {
        InternalEsClient.setEventHandler(ev, handler);
    }

    /**
     * 管理用のIndex操作オブジェクトを返します.
     * @param prefix インデックス名プレフィックス
     * @param times リトライ回数
     * @param interval リトライ間隔
     * @return Indexオブジェクト
     */
    public EsIndex idxAdmin(String prefix, int times, int interval) {
        return new EsIndexImpl(prefix + "_ad",
            EsIndex.CATEGORY_AD, times, interval, internalClient, this.esMappingAdmin);
    }

    /**
     * 管理用のIndex操作オブジェクトを返します.
     * @param prefix インデックス名プレフィックス
     * @return Indexオブジェクト
     */
    public EsIndex idxAdmin(String prefix) {
        return idxAdmin(prefix, 0, 0);
    }

    /**
     * UnitUser用のIndex操作オブジェクトを返します.
     * @param prefix インデックス名プレフィックス
     * @param userUri UnitUser名（URL)
     * @param times リトライ回数
     * @param interval リトライ間隔
     * @return Indexオブジェクト
     */
    public EsIndex idxUser(String prefix, String userUri, int times, int interval) {
        String userUriToSet = userUri;
        if (userUriToSet == null) {
            // エンコードの必要なし
            userUriToSet = "anon";
        } else {
            userUriToSet = IndexNameEncoder.encodeEsIndexName(userUriToSet);
        }
        return idxUser(prefix + "_" + userUriToSet, times, interval);
    }

    /**
     * UnitUser用のIndex操作オブジェクトを返します.
     * @param prefix インデックス名プレフィックス
     * @param userUri UnitUser名（URL)
     * @return Indexオブジェクト
     */
    public EsIndex idxUser(String prefix, String userUri) {
        return idxUser(prefix, userUri, 0, 0);
    }

    /**
     * UnitUser用のIndex操作オブジェクトを返します.
     * @param fullIndexName プレフィックスを含んだインデックス名
     * @param times リトライ回数
     * @param interval リトライ間隔
     * @return Indexオブジェクト
     */
    public EsIndex idxUser(String fullIndexName, int times, int interval) {
        // Engine専用。使用禁止
        return new EsIndexImpl(fullIndexName,
            EsIndex.CATEGORY_USR, times, interval, internalClient, this.esMappingUser);
    }

    /**
     * タイプ名とルーティングIDを指定してタイプ操作用オブジェクトを取得する.
     * @param indexName インデックス名
     * @param typeName タイプ名
     * @param routingId ルーティングID
     * @param times リトライ回数
     * @param interval リトライ間隔
     * @return タイプ操作用オブジェクト
     */
    public EsType type(String indexName, String typeName, String routingId, int times, int interval) {
        return new EsTypeImpl(indexName, typeName, routingId, times, interval, internalClient);
    }

    /**
     * タイプ名とルーティングIDを指定してタイプ操作用オブジェクトを取得する.
     * @param indexName インデックス名
     * @param typeName タイプ名
     * @param routingId ルーティングID
     * @return タイプ操作用オブジェクト
     */
    public EsType type(String indexName, String typeName, String routingId) {
        return type(indexName, typeName, routingId, 0, 0);
    }

    /**
     * Clusterの状態取得.
     * @return 状態Map
     */
    public Map<String, Object> checkHealth() {
        try {
            return internalClient.checkHealth();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * インデックスステータスを取得する.
     * @return インデックスステータス
     */
    public PersoniumIndicesStatusResponse indicesStatus() {
        try {
            return PersoniumIndicesStatusResponseImpl.getInstance(internalClient.syncGetIndicesStatus());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * クエリを指定してスクロールサーチを実行する.
     * @param index インデックス名
     * @param type タイプ名
     * @param query 検索クエリ
     * @return 検索結果
     */
    public PersoniumSearchResponse scrollSearch(String index, String type, Map<String, Object> query) {
        try {
            return PersoniumSearchResponseImpl.getInstance(internalClient.scrollSearch(index, type, query));
        } catch (ElasticsearchException e) {
            throw EsClientException.convertException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * スクロールIDを指定してスクロールサーチを継続する.
     * @param scrollId スクロールID
     * @return 検索結果
     */
    public PersoniumSearchResponse scrollSearch(String scrollId) {
        try {
            return PersoniumScrollResponse.getInstance(internalClient.scrollSearch(scrollId));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
