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

import java.util.List;
import java.util.Map;

import io.personium.common.es.response.PersoniumBulkResponse;
import io.personium.common.es.response.PersoniumMultiSearchResponse;
import io.personium.common.es.response.PersoniumSearchResponse;

/**
 * Elasticsearch Index操作用のI/F.
 */
public interface EsIndex {

    /**
     * cell登録／検索用のルーティングキーワード.
     */
    String CELL_ROUTING_KEY_NAME = "pcsCell";
    /**
     * 管理Indexを示すIndexカテゴリ.
     */
    String CATEGORY_AD = "ad";
    /**
     * Unit User Indexを示すIndexカテゴリ.
     */
    String CATEGORY_USR = "usr";

    /**
     * Getter of name.
     * @return name
     */
    String getName();

    /**
     * Getter of category.
     * @return category
     */
    String getCategory();

    /**
     * Create Index and child indices.
     */
    void create();

    /**
     * Delete Index and child indices.
     */
    void delete();

    /**
     * ドキュメントを検索.
     * @param routingId routingId
     * @param query クエリ情報
     * @return ES応答
     */
    PersoniumSearchResponse search(String routingId, Map<String, Object> query);

    /**
     * ドキュメントをマルチ検索.
     * @param routingId routingId
     * @param queryList クエリ情報一覧
     * @return ES応答
     */
    PersoniumMultiSearchResponse multiSearch(String routingId, List<Map<String, Object>> queryList);

    /**
     * Delete with query specification.
     * @param routingId routingId
     * @param query query
     */
    void deleteByQuery(String routingId, Map<String, Object> query);

    /**
     * バルクでドキュメントを登録/更新/削除する.
     * @param routingId routingId
     * @param datas バルクドキュメント
     * @param isWriteLog リクエスト情報のログ出力有無
     * @return ES応答
     */
    PersoniumBulkResponse bulkRequest(String routingId, List<EsBulkRequest> datas, boolean isWriteLog);

    /**
     * インデックスの設定を更新する.
     * @param index インデックス名
     * @param settings 更新するインデックス設定
     * @return Void
     */
    @Deprecated
    Void updateSettings(String index, Map<String, String> settings);

    /**
     * update index settings.
     * @param settings index settings to be put
     */
    void updateSettings(Map<String, String> settings);
}
