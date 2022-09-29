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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.client.RestClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.GetIndicesSettingsResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import io.personium.common.es.response.EsClientException;

/**
 * ESの設定更新のテストクラス.
 */
public class EsUpdateSettingsTest extends EsTestBase {
    /**
     * 各テスト実行前の初期化処理.
     * @throws Exception 異常が発生した場合の例外
     */
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    /**
     * 各テスト実行後のクリーンアップ処理.
     * @throws Exception 異常が発生した場合の例外
     */
    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Indexの設定が更新できること.
     */
    @Test
    public void Indexの設定が更新できること() {
        Map<String, String> settings = new HashMap<String, String>();
        settings.put("index.number_of_replicas", "3");

        String indexName = this.getIndex().getName() + "." + TYPE_FOR_TEST_1;
        try {
            var gires = getIndexSettings(indexName);
            assertEquals("0", gires.get(indexName).settings().index().numberOfReplicas());

            this.getIndex().updateSettings(settings);
            gires = getIndexSettings(indexName);
            assertEquals("3", gires.get(indexName).settings().index().numberOfReplicas());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 存在しないkeyを指定した場合EsClientExceptionがスローされること.
     */
    @Test(expected = EsClientException.class)
    public void 存在しないkeyを指定した場合EsClientExceptionがスローされること() {
        Map<String, String> settings = new HashMap<String, String>();
        settings.put("invalid_key", "0");
        this.getIndex().updateSettings(settings);
    }

    /**
     * 無効な値を指定した場合にEsClientExceptionがスローされること.
     */
    @Test(expected = EsClientException.class)
    public void 無効な値を指定した場合にEsClientExceptionがスローされること() {
        Map<String, String> settings = new HashMap<String, String>();
        settings.put("index.number_of_replicas", "invalid_value");
        this.getIndex().updateSettings(settings);
    }

    public GetIndicesSettingsResponse getIndexSettings(String indexName) throws IOException {
        try (var restClient = RestClient.builder(getTestTargetHttpHosts()).build();
                var transport = new RestClientTransport(restClient, new JacksonJsonpMapper())) {
            var esClient = new ElasticsearchClient(transport);
            var result = esClient.indices().getSettings(gis -> gis.index(indexName));
            return result;
        }
    }

}
