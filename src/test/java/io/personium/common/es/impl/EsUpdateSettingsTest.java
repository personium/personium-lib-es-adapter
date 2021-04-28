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

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
        TransportClient client = null;
        try {
            client = createTransportClient();

            assertEquals("0", getNumberOfReplicas(client, "index.number_of_replicas"));

            index.updateSettings(index.getName(), settings);
            assertEquals("3", getNumberOfReplicas(client, "index.number_of_replicas"));
        } finally {
            client.close();
        }
    }

    /**
     * 存在しないkeyを指定した場合EsClientExceptionがスローされること.
     */
    @Test(expected = EsClientException.class)
    public void 存在しないkeyを指定した場合EsClientExceptionがスローされること() {
        Map<String, String> settings = new HashMap<String, String>();
        settings.put("invalid_key", "0");
        TransportClient client = null;
        try {
            client = createTransportClient();

            index.updateSettings(index.getName(), settings);

        } finally {
            client.close();
        }
    }

    /**
     * 無効な値を指定した場合にEsClientExceptionがスローされること.
     */
    @Test(expected = EsClientException.class)
    public void 無効な値を指定した場合にEsClientExceptionがスローされること() {
        Map<String, String> settings = new HashMap<String, String>();
        settings.put("index.number_of_replicas", "invalid_value");
        TransportClient client = null;
        try {
            client = createTransportClient();

            index.updateSettings(index.getName(), settings);

        } finally {
            client.close();
        }
    }

    private TransportClient createTransportClient() {
        Settings sts = Settings.builder()
                .put("path.home", ".")
                .put("cluster.name", TESTING_CLUSTER).build();
        TransportClient client = new PreBuiltTransportClient(sts);
        String[] h = TESTING_HOSTS.split(":");
        client.addTransportAddress(new TransportAddress(new InetSocketAddress(h[0], Integer.valueOf(h[1]))));
        return client;
    }

    private String getNumberOfReplicas(TransportClient client, String key) {
        ClusterStateRequestBuilder request = client.admin().cluster().prepareState();
        ClusterStateResponse response = request.setIndices(index.getName() + ".*").execute().actionGet();
        MetaData metadata = response.getState().getMetaData();
        String firstIndexKey = metadata.getIndices().iterator().next().key;

        Settings retrievedSettings = metadata.index(firstIndexKey).getSettings();
        String numberOfReplicas = retrievedSettings.get(key);
        return numberOfReplicas;
    }
}
