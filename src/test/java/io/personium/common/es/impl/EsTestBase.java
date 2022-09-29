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

import java.util.Arrays;
import java.util.Map;

import org.apache.http.HttpHost;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import io.personium.common.es.EsClient;
import io.personium.common.es.EsIndex;

/**
 * EsModelの単体テストケース.
 */
public class EsTestBase {

    private static final String DEFAULT_ES_TESTING_HOSTS = "localhost:9200";
    private static final String DEFAULT_INDEX_FOR_TEST = "index_for_test";

    /** typename for test. */
    public static final String TYPE_FOR_TEST_1 = "type_for_test1";
    /** typename for test. */
    public static final String TYPE_FOR_TEST_2 = "type_for_test2";

    static String testTargetHosts;
    static int testTargetPort;
    static String testTargetIndex;

    EsClient esClient;
    EsIndex index;

    protected EsIndex getIndex() {
        return index;
    }

    protected HttpHost[] getTestTargetHttpHosts() {
        return Arrays.asList(testTargetHosts.split(",")).stream()
                .map(host -> HttpHost.create(host.replaceAll("\\s+", ""))).toArray(HttpHost[]::new);
    }

    protected InternalEsClient createInternalEsClient() {
        return InternalEsClient.getInstance(testTargetHosts);
    }

    /**
     * Setup target elasticsearch.
     */
    @BeforeClass
    public static void setUpBeforeClass() {
        testTargetHosts = System.getProperty("io.personium.esTestingHosts", DEFAULT_ES_TESTING_HOSTS);
        testTargetIndex = System.getProperty("io.personium.esIndexForTest", DEFAULT_INDEX_FOR_TEST);
    }

    /**
     * テストケース共通のクリーンアップ処理. テスト用のElasticsearchのNodeをクローズする
     * @throws Exception 異常が発生した場合の例外
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    /**
     * 各テスト実行前の初期化処理.
     * @throws Exception 異常が発生した場合の例外
     */
    @Before
    public void setUp() throws Exception {
        var esMappingAdmin = new EsMappingFromResources() {
            @Override
            Map<String, String> getMapTypeResPath() {
                return Map.of(TYPE_FOR_TEST_1, "es/mapping/type1.json", TYPE_FOR_TEST_2, "es/mapping/type2.json");
            }
        };
        var esMappingUser = new EsMappingUser();

        esClient = new EsClient(testTargetHosts, esMappingAdmin, esMappingUser);
        index = esClient.idxAdmin(testTargetIndex);
        index.create();
    }

    /**
     * 各テスト実行後のクリーンアップ処理.
     * @throws Exception 異常が発生した場合の例外
     */
    @After
    public void tearDown() throws Exception {
        try {
            index.delete();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
