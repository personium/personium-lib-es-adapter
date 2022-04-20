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
package io.personium.common.es.implnew;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestClient;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.BeforeClass;
import org.junit.Test;

import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import io.personium.common.es.response.EsClientException;

/**
 * EsModelの単体テストケース.
 */
public class EsTest {

    HttpHost targetEsHost;
    String targetIndex;
    String targetTypeName;

    public EsTest() {
        this.targetEsHost = new HttpHost("localhost", 9200);
        targetIndex = "index_for_test";
        targetTypeName = "TypeForTest";
    }

    /**
     * prepare indices for test.
     */
    @BeforeClass
    public static void prepareIndex() {
        var targetEsHost = new HttpHost("localhost", 9200);
        var targetIndex = "index_for_test";
        var targetTypeName = "TypeForTest";

        try (var restClient = RestClient.builder(targetEsHost).build();
                var transport = new RestClientTransport(restClient, new JacksonJsonpMapper())) {
            var esIndex = new PersoniumEsIndexImpl(transport);
            esIndex.createIndex(targetIndex, targetTypeName);
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
    }

    /**
     * ドキュメント登録チェックでドキュメントがすでに存在している場合に正常終了すること.
     * @throws ParseException ParseException
     */
    @Test
    public void ドキュメント登録チェックでドキュメントがすでに存在している場合に正常終了すること() throws ParseException {
        try (var restClient = RestClient.builder(targetEsHost).build();
                var transport = new RestClientTransport(restClient, new JacksonJsonpMapper())) {

            String id = "id00001";

            EsTypeImpl type = new EsTypeImpl(targetIndex, targetTypeName, "TestRoutingId", 5, 500, transport);
            assertNotNull(type);
            JSONObject data = (JSONObject) new JSONParser()
                    .parse("{\"u\":1406596187938,\"t\":\"K0QK5DXWT5qKIPDU2eTdhA\",\"b\":\"IKv5hMRPRDGc68BnIcVx6g\","
                            + "\"s\":{\"P003\":\"secondDynamicPropertyValue\",\"P002\":\"true\",\"P001\":\"false\","
                            + "\"P011\":\"null\",\"P012\":\"123.0\",\"P007\":\"123\",\"P006\":\"false\",\"P005\":null,"
                            + "\"P004\":\"dynamicPropertyValue\",\"P009\":\"123.123\","
                            + "\"P008\":\"true\",\"__id\":\"userdata001:\","
                            + "\"P010\":\"123.123\"},\"c\":\"Q1fp4zrWTm-gSSs7zVCJQg\",\"p\":1406596187938,"
                            + "\"n\":\"vWy9OQj2ScykYize2d7Z5A\",\"l\":[],\"h\":{}}");
            type.create(id, data);
            IndexResponse response = type.checkDocumentCreated(id, data, null);
            assertNotNull(response);
            assertEquals(id, response.getId());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * ドキュメント登録チェックでドキュメントが存在しない場合に例外が発生すること.
     * @throws ParseException ParseException
     */
    @Test(expected = EsClientException.class)
    public void ドキュメント登録チェックでドキュメントが存在しない場合に例外が発生すること() throws ParseException {
        try (var restClient = RestClient.builder(targetEsHost).build();
                var transport = new RestClientTransport(restClient, new JacksonJsonpMapper())) {

            String id = "id00001";
            EsTypeImpl type = new EsTypeImpl(targetIndex, "TypeForTest", "TestRoutingId", 5, 500, transport);
            assertNotNull(type);
            JSONObject data = (JSONObject) new JSONParser()
                    .parse("{\"u\":1406596187938,\"t\":\"K0QK5DXWT5qKIPDU2eTdhA\",\"b\":\"IKv5hMRPRDGc68BnIcVx6g\","
                            + "\"s\":{\"P003\":\"secondDynamicPropertyValue\",\"P002\":\"true\",\"P001\":\"false\","
                            + "\"P011\":\"null\",\"P012\":\"123.0\",\"P007\":\"123\",\"P006\":\"false\",\"P005\":null,"
                            + "\"P004\":\"dynamicPropertyValue\",\"P009\":\"123.123\","
                            + "\"P008\":\"true\",\"__id\":\"userdata001:\","
                            + "\"P010\":\"123.123\"},\"c\":\"Q1fp4zrWTm-gSSs7zVCJQg\",\"p\":1406596187938,"
                            + "\"n\":\"vWy9OQj2ScykYize2d7Z5A\",\"l\":[],\"h\":{}}");
            type.checkDocumentCreated(id, data, null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * ドキュメント登録チェックでElasticsearchに登録されたデータの更新日時が異なる場合に例外が発生すること.
     * @throws ParseException ParseException
     */
    @Test(expected = EsClientException.class)
    public void ドキュメント登録チェックでElasticsearchに登録されたデータの更新日時が異なる場合に例外が発生すること() throws ParseException {
        try (var restClient = RestClient.builder(targetEsHost).build();
                var transport = new RestClientTransport(restClient, new JacksonJsonpMapper())) {

            String id = "id00001";
            EsTypeImpl type = new EsTypeImpl(targetIndex, "TypeForTest", "TestRoutingId", 5, 500, transport);
            assertNotNull(type);
            JSONObject data = (JSONObject) new JSONParser()
                    .parse("{\"u\":1406596187938,\"t\":\"K0QK5DXWT5qKIPDU2eTdhA\",\"b\":\"IKv5hMRPRDGc68BnIcVx6g\","
                            + "\"s\":{\"P003\":\"secondDynamicPropertyValue\",\"P002\":\"true\",\"P001\":\"false\","
                            + "\"P011\":\"null\",\"P012\":\"123.0\",\"P007\":\"123\",\"P006\":\"false\",\"P005\":null,"
                            + "\"P004\":\"dynamicPropertyValue\",\"P009\":\"123.123\","
                            + "\"P008\":\"true\",\"__id\":\"userdata001:\","
                            + "\"P010\":\"123.123\"},\"c\":\"Q1fp4zrWTm-gSSs7zVCJQg\",\"p\":1406596187938,"
                            + "\"n\":\"vWy9OQj2ScykYize2d7Z5A\",\"l\":[],\"h\":{}}");
            type.create(id, data);

            data = (JSONObject) new JSONParser()
                    .parse("{\"u\":123456789,\"t\":\"K0QK5DXWT5qKIPDU2eTdhA\",\"b\":\"IKv5hMRPRDGc68BnIcVx6g\","
                            + "\"s\":{\"P003\":\"secondDynamicPropertyValue\",\"P002\":\"true\",\"P001\":\"false\","
                            + "\"P011\":\"null\",\"P012\":\"123.0\",\"P007\":\"123\",\"P006\":\"false\",\"P005\":null,"
                            + "\"P004\":\"dynamicPropertyValue\",\"P009\":\"123.123\","
                            + "\"P008\":\"true\",\"__id\":\"userdata001:\","
                            + "\"P010\":\"123.123\"},\"c\":\"Q1fp4zrWTm-gSSs7zVCJQg\",\"p\":1406596187938,"
                            + "\"n\":\"vWy9OQj2ScykYize2d7Z5A\",\"l\":[],\"h\":{}}");

            type.checkDocumentCreated(id, data, null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
