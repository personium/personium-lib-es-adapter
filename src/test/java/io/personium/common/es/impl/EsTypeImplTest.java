/**
 * Personium
 * Copyright 2022 Personium Project Authors
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

import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.personium.common.es.response.EsClientException.EsIndexMissingException;
import io.personium.common.es.response.impl.PersoniumNullSearchResponse;

/**
 * Unit Test for EsTypeImpl.
 */
public class EsTypeImplTest extends EsTestBase {

    private static final String TYPENAME_FOR_TEST = "type_for_test";

    @Before
    public void prepareType() {
        // prepare empty index for type
    }

    @Test
    public void getMapping_returns_collect_mappings() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        var type = esClient.type(index.getName(), "cell", "TestRoutingId");

        var response = type.getMapping();
        var mapping = response.getSourceAsMap();
        System.out.println(mapper.writeValueAsString(mapping));
    }

    @Test(expected = EsIndexMissingException.class)
    public void create_throws_EsIndexMissingException_if_index_does_not_exist() throws Exception {
        var type = esClient.type(index.getName(), "type_not_exists", "TestRoutingId");
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> doc = mapper.readValue("{\"Name\": \"DummyData\"}",
                new TypeReference<Map<String, Object>>() {
                });
        type.create(doc);
    }

    @Test
    @Ignore
    public void search_returns_PersoniumNullSearchResponse_if_query_does_not_match() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        var type = esClient.type(index.getName(), TYPENAME_FOR_TEST, "TestRoutingId");

        type.create(mapper.readValue("{\"Name\": \"DummyData\"}", new TypeReference<Map<String, Object>>() {
        }));

        String queryJson = """
                {"query": {
                    "filtered": {
                        "query": { "match_all": {} },
                        "filter": {
                            "bool": {
                                "must": [{ "exists ": { "field": "_id" }}]
                            }
                        }
                    }
                }}
                """;

        Map<String, Object> neverMatchQuery = mapper.readValue(queryJson, new TypeReference<Map<String, Object>>() {
        });
        var response = type.search(neverMatchQuery);
        assertTrue(response instanceof PersoniumNullSearchResponse);
    }

    @Test
    public void search_returns_PersoniumNullSearchResponse_if_index_not_found() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        var type = esClient.type(index.getName(), "hogehoge", "TestRoutingId");

        String queryJson = """
                {"query": { "filtered": {
                    "query": { "match_all": {}},
                    "filter": { "bool": {
                        "must": [{ "exists": {"field": "_id"}}]
                    }}
                }}}
                """;
        Map<String, Object> neverMatchQuery = mapper.readValue(queryJson, new TypeReference<Map<String, Object>>() {
        });
        var response = type.search(neverMatchQuery);
        assertTrue(response instanceof PersoniumNullSearchResponse);
    }
}
