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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.personium.common.es.response.EsClientException.EsIndexMissingException;
import io.personium.common.es.response.impl.PersoniumNullSearchResponse;

/**
 * Unit Test for EsTypeImpl.
 */
public class EsTypeImplTest extends EsTestBase {

    private InternalEsClient internalClient;

    @Before
    public void setUpEsTypeImplTest() {
        internalClient = createInternalEsClient();
        // prepare empty index for type
    }

    @After
    public void tearDownEsTypeImplTest() {
        try {
            internalClient.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Test that getMapping function returns mappings.
     * @throws Exception .
     */
    @Test
    @SuppressWarnings("unchecked")
    public void getMapping_returns_collect_mappings() throws Exception {
        var type = new EsTypeImpl(index.getName(), TYPE_FOR_TEST_1, "", 0, 0, internalClient);
        var response = type.getMapping();
        var mapping = response.getSourceAsMap();

        var properties = new HashSet<String>(Arrays.asList("alldata", "type", "c", "s", "l", "u", "p"));
        var actual = ((Map<String, Object>) mapping.get("properties")).keySet();
        assertEquals(properties, actual);
    }

    /**
     * Test that create function throws EsIndexMissingException if index does not exist.
     * @throws Exception .
     */
    @Test(expected = EsIndexMissingException.class)
    public void create_throws_EsIndexMissingException_if_index_does_not_exist() throws Exception {
        String typeName = "type_not_exists";
        var type = new EsTypeImpl(index.getName(), typeName, "", 0, 0, internalClient);

        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> doc = mapper.readValue("{\"Name\": \"DummyData\"}",
                new TypeReference<Map<String, Object>>() {
                });
        type.create(doc);
    }

    /**
     * Test that search function returns null if document does not exist.
     * @throws Exception .
     */
    @Test
    public void get_returns_null_if_document_does_not_exist() throws Exception {
        var type = new EsTypeImpl(index.getName(), TYPE_FOR_TEST_1, "", 0, 0, internalClient);

        var response = type.get("not_existing_id");
        assertNull(response);
    }

    /**
     * Test that search function returns PersoniumNullSearchResponse if index not found.
     * @throws Exception .
     */
    @Test
    public void search_returns_PersoniumNullSearchResponse_if_index_not_found() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        var type = new EsTypeImpl(index.getName(), "index_not_existing", "", 0, 0, internalClient);

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
