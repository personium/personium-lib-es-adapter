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

import org.junit.Test;

/**
 * Unit Test for EsClient.
 */
public class EsClientTest extends EsTestBase {

    /**
     * Test that indicesStatus can get list of indices created in preparation.
     */
    @Test
    public void indicesStatus_returns_PersoniumIndicesStatusReponseImpl() {
        var response = esClient.indicesStatus();

        // created indices in EsTestBase.
        assertTrue(response.getIndices().contains("index_for_test_ad.cell"));
        assertTrue(response.getIndices().contains("index_for_test_ad.domain"));
    }
}
