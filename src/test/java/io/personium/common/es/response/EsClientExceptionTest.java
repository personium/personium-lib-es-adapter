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
package io.personium.common.es.response;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.ErrorCause;
import co.elastic.clients.elasticsearch._types.ErrorResponse;

/**
 * Unit test for EsTypeImpl.
 */
public class EsClientExceptionTest {

    /**
     * Test for EsClientException convert logics.
     */
    @Test
    public void EsClientExceptionConvert() {
        assertTrue(EsClientException.convertException(new ElasticsearchException("example.com",
                new ErrorResponse.Builder()
                        .error(new ErrorCause.Builder().type("search_phase_execution_exception").reason("reason")
                                .build())
                        .status(400) // maybe this is wrong
                        .build())) instanceof EsClientException.PersoniumSearchPhaseExecutionException);
        assertTrue(EsClientException.convertException(new ElasticsearchException("example.com",
                new ErrorResponse.Builder()
                        .error(new ErrorCause.Builder().type("index_not_found_exception").reason("reason").build())
                        .status(400) // maybe this is wrong
                        .build())) instanceof EsClientException.EsIndexMissingException);
    }
}
