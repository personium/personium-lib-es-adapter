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
package io.personium.common.es.response.impl;

import co.elastic.clients.elasticsearch.indices.PutMappingResponse;
import io.personium.common.es.response.PersoniumPutMappingResponse;

/**
 * Wrapper class of PutMappingResponse.
 */
public class PersoniumPutMappingResponseImpl extends ElasticsearchResponseWrapper<PutMappingResponse>
        implements PersoniumPutMappingResponse {

    /**
     * Constructor.
     * @param response PutMappingResponse
     */
    private PersoniumPutMappingResponseImpl(PutMappingResponse response) {
        super(response);
    }

    /**
     * Factory method of PersoniumPutMappingResponse.
     * @param response PutMappingResponse
     * @return PersoniumPutMappingResponse instance.
     */
    public static PersoniumPutMappingResponse getInstance(PutMappingResponse response) {
        if (response == null) {
            return null;
        }
        return new PersoniumPutMappingResponseImpl(response);
    }
}
