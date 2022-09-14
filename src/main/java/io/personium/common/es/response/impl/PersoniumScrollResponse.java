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

import com.fasterxml.jackson.databind.node.ObjectNode;

import co.elastic.clients.elasticsearch.core.ScrollResponse;
import io.personium.common.es.response.PersoniumSearchHits;
import io.personium.common.es.response.PersoniumSearchResponse;

/**
 * Wrapper class of ScrollResponse.
 */
public class PersoniumScrollResponse extends ElasticsearchResponseWrapper<ScrollResponse<ObjectNode>>
        implements PersoniumSearchResponse {

    /**
     * Constructor with ScrollResponse.
     * @param response ScrollResponse object.
     */
    private PersoniumScrollResponse(ScrollResponse<ObjectNode> response) {
        super(response);
    }

    /**
     * Create instance from elasticsearch response.
     * @param response SeachResponse
     * @return PersoniumSearchResponse
     */
    public static PersoniumSearchResponse getInstance(ScrollResponse<ObjectNode> response) {
        if (response == null) {
            return null;
        }
        return new PersoniumScrollResponse(response);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PersoniumSearchHits getHits() {
        return PersoniumSearchHitsImpl.getInstance(this.getResponse().hits());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PersoniumSearchHits hits() {
        return this.getHits();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isNullResponse() {
        // TODO Check implementation
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getScrollId() {
        return this.getResponse().scrollId();
    }
}
