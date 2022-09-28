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

import co.elastic.clients.elasticsearch.indices.RefreshResponse;
import io.personium.common.es.response.PersoniumRefreshResponse;

/**
 * Wrapper class of RefreshResponse.
 */
public class PersoniumRefreshResponseImpl extends ElasticsearchResponseWrapper<RefreshResponse>
        implements PersoniumRefreshResponse {

    /**
     * Constructor with RefreshResponse.
     * @param response RefreshResponse object.
     */
    private PersoniumRefreshResponseImpl(RefreshResponse response) {
        super(response);
    }

    /**
     * Instanciate PersoniumRefreshResponse from RefreshResponse.
     * @param response RefreshResponse object.
     * @return Created instance.
     */
    public static PersoniumRefreshResponse getInstance(RefreshResponse response) {
        if (response == null) {
            return null;
        }
        return new PersoniumRefreshResponseImpl(response);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getSuccessfulShards() {
        return this.getResponse().shards().successful().intValue();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getFailedShards() {
        return this.getResponse().shards().failed().intValue();
    }

}
