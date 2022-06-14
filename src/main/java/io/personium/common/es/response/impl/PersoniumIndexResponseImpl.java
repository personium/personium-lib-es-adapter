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

import co.elastic.clients.elasticsearch.core.IndexResponse;
import io.personium.common.es.response.PersoniumIndexResponse;

/**
 * Wrapper of IndexResponse.
 */
public class PersoniumIndexResponseImpl extends ElasticsearchResponseWrapper<IndexResponse>
        implements PersoniumIndexResponse {

    /**
     * constructor.
     * @param response response object.
     */
    private PersoniumIndexResponseImpl(IndexResponse response) {
        super(response);
    }

    /**
     * PersoniumIndexResponse factory method.
     * @param response base IndexResponse
     * @return created PersoniumIndexResponse
     */
    public static PersoniumIndexResponse getInstance(IndexResponse response) {
        if (response == null) {
            return null;
        }
        return new PersoniumIndexResponseImpl(response);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getIndex() {
        return this.getResponse().index();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getType() {
        return this.getResponse().type();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getId() {
        return this.getResponse().id();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long version() {
        return this.getResponse().version();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getVersion() {
        return this.getResponse().version();
    }
}
