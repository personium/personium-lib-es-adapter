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
package io.personium.common.es.response.impl;

import io.personium.common.es.response.PersoniumActionResponse;

/**
 * Abstract Wrapper class for Elasticsearch responses.
 * @param <T> original response type from elasticsarch.
 */
public abstract class ElasticsearchResponseWrapper<T> implements PersoniumActionResponse {

    /**
     * Wrapped response object.
     */
    private T response;

    /**
     * Getter of response.
     * @return response
     */
    protected T getResponse() {
        return this.response;
    }

    /**
     * Constructor.
     * @param response response to be wrapped.
     */
    protected ElasticsearchResponseWrapper(T response) {
        this.response = response;
    }

    /**
     * {@inheritDoc}
     */
    public boolean isNull() {
        return response == null;
    }
}
