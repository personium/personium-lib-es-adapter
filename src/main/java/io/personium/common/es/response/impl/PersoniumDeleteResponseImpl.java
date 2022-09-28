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

import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import io.personium.common.es.response.PersoniumDeleteResponse;

/**
 * Wrapper of DeleteResponse.
 */
public class PersoniumDeleteResponseImpl extends ElasticsearchResponseWrapper<DeleteResponse>
        implements PersoniumDeleteResponse {

    /**
     * GetResponseを指定してインスタンスを生成する.
     * @param response ESからのレスポンスオブジェクト
     */
    private PersoniumDeleteResponseImpl(DeleteResponse response) {
        super(response);
    }

    /**
     * .
     * @param response .
     * @return .
     */
    public static PersoniumDeleteResponse getInstance(DeleteResponse response) {
        if (response == null) {
            return null;
        }
        return new PersoniumDeleteResponseImpl(response);
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

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isNotFound() {
        return this.getResponse().result().equals(Result.NotFound);
    }
}
