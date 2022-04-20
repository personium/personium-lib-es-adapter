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
package io.personium.common.es.response.implnew;

import co.elastic.clients.elasticsearch.core.CreateResponse;
import io.personium.common.es.response.PersoniumIndexResponse;

/**
 * CreateResponse wrapper class.
 */
public class PersoniumIndexResponseImpl implements PersoniumIndexResponse {

    //TODO: Rename from PersoniumIndexResponse.
    private CreateResponse createResponse;

    /**
     * .
     */
    private PersoniumIndexResponseImpl() {
        throw new IllegalStateException();
    }

    /**
     * GetResponseを指定してインスタンスを生成する.
     * @param response ESからのレスポンスオブジェクト
     */
    private PersoniumIndexResponseImpl(CreateResponse response) {
        this.createResponse = response;
    }

    /**
     * PersoniumIndexResponse factory method.
     * @param response base CreateResponse
     * @return created PersoniumIndexResponse
     */
    public static PersoniumIndexResponse getInstance(CreateResponse response) {
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
        return this.createResponse.index();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getType() {
        return this.createResponse.type();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getId() {
        return this.createResponse.id();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long version() {
        return this.createResponse.version();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getVersion() {
        return this.createResponse.version();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isNull() {
        return this.createResponse == null;
    }
}
