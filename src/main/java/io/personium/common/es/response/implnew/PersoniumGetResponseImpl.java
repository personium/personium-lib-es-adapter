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

import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import co.elastic.clients.elasticsearch.core.GetResponse;
import io.personium.common.es.impl.InternalEsClient;
import io.personium.common.es.response.PersoniumGetResponse;

/**
 * GetResponseのラッパークラス.
 */
public class PersoniumGetResponseImpl implements PersoniumGetResponse {
    private GetResponse<ObjectNode> getResponse;

    /**
     * .
     */
    private PersoniumGetResponseImpl() {
        throw new IllegalStateException();
    }

    /**
     * GetResponseを指定してインスタンスを生成する.
     * @param response ESからのレスポンスオブジェクト
     */
    private PersoniumGetResponseImpl(GetResponse<ObjectNode> response) {
        this.getResponse = response;
    }

    /**
     * .
     * @param response .
     * @return .
     */
    public static PersoniumGetResponse getInstance(GetResponse<ObjectNode> response) {
        if (response == null || !response.found()) {
            return null;
        }
        return new PersoniumGetResponseImpl(response);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getId() {
        return this.getResponse.id();
    }

    /**
     * {@inheritDoc}
     */

    @Override
    public String id() {
        return this.getResponse.id();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getIndex() {
        return getResponse.index();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getType() {
        return (String) this.getResponse.type();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean exists() {
        return this.getResponse.found();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isExists() {
        return this.getResponse.found();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long version() {
        return this.getResponse.version();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getVersion() {
        return this.getResponse.version();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> getSource() {
        return this.sourceAsMap();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String sourceAsString() {
        return InternalEsClient.replaceSource(2, this.getResponse.source().asText(), getType());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> sourceAsMap() {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> objMap = mapper.convertValue(this.getResponse.source(),
                new TypeReference<Map<String, Object>>() {
                });
        return InternalEsClient.deepClone(2, objMap, getType());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isNull() {
        return this.getResponse == null;
    }
}
