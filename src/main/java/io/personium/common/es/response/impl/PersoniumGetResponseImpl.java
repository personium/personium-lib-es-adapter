/**
 * Personium
 * Copyright 2014-2020 Personium Project Authors
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

import java.util.Map;

import org.elasticsearch.action.get.GetResponse;

import io.personium.common.es.impl.InternalEsClient;
import io.personium.common.es.response.PersoniumGetResponse;

/**
 * GetResponseのラッパークラス.
 */
public class PersoniumGetResponseImpl extends PersoniumActionResponseImpl implements PersoniumGetResponse {
    private GetResponse getResponse;

    /**
     * .
     */
    private PersoniumGetResponseImpl() {
        super(null);
        throw new IllegalStateException();
    }

    /**
     * GetResponseを指定してインスタンスを生成する.
     * @param response ESからのレスポンスオブジェクト
     */
    private PersoniumGetResponseImpl(GetResponse response) {
        super(response);
        this.getResponse = response;
    }

    /**
     * .
     * @param response .
     * @return .
     */
    public static PersoniumGetResponse getInstance(GetResponse response) {
        if (response == null) {
            return null;
        }
        return new PersoniumGetResponseImpl(response);
    }

    /* (non-Javadoc)
     * @see io.personium.common.es.response.impl.DcGetResponse#getId()
     */
    @Override
    public String getId() {
        return this.getResponse.getId();
    }

    /* (non-Javadoc)
     * @see io.personium.common.es.response.impl.DcGetResponse#id()
     */
    @Override
    public String id() {
        return this.getResponse.getId();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getIndex() {
        return getResponse.getIndex();
    }

    /* (non-Javadoc)
     * @see io.personium.common.es.response.impl.DcGetResponse#getType()
     */
    @Override
    public String getType() {
        return (String) this.getResponse.getSourceAsMap().get("type");
    }

    /* (non-Javadoc)
     * @see io.personium.common.es.response.impl.DcGetResponse#exists()
     */
    @Override
    public boolean exists() {
        return this.getResponse.isExists();
    }

    /* (non-Javadoc)
     * @see io.personium.common.es.response.impl.DcGetResponse#isExists()
     */
    @Override
    public boolean isExists() {
        return this.getResponse.isExists();
    }

    /* (non-Javadoc)
     * @see io.personium.common.es.response.impl.DcGetResponse#version()
     */
    @Override
    public long version() {
        return this.getResponse.getVersion();
    }

    /* (non-Javadoc)
     * @see io.personium.common.es.response.impl.DcGetResponse#getVersion()
     */
    @Override
    public long getVersion() {
        return this.getResponse.getVersion();
    }

    /* (non-Javadoc)
     * @see io.personium.common.es.response.impl.DcGetResponse#getSource()
     */
    @Override
    public Map<String, Object> getSource() {
        return InternalEsClient.deepClone(2, this.getResponse.getSource(), getType());
    }

    /* (non-Javadoc)
     * @see io.personium.common.es.response.impl.DcGetResponse#sourceAsString()
     */
    @Override
    public String sourceAsString() {
        return InternalEsClient.replaceSource(2, this.getResponse.getSourceAsString(), getType());
    }

    /* (non-Javadoc)
     * @see io.personium.common.es.response.impl.DcGetResponse#sourceAsMap()
     */
    @Override
    public Map<String, Object> sourceAsMap() {
        return InternalEsClient.deepClone(2, this.getResponse.getSourceAsMap(), getType());
    }
}
