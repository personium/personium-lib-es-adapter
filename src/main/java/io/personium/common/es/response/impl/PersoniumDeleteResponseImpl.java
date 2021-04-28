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

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.rest.RestStatus;

import io.personium.common.es.response.PersoniumDeleteResponse;

/**
 * IndexResponseのラッパークラス.
 */
public class PersoniumDeleteResponseImpl extends PersoniumActionResponseImpl implements PersoniumDeleteResponse {
    private DeleteResponse deleteResponse;

    /**
     * .
     */
    private PersoniumDeleteResponseImpl() {
        super(null);
        throw new IllegalStateException();
    }

    /**
     * GetResponseを指定してインスタンスを生成する.
     * @param response ESからのレスポンスオブジェクト
     */
    private PersoniumDeleteResponseImpl(DeleteResponse response) {
        super(response);
        this.deleteResponse = response;
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

    /* (non-Javadoc)
     * @see io.personium.common.es.response.impl.DcDeleteResponse#getId()
     */
    @Override
    public String getId() {
        return this.deleteResponse.getId();
    }

    /* (non-Javadoc)
     * @see io.personium.common.es.response.impl.DcDeleteResponse#version()
     */
    @Override
    public long version() {
        return this.deleteResponse.getVersion();
    }

    /* (non-Javadoc)
     * @see io.personium.common.es.response.impl.DcDeleteResponse#getVersion()
     */
    @Override
    public long getVersion() {
        return this.deleteResponse.getVersion();
    }

    /* (non-Javadoc)
     * @see io.personium.common.es.response.impl.DcDeleteResponse#isNotFound()
     */
    @Override
    public boolean isNotFound() {
        return this.deleteResponse.status().equals(RestStatus.NOT_FOUND);
    }
}
