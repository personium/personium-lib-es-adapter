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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.databind.node.ObjectNode;

import co.elastic.clients.elasticsearch.core.MsearchResponse;
import io.personium.common.es.response.PersoniumItem;
import io.personium.common.es.response.PersoniumMultiSearchResponse;

/**
 * Wrapper class of MsearchResponse.
 */
public class PersoniumMultiSearchResponseImpl extends ElasticsearchResponseWrapper<MsearchResponse<ObjectNode>>
        implements PersoniumMultiSearchResponse {

    /**
     * .
     */
    private PersoniumMultiSearchResponseImpl() {
        super(null);
        throw new IllegalStateException();
    }

    /**
     * GetResponseを指定してインスタンスを生成する.
     * @param response ESからのレスポンスオブジェクト
     */
    private PersoniumMultiSearchResponseImpl(MsearchResponse<ObjectNode> response) {
        super(response);
    }

    /**
     * .
     * @param response .
     * @return .
     */
    public static PersoniumMultiSearchResponse getInstance(MsearchResponse<ObjectNode> response) {
        if (response == null) {
            return null;
        }
        return new PersoniumMultiSearchResponseImpl(response);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PersoniumItem[] getResponses() {
        return this.getResponse().responses().stream()
            .map(item -> PersoniumItemImpl.getInstance(item))
            .toArray(size -> new PersoniumItemImpl[size]);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<PersoniumItem> iterator() {
        List<PersoniumItem> list = new ArrayList<PersoniumItem>();
        this.getResponse().responses().forEach(item -> {
            list.add(PersoniumItemImpl.getInstance(item));
        });
        return list.iterator();
    }
}
