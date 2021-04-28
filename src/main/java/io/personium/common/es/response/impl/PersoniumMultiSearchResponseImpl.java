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

import org.elasticsearch.action.search.MultiSearchResponse;

import io.personium.common.es.response.PersoniumItem;
import io.personium.common.es.response.PersoniumMultiSearchResponse;


/**
 * IndexResponseのラッパークラス.
 */
public class PersoniumMultiSearchResponseImpl extends PersoniumActionResponseImpl implements Iterable<PersoniumItem>,
        PersoniumMultiSearchResponse {
    private MultiSearchResponse multiSearchResponse;

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
    private PersoniumMultiSearchResponseImpl(MultiSearchResponse response) {
        super(response);
        this.multiSearchResponse = response;
    }

    /**
     * .
     * @param response .
     * @return .
     */
    public static PersoniumMultiSearchResponse getInstance(MultiSearchResponse response) {
        if (response == null) {
            return null;
        }
        return new PersoniumMultiSearchResponseImpl(response);
    }

    /* (non-Javadoc)
     * @see io.personium.common.es.response.impl.DcMultiSearchResponse#getResponses()
     */
    @Override
    public PersoniumItem[] getResponses() {
        List<PersoniumItemImpl> list = new ArrayList<PersoniumItemImpl>();
        for (MultiSearchResponse.Item item : this.multiSearchResponse.getResponses()) {
            list.add((PersoniumItemImpl) PersoniumItemImpl.getInstance(item));
        }
        return list.toArray(new PersoniumItemImpl[0]);
    }

    /* (non-Javadoc)
     * @see io.personium.common.es.response.impl.DcMultiSearchResponse#iterator()
     */
    @Override
    public Iterator<PersoniumItem> iterator() {
        List<PersoniumItem> list = new ArrayList<PersoniumItem>();
        for (MultiSearchResponse.Item item : this.multiSearchResponse.getResponses()) {
            list.add(PersoniumItemImpl.getInstance(item));
        }
        return list.iterator();
    }
}
