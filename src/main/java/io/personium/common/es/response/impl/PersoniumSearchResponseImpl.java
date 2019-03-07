/**
 * personium.io
 * Copyright 2014 FUJITSU LIMITED
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

import org.elasticsearch.action.search.SearchResponse;

import io.personium.common.es.response.PersoniumSearchHits;
import io.personium.common.es.response.PersoniumSearchResponse;

/**
 * IndexResponseのラッパークラス.
 */
public class PersoniumSearchResponseImpl extends PersoniumActionResponseImpl implements PersoniumSearchResponse {
    private SearchResponse searchResponse;

    /**
     * .
     */
    private PersoniumSearchResponseImpl() {
        super(null);
        throw new IllegalStateException();
    }

    /**
     * GetResponseを指定してインスタンスを生成する.
     * @param response ESからのレスポンスオブジェクト
     */
    private PersoniumSearchResponseImpl(SearchResponse response) {
        super(response);
        this.searchResponse = response;
    }

    /**
     * .
     * @return .
     */
    public static PersoniumSearchResponse getInstance() {
        return new PersoniumSearchResponseImpl(new SearchResponse());
    }

    /**
     * .
     * @param response .
     * @return .
     */
    public static PersoniumSearchResponse getInstance(SearchResponse response) {
        if (response == null) {
            return null;
        }
        return new PersoniumSearchResponseImpl(response);
    }

    /*
     * (non-Javadoc)
     * @see io.personium.common.es.response.impl.DcSearchResponse#getHits()
     */
    @Override
    public PersoniumSearchHits getHits() {
        // TODO use factory class
        return PersoniumSearchHitsImpl.getInstance(this.searchResponse.getHits());
    }

    /*
     * (non-Javadoc)
     * @see io.personium.common.es.response.impl.DcSearchResponse#hits()
     */
    @Override
    public PersoniumSearchHits hits() {
        // TODO use factory class
        return PersoniumSearchHitsImpl.getInstance(this.searchResponse.getHits());
    }

    /*
     * (non-Javadoc)
     * @see io.personium.common.es.response.impl.DcSearchResponse#isNullResponse()
     */
    @Override
    public boolean isNullResponse() {
        return searchResponse instanceof PersoniumNullSearchResponse;
    }

    @Override
    public String getScrollId() {
        return this.searchResponse.getScrollId();
    }
}
