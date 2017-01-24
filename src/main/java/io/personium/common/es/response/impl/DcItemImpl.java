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

import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchResponse;

import io.personium.common.es.response.DcItem;
import io.personium.common.es.response.DcSearchHit;
/**
 * .
 */
public class DcItemImpl extends MultiSearchResponse.Item implements DcItem {
    /**
     *  .
     * @param response .
     * @param failureMessage .
     */
    public DcItemImpl(SearchResponse response, Throwable throwable) {
        super(response, throwable);
    }

    /**
     *  .
     * @param source .
     * @return .
     */
    public static DcItem getInstance(MultiSearchResponse.Item source) {
        return new DcItemImpl(source.getResponse(), source.getFailure());
    }

    @Override
    public DcSearchHit[] getSearchHits() {
        return DcSearchHitsImpl.getInstance(getResponse().getHits()).getHits();
    }

}
