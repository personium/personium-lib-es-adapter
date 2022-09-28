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
import java.util.List;

import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import io.personium.common.es.response.PersoniumBulkItemResponse;
import io.personium.common.es.response.PersoniumBulkResponse;

/**
 * Wrapper class for BulkResponse.
 */
public class PersoniumBulkResponseImpl extends ElasticsearchResponseWrapper<BulkResponse>
        implements PersoniumBulkResponse {

    /**
     * .
     */
    private PersoniumBulkResponseImpl() {
        super(null);
        throw new IllegalStateException();
    }

    /**
     * Constructor with BulkReponse object.
     * @param response BulkResponse object.
     */
    private PersoniumBulkResponseImpl(BulkResponse response) {
        super(response);
    }

    /**
     * Instanciate PersoniumBulkResponse from BulkResponse object.
     * @param response BulkResponse object.
     * @return Created instance.
     */
    public static PersoniumBulkResponse getInstance(BulkResponse response) {
        if (response == null) {
            return null;
        }
        return new PersoniumBulkResponseImpl(response);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PersoniumBulkItemResponse[] items() {
        List<PersoniumBulkItemResponse> list = new ArrayList<PersoniumBulkItemResponse>();
        for (BulkResponseItem response : this.getResponse().items()) {
            list.add(PersoniumBulkItemResponseImpl.getInstance(response));
        }
        return list.toArray(new PersoniumBulkItemResponse[0]);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasFailures() {
        return this.getResponse().errors();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String buildFailureMessage() {
        return String.join(",",
                (String[]) this.getResponse().items().stream().map(item -> item.error().toString()).toArray());
    }
}
