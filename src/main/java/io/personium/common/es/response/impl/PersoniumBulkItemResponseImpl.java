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

import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import io.personium.common.es.response.PersoniumBulkItemResponse;

/**
 * Wrapper class of BulkItemResponse.
 */
public class PersoniumBulkItemResponseImpl extends ElasticsearchResponseWrapper<BulkResponseItem>
        implements PersoniumBulkItemResponse {

    /**
     * Constructor with BulkResponseItem object.
     * @param response BulkResponseItem object.
     */
    public PersoniumBulkItemResponseImpl(BulkResponseItem response) {
        super(response);
    }

    /**
     * Instantiate PersoniumBulkItemResponse from BulkResponseItem object.
     * @param response BulkResponseItem object.
     * @return Created instance.
     */
    public static PersoniumBulkItemResponse getInstance(BulkResponseItem response) {
        if (response == null) {
            return null;
        }
        return new PersoniumBulkItemResponseImpl(response);
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
    public boolean isFailed() {
        return !(this.getResponse().error() == null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long version() {
        return this.getResponse().version();
    }
}
