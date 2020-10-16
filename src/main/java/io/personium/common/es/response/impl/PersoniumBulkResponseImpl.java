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

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;

import io.personium.common.es.response.PersoniumBulkItemResponse;
import io.personium.common.es.response.PersoniumBulkResponse;

/**
 * IndexResponseのラッパークラス.
 */
public class PersoniumBulkResponseImpl extends PersoniumActionResponseImpl implements PersoniumBulkResponse {
    private BulkResponse bulkResponse;

    /**
     * .
     */
    private PersoniumBulkResponseImpl() {
        super(null);
        throw new IllegalStateException();
    }

    /**
     * GetResponseを指定してインスタンスを生成する.
     * @param response ESからのレスポンスオブジェクト
     */
    private PersoniumBulkResponseImpl(BulkResponse response) {
        super(response);
        this.bulkResponse = response;
    }

    /**
     * .
     * @param response .
     * @return .
     */
    public static PersoniumBulkResponse getInstance(BulkResponse response) {
        if (response == null) {
            return null;
        }
        return new PersoniumBulkResponseImpl(response);
    }

    /* (non-Javadoc)
     * @see io.personium.common.es.response.impl.DcBulkResponse#items()
     */
    @Override
    public PersoniumBulkItemResponse[] items() {
        List<PersoniumBulkItemResponse> list = new ArrayList<PersoniumBulkItemResponse>();
        for (BulkItemResponse response : this.bulkResponse.getItems()) {
            list.add(PersoniumBulkItemResponseImpl.getInstance(response));
        }
        return list.toArray(new PersoniumBulkItemResponse[0]);
    }

    @Override
    public boolean hasFailures() {
        return this.bulkResponse.hasFailures();
    }

    @Override
    public String buildFailureMessage() {
        return this.bulkResponse.buildFailureMessage();
    }
}
