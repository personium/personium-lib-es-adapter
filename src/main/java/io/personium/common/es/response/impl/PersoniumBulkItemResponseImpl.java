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

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;

import io.personium.common.es.response.PersoniumBulkItemResponse;

/**
 * BulkItemResponseのラッパークラス.
 */
public class PersoniumBulkItemResponseImpl extends BulkItemResponse implements PersoniumBulkItemResponse {

    /**
     * .
     * @param id .
     * @param opType .
     * @param response .
     */
    public PersoniumBulkItemResponseImpl(int id, String opType, DocWriteResponse response) {
        super(id, DocWriteRequest.OpType.fromString(opType), response);
    }

    /**
     *  .
     * @param id .
     * @param opType .
     * @param failure .
     */
    public PersoniumBulkItemResponseImpl(int id, String opType, Failure failure) {
        super(id, DocWriteRequest.OpType.fromString(opType), failure);
    }

    /**
     * .
     * @param response .
     * @return .
     */
    public static PersoniumBulkItemResponse getInstance(BulkItemResponse response) {
        if (response == null) {
            return null;
        }
        return new PersoniumBulkItemResponseImpl(response.getItemId(),
                response.getOpType().getLowercase(), response.getResponse());
    }

    @Override
    public long version() {
        return super.getVersion();
    }
}
