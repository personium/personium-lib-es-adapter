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

import org.elasticsearch.action.ActionResponse;

import io.personium.common.es.response.PersoniumPutMappingResponse;


/**
 * IndexResponseのラッパークラス.
 */
public class PersoniumPutMappingResponseImpl extends PersoniumActionResponseImpl
        implements PersoniumPutMappingResponse {

    /**
     * .
     */
    private PersoniumPutMappingResponseImpl() {
        super(null);
        throw new IllegalStateException();
    }

    /**
     * GetResponseを指定してインスタンスを生成する.
     * @param response ESからのレスポンスオブジェクト
     */
    private PersoniumPutMappingResponseImpl(ActionResponse response) {
        super(response);
    }

    /**
     * .
     * @param response .
     * @return .
     */
    public static PersoniumPutMappingResponse getInstance(ActionResponse response) {
        if (response == null) {
            return null;
        }
        return new PersoniumPutMappingResponseImpl(response);
    }
}
