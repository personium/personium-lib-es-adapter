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
import java.util.Map;

import co.elastic.clients.elasticsearch.indices.RecoveryResponse;
import co.elastic.clients.elasticsearch.indices.recovery.RecoveryStatus;
import io.personium.common.es.response.PersoniumIndicesStatusResponse;

/**
 * IndicesStatusResponseのラッパークラス. index-status classes are deprecated
 * https://discuss.elastic.co/t/deprecated-index-status-classes-in-es-1-2/17761
 */
public class PersoniumIndicesStatusResponseImpl extends ElasticsearchResponseWrapper<RecoveryResponse>
        implements PersoniumIndicesStatusResponse {

    /**
     * Constructor with RecoveryResponse instance.
     * @param response RecoveryResponse object.
     */
    private PersoniumIndicesStatusResponseImpl(RecoveryResponse response) {
        super(response);
    }

    /**
     * Instanciate from RecoveryResponse.
     * @param response RecoveryResponse object.
     * @return Created instance.
     */
    public static PersoniumIndicesStatusResponse getInstance(RecoveryResponse response) {
        if (response == null) {
            return null;
        }
        return new PersoniumIndicesStatusResponseImpl(response);
    }

    /**
     * Get list of indices.
     * @return List of indices.
     */
    @Override
    public List<String> getIndices() {
        Map<String, RecoveryStatus> indexStatus = this.getResponse().result();
        return new ArrayList<String>(indexStatus.keySet());
    }
}
