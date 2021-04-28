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

import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.indices.recovery.RecoveryState;

import io.personium.common.es.response.PersoniumIndicesStatusResponse;

/**
 * IndicesStatusResponseのラッパークラス.
 * index-status classes are deprecated
 * https://discuss.elastic.co/t/deprecated-index-status-classes-in-es-1-2/17761
 */
public class PersoniumIndicesStatusResponseImpl implements PersoniumIndicesStatusResponse {
    private RecoveryResponse indicesStatusResponse;

    /**
     * GetResponseを指定してインスタンスを生成する.
     * @param response ESからのレスポンスオブジェクト
     */
    private PersoniumIndicesStatusResponseImpl(RecoveryResponse response) {
        this.indicesStatusResponse = response;
    }

    /**
     * .
     * @param response .
     * @return .
     */
    public static PersoniumIndicesStatusResponse getInstance(RecoveryResponse response) {
        if (response == null) {
            return null;
        }
        return new PersoniumIndicesStatusResponseImpl(response);
    }

    /**
     * Indexの一覧を取得する.
     * @return Indexの一覧
     */
    @Override
    public List<String> getIndices() {
        Map<String, List<RecoveryState>> indexStatus = this.indicesStatusResponse.shardRecoveryStates();
        return new ArrayList<String>(indexStatus.keySet());
    }
}
