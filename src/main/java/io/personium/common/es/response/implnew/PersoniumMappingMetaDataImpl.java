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
package io.personium.common.es.response.implnew;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

import co.elastic.clients.elasticsearch.indices.get_mapping.IndexMappingRecord;
import io.personium.common.es.response.PersoniumMappingMetaData;

/**
 * IndexResponseのラッパークラス.
 */
public class PersoniumMappingMetaDataImpl implements PersoniumMappingMetaData {
    private IndexMappingRecord mappingMetaData;

    /**
     * .
     */
    private PersoniumMappingMetaDataImpl() {
        throw new IllegalStateException();
    }

    /**
     * GetResponseを指定してインスタンスを生成する.
     * @param response ESからのレスポンスオブジェクト
     */
    private PersoniumMappingMetaDataImpl(IndexMappingRecord meta) {
        this.mappingMetaData = meta;
    }

    /**
     * .
     * @param meta .
     * @return .
     */
    public static PersoniumMappingMetaData getInstance(IndexMappingRecord meta) {
        if (meta == null) {
            return null;
        }
        return new PersoniumMappingMetaDataImpl(meta);
    }

    /*
     * (non-Javadoc)
     * @see io.personium.common.es.response.impl.DcMappingMetaData#getSourceAsMap()
     */
    @Override
    public Map<String, Object> getSourceAsMap() throws IOException {
        Map<String, Object> result = this.mappingMetaData.mappings().properties().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> (Object) e.getValue()));
        return result;
    }
}
