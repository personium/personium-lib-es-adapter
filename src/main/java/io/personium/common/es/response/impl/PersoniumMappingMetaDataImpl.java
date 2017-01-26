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

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.cluster.metadata.MappingMetaData;

import io.personium.common.es.response.PersoniumMappingMetaData;

/**
 * IndexResponseのラッパークラス.
 */
public class PersoniumMappingMetaDataImpl implements PersoniumMappingMetaData {
    private MappingMetaData mappingMetaData;

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
    private PersoniumMappingMetaDataImpl(MappingMetaData meta) {
        this.mappingMetaData = meta;
    }

    /**
     * .
     * @param meta .
     * @return .
     */
    public static PersoniumMappingMetaData getInstance(MappingMetaData meta) {
        if (meta == null) {
            return null;
        }
        return new PersoniumMappingMetaDataImpl(meta);
    }

    /* (non-Javadoc)
     * @see io.personium.common.es.response.impl.DcMappingMetaData#getSourceAsMap()
     */
    @Override
    public Map<String, Object> getSourceAsMap() throws IOException {
        return this.mappingMetaData.getSourceAsMap();
    }
}

