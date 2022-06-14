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

import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import io.personium.common.es.response.PersoniumMappingMetaData;

/**
 * Wrapper class of TypeMapping.
 */
public class PersoniumMappingMetaDataImpl extends ElasticsearchResponseWrapper<TypeMapping>
        implements PersoniumMappingMetaData {

    /**
     * Instantiate with mapping.
     * @param mapping TypeMapping object.
     */
    private PersoniumMappingMetaDataImpl(TypeMapping mapping) {
        super(mapping);
    }

    /**
     * Instantiate with mapping.
     * @param mapping TypeMapping
     * @return instance
     */
    public static PersoniumMappingMetaData getInstance(TypeMapping mapping) {
        if (mapping == null) {
            return null;
        }
        return new PersoniumMappingMetaDataImpl(mapping);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> getSourceAsMap() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        // convert from Map<String, Property> to Map<String, Object>
        Map<String, Object> result = mapper.readValue(this.getResponse().toString(), new TypeReference<Map<String, Object>>(){});
        return result;
    }
}
