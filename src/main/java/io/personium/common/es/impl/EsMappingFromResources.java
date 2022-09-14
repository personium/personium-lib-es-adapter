/**
 * Personium
 * Copyright 2022 Personium Project Authors
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
package io.personium.common.es.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.personium.common.es.EsMappingConfig;

/**
 * Abstract class for loading mapping from resources.
 */
public abstract class EsMappingFromResources implements EsMappingConfig {

    private HashMap<String, ObjectNode> mapping;

    /**
     * Getter of map between type and resource path.
     * @return map between type and resource path
     */
    abstract Map<String, String> getMapTypeResPath();

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, ObjectNode> getMapping() {
        if (mapping == null) {
            loadMappingConfigs();
        }
        return mapping;
    }


    /**
     * Read mapping data from resources.
     */
    synchronized void loadMappingConfigs() {
        if (mapping != null) {
            return;
        }
        mapping = new HashMap<String, ObjectNode>();
        var mappingConfigs = getMapTypeResPath();
        mappingConfigs.entrySet().forEach(entry -> {
            mapping.put(entry.getKey(), readJsonResource(entry.getValue()));
        });
    }

    /**
     * Read JSON resource and return as ObjectNode.
     * @param resPath resource path
     * @return ObjectNode
     */
    private static ObjectNode readJsonResource(final String resPath) {
        ObjectMapper mapper = new ObjectMapper();
        try (var is = EsMappingFromResources.class.getClassLoader().getResourceAsStream(resPath)) {
            return mapper.readTree(is).deepCopy();
        } catch (IOException e) {
            throw new RuntimeException("exception while reading " + resPath, e);
        }
    }
}
