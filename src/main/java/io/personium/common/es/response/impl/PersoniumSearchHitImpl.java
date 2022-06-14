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
import java.util.Iterator;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;

import co.elastic.clients.elasticsearch.core.search.Hit;
import io.personium.common.es.response.PersoniumSearchHit;
import io.personium.common.es.response.PersoniumSearchHitField;

/**
 * Wrapper class of Hit.
 */
public class PersoniumSearchHitImpl implements PersoniumSearchHit {
    private Hit<ObjectNode> hit;

    private String type;

    /**
     * Constructor with Hit object.
     * @param hits Hit object.
     */
    private PersoniumSearchHitImpl(Hit<ObjectNode> hit) {
        this.hit = hit;
        this.type = (String) this.hit.source().get("type").asText();
       }

    /**
     * Instanciate PersoniumSearchHit from Hit object.
     * @param hit original Hit object.
     * @return Created instance.
     */
    public static PersoniumSearchHit getInstance(Hit<ObjectNode> hit) {
        if (hit == null) {
            return null;
        }
        return new PersoniumSearchHitImpl(hit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public float score() {
        return getScore();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public float getScore() {
        return this.hit.score().floatValue();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String index() {
        return getIndex();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getIndex() {
        return this.hit.index();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String id() {
        return getId();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getId() {
        return this.hit.id();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String type() {
        return getType();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getType() {
        return this.type;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long version() {
        return getVersion();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getVersion() {
        return this.hit.version();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] source() {
        try {
            return this.hit.source().binaryValue();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isSourceEmpty() {
        return this.hit.source() != null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> getSource() {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> objMap = mapper.convertValue(this.hit.source(), new TypeReference<Map<String, Object>>() {
        });
        return objMap;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String sourceAsString() {
        return getSourceAsString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSourceAsString() {
        return this.hit.source().asText();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> sourceAsMap() {
        return getSource();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object field(String fieldName) {
        JsonNode node = this.hit.source().get(fieldName);
        JsonNodeType nodeType = node.getNodeType();

        if (nodeType == null) {
            return null;
        } else if (nodeType == JsonNodeType.STRING) {
            return node.asText();
        } else if (nodeType == JsonNodeType.NUMBER) {
            return node.asLong();
        } else {
            return node.asText();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, PersoniumSearchHitField> fields() {
        return this.getFields();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, PersoniumSearchHitField> getFields() {
        throw new RuntimeException("Not implemented PersoniumSearchHitImpl#getFields");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object[] sortValues() {
        return getSortValues();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object[] getSortValues() {
        return this.hit.sort().toArray();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String[] matchedFilters() {
        return getMatchedFilters();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String[] getMatchedFilters() {
        return (String[]) this.hit.matchedQueries().toArray();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<PersoniumSearchHitField> iterator() {
        return fields().values().iterator();
    }
}
