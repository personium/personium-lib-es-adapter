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
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.databind.node.ObjectNode;

import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import io.personium.common.es.response.PersoniumSearchHit;
import io.personium.common.es.response.PersoniumSearchHits;

/**
 * Wrapper class of HitsMetadata.
 */
public class PersoniumSearchHitsImpl implements PersoniumSearchHits {
    private HitsMetadata<ObjectNode> hitsMetadata;

    /**
     * Create instance from HitsMetadata.
     * @param hits HitsMetadata
     */
    private PersoniumSearchHitsImpl(HitsMetadata<ObjectNode> hits) {
        this.hitsMetadata = hits;
    }

    /**
     * Get instance generated from HitsMetadata.
     * @param hits HitsMetadata
     * @return PersoniumSearchHits
     */
    public static PersoniumSearchHits getInstance(HitsMetadata<ObjectNode> hits) {
        if (hits == null) {
            return null;
        }
        return new PersoniumSearchHitsImpl(hits);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long allPages() {
        return this.getAllPages();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getAllPages() {
        return this.hitsMetadata.total().value();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getCount() {
        return this.hitsMetadata.hits().size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public float maxScore() {
        return this.getMaxScore();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public float getMaxScore() {
        return this.hitsMetadata.maxScore().floatValue();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PersoniumSearchHit[] hits() {
        return getHits();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PersoniumSearchHit[] getHits() {
        return this.hitsMetadata.hits().stream().map(hit -> PersoniumSearchHitImpl.getInstance(hit))
                .toArray(size -> new PersoniumSearchHit[size]);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PersoniumSearchHit getAt(int position) {
        return this.getHits()[0];
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<PersoniumSearchHit> iterator() {
        List<PersoniumSearchHit> list = new ArrayList<PersoniumSearchHit>();
        for (var hit : this.hitsMetadata.hits()) {
            list.add((PersoniumSearchHit) PersoniumSearchHitImpl.getInstance(hit));
        }
        return list.iterator();
    }
}
