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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.SearchHit;

import io.personium.common.es.response.PersoniumSearchHit;
import io.personium.common.es.response.PersoniumSearchHitField;

/**
 * IndexResponseのラッパークラス.
 */
public class PersoniumSearchHitImpl implements PersoniumSearchHit {
    private SearchHit searchHit;

    private String type;
    private Map<String, Object> source;

    /**
     * .
     */
    private PersoniumSearchHitImpl() {
        throw new IllegalStateException();
    }

    /**
     * GetResponseを指定してインスタンスを生成する.
     * @param hits ESからのレスポンスオブジェクト
     */
    private PersoniumSearchHitImpl(SearchHit hit) {
        this.searchHit = hit;
        this.type = (String) this.searchHit.getSourceAsMap().get("type");
        this.source = this.searchHit.getSourceAsMap();
    }

    /**
     * .
     * @param hit .
     * @return .
     */
    public static PersoniumSearchHit getInstance(SearchHit hit) {
        if (hit == null) {
            return null;
        }
        return new PersoniumSearchHitImpl(hit);
    }

    @Override
    public float score() {
        return getScore();
    }

    @Override
    public float getScore() {
        return this.searchHit.getScore();
    }

    @Override
    public String index() {
        return getIndex();
    }

    @Override
    public String getIndex() {
        return this.searchHit.getIndex();
    }

    @Override
    public String id() {
        return getId();
    }

    @Override
    public String getId() {
        return this.searchHit.getId();
    }

    @Override
    public String type() {
        return getType();
    }

    @Override
    public String getType() {
        return this.type;
    }

    @Override
    public long version() {
        return getVersion();
    }

    @Override
    public long getVersion() {
        return this.searchHit.getVersion();
    }

    @Override
    public byte[] source() {
        return this.searchHit.getSourceRef().toBytesRef().bytes;
    }

    @Override
    public boolean isSourceEmpty() {
        return this.searchHit.hasSource();
    }

    @Override
    public Map<String, Object> getSource() {
        return this.source;
    }

    @Override
    public String sourceAsString() {
        return getSourceAsString();
    }

    @Override
    public String getSourceAsString() {
        return this.searchHit.getSourceAsString();
    }

    @Override
    public Map<String, Object> sourceAsMap() {
        return getSource();
    }

    @Override
    public Object field(String fieldName) {
        return this.searchHit.field(fieldName).getValue();
    }

    @Override
    public Map<String, PersoniumSearchHitField> fields() {
        return fields();
    }

    @Override
    public Map<String, PersoniumSearchHitField> getFields() {
        Map<String, PersoniumSearchHitField> map = new HashMap<String, PersoniumSearchHitField>();
        for (Map.Entry<String, DocumentField> entry : this.searchHit.getFields().entrySet()) {
            map.put(entry.getKey(), PersoniumSearchHitFieldImpl.getInstance(entry.getValue()));
        }
        return map;
    }

    @Override
    public Object[] sortValues() {
        return getSortValues();
    }

    @Override
    public Object[] getSortValues() {
        return this.searchHit.getSortValues();
    }

    @Override
    public String[] matchedFilters() {
        return getMatchedFilters();
    }

    @Override
    public String[] getMatchedFilters() {
        return this.searchHit.getMatchedQueries();
    }

    @Override
    public Iterator<PersoniumSearchHitField> iterator() {
        return fields().values().iterator();
    }
}
