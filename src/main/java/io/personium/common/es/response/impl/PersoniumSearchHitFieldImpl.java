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

import java.util.Iterator;
import java.util.List;

import org.elasticsearch.common.document.DocumentField;

import io.personium.common.es.response.PersoniumSearchHitField;

/**
 * IndexResponseのラッパークラス.
 */
public class PersoniumSearchHitFieldImpl implements PersoniumSearchHitField {
    private DocumentField searchHitField;

    /**
     * .
     */
    private PersoniumSearchHitFieldImpl() {
        throw new IllegalStateException();
    }

    /**
     * GetResponseを指定してインスタンスを生成する.
     * @param field ESからのレスポンスオブジェクト
     */
    private PersoniumSearchHitFieldImpl(DocumentField field) {
        this.searchHitField = field;
    }

    /**
     * .
     * @param field .
     * @return .
     */
    public static PersoniumSearchHitField getInstance(DocumentField field) {
        if (field == null) {
            return null;
        }
        return new PersoniumSearchHitFieldImpl(field);
    }

    @Override
    public Iterator<Object> iterator() {
        return this.searchHitField.iterator();
    }

    @Override
    public String name() {
        return this.searchHitField.getName();
    }

    @Override
    public String getName() {
        return this.searchHitField.getName();
    }

    @Override
    public <V> V value() {
        return this.searchHitField.getValue();
    }

    @Override
    public <V> V getValue() {
        return this.searchHitField.getValue();
    }

    @Override
    public List<Object> values() {
        return this.searchHitField.getValues();
    }

    @Override
    public List<Object> getValues() {
        return this.searchHitField.getValues();
    }
}
