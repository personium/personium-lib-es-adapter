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

import java.util.Iterator;

import io.personium.common.es.response.PersoniumSearchHit;
import io.personium.common.es.response.PersoniumSearchHits;
import io.personium.common.es.response.PersoniumSearchResponse;

//TODO check this is necessary.
/**
 * インデックスが存在しない時に０件の検索結果をダミーで返すSearchResponse.
 * indexにカスタムのMapping定義が必ず存在することを保証するため、
 * ESのIndex自動生成をOFFにして運用する一方で、
 * 本アプリで、存在しないIndex指定があったときは自動生成する枠組みを提供するようにしたかった。
 * しかし、検索・取得系でIndexMissingExceptionが発生した直後にIndexを作成する処理を書いても、なぜか
 * ElasticSearchがエラーとなって、動作しなかったため、やむを得ずこれをあきらめた。
 * そのため、０件である旨をしめすResponseをシミュレートして返す。
 * Deprecatedとなっているメソッドは使わないこと。
 */
public class PersoniumNullSearchResponse implements PersoniumSearchResponse {

    class PersoniumNullSearchHits implements PersoniumSearchHits {

        @Override
        public Iterator<PersoniumSearchHit> iterator() {
            return null;
        }

        @Override
        public long allPages() {
            return this.getAllPages();
        }

        @Override
        public long getAllPages() {
            return 0;
        }

        @Override
        public long getCount() {
            return 0;
        }

        @Override
        public float maxScore() {
            return 0;
        }

        @Override
        public float getMaxScore() {
            return 0;
        }

        @Override
        public PersoniumSearchHit[] hits() {
            return this.getHits();
        }

        @Override
        public PersoniumSearchHit getAt(int position) {
            return null;
        }

        @Override
        public PersoniumSearchHit[] getHits() {
            return new PersoniumSearchHit[0];
        }

    }

    /**
     * コンストラクタ.
     */
    public PersoniumNullSearchResponse() {
        super();
    }

    @Override
    public boolean isNull() {
        return true;
    }

    @Override
    public PersoniumSearchHits getHits() {
        return new PersoniumNullSearchHits();
    }

    @Override
    public PersoniumSearchHits hits() {
        return getHits();
    }

    @Override
    public boolean isNullResponse() {
        return true;
    }

    @Override
    public String getScrollId() {
        return null;
    }
}
