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
package io.personium.common.es.query;

import io.personium.common.es.query.impl.PersoniumQueryBuildersImpl;

/**
 * QueryBuilderを生成するWrapperクラス.
 */
public class PersoniumQueryBuilders {

    private PersoniumQueryBuilders() {
    }

    /**
     * MatchQueryBuilderオブジェクトを生成する.
     * @param field field名
     * @param value fieldの値
     * @return ラップしたMatchQueryBuilder
     */
    public static PersoniumQueryBuilder matchQuery(String field, String value) {
        return PersoniumQueryBuildersImpl.matchQuery(field, value);
    }
}
