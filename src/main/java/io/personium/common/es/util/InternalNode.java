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
package io.personium.common.es.util;

import io.personium.common.es.util.impl.InternalNodeImpl;

/**
 * ElasticSearchのアクセサクラス.
 */
public class InternalNode {
    /**
     * デフォルトコンストラクタ.
     */
    private InternalNode() {
    }

    /**
     * テスト用のElasticsearchノードを起動する.
     */
    public static void startInternalNode() {
        InternalNodeImpl.startInternalNode();
    }

    /**
     * テスト用のElasticsearchノードを停止する.
     */
    public static void stopInternalNode() {
        InternalNodeImpl.stopInternalNode();
    }
}
