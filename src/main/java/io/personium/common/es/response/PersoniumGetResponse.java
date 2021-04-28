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
package io.personium.common.es.response;

import java.util.Map;

/**
 * DcGetResponseのinterface.
 */
public interface PersoniumGetResponse extends PersoniumActionResponse {

    /**
     * .
     * @return .
     */
    String getId();

    /**
     * .
     * @return .
     */
    String id();

    /**
     * Get index.
     * @return index
     */
    String getIndex();

    /**
     * .
     * @return .
     */
    String getType();

    /**
     * .
     * @return .
     */
    boolean exists();

    /**
     * .
     * @return .
     */
    boolean isExists();

    /**
     * .
     * @return .
     */
    long version();

    /**
     * .
     * @return .
     */
    long getVersion();

    /**
     * .
     * @return .
     */
    Map<String, Object> getSource();

    /**
     * .
     * @return .
     */
    String sourceAsString();

    /**
     * .
     * @return .
     */
    Map<String, Object> sourceAsMap();

}
