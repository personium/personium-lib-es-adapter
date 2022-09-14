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

import java.util.HashMap;
import java.util.Map;

/**
 * Class for get mapping for admin.
 */
public class EsMappingAdmin extends EsMappingFromResources {

    /**
     * {@inheritDoc}
     */
    @Override
    Map<String, String> getMapTypeResPath() {
        // admin category
        var adminMap = new HashMap<String, String>();
        adminMap.put("Domain", "es/mapping/domain.json");
        adminMap.put("Cell", "es/mapping/cell.json");
        return adminMap;
    }
}
