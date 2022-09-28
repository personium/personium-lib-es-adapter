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
 * Class for get mapping for user.
 */
public class EsMappingUser extends EsMappingFromResources {

    /**
     * {@inheritDoc}
     */
    @Override
    Map<String, String> getMapTypeResPath() {
        // user category
        var userMap = new HashMap<String, String>();
        userMap.put("link", "es/mapping/link.json");
        userMap.put("Account", "es/mapping/account.json");
        userMap.put("Box", "es/mapping/box.json");
        userMap.put("Role", "es/mapping/role.json");
        userMap.put("Relation", "es/mapping/relation.json");
        userMap.put("SentMessage", "es/mapping/sentMessage.json");
        userMap.put("ReceivedMessage", "es/mapping/receivedMessage.json");
        userMap.put("EntityType", "es/mapping/entityType.json");
        userMap.put("AssociationEnd", "es/mapping/associationEnd.json");
        userMap.put("Property", "es/mapping/property.json");
        userMap.put("ComplexType", "es/mapping/complexType.json");
        userMap.put("ComplexTypeProperty", "es/mapping/complexTypeProperty.json");
        userMap.put("ExtCell", "es/mapping/extCell.json");
        userMap.put("ExtRole", "es/mapping/extRole.json");
        userMap.put("dav", "es/mapping/dav.json");
        userMap.put("UserData", "es/mapping/userdata.json");
        userMap.put("Rule", "es/mapping/rule.json");
        return userMap;
    }
}
