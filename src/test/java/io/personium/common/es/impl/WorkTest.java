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
package io.personium.common.es.impl;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkTest {
    static Logger log = LoggerFactory.getLogger(WorkTest.class);

    @Ignore
    @Test
    @SuppressWarnings("unchecked")
    public void marageMappingMain() throws IOException {
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        mapper.setSerializationInclusion(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL);
        mapper.setSerializationInclusion(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY);
        mapper.enable(com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT);

        String[][] mappingInfos = {
                {"Domain", "es/mapping/domain.json"},
                {"Cell", "es/mapping/cell.json"},
                //            {"link", "es/mapping/link.json"},
                //            {"Account", "es/mapping/account.json"},
                //            {"Box", "es/mapping/box.json"},
                //            {"Role", "es/mapping/role.json"},
                //            {"Relation", "es/mapping/relation.json"},
                //            {"SentMessage", "es/mapping/sentMessage.json"},
                //            {"ReceivedMessage", "es/mapping/receivedMessage.json"},
                //            {"EntityType", "es/mapping/entityType.json"},
                //            {"AssociationEnd", "es/mapping/associationEnd.json"},
                //            {"Property", "es/mapping/property.json"},
                //            {"ComplexType", "es/mapping/complexType.json"},
                //            {"ComplexTypeProperty", "es/mapping/complexTypeProperty.json"},
                //            {"ExtCell", "es/mapping/extCell.json"},
                //            {"ExtRole", "es/mapping/extRole.json"},
                //            {"dav", "es/mapping/dav.json"},
                //            {"UserData", "es/mapping/userdata.json"},
                //            {"Rule", "es/mapping/rule.json"},
                //            {"_default_", "es/mapping/default.json"}
        };
        Map<String, Object> root = new HashMap<String, Object>();
        Map<String, Object> doc = new HashMap<String, Object>();
        root.put("_doc", doc);
        for (String[] mappingInfo : mappingInfos) {
            Map<String, Object> mapping = (Map<String, Object>) mapper
                    .readValue(EsIndexImpl.class.getClassLoader().getResourceAsStream(mappingInfo[1]), Map.class);
            marge(doc, (Map<String, Object>) mapping.get(mappingInfo[0]));
        }
        remove(root);
        mapper.writeValue(new File("/var/temp/_ad.json"), root);
    }

    @SuppressWarnings("unchecked")
    void marge(Map<String, Object> to, Map<String, Object> from) {
        for (Entry<String, Object> entry : from.entrySet()) {
            Object entryItem = to.get(entry.getKey());
            if (entryItem == null) {
                to.put(entry.getKey(), entry.getValue());
                continue;
            }
            if (entryItem instanceof Map) {
                marge((Map<String, Object>) entryItem, (Map<String, Object>) entry.getValue());
                continue;
            }
            to.put(entry.getKey(), entry.getValue());
        }
    }

    @SuppressWarnings("unchecked")
    void remove(Map<String, Object> map) {
        Map<String, Object> queryClone = new HashMap<String, Object>(map);
        for (Entry<String, Object> entry : queryClone.entrySet()) {
            if (entry.getKey().equals("index")) {
                if ((Boolean) entry.getValue()) {
                    map.remove(entry.getKey());
                }
                continue;
            }
            if (entry.getValue() instanceof List) {
                for (Object cmap : (List<Map<String, Object>>) entry.getValue()) {
                    if (cmap instanceof Map) {
                        remove((Map<String, Object>) cmap);
                    }
                }
            } else if (entry.getValue() instanceof Map) {
                remove((Map<String, Object>) entry.getValue());
            }
        }
    }
}
