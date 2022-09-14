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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

/**
 * Unit test for EsTypeImpl.
 */
public class EsMappingFromResourcesTest {

    /**
     * Test class for EsMappingFromResources.
     */
    class TestEsMappingFromResources extends EsMappingFromResources {
        @Override
        Map<String, String> getMapTypeResPath() {
            var result = new HashMap<String, String>();
            result.put("testtype1", "es/mapping/type1.json");
            result.put("testtype2", "es/mapping/type2.json");
            return result;
        }
    }

    /**
     * Test that EsMappingFromResources can load mapping json from resource.
     */
    @Test
    public void TestThatEsMappingFromResourcesCanLoadCorrectly() {
        var testClass = new EsMappingFromResources() {
            @Override
            Map<String, String> getMapTypeResPath() {
                var result = new HashMap<String, String>();
                result.put("testtype1", "es/mapping/type1.json");
                result.put("testtype2", "es/mapping/type2.json");
                return result;
            }
        };

        testClass.loadMappingConfigs();
        assertNotNull(testClass.getMapping().get("testtype1"));
        assertNotNull(testClass.getMapping().get("testtype2"));
    }

    /**
     * Test that EsMappingFromResources throws exception if json does not exist.
     */
    @Test(expected = RuntimeException.class)
    public void TestThatEsMappingFromResourcesCanLoadThrowsRuntimeException() {
        var testClass = new EsMappingFromResources() {
            @Override
            Map<String, String> getMapTypeResPath() {
                var result = new HashMap<String, String>();
                result.put("testtype2", "es/mapping/not_exist.json");
                return result;
            }
        };

        testClass.loadMappingConfigs();
        fail();
    }
}
