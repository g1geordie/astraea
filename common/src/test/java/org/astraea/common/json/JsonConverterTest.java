/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.common.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

class JsonConverterTest {

  private JsonConverter getConverter(){
    return JsonConverter.jackson();
  }

  @Test
  void testMap() {
    var jsonConverter = getConverter();
    var json = jsonConverter.toJson(Map.of("testKey", "testValue"));
    assertEquals("{\"testKey\":\"testValue\"}", json);

    Map<String, String> list =
        jsonConverter.fromJson(
            "{\"testKey\":\"testValue\"}", new TypeReference<>() {});
    assertEquals(Map.of("testKey", "testValue"), list);
  }

  @Test
  void testList() {
    var jsonConverter = getConverter();
    var json = jsonConverter.toJson(List.of("v1", "v2"));
    assertEquals("[\"v1\",\"v2\"]", json);

    List<String> list =
        jsonConverter.fromJson("[\"v1\",\"v2\"]", new TypeReference<>() {});
    assertEquals(List.of("v1", "v2"), list);
  }

  @Test
  void testSet() {
    var jsonConverter = getConverter();
    var json = jsonConverter.toJson(Set.of("v1", "v2"));
    //    equals ignore order
    assertTrue("[\"v1\",\"v2\"]".equals(json) || "[\"v2\",\"v1\"]".equals(json));

    Set<String> set =
        jsonConverter.fromJson("[\"v1\",\"v2\"]",new TypeReference<>() {});
    var expectedSet = Set.of("v1", "v2");
    //    equals ignore order
    assertTrue(expectedSet.containsAll(set) && set.containsAll(expectedSet));
  }

  @Test
  void testPrimitive() {
    var jsonConverter = getConverter();
    var testFieldClass=new TestPrimitiveClass();
    testFieldClass.doublueValue=456d;
    testFieldClass.intValue=12;
    testFieldClass.stringValue="hello";

    var json=jsonConverter.toJson(testFieldClass);
    assertEquals("{\"doublueValue\":456.0,\"intValue\":12,\"stringValue\":\"hello\"}",json);

    var convertedTestFieldClass=jsonConverter.fromJson("{\"doublueValue\":456.0,\"intValue\":12,\"stringValue\":\"hello\"}",
        TestPrimitiveClass.class);
    assertEquals(456d,convertedTestFieldClass.doublueValue);
    assertEquals(12,convertedTestFieldClass.intValue);
    assertEquals("hello",convertedTestFieldClass.stringValue);

    System.out.println(json);
  }

  @Test
  void testOptional() throws JsonProcessingException {
    var jsonConverter = getConverter();
    var testFieldClass=new TestOptionalClass();
    testFieldClass.optValue=Optional.ofNullable("hello");
    testFieldClass.nestedOpt=Optional.ofNullable(List.of("hello"));

    var objectMapper =new ObjectMapper().registerModule(new Jdk8Module());
    objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    System.out.println(objectMapper.writeValueAsString(testFieldClass));
//
//    var json=jsonConverter.toJson(testFieldClass);
//    assertEquals("{\"nestedOpt\":[\"hello\"],\"optValue\":\"hello\"}",json);
//
//    testFieldClass.optValue=Optional.empty();
//    testFieldClass.nestedOpt=Optional.empty();
//    json=jsonConverter.toJson(testFieldClass);
//    assertEquals("{}",json);

    var convertedTestFieldClass=jsonConverter.fromJson("{\"nestedOpt\":[\"hello\"],\"optValue\":\"hello\"}",
        TestOptionalClass.class);
    assertEquals("hello",convertedTestFieldClass.optValue.get());
    assertEquals(List.of("hello"),convertedTestFieldClass.nestedOpt.get());

     convertedTestFieldClass=jsonConverter.fromJson("{\"optValue\":null}",
        TestOptionalClass.class);
     assertTrue(convertedTestFieldClass.optValue.isEmpty());
    assertTrue(convertedTestFieldClass.nestedOpt.isEmpty());
  }

  @Test
  void testOptional2() throws JsonProcessingException {
    var objectMapper =new ObjectMapper();
    objectMapper.registerModule(new Jdk8Module());
    objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    objectMapper.setVisibility(objectMapper.getSerializationConfig().getDefaultVisibilityChecker()
        .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
        .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
        .withSetterVisibility(JsonAutoDetect.Visibility.NONE)
        .withCreatorVisibility(JsonAutoDetect.Visibility.NONE));

    objectMapper.setSerializationInclusion(Include.NON_ABSENT);
    var testFieldClass=new TestOptionalClass();
    testFieldClass.optValue=Optional.ofNullable("hello");
    testFieldClass.nestedOpt=Optional.ofNullable(List.of("hello"));

    System.out.println(objectMapper.writeValueAsString(testFieldClass));

    var convertedTestFieldClass=objectMapper.readValue("{}",
        TestOptionalClass.class);
    System.out.println(convertedTestFieldClass.nestedOpt.isEmpty());
  }

  @Test
  void testNestedObject() {

  }

  @Test
  void testObject() {
    var jsonConverter = getConverter();
//    var json = jsonConverter.toJson(new TestClass("testString", 45678,Optional.of("test")));
//    System.out.println(json);
//    assertEquals("{\"stringValue\":\"testString\",\"intValue\":45678}", json);

//    var testObject =
//        jsonConverter.fromJson(
//            "{\"stringValue\":\"testString\",\"intValue\":45678}",
//            new TypeToken<TestClass>() {}.getType());
//    assertEquals(new TestClass("testString", 45678), testObject);
//
//    testObject =
//        jsonConverter.fromJson(
//            "{\"stringValue\":\"testString\",\"intValue\":45678}", TestClass.class);
//    assertEquals(new TestClass("testString", 45678), testObject);
  }

  @Test
  void testToObjectIgnoreOrder() {

  }

  @Test
  void testTrim(){
    var jsonConverter = getConverter();
    var testFieldNameClass=new TestFieldNameClass();
    testFieldNameClass.beta=List.of("notMatter");
    testFieldNameClass.banana="notMatter";
    testFieldNameClass.apple="notMatter";
    testFieldNameClass.actor=123;
    testFieldNameClass.dog=new TestPrimitiveClass();
    var json=jsonConverter.toJson(testFieldNameClass);
    assertEquals(json,json.trim());
  }

  @Test
  void testFieldNameOrder(){
    var jsonConverter = getConverter();
    var testFieldNameClass=new TestFieldNameClass();
    testFieldNameClass.beta=List.of("notMatter");
    testFieldNameClass.banana="notMatter";
    testFieldNameClass.apple="notMatter";
    testFieldNameClass.actor=123;
    testFieldNameClass.dog=new TestPrimitiveClass();

    var json=jsonConverter.toJson(testFieldNameClass);
    assertEquals("{\"actor\":123,"
            + "\"apple\":\"notMatter\","
            + "\"banana\":\"notMatter\","
            + "\"beta\":[\"notMatter\"],"
            + "\"dog\":{\"intValue\":0}"
            + "}",json);
  }

  /**
   * order should be actor, apple, banana, beta, dog
   */
  private static class TestFieldNameClass {
    private List<String> beta;
    private String banana;
    private String apple;
    private int actor;
    private TestPrimitiveClass dog;
  }

  private static class TestOptionalClass {
    private Optional<String> optValue;
    private Optional<List<String>> nestedOpt;

  }

  private static class TestNestedObjectClass {
    private List<Map<String,String>> nestedList;
    private Map<String,List<String>> nestedMap;
    private TestPrimitiveClass nestedObject;
  }

  private static class TestPrimitiveClass {
    private String stringValue;
    private int intValue;
    private Double doublueValue;
  }
}
