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
package org.astraea.common.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import org.astraea.common.Utils;
import org.astraea.common.json.JsonConverter;
import org.junit.jupiter.api.Test;

class HttpExecutorTest {
  private static final JsonConverter jsonConverter = JsonConverter.gson();

  @Test
  void testGet() {
    var httpExecutor = HttpExecutor.builder().build();
    HttpTestUtil.testWithServer(
        httpServer ->
            httpServer.createContext(
                "/test",
                HttpTestUtil.createTextHandler(List.of("GET"), "{'responseValue':'testValue'}")),
        x -> Utils.packException(()-> {
          var responseHttpResponse = httpExecutor.get(getUrl(x, "/test"), TestResponse.class)
              .get();
          assertEquals("testValue", responseHttpResponse.body().responseValue());

          assertThrows(
              StringResponseException.class,
              () -> httpExecutor.get(getUrl(x, "/NotFound"), TestResponse.class));
        }));
  }

  @Test
  void testGetParameter() {
    var httpExecutor = HttpExecutor.builder().build();
    HttpTestUtil.testWithServer(
        httpServer ->
            httpServer.createContext(
                "/test",
                HttpTestUtil.createTextHandler(
                    List.of("GET"),
                    x -> assertEquals("/test?k1=v1", x.uri().toString()),
                    "{'responseValue':'testValue'}")),
        x -> Utils.packException(()->{
          var responseHttpResponse =
              httpExecutor.get(getUrl(x, "/test"), Map.of("k1", "v1"), TestResponse.class).get();
          assertEquals("testValue", responseHttpResponse.body().responseValue());

          responseHttpResponse =
              httpExecutor.get(getUrl(x, "/test"), new TestParam("v1"), TestResponse.class).get();
          assertEquals("testValue", responseHttpResponse.body().responseValue());

          assertThrows(
              StringResponseException.class,
              () -> httpExecutor.get(getUrl(x, "/NotFound"), TestResponse.class));
        }));
  }

  @Test
  void testPost() {
    var httpExecutor = HttpExecutor.builder().build();
    HttpTestUtil.testWithServer(
        httpServer ->
            httpServer.createContext(
                "/test",
                HttpTestUtil.createTextHandler(
                    List.of("Post"),
                    (x) -> {
                      var request = jsonConverter.fromJson(x.body(), TestRequest.class);
                      assertEquals("testRequestValue", request.requestValue());
                    },
                    "{'responseValue':'testValue'}")),
        x -> Utils.packException(()->{
          var request = new TestRequest();
          request.setRequestValue("testRequestValue");
          var responseHttpResponse =
              httpExecutor.post(getUrl(x, "/test"), request, TestResponse.class).get();
          assertEquals("testValue", responseHttpResponse.body().responseValue());

          // response body can't convert to testResponse
          assertThrows(
              StringResponseException.class,
              () -> httpExecutor.post(getUrl(x, "/NotFound"), request, TestResponse.class));
        }));
  }

  @Test
  void testPut() {
    var httpExecutor = HttpExecutor.builder().build();
    HttpTestUtil.testWithServer(
        httpServer ->
            httpServer.createContext(
                "/test",
                HttpTestUtil.createTextHandler(
                    List.of("Put"),
                    (x) -> {
                      var request = jsonConverter.fromJson(x.body(), TestRequest.class);
                      assertEquals("testRequestValue", request.requestValue());
                    },
                    "{'responseValue':'testValue'}")),
        x -> Utils.packException(()->{
          var request = new TestRequest();
          request.setRequestValue("testRequestValue");
          var responseHttpResponse =
              httpExecutor.put(getUrl(x, "/test"), request, TestResponse.class).get();
          assertEquals("testValue", responseHttpResponse.body().responseValue());

          assertThrows(
              StringResponseException.class,
              () -> httpExecutor.put(getUrl(x, "/NotFound"), request, TestResponse.class));
        }));
  }

  @Test
  void testDelete() {
    var httpExecutor = HttpExecutor.builder().build();
    HttpTestUtil.testWithServer(
        httpServer ->
            httpServer.createContext(
                "/test", HttpTestUtil.createTextHandler(List.of("delete"), "")),
        x -> {
          httpExecutor.delete(getUrl(x, "/test"));

          assertThrows(
              StringResponseException.class, () -> httpExecutor.delete(getUrl(x, "/NotFound")).get());
        });
  }

  private String getUrl(InetSocketAddress socketAddress, String path) {
    return "http://localhost:" + socketAddress.getPort() + path;
  }

  public static class TestParam {
    private final String k1;

    public TestParam(String k1) {
      this.k1 = k1;
    }

    public String k1() {
      return k1;
    }
  }

  public static class TestResponse {
    private String responseValue;

    public String responseValue() {
      return responseValue;
    }

    public void setResponseValue(String responseValue) {
      this.responseValue = responseValue;
    }
  }

  public static class TestRequest {
    private String requestValue;

    public String requestValue() {
      return requestValue;
    }

    public void setRequestValue(String requestValue) {
      this.requestValue = requestValue;
    }
  }
}
