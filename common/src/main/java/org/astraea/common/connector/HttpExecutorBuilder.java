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

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import org.astraea.common.Utils;
import org.astraea.common.Utils.Getter;
import org.astraea.common.Utils.Runner;
import org.astraea.common.connector.WorkerResponseException.WorkerError;

public class HttpExecutorBuilder {

  public HttpExecutor build() {
    HttpClient client = HttpClient.newHttpClient();
    var gson = new Gson();
    return new HttpExecutor() {
      @Override
      public <T> HttpResponse<T> get(URL url, Class<T> respCls) {
        return convertErrorException(
            () -> {
              HttpRequest request = HttpRequest.newBuilder().GET().uri(url.toURI()).build();

              return client.send(request, gsonResponseHandler(respCls));
            });
      }

      private Map<String, String> object2Map(Object obj) {
        return gson.fromJson(gson.toJson(obj), new TypeToken<Map<String, String>>() {}.getType());
      }

      private Gson gson = new Gson();

      @Override
      public <T> HttpResponse<T> get(URL url, Object param, Class<T> respCls) {
        return convertErrorException(
            () -> {
              HttpRequest request =
                  HttpRequest.newBuilder()
                      .GET()
                      .uri(URLUtil.getQueryUrl(url, object2Map(param)).toURI())
                      .build();

              return client.send(request, gsonResponseHandler(respCls));
            });
      }

      @Override
      public <T> HttpResponse<T> get(URL url, Type type) {
        return convertErrorException(
            () -> {
              HttpRequest request = HttpRequest.newBuilder().GET().uri(url.toURI()).build();

              return client.send(request, gsonResponseHandler(type));
            });
      }

      @Override
      public <T> HttpResponse<T> post(URL url, Object body, Class<T> respCls) {
        return convertErrorException(
            () -> {
              HttpRequest request =
                  HttpRequest.newBuilder()
                      .POST(gsonRequestHandler(body))
                      .header("Content-type", "application/json")
                      .uri(url.toURI())
                      .build();

              return client.send(request, gsonResponseHandler(respCls));
            });
      }

      @Override
      public <T> HttpResponse<T> put(URL url, Object body, Class<T> respCls) {
        return convertErrorException(
            () -> {
              HttpRequest request =
                  HttpRequest.newBuilder()
                      .PUT(gsonRequestHandler(body))
                      .header("Content-type", "application/json")
                      .uri(url.toURI())
                      .build();

              return client.send(request, gsonResponseHandler(respCls));
            });
      }

      @Override
      public void delete(URL url) {
        convertErrorException(
            () -> {
              HttpRequest request = HttpRequest.newBuilder().DELETE().uri(url.toURI()).build();

              client.send(request, HttpResponse.BodyHandlers.discarding());
            });
      }

      private <T> HttpRequest.BodyPublisher gsonRequestHandler(Object t) {
        return HttpRequest.BodyPublishers.ofString(gson.toJson(t));
      }

      private <T> HttpResponse.BodyHandler<T> gsonResponseHandler(Class<T> cls) {
        return gsonResponseHandler((Type) cls);
      }

      private <T> HttpResponse.BodyHandler<T> gsonResponseHandler(Type type) {
        return (responseInfo) ->
            HttpResponse.BodySubscribers.mapping(
                HttpResponse.BodySubscribers.ofString(StandardCharsets.UTF_8),
                (String body) -> {
                  if (responseInfo.statusCode() >= 400) {
                    throw new JsonResponseException(
                        String.format("Failed response: %s, %s.", responseInfo.statusCode(), body),
                        responseInfo,
                        body);
                  }

                  if (Objects.requireNonNull(body).isBlank()) {
                    throw new JsonResponseException(
                        String.format("Response %s is not json.", body), responseInfo, body);
                  }
                  try {
                    return gson.<T>fromJson(body, type);
                  } catch (JsonSyntaxException jsonSyntaxException) {
                    throw new JsonResponseException(
                        String.format("Response json `%s` can't convert to Object %s.", body, type),
                        responseInfo,
                        body);
                  }
                });
      }

      private <R> R convertErrorException(Getter<R> getter) {
        return Utils.packException(
            () -> {
              try {
                return getter.get();
              } catch (IOException ioException) {
                if (ioException.getCause() instanceof JsonResponseException) {
                  var realExp = (JsonResponseException) ioException.getCause();
                  var workerError = gson.fromJson(realExp.errorMsg(), WorkerError.class);
                  throw new WorkerResponseException(
                      String.format(
                          "Error code %s, %s", workerError.error_code(), workerError.message()),
                      realExp,
                      workerError);
                }
                throw ioException;
              }
            });
      }

      private void convertErrorException(Runner runner) {
        convertErrorException(
            () -> {
              runner.run();
              return null;
            });
      }
    };
  }
}
