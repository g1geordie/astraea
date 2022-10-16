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
package org.astraea.common.http;

import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.common.json.JsonConverter;
import org.astraea.common.json.TypeRef;

public class HttpExecutorBuilder {

  JsonConverter jsonConverter;

  public HttpExecutorBuilder() {
    this.jsonConverter = JsonConverter.defaultConverter();
  }

  public HttpExecutorBuilder jsonConverter(JsonConverter jsonConverter) {
    this.jsonConverter = jsonConverter;
    return this;
  }

  public HttpExecutor build() {
    var client = HttpClient.newHttpClient();
    var jsonConverter = this.jsonConverter;

    return new HttpExecutor() {
      @Override
      public <T> AstraeaHttpResponse<T> get(String url, Class<T> respCls) {
        return Utils.packException(
            () -> {
              HttpRequest request = HttpRequest.newBuilder().GET().uri(new URI(url)).build();
              return toJsonHttpResponse(
                  client.sendAsync(request, HttpResponse.BodyHandlers.ofString()), respCls);
            });
      }

      @Override
      public <T> AstraeaHttpResponse<T> get(String url, Object param, Class<T> respCls) {
        return Utils.packException(
            () -> {
              HttpRequest request =
                  HttpRequest.newBuilder().GET().uri(getParameterURI(url, param)).build();

              return toJsonHttpResponse(
                  client.sendAsync(request, HttpResponse.BodyHandlers.ofString()), respCls);
            });
      }

      @Override
      public <T> AstraeaHttpResponse<T> get(String url, TypeRef<T> typeRef) {
        return Utils.packException(
            () -> {
              HttpRequest request = HttpRequest.newBuilder().GET().uri(new URI(url)).build();

              return toJsonHttpResponse(
                  client.sendAsync(request, HttpResponse.BodyHandlers.ofString()), typeRef);
            });
      }

      @Override
      public <T> AstraeaHttpResponse<T> post(String url, Object body, Class<T> respCls) {
        return Utils.packException(
            () -> {
              HttpRequest request =
                  HttpRequest.newBuilder()
                      .POST(jsonRequestHandler(body))
                      .header("Content-type", "application/json")
                      .uri(new URI(url))
                      .build();

              return toJsonHttpResponse(
                  client.sendAsync(request, HttpResponse.BodyHandlers.ofString()), respCls);
            });
      }

      @Override
      public <T> AstraeaHttpResponse<T> put(String url, Object body, Class<T> respCls) {
        return Utils.packException(
            () -> {
              HttpRequest request =
                  HttpRequest.newBuilder()
                      .PUT(jsonRequestHandler(body))
                      .header("Content-type", "application/json")
                      .uri(new URI(url))
                      .build();

              return toJsonHttpResponse(
                  client.sendAsync(request, HttpResponse.BodyHandlers.ofString()), respCls);
            });
      }

      @Override
      public AstraeaHttpResponse<Void> delete(String url) {
        return Utils.packException(
            () -> {
              HttpRequest request = HttpRequest.newBuilder().DELETE().uri(new URI(url)).build();
              return withException(
                  client.sendAsync(request, HttpResponse.BodyHandlers.discarding()));
            });
      }

      /**
       * if return value is Json , then we can convert it to Object. Or we just simply handle
       * exception by {@link #withException(CompletableFuture)}
       */
      private <T> AstraeaHttpResponse<T> toJsonHttpResponse(
          CompletableFuture<HttpResponse<String>> asyncResponse, TypeRef<T> typeRef)
          throws StringResponseException {
        return AstraeaHttpResponse.of(asyncResponse.thenApply(x -> toJsonHttpResponse(x, typeRef)));
      }

      /**
       * if return value is Json , then we can convert it to Object. Or we just simply handle
       * exception by {@link #withException(CompletableFuture)}
       */
      private <T> AstraeaHttpResponse<T> toJsonHttpResponse(
          CompletableFuture<HttpResponse<String>> asyncResponse, Class<T> tClass)
          throws StringResponseException {
        return toJsonHttpResponse(asyncResponse, TypeRef.of(tClass));
      }

      private <T> HttpResponse<T> toJsonHttpResponse(
          HttpResponse<String> response, TypeRef<T> typeRef) throws StringResponseException {
        var innerResponse = withException(response);
        return new MappedHttpResponse<>(
            innerResponse,
            x -> {
              try {
                return jsonConverter.fromJson(x, typeRef);
              } catch (Exception e) {
                throw new StringResponseException(innerResponse, typeRef.getType());
              }
            });
      }

      /**
       * Handle exception with non json type response . If return value is json , we can use {@link
       * #toJsonHttpResponse(CompletableFuture, TypeRef)}
       */
      private <T> AstraeaHttpResponse<T> withException(
          CompletableFuture<HttpResponse<T>> response) {
        return AstraeaHttpResponse.of(response.thenApply(this::withException));
      }

      private <T> HttpResponse<T> withException(HttpResponse<T> response) {
        if (response.statusCode() >= 400) {
          throw new StringResponseException(toStringResponse(response));
        } else {
          return response;
        }
      }

      /**
       * if api return error code, then response type will be changed. So we throw an exception to
       * identify that error.
       */
      @SuppressWarnings("unchecked")
      private <T> HttpResponse<String> toStringResponse(HttpResponse<T> httpResponse)
          throws StringResponseException {
        HttpResponse<String> stringHttpResponse;
        if (Objects.isNull(httpResponse.body())) {
          stringHttpResponse = new MappedHttpResponse<>(httpResponse, (x) -> null);
        } else if (httpResponse.body() instanceof String) {
          stringHttpResponse = (HttpResponse<String>) httpResponse;
        } else {
          stringHttpResponse = new MappedHttpResponse<>(httpResponse, Object::toString);
        }
        throw new StringResponseException(stringHttpResponse);
      }

      private URI getParameterURI(String url, Object parameter) throws URISyntaxException {
        var paramMap = convert2Parameter(parameter);
        return getQueryUri(url, paramMap);
      }

      private HttpRequest.BodyPublisher jsonRequestHandler(Object t) {
        return HttpRequest.BodyPublishers.ofString(jsonConverter.toJson(t));
      }
    };
  }

  /** convert POJO to parameter. */
  static Map<String, List<String>> convert2Parameter(Object obj) {
    if (Objects.isNull(obj)) {
      return Map.of();
    } else {
      if (Map.class.isAssignableFrom(obj.getClass())) {
        Map<?, ?> map = (Map<?, ?>) obj;

        if (map.size() == 0) {
          return Map.of();
        } else {
          var valueClassOpt = map.values().stream().filter(Objects::nonNull).findAny();
          var mapStream = map.entrySet().stream().filter(Objects::nonNull);
          if (valueClassOpt.filter(x -> List.class.isAssignableFrom(x.getClass())).isPresent()) {
            return mapStream.collect(
                Collectors.toMap(
                    x -> x.getKey().toString(),
                    x ->
                        ((List<?>) x.getValue())
                            .stream().map(Object::toString).collect(Collectors.toList())));
          } else {
            return mapStream.collect(
                Collectors.toMap(
                    x -> x.getKey().toString(), x -> List.of(x.getValue().toString())));
          }
        }
      }

      var fields =
          Arrays.stream(obj.getClass().getDeclaredFields())
              .filter(x -> !x.isSynthetic())
              .collect(Collectors.toList());
      fields.forEach(x -> x.setAccessible(true));
      var map =
          fields.stream()
              .collect(
                  Collectors.toMap(
                      Field::getName, x -> Utils.packException(() -> getFieldParameter(obj, x))));

      return map.entrySet().stream()
          .filter(x -> x.getValue().size() > 0)
          .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }
  }

  private static List<String> getFieldParameter(Object obj, Field field)
      throws IllegalAccessException {
    var fieldClass = field.getType();
    var fieldObj = field.get(obj);
    if (fieldObj == null) {
      return List.of();
    }
    if (fieldClass == String.class) {
      return List.of((String) fieldObj);
    } else if (fieldClass.isPrimitive()) {
      return List.of(String.valueOf(fieldObj));
    } else if (Collection.class.isAssignableFrom(fieldClass)) {
      Collection<?> collection = (Collection<?>) fieldObj;
      return collection.stream().map(Object::toString).collect(Collectors.toList());
    } else {
      return List.of(fieldObj.toString());
    }
  }

  static URI getQueryUri(String url, Map<String, List<String>> parameters)
      throws URISyntaxException {
    var uri = new URI(url);
    var queryString =
        parameters.entrySet().stream()
            .map(
                x -> {
                  var key = x.getKey();
                  return key + "=" + getQueryValue(x.getValue());
                })
            .collect(Collectors.joining("&"));

    return new URI(
        uri.getScheme(), uri.getAuthority(), uri.getPath(), queryString, uri.getFragment());
  }

  private static String getQueryValue(List<String> values) {
    return values.stream()
        .map(x -> URLEncoder.encode(x, StandardCharsets.UTF_8))
        .collect(Collectors.joining(","));
  }
}
