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

import java.lang.reflect.Type;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;

/** Send json http request. */
public interface HttpExecutor {

  static HttpExecutorBuilder builder() {
    return new HttpExecutorBuilder();
  }
  
  <T> CompletableFuture<HttpResponse<T>> get(String url, Class<T> respCls);

  <T> CompletableFuture<HttpResponse<T>> get(String url, Object param, Class<T> respCls);

  <T> CompletableFuture<HttpResponse<T>> get(String url, Type type);

  <T> CompletableFuture<HttpResponse<T>> post(String url, Object body, Class<T> respCls);

  <T> CompletableFuture<HttpResponse<T>> put(String url, Object body, Class<T> respCls);

  CompletableFuture<HttpResponse<Void>> delete(String url);
}
