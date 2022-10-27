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

import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadLocalRandom;
import org.astraea.common.connector.WorkerResponseException.WorkerError;
import org.astraea.common.http.HttpExecutor;
import org.astraea.common.http.Response;
import org.astraea.common.http.StringResponseException;
import org.astraea.common.json.JsonConverter;
import org.astraea.common.json.TypeRef;

public class Builder {

  private List<URL> urls = List.of();

  public Builder urls(Set<URL> urls) {
    this.urls = new ArrayList<>(Objects.requireNonNull(urls));
    return this;
  }

  public Builder url(URL url) {
    this.urls = List.of(Objects.requireNonNull(url));
    return this;
  }

  public ConnectorClient build() {
    if (urls.isEmpty()) {
      throw new IllegalArgumentException("Urls should be set.");
    }
    var httpExecutor = HttpExecutor.builder().build();

    return new ConnectorClient() {
      @Override
      public CompletionStage<WorkerInfo> info() {
        return httpExecutor.get(getURL("/"), TypeRef.of(WorkerInfo.class)).handle(this::getBody);
      }

      @Override
      public CompletionStage<List<String>> connectors() {
        return httpExecutor
            .get(getURL("/connectors"), TypeRef.array(String.class))
            .handle(this::getBody);
      }

      @Override
      public CompletionStage<ConnectorInfo> connector(String name) throws WorkerResponseException {
        return httpExecutor
            .get(getURL("/connectors/" + name), TypeRef.of(ConnectorInfo.class))
            .handle(this::getBody);
      }

      @Override
      public CompletionStage<ConnectorInfo> createConnector(String name, Map<String, String> config)
          throws WorkerResponseException {
        var connectorReq = new ConnectorReq(name, config);
        return httpExecutor
            .post(getURL("/connectors"), connectorReq, TypeRef.of(ConnectorInfo.class))
            .handle(this::getBody);
      }

      @Override
      public CompletionStage<ConnectorInfo> updateConnector(String name, Map<String, String> config)
          throws WorkerResponseException {
        return httpExecutor
            .put(getURL("/connectors/" + name + "/config"), config, TypeRef.of(ConnectorInfo.class))
            .handle(this::getBody);
      }

      @Override
      public CompletionStage<Void> deleteConnector(String name) throws WorkerResponseException {
        return httpExecutor.delete(getURL("/connectors/" + name)).handle(this::getBody);
      }

      private String getURL(String path) {
        try {
          var index = ThreadLocalRandom.current().nextInt(0, urls.size());
          return urls.get(index).toURI().resolve(path).toString();
        } catch (URISyntaxException e) {
          throw new RuntimeException(e);
        }
      }

      private <R> R getBody(Response<R> response, Throwable e) {
        if (e instanceof CompletionException && e.getCause() instanceof StringResponseException) {
          var stringResponseException = (StringResponseException) e.getCause();
          var workerError =
              Objects.isNull(stringResponseException.body())
                  ? new WorkerError(stringResponseException.statusCode(), "Unspecified error")
                  : JsonConverter.defaultConverter()
                      .fromJson(stringResponseException.body(), TypeRef.of(WorkerError.class));
          throw new WorkerResponseException(stringResponseException, workerError);
        } else {
          return response.body();
        }
      }
    };
  }
}
