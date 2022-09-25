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

import com.google.gson.reflect.TypeToken;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;

public class Builder {

  public ConnectorClient build(URL url) {
    var httpExecutor = HttpExecutor.builder().build();

    return new ConnectorClient() {
      @Override
      public WorkerInfo info() {
        return httpExecutor.get(getURL("/"), WorkerInfo.class).body();
      }

      @Override
      public List<String> connectors() {
        HttpResponse<List<String>> response =
            httpExecutor.get(getURL("/connectors"), new TypeToken<List<String>>() {}.getType());
        return response.body();
      }

      @Override
      public ConnectorInfo connector(String name) {
        return httpExecutor.get(getURL("/connectors/" + name), ConnectorInfo.class).body();
      }

      @Override
      public ConnectorInfo createConnector(String name, Map<String, String> config) {
        var connectorReq = new ConnectorReq(name, config);
        return httpExecutor.post(getURL("/connectors"), connectorReq, ConnectorInfo.class).body();
      }

      @Override
      public ConnectorInfo updateConnector(String name, Map<String, String> config) {
        return httpExecutor
            .put(getURL("/connectors/" + name + "/config"), config, ConnectorInfo.class)
            .body();
      }

      @Override
      public void deleteConnector(String name) {
        httpExecutor.delete(getURL("/connectors/" + name));
      }

      private URL getURL(String path) {
        try {
          return url.toURI().resolve(path).toURL();
        } catch (MalformedURLException | URISyntaxException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }
}
