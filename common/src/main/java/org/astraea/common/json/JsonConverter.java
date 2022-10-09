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

import com.google.gson.Gson;
import java.lang.reflect.Type;

public interface JsonConverter {

  String toJson(Object src);

  <T> T fromJson(String json, Class<T> tClass);

  /** for nested generic object ,the return value should specify type , Example: List<String> */
  <T> T fromJson(String json, Type type);

  static JsonConverter gson() {
    var gson = new Gson();
    return new JsonConverter() {
      @Override
      public String toJson(Object src) {
        return gson.toJson(src);
      }

      @Override
      public <T> T fromJson(String json, Class<T> tClass) {
        return gson.fromJson(json, tClass);
      }

      @Override
      public <T> T fromJson(String json, Type type) {
        return gson.fromJson(json, type);
      }
    };
  }
}
