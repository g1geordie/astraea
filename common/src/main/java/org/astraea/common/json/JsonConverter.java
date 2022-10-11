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
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

public interface JsonConverter {

  String toJson(Object src);

  <T> T fromJson(String json, Class<T> tClass);

  /**
   * for nested generic object ,the return value should specify type , Example: List<String>
   */
  <T> T fromJson(String json, Type type);

  static JsonConverter gson() {
    var gson = new GsonBuilder()
        .registerTypeAdapter(Optional.class,new GsonOptionalDeserializer<>())
        .create();
    return new JsonConverter() {
      @Override
      public String toJson(Object src) {
        var jsonElement=gson.toJsonTree(src);

        if(jsonElement.isJsonObject()){
          var object=jsonElement.getAsJsonObject();
          object.entrySet().stream().sorted(Entry.comparingByKey()).map(x->x.g).collect(Collectors.toList());
        }
        return gson.toJson(src);
      }

      private JsonObject getOrderedObject(JsonObject jsonObject){
        var newJsonObject=new JsonObject();
        return jsonObject.entrySet().stream().sorted(Entry.comparingByKey()).map(Entry::getValue).collect(Collectors.toList());
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

  class GsonOptionalDeserializer<T>
      implements JsonSerializer<Optional<T>>, JsonDeserializer<Optional<T>> {

    @Override
    public Optional<T> deserialize(JsonElement json, Type typeOfT,
        JsonDeserializationContext context)
        throws JsonParseException {
      if (!json.isJsonNull()) {
        final T value = context.deserialize(json,
            ((ParameterizedType) typeOfT).getActualTypeArguments()[0]);
        return Optional.ofNullable(value);
      } else {
        return Optional.empty();
      }
    }

    @Override
    public JsonElement serialize(Optional<T> src, Type typeOfSrc,
        JsonSerializationContext context) {
      if (src.isPresent()) {
        return context.serialize(src.get());
      } else {
        return JsonNull.INSTANCE;
      }
    }
  }
}
