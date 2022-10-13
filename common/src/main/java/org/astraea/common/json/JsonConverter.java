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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.gson.GsonBuilder;
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
import java.util.Map.Entry;
import java.util.Optional;
import org.astraea.common.Utils;

public interface JsonConverter {

  String toJson(Object src);

  <T> T fromJson(String json, Class<T> tClass);

  /**
   * for nested generic object ,the return value should specify typeRef , Example: List<String>
   */
  <T> T fromJson(String json, TypeReference<T> typeRef);

  static JsonConverter jackson() {
    var objectMapper =new ObjectMapper();
    objectMapper.registerModule(new Jdk8Module());
    objectMapper.setVisibility(objectMapper.getSerializationConfig().getDefaultVisibilityChecker()
        .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
        .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
        .withSetterVisibility(JsonAutoDetect.Visibility.NONE)
        .withCreatorVisibility(JsonAutoDetect.Visibility.NONE));

    objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    objectMapper.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY,true);
//    objectMapper.setSerializationInclusion(Include.NON_NULL);
//    objectMapper.setSerializationInclusion(Include.NON_EMPTY);
    return new JsonConverter() {
      @Override
      public String toJson(Object src) {
        return Utils.packException(()-> objectMapper.writeValueAsString(src));
      }

      @Override
      public <T> T fromJson(String json, Class<T> tClass) {
        return Utils.packException(()-> objectMapper.readValue(json,tClass));
      }

      @Override
      public <T> T fromJson(String json,  TypeReference<T> typeRef) {
        return Utils.packException(()-> objectMapper.readValue(json, typeRef));
      }
    };
  }
    static JsonConverter gson() {
    var gson = new GsonBuilder()
        .registerTypeAdapter(Optional.class,new GsonOptionalDeserializer<>())
        .create();
    return new JsonConverter() {
      @Override
      public String toJson(Object src) {
        var jsonElement=gson.toJsonTree(src);
        return gson.toJson(getOrderedObject(jsonElement));
      }

      private JsonElement getOrderedObject(JsonElement jsonElement){
        if(jsonElement.isJsonObject()){
          return getOrderedObject(jsonElement.getAsJsonObject());
        }else{
          return jsonElement;
        }
      }

      private JsonObject getOrderedObject(JsonObject jsonObject){
        var newJsonObject=new JsonObject();
        jsonObject.entrySet().stream().sorted(Entry.comparingByKey())
            .forEach(x->newJsonObject.add(x.getKey(),getOrderedObject(x.getValue())));
        return newJsonObject;
      }

      @Override
      public <T> T fromJson(String json, Class<T> tClass) {
        return gson.fromJson(json, tClass);
      }

      @Override
      public <T> T fromJson(String json, TypeReference<T> typeRef) {
        return gson.fromJson(json, typeRef.getType());
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
