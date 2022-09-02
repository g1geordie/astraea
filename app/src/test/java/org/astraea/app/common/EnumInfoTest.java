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
package org.astraea.app.common;

import java.io.File;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

class EnumInfoTest {

  @Test
  void testAlias() {
    Assertions.assertEquals("TEST", MyTestEnum.TEST.alias());
    Assertions.assertEquals(MyTestEnum.TEST, MyTestEnum.ofAlias("test"));
  }

  @Test
  void testIgnoreCaseEnum() {
    Assertions.assertEquals(MyTestEnum.TEST, EnumInfo.ignoreCaseEnum(MyTestEnum.class, "test"));
    Assertions.assertEquals(MyTestEnum.TEST, EnumInfo.ignoreCaseEnum(MyTestEnum.class, "TEST"));
    Assertions.assertEquals(MyTestEnum.TEST, EnumInfo.ignoreCaseEnum(MyTestEnum.class, "Test"));
    Assertions.assertEquals(MyTestEnum.BANANA, EnumInfo.ignoreCaseEnum(MyTestEnum.class, "Banana"));
  }

  @ParameterizedTest
  @ArgumentsSource(EnumClassProvider.class)
  void testExtendEnumInfo(Class<?> cls) {
    Assertions.assertTrue(
        EnumInfo.class.isAssignableFrom(cls), String.format("Fail class %s", cls));
  }

  @ParameterizedTest
  @ArgumentsSource(EnumClassProvider.class)
  void testOfAlias(Class<?> cls) {
    // some enum implement anonymous class , see DistributionType
    var enumCls = getClassParentIsEnum(cls);

    var method =
        Assertions.assertDoesNotThrow(
            () -> enumCls.getDeclaredMethod("ofAlias", String.class),
            String.format("Fail class %s", cls));
    Assertions.assertEquals(enumCls, method.getReturnType());
  }

  private Class<?> getClassParentIsEnum(Class<?> cls) {
    return cls.getSuperclass() == Enum.class ? cls : getClassParentIsEnum(cls.getSuperclass());
  }

  enum MyTestEnum implements EnumInfo {
    TEST,
    BANANA;

    public static MyTestEnum ofAlias(String alias) {
      return EnumInfo.ignoreCaseEnum(MyTestEnum.class, alias);
    }
  }

  public static class EnumClassProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
      return getProductionClass().stream().filter(Enum.class::isAssignableFrom).map(Arguments::of);
    }
  }

  @Test
  void testProductionClass() {
    var productionClasses = getProductionClass();
    Assertions.assertTrue(productionClasses.size() > 100);
    productionClasses.forEach(
        x -> Assertions.assertTrue(x.getPackageName().startsWith("org.astraea.app")));
  }

  private static List<Class<?>> getProductionClass() {
    var pkg = "org/astraea/app";
    var mainDir =
        Collections.list(
                Utils.packException(() -> EnumInfoTest.class.getClassLoader().getResources(pkg)))
            .stream()
            .filter(x -> x.toExternalForm().contains("main/" + pkg))
            .findFirst()
            .map(x -> Utils.packException(() -> Path.of(x.toURI())))
            .map(x -> x.resolve("../../../").normalize())
            .orElseThrow();

    var dirFiles =
        FileUtils.listFiles(mainDir.toFile(), new String[] {"class"}, true).stream()
            .map(File::toPath)
            .map(mainDir::relativize)
            .collect(Collectors.toList());

    var classNames =
        dirFiles.stream()
            .map(Path::toString)
            .map(FilenameUtils::removeExtension)
            .map(x -> x.replace(File.separatorChar, '.'))
            .collect(Collectors.toList());

    return classNames.stream()
        .map(x -> Utils.packException(() -> Class.forName(x)))
        .collect(Collectors.toList());
  }
}
