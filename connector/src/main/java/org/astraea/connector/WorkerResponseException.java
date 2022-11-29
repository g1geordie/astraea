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
package org.astraea.connector;

import com.fasterxml.jackson.annotation.JsonAlias;

public class WorkerResponseException extends RuntimeException {

  private final WorkerError workerError;

  public WorkerResponseException(Throwable cause, WorkerError workerError) {
    super(
        String.format("Error code %s, %s", workerError.errorCode(), workerError.message()), cause);
    this.workerError = workerError;
  }

  public int errorCode() {
    return workerError.errorCode();
  }

  public String message() {
    return workerError.message();
  }

  /** worker error response object */
  public static class WorkerError {

    @JsonAlias("error_code")
    private int errorCode;

    private String message;

    public WorkerError() {}

    public WorkerError(int errorCode, String message) {
      this.errorCode = errorCode;
      this.message = message;
    }

    public int errorCode() {
      return errorCode;
    }

    public String message() {
      return message;
    }
  }
}