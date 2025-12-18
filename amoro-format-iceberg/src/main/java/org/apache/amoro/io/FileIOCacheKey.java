/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.amoro.io;

import org.apache.amoro.table.TableIdentifier;

import java.util.Objects;

/**
 * A composite key for caching FileIO instances based on table identifier and location. This class
 * provides proper equals() and hashCode() implementations to avoid collisions that could occur with
 * string concatenation approaches.
 */
class FileIOCacheKey {
  private final TableIdentifier tableIdentifier;
  private final String tableLocation;

  FileIOCacheKey(TableIdentifier tableIdentifier, String tableLocation) {
    this.tableIdentifier =
        Objects.requireNonNull(tableIdentifier, "tableIdentifier cannot be null");
    this.tableLocation = Objects.requireNonNull(tableLocation, "tableLocation cannot be null");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FileIOCacheKey that = (FileIOCacheKey) o;
    return tableIdentifier.equals(that.tableIdentifier) && tableLocation.equals(that.tableLocation);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableIdentifier, tableLocation);
  }

  @Override
  public String toString() {
    return "FileIOCacheKey{"
        + "tableIdentifier="
        + tableIdentifier
        + ", tableLocation='"
        + tableLocation
        + '\''
        + '}';
  }
}
