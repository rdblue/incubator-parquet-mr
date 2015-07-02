/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet;

import org.apache.parquet.VersionParser.ParsedVersion;

public class CorruptDeltaByteArrays {
  private static final Log LOG = Log.getLog(CorruptStatistics.class);

  private static final SemanticVersion PARQUET_246_FIXED_VERSION =
      new SemanticVersion(1, 8, 0);

  public static boolean requireSequentialReads(ParsedVersion version) {
    if (version == null) {
      return true;
    }

    if (!"parquet-mr".equals(version.application)) {
      // assume other applications don't have this bug
      return false;
    }

    if (!version.hasSemanticVersion()) {
      LOG.warn("Requiring sequential reads because created_by did not " +
          "contain a valid version (see PARQUET-246): " + version.version);
      return true;
    }

    return requireSequentialReads(version.getSemanticVersion());
  }

  public static boolean requireSequentialReads(SemanticVersion semver) {
    if (semver == null) {
      return true;
    }

    if (semver.compareTo(PARQUET_246_FIXED_VERSION) < 0) {
      LOG.info("Requiring sequential reads because this file was created " +
          "prior to " + PARQUET_246_FIXED_VERSION + ". See PARQUET-246" );
      return true;
    }

    // this file was created after the fix
    return false;
  }

  public static boolean requireSequentialReads(String createdBy) {
    if (Strings.isNullOrEmpty(createdBy)) {
      LOG.info("Requiring sequential reads because file version is empty. " +
          "See PARQUET-246");
      return true;
    }

    try {
      return requireSequentialReads(VersionParser.parse(createdBy));

    } catch (RuntimeException e) {
      warnParseError(createdBy, e);
      return true;
    } catch (VersionParser.VersionParseException e) {
      warnParseError(createdBy, e);
      return true;
    }
  }

  private static void warnParseError(String createdBy, Throwable e) {
    LOG.warn("Requiring sequential reads because created_by could not be " +
        "parsed (see PARQUET-246): " + createdBy, e);
  }
}
