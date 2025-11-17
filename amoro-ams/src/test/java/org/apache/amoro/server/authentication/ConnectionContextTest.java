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

package org.apache.amoro.server.authentication;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test for the ConnectionContext class.
 */
public class ConnectionContextTest {

  @Test
  public void testConnectionContext() {
    try {
      // Set username and IP
      ConnectionContext.setUsername("testuser");
      ConnectionContext.setIP("127.0.0.1");

      // Verify values
      Assert.assertEquals("testuser", ConnectionContext.getUsername());
      Assert.assertEquals("127.0.0.1", ConnectionContext.getIP());

      // Clear context
      ConnectionContext.clear();

      // Verify cleared values
      Assert.assertNull(ConnectionContext.getUsername());
      Assert.assertNull(ConnectionContext.getIP());
    } finally {
      // Always clear context to avoid leaks
      ConnectionContext.clear();
    }
  }

  @Test
  public void testMultipleThreads() throws InterruptedException {
    // Set main thread values
    ConnectionContext.setUsername("mainuser");
    ConnectionContext.setIP("127.0.0.1");

    // Create a new thread with different values
    Thread thread = new Thread(() -> {
      ConnectionContext.setUsername("threaduser");
      ConnectionContext.setIP("192.168.1.1");

      // Verify thread-specific values
      Assert.assertEquals("threaduser", ConnectionContext.getUsername());
      Assert.assertEquals("192.168.1.1", ConnectionContext.getIP());

      // Clear context for the thread
      ConnectionContext.clear();
    });

    // Start and join the thread
    thread.start();
    thread.join();

    // Verify main thread values are still intact
    Assert.assertEquals("mainuser", ConnectionContext.getUsername());
    Assert.assertEquals("127.0.0.1", ConnectionContext.getIP());

    // Clear context for main thread
    ConnectionContext.clear();
  }
}