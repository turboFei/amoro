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

/**
 * ConnectionContext stores the username and IP address of the current connection in ThreadLocal.
 */
public class ConnectionContext {
  private static final ThreadLocal<String> CURRENT_USERNAME = new ThreadLocal<>();
  private static final ThreadLocal<String> CURRENT_IP = new ThreadLocal<>();

  /**
   * Set the username for the current thread.
   *
   * @param username The username to set
   */
  public static void setUsername(String username) {
    CURRENT_USERNAME.set(username);
  }

  /**
   * Get the username for the current thread.
   *
   * @return The username
   */
  public static String getUsername() {
    return CURRENT_USERNAME.get();
  }

  /**
   * Set the IP address for the current thread.
   *
   * @param ip The IP address to set
   */
  public static void setIP(String ip) {
    CURRENT_IP.set(ip);
  }

  /**
   * Get the IP address for the current thread.
   *
   * @return The IP address
   */
  public static String getIP() {
    return CURRENT_IP.get();
  }

  /**
   * Clear the connection context for the current thread.
   * Should be called after handling the request to avoid memory leaks.
   */
  public static void clear() {
    CURRENT_USERNAME.remove();
    CURRENT_IP.remove();
  }
}