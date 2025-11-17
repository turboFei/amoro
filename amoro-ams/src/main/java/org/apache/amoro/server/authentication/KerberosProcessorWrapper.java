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

import org.apache.amoro.shade.thrift.org.apache.thrift.TException;
import org.apache.amoro.shade.thrift.org.apache.thrift.TProcessor;
import org.apache.amoro.shade.thrift.org.apache.thrift.protocol.TProtocol;
import org.apache.amoro.shade.thrift.org.apache.thrift.transport.TSaslServerTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.LoginException;
import javax.security.sasl.SaslException;
import java.net.InetSocketAddress;

/**
 * A TProcessor wrapper that adds Kerberos authentication and stores user connection information
 * in a ThreadLocal for later access.
 */
public class KerberosProcessorWrapper implements TProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(KerberosProcessorWrapper.class);

  private final TProcessor wrapped;

  public KerberosProcessorWrapper(TProcessor wrapped) {
    this.wrapped = wrapped;
  }

  @Override
  public boolean process(TProtocol in, TProtocol out) throws TException {
    try {
      // Extract username from the Kerberos credentials
      TSaslServerTransport saslServerTransport = getSaslServerTransport(in);
      if (saslServerTransport != null) {
        String username = saslServerTransport.getSaslServer().getAuthorizationID();
        ConnectionContext.setUsername(username);
        LOG.debug("Set authenticated username: {}", username);
      }

      // Extract client IP address
      InetSocketAddress clientAddress = getClientAddress(in);
      if (clientAddress != null) {
        String ipAddress = clientAddress.getAddress().getHostAddress();
        ConnectionContext.setIP(ipAddress);
        LOG.debug("Set client IP address: {}", ipAddress);
      }

      try {
        // Process the request with authenticated context
        return wrapped.process(in, out);
      } finally {
        // Clear the ThreadLocal to prevent memory leaks
        ConnectionContext.clear();
      }
    } catch (LoginException | SaslException e) {
      LOG.error("Authentication failed", e);
      throw new TException("Authentication failed: " + e.getMessage());
    } catch (Exception e) {
      LOG.error("Error processing request", e);
      throw new TException("Error processing request: " + e.getMessage());
    }
  }

  /**
   * Extract the TSaslServerTransport from the TProtocol if available.
   *
   * @param protocol The TProtocol to extract from
   * @return The TSaslServerTransport, or null if not available
   */
  private TSaslServerTransport getSaslServerTransport(TProtocol protocol) {
    if (protocol.getTransport() instanceof TSaslServerTransport) {
      return (TSaslServerTransport) protocol.getTransport();
    }
    return null;
  }

  /**
   * Extract the client InetSocketAddress from the TProtocol if available.
   *
   * @param protocol The TProtocol to extract from
   * @return The client InetSocketAddress, or null if not available
   */
  private InetSocketAddress getClientAddress(TProtocol protocol) {
    try {
      if (protocol.getTransport() != null &&
          protocol.getTransport().getSocket() != null &&
          protocol.getTransport().getSocket().getRemoteSocketAddress() instanceof InetSocketAddress) {
        return (InetSocketAddress) protocol.getTransport().getSocket().getRemoteSocketAddress();
      }
    } catch (Exception e) {
      LOG.warn("Could not extract client address", e);
    }
    return null;
  }
}