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

import org.apache.amoro.config.ConfigOption;
import org.apache.amoro.config.ConfigOptions;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.shade.thrift.org.apache.thrift.TProcessor;
import org.apache.amoro.shade.thrift.org.apache.thrift.transport.TSaslServerTransport;
import org.apache.amoro.shade.thrift.org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.sasl.Sasl;
import java.util.HashMap;
import java.util.Map;

/**
 * Factory for creating Kerberos authenticated processors and transport factories.
 */
public class KerberosAuthenticationFactory {
  private static final Logger LOG = LoggerFactory.getLogger(KerberosAuthenticationFactory.class);

  public static final ConfigOption<Boolean> KERBEROS_ENABLED =
      ConfigOptions.key("thrift-server.kerberos.enabled")
          .booleanType()
          .defaultValue(false)
          .withDescription("Enable Kerberos authentication for Thrift services");

  public static final ConfigOption<String> KERBEROS_PRINCIPAL =
      ConfigOptions.key("thrift-server.kerberos.principal")
          .stringType()
          .defaultValue("amoro/_HOST@REALM")
          .withDescription("Kerberos principal for Thrift services");

  public static final ConfigOption<String> KERBEROS_KEYTAB =
      ConfigOptions.key("thrift-server.kerberos.keytab")
          .stringType()
          .defaultValue("")
          .withDescription("Path to Kerberos keytab file");

  /**
   * Wrap a TProcessor with Kerberos authentication if enabled.
   *
   * @param processor The processor to wrap
   * @param conf The configuration
   * @return The wrapped processor
   */
  public static TProcessor wrapWithKerberosAuthentication(TProcessor processor, Configurations conf) {
    if (conf.getBoolean(KERBEROS_ENABLED)) {
      LOG.info("Enabling Kerberos authentication for Thrift processor");
      return new KerberosProcessorWrapper(processor);
    }
    return processor;
  }

  /**
   * Create a transport factory with Kerberos authentication if enabled.
   *
   * @param conf The configuration
   * @return The transport factory
   */
  public static TTransportFactory createKerberosTransportFactory(Configurations conf) {
    if (!conf.getBoolean(KERBEROS_ENABLED)) {
      return null;
    }

    LOG.info("Creating Kerberos transport factory");
    try {
      String principal = conf.getString(KERBEROS_PRINCIPAL);
      String keytab = conf.getString(KERBEROS_KEYTAB);

      if (principal == null || principal.isEmpty()) {
        throw new IllegalArgumentException("Kerberos principal cannot be empty when Kerberos is enabled");
      }

      if (keytab == null || keytab.isEmpty()) {
        throw new IllegalArgumentException("Kerberos keytab cannot be empty when Kerberos is enabled");
      }

      // Replace _HOST with the hostname
      principal = principal.replaceAll("_HOST", java.net.InetAddress.getLocalHost().getCanonicalHostName());

      // Set up the JAAS configuration
      setupJaasConfig(principal, keytab);

      // Create the Sasl transport factory
      Map<String, String> saslProps = new HashMap<>();
      saslProps.put(Sasl.QOP, "auth");
      saslProps.put(Sasl.SERVER_AUTH, "true");

      // Create the transport factory
      return new TSaslServerTransport.Factory();
    } catch (Exception e) {
      LOG.error("Failed to create Kerberos transport factory", e);
      throw new RuntimeException("Failed to create Kerberos transport factory: " + e.getMessage(), e);
    }
  }

  /**
   * Set up the JAAS configuration for Kerberos.
   *
   * @param principal The Kerberos principal
   * @param keytab Path to the keytab file
   */
  private static void setupJaasConfig(String principal, String keytab) {
    // Set up the JAAS configuration programmatically
    Map<String, String> options = new HashMap<>();
    options.put("keyTab", keytab);
    options.put("principal", principal);
    options.put("useKeyTab", "true");
    options.put("storeKey", "true");
    options.put("doNotPrompt", "true");
    options.put("useTicketCache", "false");
    options.put("refreshKrb5Config", "true");

    AppConfigurationEntry[] entries = new AppConfigurationEntry[1];
    entries[0] = new AppConfigurationEntry(
        "com.sun.security.auth.module.Krb5LoginModule",
        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
        options
    );

    // Create a custom configuration
    javax.security.auth.login.Configuration.setConfiguration(
        new javax.security.auth.login.Configuration() {
          @Override
          public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
            if ("AmoroKerberosServer".equals(name)) {
              return entries;
            }
            return null;
          }
        }
    );

    // Set system properties
    System.setProperty("java.security.auth.login.config", "");
    System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");
  }
}