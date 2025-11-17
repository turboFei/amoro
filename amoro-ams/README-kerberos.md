# Amoro Management Server Kerberos Authentication

This document describes how to enable Kerberos authentication for the Amoro Management Server (AMS) Thrift services.

## Prerequisites

1. A working Kerberos infrastructure (KDC)
2. A keytab file for the AMS service
3. Kerberos client configuration (`/etc/krb5.conf`)

## Configuration

Add the following configuration to your `config.yaml` file:

```yaml
ams:
  # Enable Kerberos authentication for Thrift services
  thrift-server.kerberos.enabled: true

  # Kerberos principal for Thrift services (replace REALM with your Kerberos realm)
  # _HOST will be automatically replaced with the server hostname
  thrift-server.kerberos.principal: amoro/_HOST@REALM

  # Path to Kerberos keytab file
  thrift-server.kerberos.keytab: /etc/amoro/amoro.keytab
```

## User and IP Tracking

The Kerberos authentication implementation automatically tracks the authenticated user and client IP address in a ThreadLocal variable. This information is available to other components of the AMS through the `ConnectionContext` class:

```java
import org.apache.amoro.server.authentication.ConnectionContext;

// Get the authenticated username
String username = ConnectionContext.getUsername();

// Get the client IP address
String ipAddress = ConnectionContext.getIP();
```

## Testing

To test the Kerberos authentication:

1. Configure the AMS server with Kerberos authentication
2. Start the AMS server
3. Create a Kerberos-authenticated client that connects to the server
4. Verify that the client can successfully authenticate and use the service

## Troubleshooting

If you encounter authentication issues:

1. Check the AMS server logs for authentication errors
2. Verify that the keytab file is accessible to the AMS process
3. Ensure that the principal in the keytab matches the configured principal
4. Check that the client is properly authenticated with Kerberos
5. Verify the Kerberos configuration (`/etc/krb5.conf`) on both client and server