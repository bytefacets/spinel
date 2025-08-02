# GRPC Example

This example demonstrates a client connecting to two servers and joining 
data from both of them. The example is set up so that you can observe
various connection ordering scenarios, for example:
- client waiting for servers
- server waiting for client
- client with data for left side of a join
- client with data for right side of a join
- client losing a connection to one of the servers

For more information, see the [grpc](../../../../../../../../../grpc/README.md) module.

## Classes:

### MarketDataServer
Creates a server that mocks a small set of instruments and applies price updates.

### OrderServer
Creates a server that mocks a set of orders and applies quantity updates or 
removes the orders when they have been filled (remaining quantity = 0).

### Client
Creates a client which connects to the two servers and joins the rows by a common key.

### ClientAndServersOverGrpc
A convenience class with which you can run some combination of the three in one process.