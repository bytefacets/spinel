# GRPC

This module enables inter-process communication over gRPC, allowing a client to connect
to a remove server and establish and modify subscriptions of registered outputs on the 
server.

See an [Example Client](../examples/src/main/java/com/bytefacets/diaspore/examples/grpc/Client.java),
or [Example Server](../examples/src/main/java/com/bytefacets/diaspore/examples/grpc/MarketDataServer.java).

## Creating a client
See [GrpcClient](src/main/java/com/bytefacets/diaspore/grpc/receive/GrpcClient.java)

```
ManagedChannel channel = ManagedChannelBuilder.forTarget("0.0.0.0:15000")
                             .usePlaintext()
                             .enableRetry()
                             .keepAliveTime(5, TimeUnit.MINUTES)
                             .keepAliveTimeout(20, TimeUnit.SECONDS)
                             .build();

// note that the event loop should be single threaded
EventLoop dataEventLoop = new DefaultEventLoop();
GrpcClient client = GrpcClientBuilder.grpcClient(channel, dataEventLoop)
                         .connectionInfo(new ConnectionInfo("some-server", "0.0.0.0:15000"))
                         .build();

// then set up subscriptions to the server
GrpcSource orders = GrpcSourceBuilder.grpcSource(client, "order-view")
                        .subscription(subscriptionConfig("order-view").build())
                        .getOrCreate();

// then connect the "orders" GrpcSource to further process the data

// then connect the client
client.connect();
```

## Creating a server
See [GrpcService](src/main/java/com/bytefacets/diaspore/grpc/send/GrpcService.java)

```
RegisteredOutputs registry = new RegisteredOutputs();
// ... register outputs in the registry

// note that the event loop should be single threaded
EventLoop dataEventLoop = new DefaultEventLoop();
GrpcService service = GrpcServiceBuilder.grpcService(registry, dataEventLoop).build();

Server server = ServerBuilder.forPort(15000).addService(service).executor(eventLoop).build();
server.start();
```