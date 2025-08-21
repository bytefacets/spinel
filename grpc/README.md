# GRPC

This module enables inter-process communication over gRPC, allowing a client to connect
to a remove server and establish and modify subscriptions of registered outputs on the 
server.

See an [Example Client](../examples/src/main/java/com/bytefacets/diaspore/examples/grpc/Client.java),
or [Example Server](../examples/src/main/java/com/bytefacets/diaspore/examples/grpc/MarketDataServer.java).

## Creating a client
See [GrpcClient](src/main/java/com/bytefacets/diaspore/grpc/receive/GrpcClient.java)

```java
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
GrpcSource orders = GrpcSourceBuilder.grpcSource(client, "orders")
                        .subscription(subscriptionConfig("orders").defaultAll().build())
                        .getOrCreate();

// then connect the "orders" GrpcSource to further process the data

// then connect the client
client.connect();
```

### Adding JWT Authentication
Change the ClientBuilder to specialize the gRPC stub with credentials. [Example](./src/main/java/com/bytefacets/spinel/grpc/Client.java)
```java
// import static com.bytefacets.spinel.grpc.receive.auth.JwtCallCredentials.jwtCredentials;
final var creds = jwtCredentials("bob", "bob-user", System.getenv("BOBS_SECRET"));
final var orderClient =
        GrpcClientBuilder.grpcClient(orderChannel, clientDataEventLoop)
                .connectionInfo(new ConnectionInfo("order-server", "0.0.0.0:" + orderPort))
                .withSpecializer(stub -> stub.withCallCredentials(creds))
                .build();
```

## Creating a server
See [GrpcService](src/main/java/com/bytefacets/diaspore/grpc/send/GrpcService.java)

```java
RegisteredOutputs registry = new RegisteredOutputs();
// ... register outputs in the registry
DefaultSubscriptionProvider subscriptionProvider =
     DefaultSubscriptionProvider.defaultSubscriptionProvider(registry);

// note that the event loop should be single threaded
EventLoop dataEventLoop = new DefaultEventLoop();
GrpcService service = GrpcServiceBuilder.grpcService(subscriptionProvider, dataEventLoop).build();

Server server = ServerBuilder.forPort(15000).addService(service).executor(eventLoop).build();
server.start();
```
### Adding JWT Authentication
Change the ServerBuilder to specialize the gRPC stub with credentials. [Example](./src/main/java/com/bytefacets/spinel/grpc/OrderServer.java).
The argument to `multiTenantJwt` is a `Function<String, String>` so, you can rotate secrets if you 
need. If you 
```java
// import static com.bytefacets.spinel.grpc.send.auth.MultiTenantJwtInterceptor.multiTenantJwt;
final Map<String, String> tenantSecrets = Map.of("bob", "bobs-secret");
final Server server = ServerBuilder.forPort(port)
                        .addService(ServerInterceptors.intercept(service, 
                                multiTenantJwt(tenantSecrets::get)))
                        .executor(topologyBuilder.eventLoop)
                        .build();
```