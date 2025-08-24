# Example: PermissionFilter
In this example, we're intercepting the subscription creation on the server and using the
authenticated user to apply a Filter which only lets through rows relating to specific values in
the Account field of an Orders table.

When the user connects, we take the user's name and pull out a set of permitted accounts. We
configure a Filter with a predicate that evaluates each row, limiting what passes by checking
that the Account value is in the set of permitted accounts.

In this example, we also configure a periodic task on the client side to dump the entire
contents of the view that has been received by the user to show that it only contains the
permitted accounts.

```text
                               -------------  Subscription Container --------------                       
+---------------------+        +---------------------+        +-------------------+        +-------------------+
|      (Server)       |        |       (Server)      |        |      (Server)     |        |      (Client)     |
|                     |        |                     |        |                   |        |                   |
|       Orders        +--------+        Filter       +--------+       Filter      +--------+     GrpcSource    |
|                     |        |                     |        |                   |        |                   |
|                     |        |  [Permission Check] |        | [User configured] |        |                   |
+---------------------+        +---------------------+        +-------------------+        +-------------------+
```

Below is an example of the output of running the example.

### Example output

```text
[main] INFO com.bytefacets.spinel.grpc.receive.GrpcClient -- ClientOf[name=order-server,endpoint=0.0.0.0:26001] Initiating client subscription
[grpc-default-executor-0] INFO com.bytefacets.spinel.grpc.receive.GrpcClient -- ClientOf[name=order-server,endpoint=0.0.0.0:26001] Client state change: IDLE->CONNECTING
[grpc-default-executor-1] INFO com.bytefacets.spinel.grpc.receive.GrpcClient -- ClientOf[name=order-server,endpoint=0.0.0.0:26001] Client state change: CONNECTING->READY
[grpc-default-executor-1] DEBUG com.bytefacets.spinel.grpc.receive.GrpcClient -- onNewChannelState: READY
[grpc-default-executor-1] DEBUG com.bytefacets.spinel.grpc.receive.GrpcClient -- ClientOf[name=order-server,endpoint=0.0.0.0:26001] Requesting initialization: READY (1)
[server-data-thread] INFO server-orders -- e2          ADD r1     : [Account=CAROL][Price=5.06][Qty=100][OrderId=101]
[server-data-thread] INFO com.bytefacets.spinel.grpc.send.auth.MultiTenantJwtInterceptor -- Authenticated some-issuer/bob
[server-data-thread] INFO com.bytefacets.spinel.grpc.send.GrpcService -- New session established: some-issuer/bob@/127.0.0.1:58442
[server-data-thread] DEBUG com.bytefacets.spinel.grpc.send.GrpcSession -- some-issuer/bob@/127.0.0.1:58442 Received REQUEST_TYPE_INIT (1)
[server-data-thread] INFO com.bytefacets.spinel.grpc.send.GrpcSession -- Initialization received: msg=hello
[client-data-thread] DEBUG com.bytefacets.spinel.grpc.receive.GrpcClient -- ClientOf[name=order-server,endpoint=0.0.0.0:26001] Initialization response: hello
[client-data-thread] INFO com.bytefacets.spinel.grpc.receive.GrpcClient -- ClientOf[name=order-server,endpoint=0.0.0.0:26001] Resubscribing 1 outputs if necessary
[client-data-thread] INFO com.bytefacets.spinel.grpc.receive.GrpcClient -- ClientOf[name=order-server,endpoint=0.0.0.0:26001]} Issuing subscription request: subscriptionId=2, subscription-id=2, name=orders
[server-data-thread] DEBUG com.bytefacets.spinel.grpc.send.GrpcSession -- some-issuer/bob@/127.0.0.1:58442 Received REQUEST_TYPE_SUBSCRIBE (2)
[server-data-thread] INFO PermissionedSubscriptionFactory -- Creating permission filter for bob: [BOB, CAROL]
[client-data-thread] INFO client-orders -- e0          SCH Filter-00000001: 4 fields [0,Account,String][1,Price,Double][2,Qty,Int][3,OrderId,Int]
[client-data-thread] INFO client-orders -- e0          ADD r0     : [Account=BOB][Price=5.09][Qty=400][OrderId=100]
[client-data-thread] INFO client-orders -- e0          ADD r1     : [Account=CAROL][Price=5.06][Qty=100][OrderId=101]
[server-data-thread] INFO server-orders -- e3          ADD r2     : [Account=DEREK][Price=5.03][Qty=200][OrderId=102]
[server-data-thread] INFO server-orders -- e4          ADD r3     : [Account=BOB][Price=5.09][Qty=300][OrderId=103]
[client-data-thread] INFO client-orders -- e1          ADD r2     : [Account=BOB][Price=5.09][Qty=300][OrderId=103]
[server-data-thread] INFO server-orders -- e5          ADD r4     : [Account=CAROL][Price=5.0][Qty=300][OrderId=104]
[client-data-thread] INFO client-orders -- e2          ADD r3     : [Account=CAROL][Price=5.0][Qty=300][OrderId=104]
[server-data-thread] INFO server-orders -- e6          ADD r5     : [Account=DEREK][Price=5.02][Qty=100][OrderId=105]
[server-data-thread] INFO server-orders -- e7          ADD r6     : [Account=BOB][Price=5.14][Qty=100][OrderId=106]
[client-data-thread] INFO client-orders -- e3          ADD r4     : [Account=BOB][Price=5.14][Qty=100][OrderId=106]
[client-data-thread] INFO ClientDump -- Dumping Row[0]: [Account=BOB][Price=5.09][Qty=400][OrderId=100]
[client-data-thread] INFO ClientDump -- Dumping Row[1]: [Account=CAROL][Price=5.06][Qty=100][OrderId=101]
[client-data-thread] INFO ClientDump -- Dumping Row[2]: [Account=BOB][Price=5.09][Qty=300][OrderId=103]
[client-data-thread] INFO ClientDump -- Dumping Row[3]: [Account=CAROL][Price=5.0][Qty=300][OrderId=104]
[client-data-thread] INFO ClientDump -- Dumping Row[4]: [Account=BOB][Price=5.14][Qty=100][OrderId=106]
[server-data-thread] INFO server-orders -- e8          CHG r0     : [Qty=300]
[client-data-thread] INFO client-orders -- e4          CHG r0     : [Qty=300]
[server-data-thread] INFO server-orders -- e9          REM r1     :
[client-data-thread] INFO client-orders -- e5          REM r1     :
[server-data-thread] INFO server-orders -- e10         ADD r1     : [Account=CAROL][Price=5.12][Qty=400][OrderId=107]
[client-data-thread] INFO client-orders -- e6          ADD r1     : [Account=CAROL][Price=5.12][Qty=400][OrderId=107]
[server-data-thread] INFO server-orders -- e11         CHG r2     : [Qty=100]
[server-data-thread] INFO server-orders -- e12         CHG r3     : [Qty=200]
[client-data-thread] INFO client-orders -- e7          CHG r2     : [Qty=200]
[client-data-thread] INFO ClientDump -- Dumping Row[0]: [Account=BOB][Price=5.09][Qty=300][OrderId=100]
[client-data-thread] INFO ClientDump -- Dumping Row[1]: [Account=CAROL][Price=5.12][Qty=400][OrderId=107]
[client-data-thread] INFO ClientDump -- Dumping Row[2]: [Account=BOB][Price=5.09][Qty=200][OrderId=103]
[client-data-thread] INFO ClientDump -- Dumping Row[3]: [Account=CAROL][Price=5.0][Qty=300][OrderId=104]
[client-data-thread] INFO ClientDump -- Dumping Row[4]: [Account=BOB][Price=5.14][Qty=100][OrderId=106]
[server-data-thread] INFO server-orders -- e13         CHG r4     : [Qty=200]
[client-data-thread] INFO client-orders -- e8          CHG r3     : [Qty=200]
[server-data-thread] INFO server-orders -- e14         REM r5     :
[server-data-thread] INFO server-orders -- e15         ADD r5     : [Account=DEREK][Price=5.18][Qty=400][OrderId=108]
[server-data-thread] INFO server-orders -- e16         REM r6     :
[client-data-thread] INFO client-orders -- e9          REM r4     :
[server-data-thread] INFO server-orders -- e17         ADD r6     : [Account=BOB][Price=5.1][Qty=100][OrderId=109]
[client-data-thread] INFO client-orders -- e10         ADD r4     : [Account=BOB][Price=5.1][Qty=100][OrderId=109]
[client-data-thread] INFO ClientDump -- Dumping Row[0]: [Account=BOB][Price=5.09][Qty=300][OrderId=100]
[client-data-thread] INFO ClientDump -- Dumping Row[1]: [Account=CAROL][Price=5.12][Qty=400][OrderId=107]
[client-data-thread] INFO ClientDump -- Dumping Row[2]: [Account=BOB][Price=5.09][Qty=200][OrderId=103]
[client-data-thread] INFO ClientDump -- Dumping Row[3]: [Account=CAROL][Price=5.0][Qty=200][OrderId=104]
[client-data-thread] INFO ClientDump -- Dumping Row[4]: [Account=BOB][Price=5.1][Qty=100][OrderId=109]
[server-data-thread] INFO server-orders -- e18         CHG r0     : [Qty=200]
[client-data-thread] INFO client-orders -- e11         CHG r0     : [Qty=200]
[server-data-thread] INFO server-orders -- e19         CHG r1     : [Qty=300]
[client-data-thread] INFO client-orders -- e12         CHG r1     : [Qty=300]
[server-data-thread] INFO server-orders -- e20         REM r2     :
[server-data-thread] INFO server-orders -- e21         ADD r2     : [Account=CAROL][Price=5.14][Qty=100][OrderId=110]
[client-data-thread] INFO client-orders -- e13         ADD r5     : [Account=CAROL][Price=5.14][Qty=100][OrderId=110]
[server-data-thread] INFO server-orders -- e22         CHG r3     : [Qty=100]
[client-data-thread] INFO client-orders -- e14         CHG r2     : [Qty=100]
[client-data-thread] INFO ClientDump -- Dumping Row[0]: [Account=BOB][Price=5.09][Qty=200][OrderId=100]
[client-data-thread] INFO ClientDump -- Dumping Row[1]: [Account=CAROL][Price=5.12][Qty=300][OrderId=107]
[client-data-thread] INFO ClientDump -- Dumping Row[2]: [Account=BOB][Price=5.09][Qty=100][OrderId=103]
[client-data-thread] INFO ClientDump -- Dumping Row[3]: [Account=CAROL][Price=5.0][Qty=200][OrderId=104]
[client-data-thread] INFO ClientDump -- Dumping Row[4]: [Account=BOB][Price=5.1][Qty=100][OrderId=109]
[client-data-thread] INFO ClientDump -- Dumping Row[5]: [Account=CAROL][Price=5.14][Qty=100][OrderId=110]
[server-data-thread] INFO server-orders -- e23         CHG r4     : [Qty=100]
[client-data-thread] INFO client-orders -- e15         CHG r3     : [Qty=100]
[server-data-thread] INFO server-orders -- e24         CHG r5     : [Qty=300]
[server-data-thread] INFO server-orders -- e25         REM r6     :
[client-data-thread] INFO client-orders -- e16         REM r4     :
[server-data-thread] INFO server-orders -- e26         ADD r6     : [Account=DEREK][Price=5.01][Qty=400][OrderId=111]
[server-data-thread] INFO server-orders -- e27         CHG r0     : [Qty=100]
[client-data-thread] INFO client-orders -- e17         CHG r0     : [Qty=100]
[client-data-thread] INFO ClientDump -- Dumping Row[0]: [Account=BOB][Price=5.09][Qty=100][OrderId=100]
[client-data-thread] INFO ClientDump -- Dumping Row[1]: [Account=CAROL][Price=5.12][Qty=300][OrderId=107]
[client-data-thread] INFO ClientDump -- Dumping Row[2]: [Account=BOB][Price=5.09][Qty=100][OrderId=103]
[client-data-thread] INFO ClientDump -- Dumping Row[3]: [Account=CAROL][Price=5.0][Qty=100][OrderId=104]
[client-data-thread] INFO ClientDump -- Dumping Row[5]: [Account=CAROL][Price=5.14][Qty=100][OrderId=110]
[server-data-thread] INFO server-orders -- e28         CHG r1     : [Qty=200]
[client-data-thread] INFO client-orders -- e18         CHG r1     : [Qty=200]
[server-data-thread] INFO server-orders -- e29         REM r2     :
[client-data-thread] INFO client-orders -- e19         REM r5     :
[server-data-thread] INFO server-orders -- e30         ADD r2     : [Account=BOB][Price=5.04][Qty=300][OrderId=112]
[client-data-thread] INFO client-orders -- e20         ADD r5     : [Account=BOB][Price=5.04][Qty=300][OrderId=112]
[server-data-thread] INFO server-orders -- e31         REM r3     :
[client-data-thread] INFO client-orders -- e21         REM r2     :
```
