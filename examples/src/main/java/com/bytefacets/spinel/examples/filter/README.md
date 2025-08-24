# Example: ClientServerFilter 
## Modifying a Filter through a client subscription

In this example, a client connects to a server and issues requests to change the filter criteria on
the server. 

1. The server sets up a table with 10 records where one of the fields is "Account" and the 10 rows
loop through 3 accounts ACC0, ACC1, and ACC2.
2. The client creates a GrpcClient to talk to the server and a GrpcSource to establish a 
subscription to the orders table.
3. The client then periodically changes which account it's filtering for, e.g. 
`Sending filter for Account == 'ACC0'`. 
4. The server receives the request and applies it to the filter, then returns a response.
5. When the client receives the response, it prints the response and does a full dump of the 
contents of the table.

Below is an example of the output of running the example.

### Example output

```text
[main] INFO server-orders -- e0          SCH Order: 4 fields [0,Account,String][1,Price,Double][2,Qty,Int][3,OrderId,Int]
[main] INFO server-orders -- e0          ADD r0     : [Account=ACC0][Price=5.4][Qty=100][OrderId=0]
[main] INFO server-orders -- e0          ADD r1     : [Account=ACC1][Price=6.4][Qty=200][OrderId=1]
[main] INFO server-orders -- e0          ADD r2     : [Account=ACC2][Price=7.4][Qty=300][OrderId=2]
[main] INFO server-orders -- e0          ADD r3     : [Account=ACC0][Price=8.4][Qty=400][OrderId=3]
[main] INFO server-orders -- e0          ADD r4     : [Account=ACC1][Price=9.4][Qty=500][OrderId=4]
[main] INFO server-orders -- e0          ADD r5     : [Account=ACC2][Price=10.4][Qty=600][OrderId=5]
[main] INFO server-orders -- e0          ADD r6     : [Account=ACC0][Price=11.4][Qty=700][OrderId=6]
[main] INFO server-orders -- e0          ADD r7     : [Account=ACC1][Price=12.4][Qty=800][OrderId=7]
[main] INFO server-orders -- e0          ADD r8     : [Account=ACC2][Price=13.4][Qty=900][OrderId=8]
[main] INFO server-orders -- e0          ADD r9     : [Account=ACC0][Price=14.4][Qty=1000][OrderId=9]
[main] INFO com.bytefacets.spinel.grpc.receive.GrpcClient -- ClientOf[name=order-server,endpoint=0.0.0.0:26001] Initiating client subscription
[grpc-default-executor-0] INFO com.bytefacets.spinel.grpc.receive.GrpcClient -- ClientOf[name=order-server,endpoint=0.0.0.0:26001] Client state change: IDLE->CONNECTING
[grpc-default-executor-0] INFO com.bytefacets.spinel.grpc.receive.GrpcClient -- ClientOf[name=order-server,endpoint=0.0.0.0:26001] Client state change: CONNECTING->READY
[grpc-default-executor-0] DEBUG com.bytefacets.spinel.grpc.receive.GrpcClient -- onNewChannelState: READY
[grpc-default-executor-0] DEBUG com.bytefacets.spinel.grpc.receive.GrpcClient -- ClientOf[name=order-server,endpoint=0.0.0.0:26001] Requesting initialization: READY (1)
[server-data-thread] INFO com.bytefacets.spinel.grpc.send.GrpcService -- New session established: anon/anon@
[server-data-thread] DEBUG com.bytefacets.spinel.grpc.send.GrpcSession -- anon/anon@ Received REQUEST_TYPE_INIT (1)
[server-data-thread] INFO com.bytefacets.spinel.grpc.send.GrpcSession -- Initialization received: msg=hello
[client-data-thread] DEBUG com.bytefacets.spinel.grpc.receive.GrpcClient -- ClientOf[name=order-server,endpoint=0.0.0.0:26001] Initialization response: hello
[client-data-thread] INFO com.bytefacets.spinel.grpc.receive.GrpcClient -- ClientOf[name=order-server,endpoint=0.0.0.0:26001] Resubscribing 1 outputs if necessary
[client-data-thread] INFO com.bytefacets.spinel.grpc.receive.GrpcClient -- ClientOf[name=order-server,endpoint=0.0.0.0:26001]} Issuing subscription request: subscriptionId=2, subscription-id=2, name=orders
[server-data-thread] DEBUG com.bytefacets.spinel.grpc.send.GrpcSession -- anon/anon@ Received REQUEST_TYPE_SUBSCRIBE (2)
[client-data-thread] INFO client-orders -- e0          SCH Filter-00000000: 4 fields [0,Account,String][1,Price,Double][2,Qty,Int][3,OrderId,Int]
[client-data-thread] INFO client-orders -- e0          ADD r0     : [Account=ACC0][Price=5.4][Qty=100][OrderId=0]
[client-data-thread] INFO client-orders -- e0          ADD r1     : [Account=ACC1][Price=6.4][Qty=200][OrderId=1]
[client-data-thread] INFO client-orders -- e0          ADD r2     : [Account=ACC2][Price=7.4][Qty=300][OrderId=2]
[client-data-thread] INFO client-orders -- e0          ADD r3     : [Account=ACC0][Price=8.4][Qty=400][OrderId=3]
[client-data-thread] INFO client-orders -- e0          ADD r4     : [Account=ACC1][Price=9.4][Qty=500][OrderId=4]
[client-data-thread] INFO client-orders -- e0          ADD r5     : [Account=ACC2][Price=10.4][Qty=600][OrderId=5]
[client-data-thread] INFO client-orders -- e0          ADD r6     : [Account=ACC0][Price=11.4][Qty=700][OrderId=6]
[client-data-thread] INFO client-orders -- e0          ADD r7     : [Account=ACC1][Price=12.4][Qty=800][OrderId=7]
[client-data-thread] INFO client-orders -- e0          ADD r8     : [Account=ACC2][Price=13.4][Qty=900][OrderId=8]
[client-data-thread] INFO client-orders -- e0          ADD r9     : [Account=ACC0][Price=14.4][Qty=1000][OrderId=9]
[client-data-thread] INFO Example -- Sending filter for Account == 'ACC0'
[client-data-thread] INFO com.bytefacets.spinel.grpc.receive.GrpcClient -- ClientOf[name=order-server,endpoint=0.0.0.0:26001]} Issuing subscription request: subscriptionId=3, subscription-id=2, name=
[server-data-thread] DEBUG com.bytefacets.spinel.grpc.send.GrpcSession -- anon/anon@ Received REQUEST_TYPE_MODIFY (3)
[server-data-thread] DEBUG com.bytefacets.spinel.comms.send.FilterExpressionManager -- anon/anon@ Adding expression: Account == 'ACC0'
[client-data-thread] INFO client-orders -- e1          REM r1     :
[client-data-thread] INFO client-orders -- e1          REM r2     :
[client-data-thread] INFO client-orders -- e1          REM r4     :
[client-data-thread] INFO client-orders -- e1          REM r5     :
[client-data-thread] INFO client-orders -- e1          REM r7     :
[client-data-thread] INFO client-orders -- e1          REM r8     :
[client-data-thread] INFO Example -- Client received response to Add ModificationRequest[target='filter', action='apply', arguments=[Account == 'ACC0']]: ModificationResponse[success=true, message=, exception=null]
[client-data-thread] INFO OrdersDumper -- Dumping Row[0]: [Account=ACC0][Price=5.4][Qty=100][OrderId=0]
[client-data-thread] INFO OrdersDumper -- Dumping Row[3]: [Account=ACC0][Price=8.4][Qty=400][OrderId=3]
[client-data-thread] INFO OrdersDumper -- Dumping Row[6]: [Account=ACC0][Price=11.4][Qty=700][OrderId=6]
[client-data-thread] INFO OrdersDumper -- Dumping Row[9]: [Account=ACC0][Price=14.4][Qty=1000][OrderId=9]
[client-data-thread] INFO Example -- Sending filter for Account == 'ACC1'
[client-data-thread] INFO com.bytefacets.spinel.grpc.receive.GrpcClient -- ClientOf[name=order-server,endpoint=0.0.0.0:26001]} Issuing subscription request: subscriptionId=4, subscription-id=2, name=
[client-data-thread] INFO Example -- Sending REMOVE for Account == 'ACC0'
[client-data-thread] INFO com.bytefacets.spinel.grpc.receive.GrpcClient -- ClientOf[name=order-server,endpoint=0.0.0.0:26001]} Issuing subscription request: subscriptionId=5, subscription-id=2, name=
[server-data-thread] DEBUG com.bytefacets.spinel.grpc.send.GrpcSession -- anon/anon@ Received REQUEST_TYPE_MODIFY (4)
[server-data-thread] DEBUG com.bytefacets.spinel.comms.send.FilterExpressionManager -- anon/anon@ Adding expression: Account == 'ACC1'
[server-data-thread] DEBUG com.bytefacets.spinel.grpc.send.GrpcSession -- anon/anon@ Received REQUEST_TYPE_MODIFY (5)
[client-data-thread] INFO client-orders -- e2          ADD r8     : [Account=ACC1][Price=6.4][Qty=200][OrderId=1]
[server-data-thread] DEBUG com.bytefacets.spinel.comms.send.FilterExpressionManager -- anon/anon@ Removing expression: Account == 'ACC0'
[client-data-thread] INFO client-orders -- e2          ADD r7     : [Account=ACC1][Price=9.4][Qty=500][OrderId=4]
[client-data-thread] INFO client-orders -- e2          ADD r5     : [Account=ACC1][Price=12.4][Qty=800][OrderId=7]
[client-data-thread] INFO Example -- Client received response to Add ModificationRequest[target='filter', action='apply', arguments=[Account == 'ACC1']]: ModificationResponse[success=true, message=, exception=null]
[client-data-thread] INFO OrdersDumper -- Dumping Row[0]: [Account=ACC0][Price=5.4][Qty=100][OrderId=0]
[client-data-thread] INFO OrdersDumper -- Dumping Row[3]: [Account=ACC0][Price=8.4][Qty=400][OrderId=3]
[client-data-thread] INFO OrdersDumper -- Dumping Row[5]: [Account=ACC1][Price=12.4][Qty=800][OrderId=7]
[client-data-thread] INFO OrdersDumper -- Dumping Row[6]: [Account=ACC0][Price=11.4][Qty=700][OrderId=6]
[client-data-thread] INFO OrdersDumper -- Dumping Row[7]: [Account=ACC1][Price=9.4][Qty=500][OrderId=4]
[client-data-thread] INFO OrdersDumper -- Dumping Row[8]: [Account=ACC1][Price=6.4][Qty=200][OrderId=1]
[client-data-thread] INFO OrdersDumper -- Dumping Row[9]: [Account=ACC0][Price=14.4][Qty=1000][OrderId=9]
[client-data-thread] INFO client-orders -- e3          REM r0     :
[client-data-thread] INFO client-orders -- e3          REM r3     :
[client-data-thread] INFO client-orders -- e3          REM r6     :
[client-data-thread] INFO client-orders -- e3          REM r9     :
[client-data-thread] INFO Example -- Client received response to Remove ModificationRequest[target='filter', action='apply', arguments=[Account == 'ACC0']]: ModificationResponse[success=true, message=, exception=null]
[client-data-thread] INFO OrdersDumper -- Dumping Row[5]: [Account=ACC1][Price=12.4][Qty=800][OrderId=7]
[client-data-thread] INFO OrdersDumper -- Dumping Row[7]: [Account=ACC1][Price=9.4][Qty=500][OrderId=4]
[client-data-thread] INFO OrdersDumper -- Dumping Row[8]: [Account=ACC1][Price=6.4][Qty=200][OrderId=1]

```