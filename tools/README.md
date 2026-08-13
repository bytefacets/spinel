# Tools

## Console Renderer

The Console Renderer is a command-line, terminal-based viewer for a remote table over gRPC.

It supports optional JWT arguments. It uses the jansi library to perform the terminal control.

![Spinel Console Renderer Example](https://bytefacets.github.io/site/assets/images/console-example.gif)

To try it out against one of the example servers, run the example [OrderServer](../examples/src/main/java/com/bytefacets/spinel/examples/grpc/OrderServer.java).
This will create a gRPC server at localhost:25001. If you build the tools module, or run from your IDE, use the arguments
```shell
console --endpoint 0.0.0.0:25001 --output order-view --jwt-secret bobs-secret --jwt-issuer bob --jwt-user $USER
```
$USER may need to be $USERNAME if you're on Windows.
or after unzipping build/distributions/tools-0.0.1-SNAPSHOT.zip
```shell
./tools-0.0.1-SNAPSHOT/bin/tools console --endpoint 0.0.0.0:25001 --output order-view --jwt-secret bobs-secret --jwt-issuer bob --jwt-user $USER
```
You should see something like this, but it's updating.
```shell
   OrderId     Price     Qty    InstrumentId  Symbol   
+---------++--------++------++--------------++--------+
       113    587.60       0               4  GJH      
       123    639.60       0               4  GJH      
       128      0.00    1100               9  JHY      
       124    644.80     200               5  JPS      
       130     10.40    1600               1  DPLO     
       127    660.40     800               8  NXQ      
       125    650.00     500               6  UL     
```
