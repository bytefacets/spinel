[![Sponsor](https://img.shields.io/badge/Sponsor-%E2%9D%A4-lightgrey?logo=github)](https://github.com/sponsors/bytefacets)

# ByteFacets Spinel

**High-Performance Column-Oriented Data Streaming Library**

Spinel is an efficient, column-oriented data streaming framework designed for real-time tabular data processing. Built for both embedded and distributed architectures, Spinel enables efficient data transformations with minimal memory overhead and maximum throughput.

## 🚀 Key Benefits

### **Exceptional Performance**
- **Column-Oriented Storage**: Optimized memory layout with superior cache locality and reduced fragmentation
- **Reference-Passing Design**: Zero-copy data access through pointer-like references to source rows
- **Single-Pass Processing**: Data flows directly through transformations without intermediate copies
- **Minimal Memory Footprint**: Array-based storage eliminates object overhead

### **Real-Time Capabilities**
- **Event-Driven Architecture**: Push-based updates with minimal latencies
- **Live Data Streaming**: WebSocket and gRPC integration for real-time client updates
- **Incremental Processing**: Only changed data is processed and transmitted

### **Flexible Integration**
- **Embeddable Library**: Seamlessly integrate into existing applications (Kafka, Spring Boot, microservices)
- **Multi-Process Communication**: Built-in IPC operators for distributed systems
- **Protocol Agnostic**: Support for WebSockets, gRPC, and custom protocols

## 🎯 Use Cases

### **Real-Time Dashboards**
Perfect for financial trading platforms, IoT monitoring, and business intelligence dashboards requiring:
- Live market data feeds with sub-millisecond updates
- Real-time KPI monitoring and alerting
- Interactive data exploration with instant filtering and aggregation
- Multi-user concurrent access with efficient resource utilization

### **Process-to-Process Data Pipelines**
Ideal for microservices architectures and data processing workflows:
- High-throughput ETL pipelines with complex joins and transformations
- Real-time analytics engines processing streaming data
- Event sourcing systems with live projections
- Inter-service communication with structured data contracts

### **Embedded Analytics**
Seamlessly embed into existing applications:
```java
// Embed in Kafka Streams applications
SpinelTable orders = intIndexedStructTable(Order.class).build();
Join orderView = JoinBuilder.lookupJoin("enriched-orders")
    .inner().joinOn("productId").build();
```

## 🏗️ Example Architecture Overview

```
         +-----------+     +-----------+
         |   order   |     |  product  |
         |   table   |     |   table   |
         +---------+-+     +-+---------+
                   |         |
               +---v---------v---+
               |      join       |
               | (on product_id) |
               +--------+--------+
               |                 |
        +------v------+   +------v------+
        |    filter   |   |   group-by  |
        |    (user)   |   |  (category) |
        +-------------+   +-------------+
               |                 |
       +-------v-------+ +-------v-------+
       |   grpc sink   | |   web-socket  |
       |               | |     client    |
       +---------------+ +---------------+
```

## 🛠️ Core Operators

**Data Storage & Access**
- **KeyedTables**: Indexed tables by primitive types (int, long, string)
- **Column-Oriented Fields**: Each field stored in optimized arrays

**Transformations**
- **Filter**: High-performance row filtering with custom predicates
- **Join**: Inner/outer joins with configurable key handling strategies
- **GroupBy**: Real-time aggregations with incremental updates
- **Projection**: Field selection, aliasing, reordering, and calculated fields
- **Union**: Merge multiple data streams into unified output
- **Conflation**: Reduce update frequency to conserve CPU and network bandwidth 
- **Projection**: Projections for calculated fields, field selection, and aliasing  

**Integration & Communication**
- **Subscription Management**: Multi-client subscription handling
- **Protocol Adapters**: WebSocket, gRPC, and custom protocol support

## 🔧 Performance Characteristics

**Memory Efficiency**
- Column-oriented storage reduces memory fragmentation
- Reference-passing eliminates unnecessary data copying
- Compact, array-based field indexing

**Processing Speed**
- Single-pass transformations minimize CPU cycles
- Cache-friendly data access patterns

**Network Efficiency**
- Protocol Buffer encoding for minimal bandwidth usage
- Incremental updates reduce network traffic by 90%
- Built-in conflation prevents message flooding

## 📦 Integration Examples

### Spring Boot Integration (in development)
```java
@Configuration
public class SpinelConfig {
    @Bean
    public IntIndexedStructTable<Order> orderTable() {
        return intIndexedStructTable(Order.class).build();
    }
    
    @Bean
    public WebSocketHandler spinelHandler(OutputRegistry registry) {
        return new SpinelWebSocketHandler(
            defaultSubscriptionProvider(registry), eventLoop);
    }
}
```

### Kafka Streams Integration (coming soon)
Use Spinel transformations to operate over state stores.

### NATS.io Integration (coming soon)
Use Spinel transformations to operate over KV stores.

## 🚀 Quick Start

### Basic Table Operations
```java
// Create a keyed table
IntIndexedStructTable<Order> orders = intIndexedStructTable(Order.class).build();
Order facade = orders.createFacade(); // reusable facade

// Add data
orders.beginAdd(1, facade)
    .setInstrumentId(100)
    .setQuantity(500)
    .setPrice(25.50);
orders.endAdd();

// Query data
int row = orders.lookupKeyRow(1);
orders.moveToRow(facade, row);
double price = facade.getPrice(); // 25.50
```

### Real-Time Transformations
```java
// Create a join between orders and instruments
Join orderView = JoinBuilder.lookupJoin("order-view")
    .inner()
    .joinOn(List.of("InstrumentId"), List.of("InstrumentId"), 10)
    .build();

// Connect data sources
Connector.connectInputToOutput(orderView.leftInput(), orders);
Connector.connectInputToOutput(orderView.rightInput(), instruments);

// Register for real-time subscriptions
OutputRegistry registry = new RegisteredOutputsTable();
registry.register("enriched-orders", orderView.output());
```

## 🏗️ Topology Composition

Spinel offers two powerful approaches for building data processing topologies:

### **1. TransformBuilder API** - Declarative Approach
The [`TransformBuilder`](spinel/src/main/java/com/bytefacets/spinel/transform/TransformBuilder.java) provides a fluent, declarative API for complex topologies:

```java
TransformBuilder transform = TransformBuilder.transform();
transform.intIndexedStructTable(Order.class)
    .then()
         .filter("open-orders").where("open == true")
    .then()
         .groupBy("open-by-instrument")
           .groupByFields("InstrumentId")
           .addAggregation(sumToInt("Quantity", "TotalQuantity"))
           .addAggregation(sumToInt("Notional", "TotalNotional"));
transform.build();
```

### **2. Direct Builder Usage** - Explicit Control
Use individual `*Builder` classes for fine-grained control:

```java
// Create components explicitly
IntIndexedStructTable<Order> orders = intIndexedStructTable(Order.class)
    .initialSize(1000)
    .chunkSize(256)
    .build();

Filter openOrders = FilterBuilder.filter("open-orders")
    .where("open == true")
    .build();

GroupBy byInstrument = GroupByBuilder.groupBy("open-by-instrument")
    .groupByFields("InstrumentId")
    .addAggregation(sumToInt("Quantity", "TotalQuantity"))
    .addAggregation(sumToInt("Notional", "TotalNotional"))
    .build();

// Connect manually for precise control
Connector.connectInputToOutput(openOrders, orders);
Connector.connectInputToOutput(byInstrument, openOrders);
```

## 🔌 Integration Patterns

### **Real-Time Web Dashboards**
```java
// Backend: WebSocket streaming
@Bean
public WebSocketHandler spinelHandler(OutputRegistry registry) {
    return new SpinelWebSocketHandler(
        defaultSubscriptionProvider(registry),
        eventLoop
    );
}

// Frontend: JavaScript client
const client = new SpinelClient();
await client.connect();
const subscription = client.subscribe('live-orders');

subscription.attachInput(new class extends TransformInput {
    rowsAdded(rows) {
        // Update dashboard in real-time
        updateOrdersTable(rows);
    }
});
```

### **Microservices Communication**
```java
// Server: Expose data via gRPC
RegisteredOutputsTable registry = new RegisteredOutputsTable();
registry.register("market-data", marketDataTable);

GrpcService service = GrpcServiceBuilder
    .grpcService(defaultSubscriptionProvider(registry), eventLoop)
    .build();

// Client: Subscribe to real-time updates
GrpcClient client = GrpcClientBuilder
    .grpcClient(channel, eventLoop)
    .build();

GrpcSource marketData = GrpcSourceBuilder
    .grpcSource(client, "market-data")
    .subscription(SubscriptionConfig.subscriptionConfig("market-data").defaultAll().build())
    .build();
```

## 📁 Examples & Documentation

### **Core Examples** ([`examples/`](examples/))
- **[Table Operations](examples/src/main/java/com/bytefacets/spinel/examples/table/)**: Basic CRUD operations and data modeling
- **[Filtering](examples/src/main/java/com/bytefacets/spinel/examples/filter/)**: Dynamic filtering with client-server communication
- **[gRPC Integration](examples/src/main/java/com/bytefacets/spinel/examples/grpc/)**: Multi-server distributed data processing
- **[Subscriptions](examples/src/main/java/com/bytefacets/spinel/examples/subscriptions/)**: Permission-based data access

### **Spring Boot Integration** ([`spring-examples/`](spring-examples/)) (In development)
Real-time dashboard example with:
- WebSocket streaming to browser clients
- Protocol Buffer encoding for efficiency
- Live order and instrument data with joins
- Interactive filtering and subscription management

![Spinel Web Example](https://bytefacets.github.io/site/assets/images/spinel-web-example.gif)

**Run the example:**
```bash
./gradlew :spring-examples:bootRun
# Open http://localhost:8080
```

### **JavaFX Integration** (Coming soon)

## 🎯 When to Use Spinel

**✅ Perfect For:**
- Real-time financial trading platforms
- IoT sensor data aggregation and monitoring
- Live business intelligence dashboards
- High-frequency data processing pipelines
- Event sourcing with live projections
- Microservices requiring structured data exchange

## 🚀 Getting Started

1. **Add Spinel to your project:**
   ```gradle
   implementation 'com.bytefacets:spinel:latest'
   implementation 'com.bytefacets:spinel-grpc:latest' // For gRPC integration
   ```

2. **Define your data model:**
   ```java
   interface Order {
       int getOrderId();
       String getSymbol(); Order setSymbol(String value);
       int getQuantity(); Order setQuantity(int value);
       double getPrice(); Order setPrice(double value);
   }
   ```

3. **Create and populate tables:**
   ```java
   IntIndexedStructTable<Order> orders = intIndexedStructTable(Order.class).build();
   Order facade = orders.createFacade();
   
   orders.beginAdd(1, facade)
       .setSymbol("AAPL")
       .setQuantity(100)
       .setPrice(150.25);
   orders.endAdd();
   orders.fireChanges();
   ```

4. **Set up real-time streaming:**
   ```java
   RegisteredOutputsTable registry = new RegisteredOutputsTable();
   registry.register("orders", orders);
   
   // WebSocket, gRPC, or custom protocol integration
   ```

## 📚 Learn More

- **[Spring Examples](spring-examples/README.md)**: Complete real-time dashboard example
- **[Core Examples](examples/)**: Fundamental usage patterns

---

**Ready to build high-performance real-time applications?** Start with the [Spring Examples](spring-examples/) for a complete working dashboard, or explore the [Core Examples](examples/) for specific integration patterns.
