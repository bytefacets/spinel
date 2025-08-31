# Spinel Spring Examples

This project demonstrates how to use the Spinel library with Spring Boot to create real-time data streaming applications with WebSocket connectivity and protobuf messaging.

## Overview

Spinel is a high-performance data streaming library that provides:

- **Columnar Data Storage**: Efficient memory layout with each field storing its own data indexed by row
- **Real-time Updates**: WebSocket-based streaming with protobuf encoding for minimal overhead
- **Transform Pipeline Architecture**: Clean separation between data producers (TransformOutput) and consumers (TransformInput)
- **Schema-driven Access**: Type-safe data access through schema definitions
- **Join Operations**: Support for complex data relationships and aggregations

## Architecture

### Backend (Java)
- **TopologyBuilder**: Configures the data topology with tables, joins, and data generation
- **SpinelWebSocketHandler**: Handles WebSocket connections and protobuf message routing
- **OutputRegistry**: Manages available data outputs for subscription
- **Data Generation**: Simulates real-time order and instrument data

### Frontend (JavaScript)
- **SpinelClient**: WebSocket client with protobuf support
- **Columnar Storage**: Field/SchemaField pattern matching the Java implementation
- **TransformInput/Output**: Clean data flow architecture
- **Example Table**: Demonstrates how to consume and display streaming data

## Key Benefits

### Performance
- **Columnar Storage**: Better cache locality and memory efficiency
- **Single-pass Processing**: Data is processed once directly into final storage
- **Compact Field IDs**: Direct array indexing instead of hash maps
- **Binary Protocol**: Protobuf encoding minimizes network overhead

### Architecture
- **Clean Separation**: Library code separated from example/application code
- **Consistent Patterns**: JavaScript implementation mirrors Java patterns
- **Type Safety**: Schema-driven data access prevents runtime errors
- **Extensible**: Easy to add new data transformations and visualizations

### Developer Experience
- **Real-time Updates**: See data changes immediately in the browser
- **Configurable**: Topology parameters controlled via Spring Boot configuration
- **Observable**: Built-in logging and event system for debugging
- **Modular**: Components can be used independently or combined

## Project Structure

```
spring-examples/
├── src/main/java/com/bytefacets/spinel/spring/
│   ├── Application.java              # Spring Boot main application
│   ├── TopologyBuilder.java          # Data topology configuration
│   ├── SpinelWebSocketHandler.java   # WebSocket message handling
│   ├── FluxAdapter.java              # Reactive streams integration
│   └── ResilientWebSocketClient.java # Example Java-based reactive WebSocket client implementation
├── src/main/resources/
│   ├── application.yml               # Spring Boot configuration
│   └── public/                       # Web assets
│       ├── index.html                # Example web page
│       ├── spinel-lib.js             # Spinel JavaScript library
│       ├── example-table.js          # Example table implementation
│       └── data-service.proto        # Protobuf schema definitions
└── README.md                         # This file
```

## Configuration

The topology behavior is controlled via [`application.yml`](src/main/resources/application.yml):

```yaml
spinel:
  topology:
    num-instruments: 10      # Number of instruments to generate
    max-orders: 15          # Maximum concurrent orders
    change-rate-millis: 100 # Update frequency in milliseconds
```

## Data Model

### Tables
- **instruments**: Static reference data with InstrumentId and Symbol
- **orders**: Dynamic order data with OrderId, InstrumentId, Quantity, and Price
- **order-view**: Join of orders and instruments showing enriched order data

### Data Flow
1. **Instrument Registration**: Static instruments are created on startup
2. **Order Generation**: Orders are continuously created, updated, and removed
3. **Join Processing**: Orders are joined with instruments to create enriched views
4. **WebSocket Streaming**: Changes are streamed to connected clients via protobuf
5. **Client Display**: JavaScript clients receive and display real-time updates

## JavaScript Library Usage

### Basic Setup
```javascript
// Initialize the client
const client = new SpinelClient({
    logger: console.log
});

await client.initialize();
await client.connect();
```

### Subscribe to Data
```javascript
// Subscribe to a table
const subscriptionOutput = client.subscribe('order-view');

// Create a custom input to receive updates
class MyTableInput extends TransformInput {
    schemaUpdated(schemaFields) {
        // Handle schema changes
        console.log('Schema updated:', schemaFields.map(f => f.getName()));
    }
    
    rowsAdded(rows) {
        // Handle new rows
        console.log('Rows added:', rows);
    }
    
    rowsChanged(rows, changedFields) {
        // Handle row updates
        console.log('Rows changed:', rows, 'Fields:', Array.from(changedFields));
    }
    
    rowsRemoved(rows) {
        // Handle row deletions
        console.log('Rows removed:', rows);
    }
}

// Attach your input to receive updates
const myInput = new MyTableInput();
subscriptionOutput.attachInput(myInput);
```

### Access Data
```javascript
// Get value by row and field
const value = subscriptionOutput.getValue(rowId, fieldId);

// Get schema field information
const schemaField = subscriptionOutput.getSchemaField(fieldId);
const fieldName = schemaField.getName();
const fieldValue = schemaField.objectValueAt(rowId);

// Get all active rows
const allRows = subscriptionOutput.getAllRowIds();
```

## Running the Example

1. **Start the Server**:
   ```bash
   ./gradlew :spring-examples:bootRun
   ```

2. **Open the Web Interface**:
   Navigate to `http://localhost:8080` in your browser

3. **Interact with the Data**:
   - Click "Connect" to establish WebSocket connection
   - Subscribe to different tables (orders, instruments, order-view)
   - Watch real-time data updates in the table displays
   - Monitor connection status and logs

## Key Features Demonstrated

### Real-time Data Streaming
- Orders are continuously created, modified, and removed
- Changes are immediately streamed to all connected clients
- No polling required - push-based updates

### Efficient Data Handling
- Columnar storage for optimal memory usage
- Single-pass data processing eliminates intermediate structures
- Schema-driven access ensures type safety

### Scalable Architecture
- Clean separation between data generation and consumption
- Modular components that can be extended or replaced
- Event-driven design supports multiple concurrent clients

### Developer-Friendly
- Comprehensive logging and error handling
- Configurable parameters via Spring Boot properties
- Clear separation between library and application code

## Extending the Example

### Add New Data Types
1. Define new interfaces in [`TopologyBuilder.java`](src/main/java/com/bytefacets/spinel/spring/TopologyBuilder.java)
2. Create tables and register them in the OutputRegistry
3. Add data generation logic
4. Update the web interface to subscribe to new tables

### Custom Transformations
1. Implement new TransformInput classes in JavaScript
2. Add custom data processing logic
3. Create specialized UI components for your data

### Additional Joins
1. Use JoinBuilder to create new data relationships
2. Register joined outputs for subscription
3. Configure join keys and handling strategies

## Performance Considerations

- **Memory Efficiency**: Columnar storage reduces memory fragmentation
- **Network Efficiency**: Protobuf binary encoding minimizes bandwidth
- **Processing Efficiency**: Single-pass data processing reduces CPU overhead
- **Scalability**: Event-driven architecture supports many concurrent connections

## Troubleshooting

### Common Issues
- **Connection Failed**: Ensure the server is running on port 8080
- **Protobuf Errors**: Verify [`data-service.proto`](src/main/resources/public/data-service.proto) is accessible
- **No Data**: Check that tables are properly registered in TopologyBuilder
- **Schema Mismatch**: Ensure JavaScript and Java use the same protobuf definitions

### Debug Tips
- Enable debug logging in [`application.yml`](src/main/resources/application.yml)
- Use browser developer tools to inspect WebSocket messages
- Check server logs for data generation and subscription activity
- Monitor the example table logs for data flow verification