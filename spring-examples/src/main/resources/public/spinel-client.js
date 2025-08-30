// Spinel WebSocket Client with Protobuf Support

class SpinelClient {
    constructor() {
        this.websocket = null;
        this.protobufRoot = null;
        this.subscriptionId = 1;
        this.msgToken = 1;
        this.tables = new Map(); // Map of subscriptionId -> InMemoryTable
        this.schemas = new Map(); // Map of subscriptionId -> schema
        this.isConnected = false;
        
        this.initProtobuf();
    }

    async initProtobuf() {
        try {
            // Load the protobuf definitions
            this.protobufRoot = await protobuf.load('data-service.proto');
            this.SubscriptionRequest = this.protobufRoot.lookupType('com.bytefacets.spinel.grpc.proto.SubscriptionRequest');
            this.SubscriptionResponse = this.protobufRoot.lookupType('com.bytefacets.spinel.grpc.proto.SubscriptionResponse');
            this.CreateSubscription = this.protobufRoot.lookupType('com.bytefacets.spinel.grpc.proto.CreateSubscription');
            this.RequestType = this.protobufRoot.lookupEnum('com.bytefacets.spinel.grpc.proto.RequestType');
            this.ResponseType = this.protobufRoot.lookupEnum('com.bytefacets.spinel.grpc.proto.ResponseType');
            
            this.log('Protobuf definitions loaded successfully', 'success');
        } catch (error) {
            this.log('Error loading protobuf definitions: ' + error.message, 'error');
        }
    }

    connect() {
        if (this.websocket && this.websocket.readyState === WebSocket.OPEN) {
            this.log('Already connected', 'info');
            return;
        }

        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${window.location.host}/ws/spinel`;
        
        this.log(`Connecting to ${wsUrl}...`, 'info');
        
        this.websocket = new WebSocket(wsUrl);
        this.websocket.binaryType = 'arraybuffer';
        
        this.websocket.onopen = () => {
            this.isConnected = true;
            this.updateConnectionStatus(true);
            this.log('WebSocket connected', 'success');
        };
        
        this.websocket.onmessage = (event) => {
            this.handleMessage(event.data);
        };
        
        this.websocket.onclose = () => {
            this.isConnected = false;
            this.updateConnectionStatus(false);
            this.log('WebSocket disconnected', 'info');
        };
        
        this.websocket.onerror = (error) => {
            this.log('WebSocket error: ' + error, 'error');
        };
    }

    disconnect() {
        if (this.websocket) {
            this.websocket.close();
            this.websocket = null;
        }
    }

    handleMessage(data) {
        try {
            const uint8Array = new Uint8Array(data);
            const response = this.SubscriptionResponse.decode(uint8Array);
            
            this.log(`Received response: type=${response.responseType}, subscriptionId=${response.subscriptionId}`, 'debug');
            
            if (response.schema) {
                this.handleSchemaUpdate(response.schema, response.subscriptionId);
            }
            
            if (response.data) {
                this.handleDataUpdate(response.data, response.subscriptionId);
            }
            
            if (response.response) {
                this.log(`Server response: ${response.response.success ? 'SUCCESS' : 'FAILURE'} - ${response.response.message}`, 
                         response.response.success ? 'success' : 'error');
            }
            
        } catch (error) {
            this.log('Error parsing message: ' + error.message, 'error');
        }
    }

    handleSchemaUpdate(schema, subscriptionId) {
        this.log(`Schema update for subscription ${subscriptionId}, table: ${schema.name}`, 'info');
        this.schemas.set(subscriptionId, schema);

        // Create or update the in-memory table
        if (!this.tables.has(subscriptionId)) {
            this.log('Received schema for unknown subscription: ' + subscriptionId, 'error');
        } else {
            // Update the table with schema field definitions
            const table = this.tables.get(subscriptionId);
            table.updateSchema(schema.fields);
            this.updateTableDisplay();
        }
    }

    handleDataUpdate(dataUpdate, subscriptionId) {
        const table = this.tables.get(subscriptionId);
        if (!table) {
            this.log(`Table for subscription ${subscriptionId} not found`, 'error');
            return;
        }
        
        const tableName = table.name || `Subscription ${subscriptionId}`;
        this.log(`Data update for table: ${tableName}, rows: ${dataUpdate.rows.length}`, 'debug');
        
        // Process each data type (columns), then fields, then rows
        this.processDataFields(table, dataUpdate.rows, dataUpdate.boolData, 'bool');
        this.processDataFields(table, dataUpdate.rows, dataUpdate.byteData, 'byte');
        this.processDataFields(table, dataUpdate.rows, dataUpdate.int32Data, 'int32');
        this.processDataFields(table, dataUpdate.rows, dataUpdate.int64Data, 'int64');
        this.processDataFields(table, dataUpdate.rows, dataUpdate.floatData, 'float');
        this.processDataFields(table, dataUpdate.rows, dataUpdate.doubleData, 'double');
        this.processDataFields(table, dataUpdate.rows, dataUpdate.stringData, 'string');
        this.processDataFields(table, dataUpdate.rows, dataUpdate.genericData, 'generic');
        
        this.updateTableDisplay();
    }

    processDataFields(table, rows, dataArray, dataType) {
        if (!dataArray || dataArray.length === 0) return;
        
        // Iterate through each field in this data type
        dataArray.forEach(fieldData => {
            const fieldId = fieldData.fieldId;
            
            // For each field, iterate through all rows
            rows.forEach((rowId, rowIndex) => {
                let value = fieldData.values[rowIndex];
                if (value !== undefined) {
                    this.log('Row['+rowId+'] Setting ' + value + ' to field['+fieldId+']' + table.fieldNames.get(fieldId), 'debug')
                    table.setValue(rowId, fieldId, value);
                }
            });
        });
    }


    subscribe(tableName) {
        if (!this.isConnected || !this.SubscriptionRequest) {
            this.log('Not connected or protobuf not loaded', 'error');
            return;
        }

        const currentSubscriptionId = this.subscriptionId++;

        // Allocate the InMemoryTable and map it to the subscriptionId
        this.tables.set(currentSubscriptionId, new InMemoryTable(tableName, []));

        const createSubscription = this.CreateSubscription.create({
            name: tableName,
            fieldNames: [], // Empty means all fields
            defaultAll: true,
            modifications: []
        });

        const request = this.SubscriptionRequest.create({
            msgToken: this.msgToken++,
            subscriptionId: currentSubscriptionId,
            requestType: this.RequestType.values.REQUEST_TYPE_SUBSCRIBE,
            subscription: createSubscription
        });

        const buffer = this.SubscriptionRequest.encode(request).finish();
        this.websocket.send(buffer);
        
        this.log(`Subscription request sent for table: ${tableName} (subscriptionId: ${currentSubscriptionId})`, 'info');
    }

    updateConnectionStatus(connected) {
        const statusElement = document.getElementById('status');
        const connectBtn = document.getElementById('connectBtn');
        const disconnectBtn = document.getElementById('disconnectBtn');
        const subscribeButtons = document.querySelectorAll('[id$="Btn"]:not(#connectBtn):not(#disconnectBtn)');
        
        if (connected) {
            statusElement.textContent = 'Connected';
            statusElement.className = 'status connected';
            connectBtn.disabled = true;
            disconnectBtn.disabled = false;
            subscribeButtons.forEach(btn => btn.disabled = false);
        } else {
            statusElement.textContent = 'Disconnected';
            statusElement.className = 'status disconnected';
            connectBtn.disabled = false;
            disconnectBtn.disabled = true;
            subscribeButtons.forEach(btn => btn.disabled = true);
        }
    }

    updateTableDisplay() {
        const tablesContainer = document.getElementById('tables');
        tablesContainer.innerHTML = '';
        
        this.tables.forEach((table, subscriptionId) => {
            const tableName = table.name || `Subscription ${subscriptionId}`;
            const tableDiv = document.createElement('div');
            tableDiv.innerHTML = `
                <h4>${tableName} (Subscription ${subscriptionId})</h4>
                ${table.toHTML()}
            `;
            tablesContainer.appendChild(tableDiv);
        });
    }

    log(message, type = 'info') {
        const logElement = document.getElementById('log');
        const timestamp = new Date().toLocaleTimeString();
        const logEntry = document.createElement('div');
        logEntry.className = `log-entry ${type}`;
        logEntry.textContent = `[${timestamp}] ${message}`;
        logElement.appendChild(logEntry);
        logElement.scrollTop = logElement.scrollHeight;
        
        console.log(`[${type.toUpperCase()}] ${message}`);
    }
}

// In-Memory Table class for storing data accessible by rowId and fieldId
class InMemoryTable {
    constructor(name, fields) {
        this.name = name;
        this.fields = fields || [];
        this.data = new Map(); // Map of rowId -> Map of fieldId -> value
        this.fieldNames = new Map(); // Map of fieldId -> fieldName
        
        // Build field name mapping - use array index as fieldId since FieldDefinition doesn't have fieldId
        this.fields.forEach((field, index) => {
            this.fieldNames.set(index, field.name);
        });
    }

    updateSchema(fields) {
        this.fields = fields || [];
        this.fieldNames.clear();
        
        // Build field name mapping from schema FieldDefinition
        // The fieldId in data messages corresponds to the array index in the schema
        this.fields.forEach((field, index) => {
            this.fieldNames.set(index, field.name);
        });
    }

    setValue(rowId, fieldId, value) {
        if (!this.data.has(rowId)) {
            this.data.set(rowId, new Map());
        }
        this.data.get(rowId).set(fieldId, value);
    }

    getValue(rowId, fieldId) {
        const row = this.data.get(rowId);
        return row ? row.get(fieldId) : undefined;
    }

    getRow(rowId) {
        return this.data.get(rowId);
    }

    getAllRows() {
        return Array.from(this.data.keys());
    }

    toHTML() {
        if (this.data.size === 0) {
            return '<p>No data available</p>';
        }

        // Get all unique field IDs
        const allFieldIds = new Set();
        this.data.forEach(row => {
            row.forEach((value, fieldId) => {
                allFieldIds.add(fieldId);
            });
        });

        const fieldIdArray = Array.from(allFieldIds).sort((a, b) => a - b);
        
        let html = '<table><thead><tr><th>Row ID</th>';
        fieldIdArray.forEach(fieldId => {
            const fieldName = this.fieldNames.get(fieldId) || `Field ${fieldId}`;
            html += `<th>${fieldName} (${fieldId})</th>`;
        });
        html += '</tr></thead><tbody>';

        // Sort rows by ID
        const sortedRowIds = Array.from(this.data.keys()).sort((a, b) => a - b);
        
        sortedRowIds.forEach(rowId => {
            html += `<tr><td>${rowId}</td>`;
            const row = this.data.get(rowId);
            fieldIdArray.forEach(fieldId => {
                const value = row.get(fieldId);
                const displayValue = value !== undefined ? this.formatValue(value) : '';
                html += `<td>${displayValue}</td>`;
            });
            html += '</tr>';
        });

        html += '</tbody></table>';
        return html;
    }

    formatValue(value) {
        if (value === null || value === undefined) {
            return '';
        }
        if (typeof value === 'object' && value.constructor === Uint8Array) {
            return `[${value.length} bytes]`;
        }
        if (typeof value === 'number') {
            return value.toLocaleString();
        }
        return String(value);
    }
}

// Global client instance
const spinelClient = new SpinelClient();

// Global functions for UI
function connect() {
    spinelClient.connect();
}

function disconnect() {
    spinelClient.disconnect();
}

function subscribeToOrders() {
    spinelClient.subscribe('orders');
}

function subscribeToInstruments() {
    spinelClient.subscribe('instruments');
}

function subscribeToOrderView() {
    spinelClient.subscribe('order-view');
}

// Initialize when page loads
document.addEventListener('DOMContentLoaded', () => {
    spinelClient.log('Spinel Client initialized', 'success');
});