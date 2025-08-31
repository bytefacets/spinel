// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT

/**
 * Spinel WebSocket Client Library
 *
 * A library for connecting to Spinel data services via WebSocket and handling
 * protobuf-encoded subscription requests and responses.
 *
 * Follows the OutputManager/TransformInput/TransformOutput pattern from the Java library.
 * Uses columnar data storage where each field stores its own data indexed by row.
 */

/**
 * Event types that can be emitted by the SpinelClient
 */
const SpinelEvents = {
    CONNECTED: 'connected',
    DISCONNECTED: 'disconnected',
    ERROR: 'error',
    SERVER_RESPONSE: 'serverResponse'
};

/**
 * Field interface for storing columnar data
 */
class Field {
    constructor(typeId) {
        this.typeId = typeId;
        this.data = []; // Columnar storage indexed by row
    }

    /**
     * Get value at specific row
     * @param {number} row - Row index
     * @returns {*} - Value at row
     */
    objectValueAt(row) {
        return this.data[row];
    }

    /**
     * Set value at specific row
     * @param {number} row - Row index
     * @param {*} value - Value to set
     */
    setValueAt(row, value) {
        this.data[row] = value;
    }

    /**
     * Get the type ID of this field
     * @returns {number} - Type ID
     */
    getTypeId() {
        return this.typeId;
    }

    /**
     * Clear all data
     */
    clear() {
        this.data.length = 0;
    }
}

/**
 * SchemaField combines field metadata with the actual data storage
 */
class SchemaField {
    constructor(fieldId, name, field) {
        this.fieldId = fieldId;
        this.name = name;
        this.field = field;
    }

    /**
     * Get the field ID
     * @returns {number} - Field ID
     */
    getFieldId() {
        return this.fieldId;
    }

    /**
     * Get the field name
     * @returns {string} - Field name
     */
    getName() {
        return this.name;
    }

    /**
     * Get the underlying field storage
     * @returns {Field} - Field storage
     */
    getField() {
        return this.field;
    }

    /**
     * Get value at specific row
     * @param {number} row - Row index
     * @returns {*} - Value at row
     */
    objectValueAt(row) {
        return this.field.objectValueAt(row);
    }

    /**
     * Set value at specific row
     * @param {number} row - Row index
     * @param {*} value - Value to set
     */
    setValueAt(row, value) {
        this.field.setValueAt(row, value);
    }
}

/**
 * Interface for receiving data updates (similar to TransformInput)
 */
class TransformInput {
    /**
     * Called when the source output is set
     * @param {TransformOutput|null} output - The source output or null when detached
     */
    setSource(output) {
        // Override in subclass
    }

    /**
     * Called when the schema is updated
     * @param {SchemaField[]} schemaFields - Array of SchemaField objects or null when cleared
     */
    schemaUpdated(schemaFields) {
        throw new Error('schemaUpdated must be implemented by subclass');
    }

    /**
     * Called when rows are added
     * @param {number[]} rows - Array of row IDs that were added
     */
    rowsAdded(rows) {
        throw new Error('rowsAdded must be implemented by subclass');
    }

    /**
     * Called when rows are changed
     * @param {number[]} rows - Array of row IDs that were changed
     * @param {Set} changedFields - Set of field IDs that changed
     */
    rowsChanged(rows, changedFields) {
        throw new Error('rowsChanged must be implemented by subclass');
    }

    /**
     * Called when rows are removed
     * @param {number[]} rows - Array of row IDs that were removed
     */
    rowsRemoved(rows) {
        throw new Error('rowsRemoved must be implemented by subclass');
    }
}

/**
 * Interface for providing data (similar to TransformOutput)
 */
class TransformOutput {
    /**
     * Attach an input to receive updates from this output
     * @param {TransformInput} input - The input to attach
     */
    attachInput(input) {
        throw new Error('attachInput must be implemented by subclass');
    }

    /**
     * Detach an input from this output
     * @param {TransformInput} input - The input to detach
     */
    detachInput(input) {
        throw new Error('detachInput must be implemented by subclass');
    }

    /**
     * Get the current schema fields
     * @returns {SchemaField[]|null} - Array of SchemaField objects or null if not available
     */
    getSchemaFields() {
        throw new Error('getSchemaFields must be implemented by subclass');
    }

    /**
     * Get a specific schema field by field ID
     * @param {number} fieldId - Field ID
     * @returns {SchemaField|null} - SchemaField or null if not found
     */
    getSchemaField(fieldId) {
        throw new Error('getSchemaField must be implemented by subclass');
    }
}

/**
 * Subscription output that manages inputs and provides columnar data storage
 */
class SubscriptionOutput extends TransformOutput {
    constructor(subscriptionId, tableName) {
        super();
        this.subscriptionId = subscriptionId;
        this.tableName = tableName;
        this.inputs = new Set();
        this.schemaFields = []; // Array of SchemaField objects, indexed by fieldId
        this.rowIds = new Set(); // Set of active row IDs
    }

    attachInput(input) {
        if (!this.inputs.has(input)) {
            this.inputs.add(input);
            this._initializeInput(input);
        }
    }

    detachInput(input) {
        if (this.inputs.has(input)) {
            this.inputs.delete(input);
            input.setSource(null);
        }
    }

    getSchemaFields() {
        return this.schemaFields;
    }

    getSchemaField(fieldId) {
        return this.schemaFields[fieldId] || null;
    }

    /**
     * Get value at specific row and field
     * @param {number} rowId - Row ID
     * @param {number} fieldId - Field ID
     * @returns {*} - Value or undefined if not found
     */
    getValue(rowId, fieldId) {
        const schemaField = this.getSchemaField(fieldId);
        return schemaField ? schemaField.objectValueAt(rowId) : undefined;
    }

    /**
     * Get all row IDs
     * @returns {number[]} - Array of row IDs
     */
    getAllRowIds() {
        return Array.from(this.rowIds);
    }

    // Internal methods for managing the subscription

    _initializeInput(input) {
        input.setSource(this);
        if (this.schemaFields.length > 0) {
            input.schemaUpdated(this.schemaFields);
            if (this.rowIds.size > 0) {
                input.rowsAdded(Array.from(this.rowIds));
            }
        }
    }

    _updateSchema(schema) {
        // Clear existing schema
        this.schemaFields = [];

        // Create SchemaField objects from protobuf schema
        if (schema.fields) {
            schema.fields.forEach((fieldDef, index) => {
                const field = new Field(fieldDef.typeId || 0);
                const schemaField = new SchemaField(index, fieldDef.name, field);
                this.schemaFields[index] = schemaField; // Direct array indexing since fieldIds are compact
            });
        }

        // Notify inputs
        this.inputs.forEach(input => {
            input.schemaUpdated(this.schemaFields);
        });
    }

    _processDataUpdate(dataUpdate, responseType) {
        const changedFields = new Set();

        // Process each data type directly into columnar storage (process only once)
        const dataTypes = [
            { data: dataUpdate.boolData, type: 'bool' },
            { data: dataUpdate.byteData, type: 'byte' },
            { data: dataUpdate.int32Data, type: 'int32' },
            { data: dataUpdate.int64Data, type: 'int64' },
            { data: dataUpdate.floatData, type: 'float' },
            { data: dataUpdate.doubleData, type: 'double' },
            { data: dataUpdate.stringData, type: 'string' },
            { data: dataUpdate.genericData, type: 'generic' }
        ];

        dataTypes.forEach(({ data, type }) => {
            if (data && data.length > 0) {
                data.forEach(fieldData => {
                    const fieldId = fieldData.fieldId;
                    const schemaField = this.schemaFields[fieldId]; // Direct array access
                    
                    if (schemaField) {
                        changedFields.add(fieldId);
                        
                        // Store data directly in the field's columnar storage
                        dataUpdate.rows.forEach((rowId, rowIndex) => {
                            let value;
                            
                            if (type === 'byte' || type === 'generic') {
                                value = fieldData.values; // Keep as bytes
                            } else if (Array.isArray(fieldData.values)) {
                                value = fieldData.values[rowIndex];
                            } else {
                                value = fieldData.values;
                            }
                            
                            if (value !== undefined) {
                                schemaField.setValueAt(rowId, value);
                                this.rowIds.add(rowId);
                            }
                        });
                    }
                });
            }
        });

        // Notify inputs based on response type
        this.inputs.forEach(input => {
            switch (responseType) {
                case 'RESPONSE_TYPE_ADD':
                case 'RESPONSE_TYPE_INIT':
                    input.rowsAdded(dataUpdate.rows);
                    break;
                case 'RESPONSE_TYPE_CHG':
                    input.rowsChanged(dataUpdate.rows, changedFields);
                    break;
                case 'RESPONSE_TYPE_REM':
                    // Remove from row tracking
                    dataUpdate.rows.forEach(rowId => this.rowIds.delete(rowId));
                    input.rowsRemoved(dataUpdate.rows);
                    break;
                default:
                    // Default to change notification
                    input.rowsChanged(dataUpdate.rows, changedFields);
            }
        });
    }
}

/**
 * Main Spinel client class for handling WebSocket connections and protobuf messaging
 */
class SpinelClient {
    /**
     * Create a new SpinelClient instance
     * @param {Object} options - Configuration options
     * @param {string} [options.wsUrl] - WebSocket URL (defaults to current host)
     * @param {string} [options.protoPath='data-service.proto'] - Path to protobuf definition file
     * @param {boolean} [options.autoReconnect=false] - Whether to automatically reconnect on disconnect
     * @param {Function} [options.logger] - Custom logging function
     */
    constructor(options = {}) {
        this.options = {
            wsUrl: null, // Will be auto-generated if not provided
            protoPath: 'data-service.proto',
            autoReconnect: false,
            logger: console.log,
            ...options
        };

        this.websocket = null;
        this.protobufRoot = null;
        this.subscriptionId = 1;
        this.msgToken = 1;
        this.isConnected = false;
        this.subscriptions = new Map(); // Map of subscriptionId -> SubscriptionOutput
        this.eventListeners = new Map(); // Map of event type -> array of listeners

        // Protobuf message types (will be loaded during initialization)
        this.SubscriptionRequest = null;
        this.SubscriptionResponse = null;
        this.CreateSubscription = null;
        this.RequestType = null;
        this.ResponseType = null;
    }

    /**
     * Initialize the client by loading protobuf definitions
     * @returns {Promise<void>}
     */
    async initialize() {
        try {
            this.protobufRoot = await protobuf.load(this.options.protoPath);
            this.SubscriptionRequest = this.protobufRoot.lookupType('com.bytefacets.spinel.grpc.proto.SubscriptionRequest');
            this.SubscriptionResponse = this.protobufRoot.lookupType('com.bytefacets.spinel.grpc.proto.SubscriptionResponse');
            this.CreateSubscription = this.protobufRoot.lookupType('com.bytefacets.spinel.grpc.proto.CreateSubscription');
            this.RequestType = this.protobufRoot.lookupEnum('com.bytefacets.spinel.grpc.proto.RequestType');
            this.ResponseType = this.protobufRoot.lookupEnum('com.bytefacets.spinel.grpc.proto.ResponseType');
            
            this._log('Protobuf definitions loaded successfully');
        } catch (error) {
            this._emit(SpinelEvents.ERROR, { type: 'protobuf_load_error', message: error.message, error });
            throw error;
        }
    }

    /**
     * Connect to the WebSocket server
     * @returns {Promise<void>}
     */
    connect() {
        return new Promise((resolve, reject) => {
            if (this.websocket && this.websocket.readyState === WebSocket.OPEN) {
                resolve();
                return;
            }

            if (!this.SubscriptionRequest) {
                reject(new Error('Client not initialized. Call initialize() first.'));
                return;
            }

            const wsUrl = this.options.wsUrl || this._generateWebSocketUrl();
            this._log(`Connecting to ${wsUrl}...`);

            this.websocket = new WebSocket(wsUrl);
            this.websocket.binaryType = 'arraybuffer';

            this.websocket.onopen = () => {
                this.isConnected = true;
                this._log('WebSocket connected');
                this._emit(SpinelEvents.CONNECTED);
                resolve();
            };

            this.websocket.onmessage = (event) => {
                this._handleMessage(event.data);
            };

            this.websocket.onclose = () => {
                this.isConnected = false;
                this._log('WebSocket disconnected');
                this._emit(SpinelEvents.DISCONNECTED);
                
                if (this.options.autoReconnect) {
                    setTimeout(() => this.connect(), 1000);
                }
            };

            this.websocket.onerror = (error) => {
                this._log(`WebSocket error: ${error}`);
                this._emit(SpinelEvents.ERROR, { type: 'websocket_error', error });
                reject(error);
            };
        });
    }

    /**
     * Disconnect from the WebSocket server
     */
    disconnect() {
        if (this.websocket) {
            this.websocket.close();
            this.websocket = null;
        }
        this.isConnected = false;
    }

    /**
     * Subscribe to a data table
     * @param {string} tableName - Name of the table to subscribe to
     * @param {Object} [options] - Subscription options
     * @param {string[]} [options.fieldNames] - Specific field names to subscribe to (empty = all fields)
     * @param {boolean} [options.defaultAll=true] - Whether to include all fields by default
     * @param {Array} [options.modifications] - Subscription modifications
     * @returns {SubscriptionOutput} - The subscription output that can be attached to inputs
     */
    subscribe(tableName, options = {}) {
        if (!this.isConnected || !this.SubscriptionRequest) {
            throw new Error('Not connected or not initialized');
        }

        const currentSubscriptionId = this.subscriptionId++;
        const subscriptionOptions = {
            fieldNames: [],
            defaultAll: true,
            modifications: [],
            ...options
        };

        // Create subscription output
        const subscriptionOutput = new SubscriptionOutput(currentSubscriptionId, tableName);
        this.subscriptions.set(currentSubscriptionId, subscriptionOutput);

        const createSubscription = this.CreateSubscription.create({
            name: tableName,
            fieldNames: subscriptionOptions.fieldNames,
            defaultAll: subscriptionOptions.defaultAll,
            modifications: subscriptionOptions.modifications
        });

        const request = this.SubscriptionRequest.create({
            msgToken: this.msgToken++,
            subscriptionId: currentSubscriptionId,
            requestType: this.RequestType.values.REQUEST_TYPE_SUBSCRIBE,
            subscription: createSubscription
        });

        const buffer = this.SubscriptionRequest.encode(request).finish();
        this.websocket.send(buffer);

        this._log(`Subscription request sent for table: ${tableName} (subscriptionId: ${currentSubscriptionId})`);
        return subscriptionOutput;
    }

    /**
     * Add an event listener
     * @param {string} eventType - Type of event (use SpinelEvents constants)
     * @param {Function} listener - Event listener function
     */
    on(eventType, listener) {
        if (!this.eventListeners.has(eventType)) {
            this.eventListeners.set(eventType, []);
        }
        this.eventListeners.get(eventType).push(listener);
    }

    /**
     * Remove an event listener
     * @param {string} eventType - Type of event
     * @param {Function} listener - Event listener function to remove
     */
    off(eventType, listener) {
        if (this.eventListeners.has(eventType)) {
            const listeners = this.eventListeners.get(eventType);
            const index = listeners.indexOf(listener);
            if (index > -1) {
                listeners.splice(index, 1);
            }
        }
    }

    /**
     * Get subscription output by subscription ID
     * @param {number} subscriptionId - The subscription ID
     * @returns {SubscriptionOutput|null} - Subscription output or null if not found
     */
    getSubscription(subscriptionId) {
        return this.subscriptions.get(subscriptionId) || null;
    }

    /**
     * Get all active subscriptions
     * @returns {Map} - Map of subscriptionId -> SubscriptionOutput
     */
    getAllSubscriptions() {
        return new Map(this.subscriptions);
    }

    /**
     * Check if the client is connected
     * @returns {boolean}
     */
    isClientConnected() {
        return this.isConnected;
    }

    // Private methods

    _generateWebSocketUrl() {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        return `${protocol}//${window.location.host}/ws/spinel`;
    }

    _handleMessage(data) {
        try {
            const uint8Array = new Uint8Array(data);
            const response = this.SubscriptionResponse.decode(uint8Array);

            this._log(`Received response: type=${response.responseType}, subscriptionId=${response.subscriptionId}`);

            if (response.schema) {
                this._handleSchemaUpdate(response.schema, response.subscriptionId);
            }

            if (response.data) {
                this._handleDataUpdate(response.data, response.subscriptionId, response.responseType);
            }

            if (response.response) {
                this._emit(SpinelEvents.SERVER_RESPONSE, {
                    subscriptionId: response.subscriptionId,
                    success: response.response.success,
                    message: response.response.message,
                    response: response.response
                });
            }

        } catch (error) {
            this._log(`Error parsing message: ${error.message}`);
            this._emit(SpinelEvents.ERROR, { type: 'message_parse_error', message: error.message, error });
        }
    }

    _handleSchemaUpdate(schema, subscriptionId) {
        this._log(`Schema update for subscription ${subscriptionId}, table: ${schema.name}`);

        const subscription = this.subscriptions.get(subscriptionId);
        if (subscription) {
            subscription._updateSchema(schema);
        } else {
            this._log(`Received schema for unknown subscription: ${subscriptionId}`);
        }
    }

    _handleDataUpdate(dataUpdate, subscriptionId, responseType) {
        const subscription = this.subscriptions.get(subscriptionId);
        if (!subscription) {
            this._log(`Subscription ${subscriptionId} not found`);
            return;
        }

        this._log(`Data update for table: ${subscription.tableName}, rows: ${dataUpdate.rows.length}`);

        // Get response type string
        const responseTypeString = this._getResponseTypeString(responseType);

        // Pass dataUpdate directly to subscription for columnar processing
        subscription._processDataUpdate(dataUpdate, responseTypeString);
    }

    _getResponseTypeString(responseType) {
        // Convert numeric response type to string
        const typeMap = {
            0: 'RESPONSE_TYPE_MESSAGE',
            1: 'RESPONSE_TYPE_SCHEMA',
            2: 'RESPONSE_TYPE_ADD',
            3: 'RESPONSE_TYPE_CHG',
            4: 'RESPONSE_TYPE_REM',
            5: 'RESPONSE_TYPE_INIT'
        };
        return typeMap[responseType] || 'RESPONSE_TYPE_MESSAGE';
    }

    _emit(eventType, data) {
        if (this.eventListeners.has(eventType)) {
            this.eventListeners.get(eventType).forEach(listener => {
                try {
                    listener(data);
                } catch (error) {
                    this._log(`Error in event listener for ${eventType}: ${error.message}`);
                }
            });
        }
    }

    _log(message) {
        if (this.options.logger) {
            this.options.logger(`[SpinelClient] ${message}`);
        }
    }
}

// Export for use as a module or global
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { SpinelClient, SpinelEvents, TransformInput, TransformOutput, SubscriptionOutput };
} else {
    window.SpinelClient = SpinelClient;
    window.SpinelEvents = SpinelEvents;
    window.TransformInput = TransformInput;
    window.TransformOutput = TransformOutput;
    window.SubscriptionOutput = SubscriptionOutput;
}