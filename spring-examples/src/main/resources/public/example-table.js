// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT

/**
 * Example table implementation using the Spinel library
 * This demonstrates how to use the SpinelClient library with a custom table implementation
 * following the TransformInput/TransformOutput pattern
 */

/**
 * Simple table implementation that extends TransformInput
 */
class ExampleTableInput extends TransformInput {
    constructor(name) {
        super();
        this.name = name;
        this.source = null;
        this.schemaFields = []; // Array of SchemaField objects
        this.fieldNames = new Map(); // Map of fieldId -> fieldName
        this.fieldCount = 0; // Number of fields in the schema
        this.onUpdateCallback = null;
    }

    /**
     * Set a callback to be called when the table is updated
     * @param {Function} callback - Callback function
     */
    setUpdateCallback(callback) {
        this.onUpdateCallback = callback;
    }

    setSource(output) {
        this.source = output;
    }

    schemaUpdated(schemaFields) {
        this.schemaFields = schemaFields || [];
        this.fieldNames.clear();
        this.fieldCount = this.schemaFields.length;
        
        // Build field name mapping from SchemaField objects
        this.schemaFields.forEach((schemaField, index) => {
            this.fieldNames.set(index, schemaField.getName());
        });
        
        this._notifyUpdate('schema');
    }

    rowsAdded(rows) {
        this._notifyUpdate('add', rows);
    }

    rowsChanged(rows, changedFields) {
        this._notifyUpdate('change', rows, changedFields);
    }

    rowsRemoved(rows) {
        this._notifyUpdate('remove', rows);
    }

    /**
     * Notify callback of updates
     * @param {string} type - Type of update
     * @param {Array} rows - Affected rows
     * @param {Set} changedFields - Changed fields (for change events)
     */
    _notifyUpdate(type, rows = [], changedFields = null) {
        if (this.onUpdateCallback) {
            this.onUpdateCallback({
                type,
                table: this,
                rows,
                changedFields
            });
        }
    }

    /**
     * Get a value for a specific row and field
     * @param {number} rowId - Row identifier
     * @param {number} fieldId - Field identifier
     * @returns {*} - The value or undefined if not found
     */
    getValue(rowId, fieldId) {
        return this.source ? this.source.getValue(rowId, fieldId) : undefined;
    }

    /**
     * Get all data for a specific row
     * @param {number} rowId - Row identifier
     * @returns {Array} - Array where fieldId is the index
     */
    getRow(rowId) {
        if (!this.source) return [];
        
        const row = [];
        for (let fieldId = 0; fieldId < this.fieldCount; fieldId++) {
            row[fieldId] = this.source.getValue(rowId, fieldId);
        }
        return row;
    }

    /**
     * Get all row IDs
     * @returns {number[]} - Array of row IDs
     */
    getAllRows() {
        return this.source ? this.source.getAllRowIds() : [];
    }

    /**
     * Generate HTML representation of the table
     * @returns {string} - HTML string
     */
    toHTML() {
        const allRows = this.getAllRows();
        if (allRows.length === 0) {
            return '<p>No data available</p>';
        }

        // Get all field IDs from schema (0 to fieldCount-1)
        const fieldIdArray = [];
        for (let i = 0; i < this.fieldCount; i++) {
            fieldIdArray.push(i);
        }
        
        let html = '<table><thead><tr><th>Row ID</th>';
        fieldIdArray.forEach(fieldId => {
            const fieldName = this.fieldNames.get(fieldId) || `Field ${fieldId}`;
            html += `<th>${fieldName} (${fieldId})</th>`;
        });
        html += '</tr></thead><tbody>';

        // Sort rows by ID
        const sortedRowIds = allRows.sort((a, b) => a - b);
        
        sortedRowIds.forEach(rowId => {
            html += `<tr><td>${rowId}</td>`;
            fieldIdArray.forEach(fieldId => {
                const value = this.getValue(rowId, fieldId);
                const displayValue = value !== undefined ? this.formatValue(value) : '';
                html += `<td>${displayValue}</td>`;
            });
            html += '</tr>';
        });

        html += '</tbody></table>';
        return html;
    }

    /**
     * Format a value for display
     * @param {*} value - Value to format
     * @returns {string} - Formatted value
     */
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

    /**
     * Clear all data from the table
     */
    clear() {
        // Clear data from source's columnar storage
        if (this.source) {
            this.schemaFields.forEach(schemaField => {
                schemaField.getField().clear();
            });
        }
        this._notifyUpdate('clear');
    }

    /**
     * Get table statistics
     * @returns {Object} - Statistics object
     */
    getStats() {
        return {
            name: this.name,
            rowCount: this.getAllRows().length,
            fieldCount: this.fieldNames.size,
            hasSchema: this.schemaFields.length > 0,
            hasSource: !!this.source
        };
    }
}

/**
 * Example application that demonstrates using the Spinel library
 */
class ExampleApplication {
    constructor() {
        this.client = null;
        this.tables = new Map(); // Map of table name -> ExampleTableInput
        this.subscriptions = new Map(); // Map of table name -> SubscriptionOutput
        this.isInitialized = false;
    }

    /**
     * Initialize the application
     */
    async initialize() {
        try {
            // Create and initialize the Spinel client
            this.client = new SpinelClient({
                logger: (message) => this.log(message, 'info')
            });

            await this.client.initialize();

            // Set up event listeners for connection events
            this.client.on(SpinelEvents.CONNECTED, () => {
                this.log('Connected to Spinel server', 'success');
                this.updateConnectionStatus(true);
            });

            this.client.on(SpinelEvents.DISCONNECTED, () => {
                this.log('Disconnected from Spinel server', 'info');
                this.updateConnectionStatus(false);
            });

            this.client.on(SpinelEvents.ERROR, (errorData) => {
                this.log(`Error: ${errorData.message}`, 'error');
            });

            this.client.on(SpinelEvents.SERVER_RESPONSE, (response) => {
                this.log(`Server response: ${response.success ? 'SUCCESS' : 'FAILURE'} - ${response.message}`, 
                         response.success ? 'success' : 'error');
            });

            this.isInitialized = true;
            this.log('Application initialized successfully', 'success');

        } catch (error) {
            this.log(`Failed to initialize: ${error.message}`, 'error');
            throw error;
        }
    }

    /**
     * Connect to the server
     */
    async connect() {
        if (!this.isInitialized) {
            throw new Error('Application not initialized');
        }
        await this.client.connect();
    }

    /**
     * Disconnect from the server
     */
    disconnect() {
        if (this.client) {
            this.client.disconnect();
        }
    }

    /**
     * Subscribe to a table
     * @param {string} tableName - Name of the table to subscribe to
     * @returns {ExampleTableInput} - The table input that will receive updates
     */
    subscribeToTable(tableName) {
        if (!this.client || !this.client.isClientConnected()) {
            throw new Error('Not connected to server');
        }

        // Create table input
        const tableInput = new ExampleTableInput(tableName);
        tableInput.setUpdateCallback((updateInfo) => {
            this.handleTableUpdate(updateInfo);
        });

        // Subscribe and get the output
        const subscriptionOutput = this.client.subscribe(tableName);
        
        // Attach the input to the output
        subscriptionOutput.attachInput(tableInput);

        // Store references
        this.tables.set(tableName, tableInput);
        this.subscriptions.set(tableName, subscriptionOutput);

        this.log(`Subscribed to table: ${tableName}`, 'info');
        return tableInput;
    }

    /**
     * Unsubscribe from a table
     * @param {string} tableName - Name of the table to unsubscribe from
     */
    unsubscribeFromTable(tableName) {
        const tableInput = this.tables.get(tableName);
        const subscriptionOutput = this.subscriptions.get(tableName);

        if (tableInput && subscriptionOutput) {
            subscriptionOutput.detachInput(tableInput);
            this.tables.delete(tableName);
            this.subscriptions.delete(tableName);
            this.log(`Unsubscribed from table: ${tableName}`, 'info');
            this.updateTableDisplay();
        }
    }

    /**
     * Handle table updates
     * @param {Object} updateInfo - Update information
     */
    handleTableUpdate(updateInfo) {
        const { type, table, rows, changedFields } = updateInfo;
        
        switch (type) {
            case 'schema':
                this.log(`Schema updated for table: ${table.name}`, 'info');
                break;
            case 'add':
                this.log(`Rows added to table: ${table.name}, count: ${rows.length}`, 'debug');
                break;
            case 'change':
                this.log(`Rows changed in table: ${table.name}, count: ${rows.length}`, 'debug');
                break;
            case 'remove':
                this.log(`Rows removed from table: ${table.name}, count: ${rows.length}`, 'debug');
                break;
        }
        
        this.updateTableDisplay();
    }

    /**
     * Update the connection status in the UI
     * @param {boolean} connected - Whether connected or not
     */
    updateConnectionStatus(connected) {
        const statusElement = document.getElementById('status');
        const connectBtn = document.getElementById('connectBtn');
        const disconnectBtn = document.getElementById('disconnectBtn');
        const subscribeButtons = document.querySelectorAll('[id$="Btn"]:not(#connectBtn):not(#disconnectBtn)');
        
        if (statusElement) {
            if (connected) {
                statusElement.textContent = 'Connected';
                statusElement.className = 'status connected';
                if (connectBtn) connectBtn.disabled = true;
                if (disconnectBtn) disconnectBtn.disabled = false;
                subscribeButtons.forEach(btn => btn.disabled = false);
            } else {
                statusElement.textContent = 'Disconnected';
                statusElement.className = 'status disconnected';
                if (connectBtn) connectBtn.disabled = false;
                if (disconnectBtn) disconnectBtn.disabled = true;
                subscribeButtons.forEach(btn => btn.disabled = true);
            }
        }
    }

    /**
     * Update the table display in the UI
     */
    updateTableDisplay() {
        const tablesContainer = document.getElementById('tables');
        if (!tablesContainer) return;
        
        tablesContainer.innerHTML = '';
        
        this.tables.forEach((table, tableName) => {
            const stats = table.getStats();
            const tableDiv = document.createElement('div');
            tableDiv.innerHTML = `
                <h4>${tableName} (${stats.rowCount} rows, ${stats.fieldCount} fields)</h4>
                ${table.toHTML()}
            `;
            tablesContainer.appendChild(tableDiv);
        });
    }

    /**
     * Log a message to the UI and console
     * @param {string} message - Message to log
     * @param {string} type - Type of message (info, error, success, debug)
     */
    log(message, type = 'info') {
        const logElement = document.getElementById('log');
        if (logElement) {
            const timestamp = new Date().toLocaleTimeString();
            const logEntry = document.createElement('div');
            logEntry.className = `log-entry ${type}`;
            logEntry.textContent = `[${timestamp}] ${message}`;
            logElement.appendChild(logEntry);
            logElement.scrollTop = logElement.scrollHeight;
        }
        
        console.log(`[${type.toUpperCase()}] ${message}`);
    }

    /**
     * Get application statistics
     * @returns {Object} - Statistics object
     */
    getStats() {
        return {
            isInitialized: this.isInitialized,
            isConnected: this.client ? this.client.isClientConnected() : false,
            tableCount: this.tables.size,
            tables: Array.from(this.tables.values()).map(table => table.getStats())
        };
    }
}

// Export for use as a module or global
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { ExampleTableInput, ExampleApplication };
} else {
    window.ExampleTableInput = ExampleTableInput;
    window.ExampleApplication = ExampleApplication;
}