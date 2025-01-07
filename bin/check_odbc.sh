#!/bin/bash

# Configuration
DRIVER_PATH="/opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.3.dylib"
SERVER="localhost"
DATABASE="master"
USER="sa"
DSN="sqlserver"

check_unixodbc() {
    if ! command -v odbcinst &> /dev/null; then
        echo "❌ unixODBC not found"
        return 1
    fi
    echo "✅ unixODBC installed: $(odbcinst --version)"
    return 0
}

check_driver() {
    if [ -f "$DRIVER_PATH" ]; then
        echo "✅ SQL Server driver found at $DRIVER_PATH"
        return 0
    fi
    echo "❌ SQL Server driver not found at $DRIVER_PATH"
    return 1
}

check_config() {
    local odbc_ini="/opt/homebrew/etc/odbcinst.ini"
    if [ -f "$odbc_ini" ]; then
        echo "✅ ODBC configuration found:"
        cat "$odbc_ini"
    else
        echo "❌ No ODBC configuration at $odbc_ini"
        return 1
    fi
}

test_connection() {
    echo "Testing connection to $SERVER..."
    if echo "SELECT @@version;" | isql -v $DSN $USER $1 > /dev/null 2>&1; then
        echo "✅ Connection successful"
        echo "Available databases:"
        echo "SELECT name FROM sys.databases;" | isql -v $DSN $USER $1
        return 0
    else
        echo "❌ Connection failed"
        return 1
    fi
}

main() {
    echo "=== ODBC Environment Check ==="
    check_unixodbc
    echo -e "\n=== Driver Check ==="
    check_driver
    echo -e "\n=== Configuration Check ==="
    check_config
    
    if [ $# -eq 1 ]; then
        echo -e "\n=== Connection Test ==="
        test_connection "$1"
    else
        echo -e "\n❌ Password required for connection test"
        echo "Usage: $0 <password>"
    fi
}

main "$@"