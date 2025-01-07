#!/bin/bash

# Exit on any error
set -e

echo "Step 1: Running data import from SQLite to SQL Server..."
python src/loadin/sqlite_to_sqlserver.py
if [ $? -eq 0 ]; then
    echo "Data import completed successfully!"
else
    echo "Error: Data import failed!"
    exit 1
fi

echo -e "\nStep 2: Creating foreign key relationships..."
python src/loadin/create_foreign_keys.py
if [ $? -eq 0 ]; then
    echo "Foreign key creation completed successfully!"
else
    echo "Error: Foreign key creation failed!"
    exit 1
fi

echo -e "\nAll steps completed successfully!"
