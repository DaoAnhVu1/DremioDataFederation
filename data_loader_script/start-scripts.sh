#!/bin/sh

# Wait for Dremio to be ready
echo "Waiting for Dremio to be ready..."

# Run the SQL script
echo "Running SQL script..."
node script.sql.js

# Check if the SQL script ran successfully
if [ $? -eq 0 ]; then
    echo "SQL script ran successfully."
else
    echo "SQL script failed."
    exit 1
fi

# Run the PostgreSQL script
echo "Running PostgreSQL script..."
node script.postgres.js

# Check if the PostgreSQL script ran successfully
if [ $? -eq 0 ]; then
    echo "PostgreSQL script ran successfully."
else
    echo "PostgreSQL script failed."
    exit 1
fi