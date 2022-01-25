#!/bin/sh

# Abort on any error (incuding if wait-for fails)
set -e

# Wait for the warehouse to be up
if [ -n "warehouse" ]; then
    ./wait-for.sh "warehouse:5432"
fi 

# Run the main container command
exec "$@"