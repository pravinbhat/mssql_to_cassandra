#!/bin/bash
# Helper script to run commands with correct Java version for PySpark 4.x

# Set Java 17 for PySpark 4.x compatibility
export JAVA_HOME=/Users/pravinbhat/.sdkman/candidates/java/17.0.16-tem
export PATH=$JAVA_HOME/bin:$HOME/.local/bin:$PATH

# Load environment variables from .env if it exists
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

# Run the command passed as arguments
exec "$@"
