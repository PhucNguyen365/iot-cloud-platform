#!/bin/bash

echo "Setting up project..."

# Copy hook
cp scripts/post-merge.sample .git/hooks/post-merge
chmod +x .git/hooks/post-merge

# Make init script executable
chmod +x scripts/init_server.sh