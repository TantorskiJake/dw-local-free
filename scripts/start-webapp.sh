#!/bin/bash
# Start the Flask web application

cd "$(dirname "$0")/.." || exit

# Activate virtual environment if it exists
if [ -d ".venv" ]; then
    source .venv/bin/activate
fi

# Start Flask app
cd webapp || exit
python app.py

