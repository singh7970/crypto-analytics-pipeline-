import sys
import os
import json

# Add project root to sys path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from lambda_handler.handler import lambda_handler

if __name__ == "__main__":
    print("Invoking Ingestion Lambda locally...")
    response = lambda_handler({}, None)
    print("Response:")
    print(json.dumps(response, indent=2))
