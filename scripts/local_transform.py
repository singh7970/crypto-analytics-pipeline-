import sys
import os
import json

# Add project root to sys path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from transform_lambda.handler import lambda_handler

if __name__ == "__main__":
    print("Invoking Transform Lambda locally...")
    # Sending an empty event triggers the fallback logic to find today's data in the bronze layer
    event = {}
    response = lambda_handler(event, None)
    print("Response:")
    print(json.dumps(response, indent=2))
