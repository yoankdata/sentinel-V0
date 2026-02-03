import os
import json
from datetime import datetime, timezone
from dotenv import load_dotenv
from google.cloud import storage
import sys

def debug_setup():
    print("--- 1. LOADING ENVIRONMENT VARIABLES ---")
    load_dotenv()
    api_key = os.getenv("OPENWEATHER_API_KEY")
    bucket_name = os.getenv("GCS_BRONZE_BUCKET")
    
    # Mask key for privacy in logs
    masked_key = f"{api_key[:4]}...{api_key[-4:]}" if api_key and len(api_key) > 8 else "INVALID"
    
    print(f"API Key loaded: {'YES' if api_key else 'NO'} ({masked_key})")
    print(f"Target Bucket: {bucket_name}")
    
    if not bucket_name:
        print("❌ CRITICAL: GCS_BRONZE_BUCKET is missing in .env")
        return

    print("\n--- 2. TESTING GOOGLE CLOUD STORAGE (GCS) ACCESS ---")
    print("Attempting to write a test file to GCS using Application Default Credentials (ADC)...")
    
    dummy_data = {
        "status": "debug_ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "message": "This is a test file to verify GCS permissions from Sentinel."
    }
    
    try:
        # Explicitely look for the project if needed, but usually Client() infers it from ADC
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        
        # Try to list to check read permissions (optional, but good test)
        # print("Checking bucket existence...")
        # if not bucket.exists():
        #    print(f"❌ Bucket {bucket_name} does not exist or is not accessible.")
        #    return

        blob = bucket.blob("debug/connection_test.json")
        blob.upload_from_string(json.dumps(dummy_data), content_type="application/json")
        
        print(f"✅ SUCCESS: Wrote successfully to gs://{bucket_name}/debug/connection_test.json")
        print("Your Google Cloud credentials are working correctly.")
        
    except Exception as e:
        print(f"\n❌ GCS ERROR: Could not write to bucket.")
        print(f"Details: {e}")
        print("\n--- TROUBLESHOOTING ---")
        print("1. Have you run: 'gcloud auth application-default login' ?")
        print("2. Does the bucket 'sentinel-bronze' exist in your project?")
        print("3. Does your account have 'Storage Object Admin' or similar permissions?")

if __name__ == "__main__":
    debug_setup()
