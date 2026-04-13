import os
from google.cloud import aiplatform
from google.oauth2 import service_account

# Show what Python sees
print("GOOGLE_APPLICATION_CREDENTIALS =", os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))

# Load credentials explicitly from the JSON file
creds = service_account.Credentials.from_service_account_file(
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
)

# Initialize Vertex AI client
aiplatform.init(
    project="alpine-anvil-477306",   # must match your project ID
    location="us-central1",          # region
    credentials=creds
)

# Try listing models
try:
    models = aiplatform.Model.list()
    print("✅ Vertex AI connection succeeded.")
    print(f"Found {len(models)} models.")
    for m in models[:5]:
        print(m.resource_name)
except Exception as e:
    print("❌ Vertex AI connection failed.")
    print(e)