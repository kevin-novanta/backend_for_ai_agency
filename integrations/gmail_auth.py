

import os
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

SCOPES = ['https://www.googleapis.com/auth/gmail.send']
CREDENTIALS_PATH = "Creds/credentials.json"
TOKEN_PATH = "Creds/token.json"

def authenticate_gmail():
    creds = None

    # Load existing token if available
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)

    # If no valid token, go through OAuth2 flow
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_PATH, SCOPES)
            creds = flow.run_local_server(port=0)

        # Save the token for future use
        with open(TOKEN_PATH, 'w') as token_file:
            token_file.write(creds.to_json())

    print("✅ Gmail authenticated and token saved.")

if __name__ == "__main__":
    authenticate_gmail()