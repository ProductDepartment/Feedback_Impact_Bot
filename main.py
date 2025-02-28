import base64
import json
import os
import requests
import urllib.parse

oauth_client_id = "15ed872b-594c-811f-a894-0037a8e26960"
oauth_client_secret = "secret_XZsJnPBFxBOwqslI1uAjSMBz3v9WvLHRGqqqn4Sdiod"
redirect_uri = "https://www.impact-admissions.com"

parsed_redirect_uri = urllib.parse.quote_plus(redirect_uri)

auth_url = f"https://api.notion.com/v1/oauth/authorize?owner=user&client_id={oauth_client_id}&redirect_uri={parsed_redirect_uri}&response_type=code"
print(auth_url)

redirect_url_response = "https://www.impact-admissions.com/?code=035a4898-9896-41e0-b838-67fc69a3d1cd&state="

auth_code = redirect_url_response.split('code=')[-1].split('&state=')[0]
print(auth_code)

key_secret = '{}:{}'.format(oauth_client_id, oauth_client_secret).encode('ascii')
b64_encoded_key = base64.b64encode(key_secret)
b64_encoded_key = b64_encoded_key.decode('ascii')

base_url = 'https://api.notion.com/v1/oauth/token'

auth_headers = {
    'Authorization': 'Basic {}'.format(b64_encoded_key),
    'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
}

auth_data = {
    'grant_type': 'authorization_code',
    'code': auth_code,
    'redirect_uri':redirect_uri,
}

auth_resp = requests.post(base_url, headers=auth_headers, data=auth_data)
print(auth_resp.json())

