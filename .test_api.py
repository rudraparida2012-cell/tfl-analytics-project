import os
import requests
from dotenv import load_dotenv

load_dotenv()

app_id = os.getenv("TFL_APP_ID")
app_key = os.getenv("TFL_APP_KEY")

url = f"https://api.tfl.gov.uk/Line/Mode/tube/Status?app_id={app_id}&app_key={app_key}"

response = requests.get(url)

print("Status Code:", response.status_code)

if response.status_code == 200:
    data = response.json()
    print("Number of records:", len(data))
    print("First record:", data[0])
else:
    print("Error response:", response.text)
