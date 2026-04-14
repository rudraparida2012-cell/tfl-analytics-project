import os
import requests
from dotenv import load_dotenv

load_dotenv()

app_key = os.getenv("TFL_APP_KEY")

url = "https://api.tfl.gov.uk/Line/victoria/Arrivals/940GZZLUVIC"
params = {"app_key": app_key}

response = requests.get(url, params=params, timeout=30)

print("Status Code:", response.status_code)
print("Response Text:", response.text[:1000])
