from fastapi import FastAPI, Request
import requests
import os
from dotenv import load_dotenv
from aws_requests_auth.aws_auth import AWSRequestsAuth
from datetime import datetime, timedelta

load_dotenv()

app = FastAPI()

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")

REGION = "us-east-1"
HOST = "sellingpartnerapi-na.amazon.com"
SERVICE = "execute-api"


# 🔑 Get LWA Token
def get_access_token():
    url = "https://api.amazon.com/auth/o2/token"

    headers = {
        "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"
    }

    data = {
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }

    response = requests.post(url, headers=headers, data=data)
    response.raise_for_status()

    return response.json().get("access_token")


# 🔐 AWS Auth
def get_auth():
    return AWSRequestsAuth(
        aws_access_key=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        aws_host=HOST,
        aws_region=REGION,
        aws_service=SERVICE,
    )


# ✅ Health Check
@app.get("/")
def root():
    return {"message": "SP-API running"}


# 🚀 MAIN ENDPOINT (INCREMENTAL + PAGINATION)
@app.get("/getShipments")
def get_shipments(request: Request):
    try:
        access_token = get_access_token()
        auth = get_auth()

        url = f"https://{HOST}/fba/inbound/v0/shipments"

        headers = {
            "x-amz-access-token": access_token,
            "Content-Type": "application/json",
            "x-amz-date": datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        }

        # ✅ Read query param from Make
        last_updated_after = request.query_params.get("lastUpdatedAfter")

        # 👉 Default = last 30 days
        if not last_updated_after:
            last_updated_after = (
                datetime.utcnow() - timedelta(days=30)
            ).strftime('%Y-%m-%dT%H:%M:%SZ')

        all_shipments = []

        statuses = ["WORKING", "SHIPPED", "IN_TRANSIT", "RECEIVING", "CLOSED"]

        for status in statuses:

            params = {
                "ShipmentStatusList": [status],
                "MarketplaceId": "ATVPDKIKX0DER",
                "LastUpdatedAfter": last_updated_after
            }

            next_token = None

            # 🔥 Pagination control
            if status == "CLOSED":
                max_pages = 1
            else:
                max_pages = 2

            page_count = 0

            while page_count < max_pages:
                page_count += 1

                if next_token:
                    params["NextToken"] = next_token

                response = requests.get(
                    url, auth=auth, headers=headers, params=params
                )
                response.raise_for_status()

                data = response.json()

                shipments = data.get("payload", {}).get("ShipmentData", [])
                all_shipments.extend(shipments)

                next_token = data.get("payload", {}).get("NextToken")

                if not next_token:
                    break

        return {
            "count": len(all_shipments),
            "lastUpdatedAfter": last_updated_after,
            "payload": {
                "ShipmentData": all_shipments
            }
        }

    except Exception as e:
        return {"error": str(e)}

