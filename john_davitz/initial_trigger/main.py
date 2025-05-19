import requests
import google.auth
import google.auth.transport.requests

def call_other_service(url):
    #url = "https://your-service-abc123.a.run.app"  # target Cloud Run service
    print(f"calling {url}")
    data = {"message": "Hello from another service!"}
    response = requests.post(url, json=data)
    print(response.status_code, response.text)

def run_cloud_run_job(job_name):
    print(f"Calling {job_name}")
    project = "instfinal-459621"
    location = "us-central1"

    # Construct the Cloud Run Jobs API URL
    url = f"https://{location}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/{project}/jobs/{job_name}:run"

    # Get an access token from ADC (Application Default Credentials)
    credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)
    access_token = credentials.token

    # Call the Cloud Run Jobs API
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    response = requests.post(url, headers=headers)

    return {
        "status": response.status_code,
        "body": response.text
    }

def fetch_and_upload(request):
    #weather
    call_other_service("https://fetch-weather-api-589971297968.us-central1.run.app")

    #traffic 
    call_other_service("https://fetch-traffic-api-589971297968.us-central1.run.app")

    #metro
    run_cloud_run_job("cloud-metro")
    run_cloud_run_job('cloud-metro2')

    return "done"