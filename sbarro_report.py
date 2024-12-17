import asyncio
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import json
from datetime import datetime, timedelta, timezone
import smtplib
import bson
from dotenv import load_dotenv
import os
from time import time

from fpdf import FPDF, XPos, YPos
from viam.rpc.dial import DialOptions, Credentials
from viam.app.viam_client import ViamClient, DataClient
from viam.app.viam_client import AppClient

load_dotenv()

API_KEY = os.getenv("API_KEY")
API_KEY_ID = os.getenv("API_KEY_ID")
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
LOC_IDS = os.getenv("LOC_IDS").split(",")
ORG_ID = os.getenv("ORG_ID")
PDF_OUTPUT_FILENAME = "sbarro_weekly_report.pdf"
SENSOR_MODULE_TRIPLET = "brad-grigsby:my-sbarro-sensor:sbarro-data"

async def connect(api_key: str, api_key_id: str) -> ViamClient:
    """Connect to Viam Cloud using provided API key and API key ID."""
    dial_options = DialOptions.with_api_key(api_key, api_key_id)
    return await ViamClient.create_from_dial_options(dial_options)

async def fetch_sensor_readings(loc_id: str, data_client: DataClient):
    """Fetch the latest tabular data from the sensor module for a location."""
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(hours=5)
    try:
        response = await data_client.tabular_data_by_mql(
            organization_id=ORG_ID,
            mql_binary=[
                bson.encode(
                    {
                        "$match": {
                            "location_id": loc_id,
                            "$expr": {
                                "$and": [
                                    {"$gte": ["$time_received", {"$toDate": start_date}]},
                                    {"$lte": ["$time_received", {"$toDate": end_date}]}
                                ]
                            }
                        }
                    }
                )
            ]
        )
        return response if response else print(f"No data for location: {loc_id}")
    except Exception as e:
        print(f"Error fetching data for sensor: {e}")
        return []
    
async def get_location_name(loc_id, viam_client: ViamClient):
    app_client = viam_client.app_client
    location = await app_client.get_location(loc_id)
    return location.name

def aggregate_metrics(data, daily_metrics, location_name):
    # Get all the daily_metrics for the location
    for records in data:
        readings = records["data"]["readings"]["readings"]
        total_trays = len(readings)
        for reading in readings:
            try:
                initial_timestamp = datetime.strptime(reading["initial_timestamp"], "%Y%m%d_%H%M%S")
                current_timestamp = datetime.strptime(reading["current_timestamp"], "%Y-%m-%d %H:%M:%S")
                hold_time = current_timestamp - initial_timestamp

                day = initial_timestamp.strftime("%Y-%m-%d")
                if day not in daily_metrics:
                    daily_metrics[day] = {}
                if location_name not in daily_metrics[day]:
                    daily_metrics[day][location_name] = {
                        "first_tray_time": initial_timestamp,
                        "last_tray_count": total_trays,
                        "last_tray_time": initial_timestamp,
                        "percentage_trays_over_hold": 0,
                        "total_trays": total_trays,
                        "trays_exceeding_hold_time": 1 if hold_time > timedelta(hours=4) else 0,
                        "total_hold_time": hold_time,
                    }
                else:
                    store_metrics = daily_metrics[day][location_name]

                    store_metrics["first_tray_time"] = min(store_metrics["first_tray_time"], initial_timestamp)

                    # Update last tray time based on total trays and timestamp
                    if total_trays > store_metrics["last_tray_count"] or (
                        total_trays == store_metrics["last_tray_count"]
                        and initial_timestamp > store_metrics["last_tray_time"]
                    ):
                        store_metrics["last_tray_time"] = initial_timestamp
                        store_metrics["last_tray_count"] = total_trays

                    store_metrics["total_trays"] = max(total_trays, store_metrics["total_trays"])
                    store_metrics["total_hold_time"] += hold_time

                    # Update trays exceeding hold time and percentage trays over hold
                    if hold_time > timedelta(hours=4):
                        store_metrics["trays_exceeding_hold_time"] += 1
                        store_metrics["percentage_trays_over_hold"] = (store_metrics["trays_exceeding_hold_time"] / store_metrics["total_trays"]) * 100
            except KeyError:
                print("KeyError: ", reading)
                continue

    # Calculate average hold time for each day and each location
    for day, locations in daily_metrics.items():
        for location_name, metrics in locations.items():
            total_hold_time = metrics["total_hold_time"]
            total_trays = metrics["total_trays"]
            metrics["average_hold_time"] = total_hold_time / total_trays if total_trays > 0 else 0

    daily_metrics = {day: dict(sorted(stores.items())) for day, stores in sorted(daily_metrics.items())}
    return daily_metrics

# PDF Generation Class
class AnalyticsPDF(FPDF):
    def __init__(self):
        super().__init__()
        self.first_page = True

    def header(self):
        if self.first_page:
            self.add_font("SpaceGrotesk", style="", fname="fonts/Space_Grotesk/SpaceGrotesk-Regular.ttf")
            self.add_font("SpaceGrotesk", style="B", fname="fonts/Space_Grotesk/SpaceGrotesk-Bold.ttf")
            self.set_font("SpaceGrotesk", "B", 24)
            self.cell(0, 10, "Sbarro Analytics Report", align="C", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
            self.ln(10)
            self.cell(0, 10, "Sbarro x Viam", align="C", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    def footer(self):
        self.set_y(-15)
        self.set_font("helvetica", "I", 10)
        self.cell(0, 10, f"Page {self.page_no()}", align="C")

    def add_day_title(self, title):
        self.set_font("helvetica", "B", 12)
        self.cell(0, 10, title, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="C")
        self.ln(5)

    def add_store_title(self, title):
        self.set_font("helvetica", "B", 12)
        self.cell(0, 10, title, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="L")
        self.ln(5)

    def add_table_headers(self, values):
        self.set_font("helvetica", "B", size=10)
        # col_widths = [26, 28, 28, 23, 29, 25, 27] # This is with an extra header
        col_widths = [30, 30, 23, 33, 25, 30]
        for i, value in enumerate(values):
            self.cell(col_widths[i], 10, str(value), border=1, align="C")
        self.ln()

    def add_table_row(self, values):
        self.set_font("helvetica", size=10)
        # col_widths = [26, 28, 28, 23, 29, 25, 27] # This is with an extra header
        col_widths = [30, 30, 23, 33, 25, 30]
        for i, value in enumerate(values):
            self.cell(col_widths[i], 10, str(value), border=1, align="C")
        self.ln()

# Generate PDF Report
def generate_pdf(daily_metrics):
    pdf = AnalyticsPDF()
    pdf.add_page()
    pdf.first_page = False

    reporting_week = f"{(datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')} to {datetime.now().strftime('%Y-%m-%d')}"
    pdf.set_font("helvetica", size=12)
    pdf.cell(0, 10, f"Reporting Week: {reporting_week}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="C")
    pdf.ln(10)

    if not daily_metrics:
        pdf.add_day_title("No data found for this week.")

    for day, stores in daily_metrics.items():
        pdf.add_day_title(f"Day: {day}")

        for location_name, metrics in stores.items():
            pdf.add_store_title(f"Store: {location_name}")
            headers = [
                "First Tray Time",
                "Last Tray Time",
                "Total Trays",
                "Trays Over Hold",
                "% Over Hold",
                "Avg Hold Time (s)"
            ]

            pdf.set_font("helvetica", "B", 10)
            pdf.add_table_headers(headers)

            row = [
                metrics["first_tray_time"].strftime("%H:%M:%S"),
                metrics["last_tray_time"].strftime("%H:%M:%S"),
                int(metrics["total_trays"]),
                metrics["trays_exceeding_hold_time"],
                f"{metrics['percentage_trays_over_hold']:.2f}%",
                metrics["average_hold_time"].seconds // 60,
            ]

            pdf.add_table_row(row)
            pdf.ln(10)

    pdf.output(PDF_OUTPUT_FILENAME)
    print(f"PDF report generated: {PDF_OUTPUT_FILENAME}")

# Email Sending Function
def send_email(subject, body, to_email, pdf_path):
    msg = MIMEMultipart()
    msg["From"] = EMAIL_USER
    msg["To"] = to_email
    msg["Subject"] = subject

    msg.attach(MIMEText(body, "plain"))
    with open(pdf_path, "rb") as file:
        pdf = MIMEBase("application", "octet-stream")
        pdf.set_payload(file.read())
        encoders.encode_base64(pdf)
        pdf.add_header("Content-Disposition", f"attachment; filename={os.path.basename(pdf_path)}")
        msg.attach(pdf)

    with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
        smtp.starttls()
        smtp.login(EMAIL_USER, EMAIL_PASSWORD)
        smtp.send_message(msg)

    print("Email sent successfully!")
async def main():
    # Connect to Viam & Data Client
    viam_client: ViamClient = await connect(API_KEY, API_KEY_ID)
    data_client: DataClient = viam_client.data_client

    # Fetch data and aggregate metrics
    daily_metrics = {}
    for loc_id in LOC_IDS:
        data = await fetch_sensor_readings(loc_id, data_client)
        location_name = await get_location_name(loc_id, viam_client)
        daily_metrics = aggregate_metrics(data, daily_metrics, location_name)

    generate_pdf(daily_metrics)
    send_email(
        subject="Sbarro Analytics Report",
        body="Please find the attached report.",
        to_email="brad.grigsby@viam.com",
        pdf_path=PDF_OUTPUT_FILENAME
    )

if __name__ == "__main__":
    asyncio.run(main())