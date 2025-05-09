# import tkinter as tk
# from tkinter import messagebox
# import requests

# def get_uber_products():
#     latitude = latitude_entry.get()
#     longitude = longitude_entry.get()
#     access_token = access_token_entry.get()  # In a real app, don't store this in the UI
    
#     if not latitude or not longitude or not access_token:
#         messagebox.showerror("Error", "Please fill in all fields")
#         return
    
#     try:
#         # Convert to float to validate coordinates
#         lat = float(latitude)
#         lon = float(longitude)
#     except ValueError:
#         messagebox.showerror("Error", "Invalid coordinates. Please enter valid numbers")
#         return
    
#     headers = {
#         'Authorization': f'Bearer {access_token}',
#         'Accept-Language': 'en_US',
#         'Content-Type': 'application/json'
#     }
    
#     params = {
#         'latitude': lat,
#         'longitude': lon
#     }
    
#     try:
#         response = requests.get(
#             'https://api.uber.com/v1.2/products',
#             headers=headers,
#             params=params
#         )
        
#         if response.status_code == 200:
#             products = response.json().get('products', [])
#             messagebox.showinfo("Success", f"Found {len(products)} Uber products at this location")
#         else:
#             error_msg = f"API Error: {response.status_code}\n"
#             if response.text:
#                 error_msg += response.json().get('message', 'Unknown error')
#             messagebox.showerror("API Error", error_msg)
            
#     except requests.exceptions.RequestException as e:
#         messagebox.showerror("Connection Error", f"Failed to connect to Uber API: {str(e)}")

# # Create the main window
# root = tk.Tk()
# root.title("shortest route")
# root.geometry("800x600")

# # Create and place the input fields
# tk.Label(root, text="Access Token:").grid(row=0, column=0, padx=10, pady=5, sticky="e")
# access_token_entry = tk.Entry(root, width=40)
# access_token_entry.grid(row=0, column=1, padx=10, pady=5)

# tk.Label(root, text="Latitude:").grid(row=1, column=0, padx=10, pady=5, sticky="e")
# latitude_entry = tk.Entry(root, width=40)
# latitude_entry.grid(row=1, column=1, padx=10, pady=5)

# tk.Label(root, text="Longitude:").grid(row=2, column=0, padx=10, pady=5, sticky="e")
# longitude_entry = tk.Entry(root, width=40)
# longitude_entry.grid(row=2, column=1, padx=10, pady=5)

# # Create and place the submit button
# submit_button = tk.Button(root, text="Get Uber Products", command=get_uber_products)
# submit_button.grid(row=3, column=0, columnspan=2, pady=10)

# # Run the application
# root.mainloop()






import math
import tempfile
import os
import tkinter as tk
from tkinter import messagebox, ttk, filedialog
import requests
from tkintermapview import TkinterMapView
from threading import Thread
from datetime import datetime
from google.cloud import bigquery
import json
import time
import pandas as pd  # For Excel export

class UberMapApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Uber Products Finder")
        self.root.geometry("1000x800")

        # API keys
        self.WMATA_API_KEY = "aa7862f44c1247ca86011141a6f9a5b3"
        self.OPENROUTE_API_KEY = 'Bearer 5b3ce3597851110001cf62488eda4cc29f004e91aee24dfaf11a8a79'
        
        # Initialize BigQuery client
        self.bq_client = bigquery.Client()
        self.dataset_id = "uber_metro_data"
        self.table_id = "travel_estimates"
        
        # Coordinates
        self.latitude = 38.8512
        self.longitude = -77.0402
        self.marker = None

        # Destination coordinates
        self.destinations = {
            "Dulles Airport": (-77.4565, 38.9531),
            "Ronald Reagan Washington Airport": (-77.0402, 38.8512)
        }
        self.selected_destination = tk.StringVar(value="Dulles Airport")

        self.create_widgets()
        self.setup_map()
        self.setup_bigquery()

    def setup_bigquery(self):
        """Create BigQuery dataset and table if they don't exist"""
        try:
            # Check if dataset exists, create if not
            dataset_ref = self.bq_client.dataset(self.dataset_id)
            try:
                self.bq_client.get_dataset(dataset_ref)
            except Exception:
                dataset = bigquery.Dataset(dataset_ref)
                self.bq_client.create_dataset(dataset)
            
            # Check if table exists, create if not
            table_ref = dataset_ref.table(self.table_id)
            try:
                self.bq_client.get_table(table_ref)
            except Exception:
                schema = [
                    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
                    bigquery.SchemaField("source_lat", "FLOAT", mode="REQUIRED"),
                    bigquery.SchemaField("source_lng", "FLOAT", mode="REQUIRED"),
                    bigquery.SchemaField("destination_name", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("destination_lat", "FLOAT", mode="REQUIRED"),
                    bigquery.SchemaField("destination_lng", "FLOAT", mode="REQUIRED"),
                    bigquery.SchemaField("uber_data", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("metro_data", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("comparison_result", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("full_response", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("weather_data", "STRING", mode="NULLABLE")
                ]
                table = bigquery.Table(table_ref, schema=schema)
                self.bq_client.create_table(table)
        except Exception as e:
            print(f"BigQuery setup error: {str(e)}")

    def get_all_records(self):
        """Retrieve all records from BigQuery for export"""
        try:
            query = f"""
            SELECT 
                timestamp,
                source_lat,
                source_lng,
                destination_name,
                destination_lat,
                destination_lng,
                JSON_EXTRACT_SCALAR(uber_data, '$.duration_minutes') as uber_duration_min,
                JSON_EXTRACT_SCALAR(uber_data, '$.distance_km') as uber_distance_km,
                JSON_EXTRACT_SCALAR(metro_data, '$.duration_minutes') as metro_duration_min,
                JSON_EXTRACT_SCALAR(metro_data, '$.num_stations') as metro_stations,
                comparison_result,
                JSON_EXTRACT_SCALAR(weather_data, '$.current_temp') as current_temp,
                JSON_EXTRACT_SCALAR(weather_data, '$.current_precipitation') as current_precip,
                JSON_EXTRACT_SCALAR(weather_data, '$.is_rainy') as is_rainy,
                JSON_EXTRACT_SCALAR(weather_data, '$.is_cold') as is_cold
            FROM `{self.dataset_id}.{self.table_id}`
            ORDER BY timestamp DESC
            """
            query_job = self.bq_client.query(query)
            results = query_job.result()
            
            # Convert to list of dictionaries
            records = [dict(row.items()) for row in results]
            
            return records
        except Exception as e:
            print(f"Error fetching records for export: {str(e)}")
            return None

    def export_to_excel(self):
        """Export BigQuery data to Excel file"""
        try:
            records = self.get_all_records()
            if not records:
                messagebox.showinfo("Info", "No records found to export")
                return
            
            # Convert to DataFrame
            df = pd.DataFrame(records)
            
            # Clean up data types
            numeric_cols = ['source_lat', 'source_lng', 'destination_lat', 'destination_lng',
                          'uber_duration_min', 'uber_distance_km', 'metro_duration_min',
                          'current_temp', 'current_precip']
            
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Convert timestamp to readable format
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # Convert boolean fields
            bool_cols = ['is_rainy', 'is_cold']
            for col in bool_cols:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: 'Yes' if x == 'true' else 'No')
            
            # Ask user for save location
            file_path = filedialog.asksaveasfilename(
                defaultextension=".xlsx",
                filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")],
                title="Save Excel File As"
            )
            
            if not file_path:  # User cancelled
                return
            
            # Write to Excel
            df.to_excel(file_path, index=False)
            messagebox.showinfo("Success", f"Data successfully exported to:\n{file_path}")
            
        except Exception as e:
            messagebox.showerror("Export Error", f"Failed to export data: {str(e)}")

    def get_last_saved_record(self):
        """Retrieve the most recent record from BigQuery"""
        try:
            query = f"""
            SELECT * 
            FROM `{self.dataset_id}.{self.table_id}`
            ORDER BY timestamp DESC
            LIMIT 1
            """
            query_job = self.bq_client.query(query)
            results = list(query_job.result())
            
            if results:
                # Convert BigQuery row to dictionary
                record = dict(results[0].items())
                
                # Parse JSON fields and handle datetime
                def parse_value(value):
                    if isinstance(value, datetime):
                        return value.isoformat()
                    return value
                
                record = {k: parse_value(v) for k, v in record.items()}
                
                # Parse JSON strings
                for field in ['uber_data', 'metro_data', 'weather_data', 'full_response']:
                    if record.get(field):
                        try:
                            record[field] = json.loads(record[field])
                        except (TypeError, json.JSONDecodeError):
                            pass
                    
                return record
            return None
        except Exception as e:
            print(f"Error fetching last record: {str(e)}")
            return None

    def save_to_bigquery(self, data):
        """Save data to BigQuery using a load job (to avoid streaming insert)"""
        try:
            table_ref = self.bq_client.dataset(self.dataset_id).table(self.table_id)
            
            # Helper function to safely serialize data
            def safe_serialize(obj):
                if isinstance(obj, (dict, list, str, int, float, bool, type(None))):
                    return obj
                return str(obj)
            
            row = {
                "timestamp": datetime.utcnow().isoformat(),
                "source_lat": data["source_lat"],
                "source_lng": data["source_lng"],
                "destination_name": data["destination_name"],
                "destination_lat": data["destination_lat"],
                "destination_lng": data["destination_lng"],
                "uber_data": json.dumps(data["uber_data"], default=safe_serialize) if data.get("uber_data") else None,
                "metro_data": json.dumps(data["metro_data"], default=safe_serialize) if data.get("metro_data") else None,
                "comparison_result": data["comparison_result"],
                "full_response": json.dumps(data["full_response"], default=safe_serialize) if data.get("full_response") else None,
                "weather_data": json.dumps(data["weather_data"], default=safe_serialize) if data.get("weather_data") else None
            }

            # Rest of the method remains the same...
            with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as tmpfile:
                tmpfile.write(json.dumps(row) + "\n")
                tmpfile_path = tmpfile.name

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                schema=[
                    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
                    bigquery.SchemaField("source_lat", "FLOAT", mode="REQUIRED"),
                    bigquery.SchemaField("source_lng", "FLOAT", mode="REQUIRED"),
                    bigquery.SchemaField("destination_name", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("destination_lat", "FLOAT", mode="REQUIRED"),
                    bigquery.SchemaField("destination_lng", "FLOAT", mode="REQUIRED"),
                    bigquery.SchemaField("uber_data", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("metro_data", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("comparison_result", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("full_response", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("weather_data", "STRING", mode="NULLABLE")
                ],
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND
            )

            with open(tmpfile_path, "rb") as source_file:
                load_job = self.bq_client.load_table_from_file(
                    source_file,
                    table_ref,
                    job_config=job_config
                )
            load_job.result()  # Wait for job to complete

            os.remove(tmpfile_path)
            return True

        except Exception as e:
            print(f"Error saving to BigQuery via load job: {str(e)}")
            return False

    def create_widgets(self):
        control_frame = tk.Frame(self.root)
        control_frame.pack(fill=tk.X, padx=10, pady=10)

        tk.Label(control_frame, text="Latitude:").grid(row=0, column=0, sticky="e")
        self.lat_label = tk.Label(control_frame, text=str(self.latitude))
        self.lat_label.grid(row=0, column=1, sticky="w")

        tk.Label(control_frame, text="Longitude:").grid(row=1, column=0, sticky="e")
        self.lng_label = tk.Label(control_frame, text=str(self.longitude))
        self.lng_label.grid(row=1, column=1, sticky="w")

        # Dropdown for destination
        tk.Label(control_frame, text="Destination:").grid(row=2, column=0, sticky="e")
        self.destination_dropdown = ttk.Combobox(control_frame, textvariable=self.selected_destination, state="readonly")
        self.destination_dropdown['values'] = list(self.destinations.keys())
        self.destination_dropdown.grid(row=2, column=1, sticky="w")

        # Buttons
        tk.Button(
            control_frame, 
            text="Show Last Saved", 
            command=self.show_last_record
        ).grid(row=3, column=0, columnspan=2, pady=5)
        
        tk.Button(
            control_frame, 
            text="Get Route Timing", 
            command=self.get_uber_products
        ).grid(row=4, column=0, columnspan=2, pady=5)
        
        tk.Button(
            control_frame, 
            text="Export to Excel", 
            command=self.export_to_excel
        ).grid(row=5, column=0, columnspan=2, pady=5)

    """ def setup_map(self):
        self.map_widget = TkinterMapView(self.root, width=980, height=600, corner_radius=0)
        self.map_widget.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        self.map_widget.set_position(self.latitude, self.longitude)
        self.map_widget.set_zoom(14)
        self.map_widget.pack(fill=tk.BOTH, expand=True)
        self.marker = self.map_widget.set_marker(self.latitude, self.longitude,
                                                text=f"Selected Location\nLat: {self.latitude:.6f}\nLng: {self.longitude:.6f}")
        
        # Fix for the mouse movement error
        self.map_widget.last_mouse_down_position = (0, 0)
        self.map_widget.last_mouse_down_time = time.time()       
        self.map_widget.canvas.bind("<Button-1>", self.on_map_click)
 """
    def setup_map(self):
        # Create map widget
        self.map_widget = TkinterMapView(
            self.root,
            width=980,
            height=600,
            corner_radius=0
        )
        # Use OSM tiles (free, public)
        self.map_widget.set_tile_server("https://a.tile.openstreetmap.org/{z}/{x}/{y}.png")

        # Position & zoom
        self.map_widget.set_position(self.latitude, self.longitude)
        self.map_widget.set_zoom(14)

        # Add to layout
        self.map_widget.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # Initial marker
        self.marker = self.map_widget.set_marker(
            self.latitude,
            self.longitude,
            text=f"Selected Location\nLat: {self.latitude:.6f}\nLng: {self.longitude:.6f}"
        )

        # Fix for the mouse‐click handling bug
        self.map_widget.last_mouse_down_position = (0, 0)
        self.map_widget.last_mouse_down_time = time.time()
        self.map_widget.canvas.bind("<Button-1>", self.on_map_click)

 
    def on_map_click(self, event):
        coords = self.map_widget.convert_canvas_coords_to_decimal_coords(event.x, event.y)
        if coords:
            self.latitude, self.longitude = coords
            self.lat_label.config(text=f"{self.latitude:.6f}")
            self.lng_label.config(text=f"{self.longitude:.6f}")

            if self.marker:
                self.map_widget.delete(self.marker)
            self.marker = self.map_widget.set_marker(self.latitude, self.longitude,
                                                     text=f"Selected Location\nLat: {self.latitude:.6f}\nLng: {self.longitude:.6f}")

    def show_last_record(self):
        """Display the last saved record from BigQuery in a larger window"""
        last_record = self.get_last_saved_record()
        if last_record:
            # Create a new top-level window
            popup = tk.Toplevel(self.root)
            popup.title("Last Saved Record - Detailed View")
            popup.geometry("800x600")  # Larger window size
            
            # Create a text widget with scrollbar
            text_frame = tk.Frame(popup)
            text_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
            
            scrollbar = tk.Scrollbar(text_frame)
            scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
            
            text = tk.Text(
                text_frame,
                wrap=tk.WORD,
                yscrollcommand=scrollbar.set,
                font=('Arial', 10),
                padx=10,
                pady=10
            )
            text.pack(fill=tk.BOTH, expand=True)
            
            scrollbar.config(command=text.yview)
            
            # Create a function to safely serialize objects for display
            def safe_serialize(obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                elif isinstance(obj, (dict, list, str, int, float, bool, type(None))):
                    return obj
                return str(obj)
            
            # Format the display text
            try:
                display_text = (
                    f"TIMESTAMP: {last_record['timestamp']}\n\n"
                    f"FROM LOCATION:\n"
                    f"  - Latitude: {last_record['source_lat']}\n"
                    f"  - Longitude: {last_record['source_lng']}\n\n"
                    f"DESTINATION:\n"
                    f"  - Name: {last_record['destination_name']}\n"
                    f"  - Latitude: {last_record['destination_lat']}\n"
                    f"  - Longitude: {last_record['destination_lng']}\n\n"
                    f"COMPARISON: {last_record['comparison_result'] or 'N/A'}\n\n"
                    f"FULL RESPONSE:\n{last_record['full_response']}\n\n"
                    f"WEATHER DATA:\n{json.dumps(last_record.get('weather_data', {}), default=safe_serialize, indent=2)}\n\n"
                    f"RAW DATA:\n{json.dumps({k:safe_serialize(v) for k,v in last_record.items() if k != 'weather_data'}, indent=2)}"
                )
            except Exception as e:
                display_text = f"Error formatting record: {str(e)}\n\nRaw data:\n{last_record}"
            
            # Insert text and make it read-only
            text.insert(tk.END, display_text)
            text.config(state=tk.DISABLED)
            
            # Add close button
            close_button = tk.Button(
                popup,
                text="Close",
                command=popup.destroy,
                padx=10,
                pady=5
            )
            close_button.pack(pady=10)
        else:
            self.root.after(0, lambda: messagebox.showinfo("Info", "No records found in BigQuery"))

    @staticmethod
    def haversine(lat1, lon1, lat2, lon2):
        R = 6371  # km
        d_lat = math.radians(lat2 - lat1)
        d_lon = math.radians(lon2 - lon1)
        a = math.sin(d_lat / 2)**2 + math.cos(math.radians(lat1)) * \
            math.cos(math.radians(lat2)) * math.sin(d_lon / 2)**2
        return R * (2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)))

    def get_nearest_station(self, lat, lon):
        url = "https://api.wmata.com/Rail.svc/json/jStations"
        headers = {"api_key": self.WMATA_API_KEY}
        response = requests.get(url, headers=headers).json()
        
        stations = response.get("Stations", [])
        closest = min(
            stations, 
            key=lambda s: self.haversine(lat, lon, s["Lat"], s["Lon"])
        )
        return closest["Code"], closest["Name"]

    def get_rail_path(self, from_code, to_code):
        url = f"https://api.wmata.com/Rail.svc/json/jPath?FromStationCode={from_code}&ToStationCode={to_code}"
        headers = {"api_key": self.WMATA_API_KEY}
        return requests.get(url, headers=headers).json()

    def get_travel_time(self, from_code, to_code):
        url = f"https://api.wmata.com/Rail.svc/json/jSrcStationToDstStationInfo?FromStationCode={from_code}&ToStationCode={to_code}"
        headers = {"api_key": self.WMATA_API_KEY}
        return requests.get(url, headers=headers).json()

    def get_uber_products(self):
        dest_name = self.selected_destination.get()
        dest_lng, dest_lat = self.destinations[dest_name]

        headers = {
            'Authorization': self.OPENROUTE_API_KEY,
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }

        body = {
            'coordinates': [[self.longitude, self.latitude], [dest_lng, dest_lat]]
        }

        try:
            # Data object to store all information for BigQuery
            bq_data = {
                "source_lat": self.latitude,
                "source_lng": self.longitude,
                "destination_name": dest_name,
                "destination_lat": dest_lat,
                "destination_lng": dest_lng,
                "uber_data": None,
                "metro_data": None,
                "comparison_result": None,
                "full_response": None,
                "weather_data": None
            }

            # --- Open-Meteo Weather API Call ---
            weather_url = f"https://api.open-meteo.com/v1/forecast?latitude={self.latitude}&longitude={self.longitude}&current=temperature_2m,wind_speed_10m,precipitation&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation"
            weather_response = requests.get(weather_url)
            
            weather_info = {}
            is_rainy = False
            is_cold = False
            if weather_response.status_code == 200:
                weather_data = weather_response.json()
                current = weather_data.get('current', {})
                hourly = weather_data.get('hourly', {})
                
                # Get current temperature
                current_temp = current.get('temperature_2m', 20)  # Default to 20 if not available
                
                # Detect conditions
                current_precip = current.get('precipitation', 0)
                hourly_precip = hourly.get('precipitation', [0]*24)[:24]  # Get first 24 hours
                
                # Consider it rainy if current precipitation > 0.5mm or any significant precipitation in next 3 hours
                is_rainy = current_precip > 0.5 or any(p > 0.5 for p in hourly_precip[:3])
                
                # Consider it cold if temperature is below 16°C
                is_cold = current_temp < 16
                
                weather_info = {
                    "status": "success",
                    "current_temp": current_temp,
                    "current_wind": current.get('wind_speed_10m'),
                    "current_precipitation": current_precip,
                    "is_rainy": is_rainy,
                    "is_cold": is_cold,
                    "hourly_temps": hourly.get('temperature_2m', [])[:24],
                    "hourly_humidity": hourly.get('relative_humidity_2m', [])[:24],
                    "hourly_wind": hourly.get('wind_speed_10m', [])[:24],
                    "hourly_precipitation": hourly_precip,
                    "response": weather_data
                }
            else:
                weather_info = {
                    "status": "error",
                    "error": weather_response.text
                }
            bq_data["weather_data"] = weather_info

            # --- Uber Driving Route ---
            uber_response = requests.post(
                'https://api.openrouteservice.org/v2/directions/driving-car',
                headers=headers,
                json=body
            )

            uber_data = {}
            if uber_response.status_code == 200:
                result = uber_response.json()
                summary = result['routes'][0]['summary']
                uber_data = {
                    "status": "success",
                    "distance_km": summary['distance']/1000,
                    "duration_minutes": summary['duration']/60,
                    "response": result
                }
            else:
                uber_data = {
                    "status": "error",
                    "error": uber_response.text
                }
            bq_data["uber_data"] = uber_data

            # --- Metro Route ---
            from_code, from_station = self.get_nearest_station(self.latitude, self.longitude)
            to_code, to_station = self.get_nearest_station(dest_lat, dest_lng)

            # Get Metro data
            path_response = self.get_rail_path(from_code, to_code)
            travel_response = self.get_travel_time(from_code, to_code)
            travel_info = travel_response.get("StationToStationInfos", [])
            metro_duration = travel_info[0]['RailTime'] if travel_info else None
            
            metro_data = {
                "from_station": from_station,
                "from_code": from_code,
                "to_station": to_station,
                "to_code": to_code,
                "num_stations": len(path_response.get("Path", [])),
                "duration_minutes": metro_duration,
                "path_response": path_response,
                "travel_response": travel_response
            }
            bq_data["metro_data"] = metro_data

            # Compare durations and consider weather
            comparison = ""
            uber_duration = uber_data.get("duration_minutes") if uber_data.get("status") == "success" else None
            
            if metro_duration and uber_duration:
                if is_rainy:
                    comparison = "⚠️ It's raining - better to take a taxi than metro!"
                elif is_cold:
                    comparison = "❄️ It's cold (below 16°C) and could rain - better to take a taxi than metro!"
                elif metro_duration < uber_duration:
                    comparison = "Metro is faster than driving"
                elif uber_duration < metro_duration:
                    comparison = "Driving is faster than Metro"
                else:
                    comparison = "Both routes take about the same time"
            
            bq_data["comparison_result"] = comparison
            
            # Generate summary
            summary_parts = []
            
            # Add weather information to summary
            if weather_info["status"] == "success":
                weather_status = "🌧️ Rainy weather" if is_rainy else "❄️ Cold weather" if is_cold else "☀️ Fair weather"
                temp_status = f"{weather_info['current_temp']}°C ({'cold' if is_cold else 'moderate'})"
                precip_status = f"{weather_info['current_precipitation']} mm precipitation" if is_rainy else "No significant precipitation"
                
                summary_parts.append(
                    f"{weather_status}\n"
                    f"  - Temperature: {temp_status}\n"
                    f"  - Wind Speed: {weather_info['current_wind']} km/h\n"
                    f"  - Precipitation: {precip_status}\n"
                    f"  - Next 24h Forecast:\n"
                    f"    - Avg Temp: {sum(weather_info['hourly_temps'])/len(weather_info['hourly_temps']):.1f}°C\n"
                    f"    - Avg Humidity: {sum(weather_info['hourly_humidity'])/len(weather_info['hourly_humidity']):.1f}%\n"
                    f"    - Max Precipitation: {max(weather_info['hourly_precipitation']):.1f} mm"
                )
            else:
                summary_parts.append(f"🌦️ Weather Data Error: {weather_info['error']}")
                
            summary_parts.append("\n")  # Add space between sections
                    
            if uber_data["status"] == "success":
                summary_parts.append(
                    f"🚗 Uber Driving Route:\n"
                    f"  - Distance: {uber_data['distance_km']:.2f} km\n"
                    f"  - Time: {uber_data['duration_minutes']:.1f} minutes"
                )
            else:
                summary_parts.append(f"🚗 Uber Route Error: {uber_data['error']}")
            
            summary_parts.append(
                f"\n🚇 Metro Path:\n"
                f"  - From: {from_station} ({from_code})\n"
                f"  - To: {to_station} ({to_code})\n"
                f"  - Stations: {metro_data['num_stations']}\n"
                f"  - Time: {metro_duration} minutes"
            )
            
            if comparison:
                summary_parts.append(f"\n\n{comparison}")
            
            final_summary = "\n".join(summary_parts)
            bq_data["full_response"] = final_summary
            
            # Save to BigQuery
            if self.save_to_bigquery(bq_data):
                # Show current results
                self.root.after(0, lambda: messagebox.showinfo("Travel Estimates", final_summary))
                
                # Show last saved record (optional)
                last_record = self.get_last_saved_record()
                if last_record:
                    print("Last saved record:", last_record)
            else:
                self.root.after(0, lambda: messagebox.showerror("Error", "Failed to save data to BigQuery"))

        except requests.exceptions.RequestException as e:
            error_msg = f"Failed to connect: {str(e)}"
            self.root.after(0, lambda: messagebox.showerror("Connection Error", error_msg))

if __name__ == "__main__":
    root = tk.Tk()
    app = UberMapApp(root)
    root.mainloop()