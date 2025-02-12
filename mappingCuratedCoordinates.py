import folium
import pandas as pd
import json

curatedCoordinates = pd.read_csv('Enter File Path Here.') # Download @ https://github.com/ClaytonDuffin/Batch-Processing-ETL-Orchestration/blob/main/curatedCoordinates.csv
geoJSONFilePath = 'Enter File Path Here.' # Download @ https://github.com/PublicaMundi/MappingAPI/blob/master/data/geojson/us-states.json

with open(geoJSONFilePath) as f:
    geoJSONData = json.load(f)

USMap = folium.Map(location=[42, -102], zoom_start=4, tiles='Esri WorldImagery')

folium.GeoJson(
    geoJSONData,
    style_function=lambda x: {
        'fillColor': '#000000',
        'color': '#000000',
        'weight': 1,
        'fillOpacity': 0.1
    }).add_to(USMap)

for _, location in curatedCoordinates.iterrows():
    latitude, longitude = location['Latitude'], location['Longitude']
    onCoordinateClick = folium.Popup(f"<span style='color:black;'>Latitude: {latitude}<br>Longitude: {longitude}</span>", max_width=300)

    folium.CircleMarker(
        location=[latitude, longitude],
        radius=2,
        color='lawngreen',
        fill=True,
        fill_color='lawngreen',
        fill_opacity=1,
        popup=onCoordinateClick
    ).add_to(USMap)

USMap.save("interactiveMapCuratedCoordinates.html")
