import requests

def get_weather_paris():
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 48.8566,
        "longitude": 2.3522,
        "current": "temperature_2m,precipitation,rain,wind_speed_10m,weather_code",
        "timezone": "Europe/Paris",
    }
    r = requests.get(url, params=params, timeout=10,verify=False) #TODO : Supprimer le SSL bypass
    r.raise_for_status()
    return r.json()["current"]

print(get_weather_paris())
