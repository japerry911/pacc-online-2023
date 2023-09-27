import httpx  # requests capability, but can work with async
from prefect import flow, task, serve

from random import randint


@task
def fetch_weather(lat: float, lon: float):
    base_url = "https://api.open-meteo.com/v1/forecast/"
    weather = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="temperature_2m"),
    )
    most_recent_temp = float(weather.json()["hourly"]["temperature_2m"][0])
    return most_recent_temp


@task
def fetch_weather_wind_speed_10(lat: float, lon: float):
    base_url = "https://api.open-meteo.com/v1/forecast/"
    weather = httpx.get(
        base_url,
        params=dict(
            latitude=lat,
            longitude=lon,
            hourly="windspeed_10m",
        ),
    )
    most_recent_windspeed_10m = float(weather.json()["hourly"]["windspeed_10m"][0])
    return most_recent_windspeed_10m


@task
def save_weather(value: float, file_suffix: str):
    with open(f"weather.csv-{file_suffix}", "w+") as w:
        w.write(f"{value}")
    return "Successfully wrote temp"


@flow
def pipeline(
    lat: float,
    lon: float,
    wind: bool,
):
    if not wind:
        temp = fetch_weather(lat, lon)
        save_weather(temp, "temp")
    else:
        wind = fetch_weather_wind_speed_10(lat, lon)
        save_weather(wind, "wind")


if __name__ == "__main__":
    # pipeline(38.9, -77.0)

    weather_pipeline_temp = pipeline.to_deployment(
        name="weather_pipeline_temp",
        parameters={
            "wind": False,
        },
    )
    weather_pipeline_wind = pipeline.to_deployment(
        name="weather_pipeline_wind",
        parameters={
            "wind": True,
        },
    )

    serve(weather_pipeline_temp, weather_pipeline_wind)

# - Make an email notification automation for any flow
# run completion
# - Run a flow a few times from the CLI
# - See the event feed in the UI
# - Create multiple deployments at once with the serve
# method
# - Stretch: create a custom event - run it and see the
# event in the UI


# https://open-meteo.com/en/docs
# Use Open-Meteo API
# - Authenticate your CLI to Prefect Cloud x
# - Fine to use a personal account or a workspace x
# - Write flow code that fetches other weather metrics x
# - Make at least 3 tasks x
# - Use .serve() method to deploy your flow x
# - Run your flow from the UI and CLI x
