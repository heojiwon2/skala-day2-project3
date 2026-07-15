import asyncio
import httpx


WEATHER_URL = "https://api.open-meteo.com/v1/forecast?latitude=37.5665&longitude=126.9780&hourly=temperature_2m,precipitation_probability&forecast_days=3&timezone=Asia/Seoul"

COUNTRY_URL = "https://countries.dev/alpha/KR"

IP_URL = "http://ip-api.com/json/8.8.8.8"


async def fetch_weather(client):
    response = await client.get(WEATHER_URL)
    response.raise_for_status()
    return response.json()


async def fetch_country(client):
    response = await client.get(COUNTRY_URL)
    response.raise_for_status()
    return response.json()


async def fetch_ip(client):
    response = await client.get(IP_URL)
    response.raise_for_status()
    return response.json()


async def main():
    async with httpx.AsyncClient(timeout=10) as client:

        weather, country, ip = await asyncio.gather(
            fetch_weather(client),
            fetch_country(client),
            fetch_ip(client)
        )

    print("===== Open-Meteo =====")
    print(weather)

    print("\n===== RestCountries =====")
    print(country)

    print("\n===== ip-api =====")
    print(ip)

    return weather, country, ip

if __name__ == "__main__":
    asyncio.run(main())