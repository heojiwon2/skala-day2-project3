"""
비동기 API 수집 모듈
====================================================================
외부 API 3개(Open-Meteo 날씨, RestCountries 국가정보, ip-api IP조회)를
httpx.AsyncClient + asyncio.gather()로 동시에 호출한다.

동시 호출을 쓰는 이유: 3개 API는 서로 의존관계가 없는 독립 호출이므로
순차 호출(합산 대기시간) 대신 동시 호출(가장 느린 응답 하나의 대기시간)로
전체 수집 시간을 줄일 수 있다.
"""
import asyncio

import httpx

# 서울(위도/경도) 기준 3일치 시간별 기온/강수확률 예보
WEATHER_URL = "https://api.open-meteo.com/v1/forecast?latitude=37.5665&longitude=126.9780&hourly=temperature_2m,precipitation_probability&forecast_days=3&timezone=Asia/Seoul"

# 대한민국(KR) 국가 정보 (국가명·지역·인구 등)
COUNTRY_URL = "https://countries.dev/alpha/KR"

# 테스트용 고정 IP(구글 DNS 8.8.8.8)의 위치 정보 조회
IP_URL = "http://ip-api.com/json/8.8.8.8"


async def fetch_weather(client: httpx.AsyncClient) -> dict:
    """Open-Meteo에서 날씨 예보 JSON을 가져온다.

    예외 처리: 4xx/5xx 응답, 타임아웃, 연결 실패를 모두
    httpx 예외로 잡아 어떤 API가 실패했는지 메시지에 명시한 뒤 다시 던진다.
    (asyncio.gather는 기본적으로 첫 예외를 그대로 전파하지만,
    원인 API를 바로 알 수 있도록 메시지를 붙여준다)
    """
    try:
        response = await client.get(WEATHER_URL)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        raise httpx.HTTPStatusError(
            f"[weather] API 응답 실패: status={e.response.status_code}",
            request=e.request,
            response=e.response,
        ) from e
    except httpx.RequestError as e:
        raise httpx.RequestError(f"[weather] API 요청 실패: {e}", request=e.request) from e


async def fetch_country(client: httpx.AsyncClient) -> dict:
    """RestCountries에서 국가 정보 JSON을 가져온다. (예외 처리는 fetch_weather와 동일한 방식)"""
    try:
        response = await client.get(COUNTRY_URL)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        raise httpx.HTTPStatusError(
            f"[country] API 응답 실패: status={e.response.status_code}",
            request=e.request,
            response=e.response,
        ) from e
    except httpx.RequestError as e:
        raise httpx.RequestError(f"[country] API 요청 실패: {e}", request=e.request) from e


async def fetch_ip(client: httpx.AsyncClient) -> dict:
    """ip-api에서 IP 위치 정보 JSON을 가져온다. (예외 처리는 fetch_weather와 동일한 방식)"""
    try:
        response = await client.get(IP_URL)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        raise httpx.HTTPStatusError(
            f"[ip] API 응답 실패: status={e.response.status_code}",
            request=e.request,
            response=e.response,
        ) from e
    except httpx.RequestError as e:
        raise httpx.RequestError(f"[ip] API 요청 실패: {e}", request=e.request) from e


async def main() -> tuple[dict, dict, dict]:
    """3개 API를 asyncio.gather()로 동시에 호출하고 (weather, country, ip) 튜플을 반환한다."""
    async with httpx.AsyncClient(timeout=10) as client:
        # gather()는 세 코루틴을 동시에 실행하고, 셋 다 끝나야 다음 줄로 진행한다.
        # 하나라도 예외가 나면 나머지가 끝났든 안 끝났든 그 예외를 즉시 전파한다.
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
