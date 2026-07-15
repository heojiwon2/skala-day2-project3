'''api 를 이용해 호출한 데이터의 key를 확인하는 테스트 코드입니다.'''

from api import main
import asyncio


async def run_pipeline_logic():
    print("\n🚀 API 데이터 수집 중...")

    weather_raw, country_raw, ip_raw = await main()

    print("✓ 데이터 수집 완료!\n")


    # 가져온 데이터 전체 확인
    print("===== Weather RAW =====")
    print(weather_raw)

    print("\n===== Country RAW =====")
    print(country_raw)

    print("\n===== IP RAW =====")
    print(ip_raw)


    # key만 확인
    print("\n===== Weather Keys =====")
    print(list(weather_raw.keys()))

    print("\n===== Country Keys =====")
    print(list(country_raw.keys()))

    print("\n===== IP Keys =====")
    print(list(ip_raw.keys()))


if __name__ == "__main__":
    asyncio.run(run_pipeline_logic())
