import asyncio
import os
import time
import csv
import pyarrow as pa
import pyarrow.parquet as pq
from typing import List
from pydantic import BaseModel, Field, ValidationError
from typing_extensions import Annotated
import pytest

# 1. API 가져오는 기존 코드
from api import main ##api.py에서 main() 함수를 가져옵니다.

# =====================================================================
# 2. Pydantic v2 스키마 정의
# =====================================================================
Latitude = Annotated[float, Field(ge=-90.0, le=90.0)]
Longitude = Annotated[float, Field(ge=-180.0, le=180.0)]

class WeatherHourly(BaseModel):
    time: List[str]  

class WeatherSchema(BaseModel):
    latitude: Latitude
    longitude: Longitude
    timezone: str
    hourly: WeatherHourly

class CountrySchema(BaseModel):
    name: str  
    alpha2Code: str = Field(alias="alpha2Code") 
    population: Annotated[int, Field(ge=0)]     
    region: str

class IpSchema(BaseModel):
    status: str
    query: str  
    countryCode: str
    city: str
    lat: Latitude
    lon: Longitude


# ==========================================================
# CSV 저장 / 읽기
# ==========================================================

def save_csv(data, path):

    fields = list(data[0].keys())

    with open(
        path,
        "w",
        newline="",
        encoding="utf-8"
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=fields
        )

        writer.writeheader()
        writer.writerows(data)



def read_csv(path):

    result = []

    with open(
        path,
        "r",
        encoding="utf-8"
    ) as f:

        reader = csv.DictReader(f)

        for row in reader:
            result.append(row)

    return result



# ==========================================================
# Parquet 저장 / 읽기
# ==========================================================

def save_parquet(data, path):

    table = pa.Table.from_pylist(data)

    pq.write_table(
        table,
        path,
        compression="SNAPPY"
    )


def read_parquet(path):

    table = pq.read_table(path)

    return table.to_pylist()



# ==========================================================
# 성능 테스트
# ==========================================================

def benchmark(name, data):

    print("\n" + "=" * 60)
    print(f"📂 {name.upper()} 데이터 테스트")

    csv_path = f"{name}.csv"
    parquet_path = f"{name}.parquet"


    # -------------------------
    # CSV
    # -------------------------

    start = time.time()

    save_csv(
        data,
        csv_path
    )

    csv_write = time.time() - start


    start = time.time()

    csv_result = read_csv(
        csv_path
    )

    csv_read = time.time() - start



    # -------------------------
    # Parquet
    # -------------------------

    start = time.time()

    save_parquet(
        data,
        parquet_path
    )

    parquet_write = time.time() - start


    start = time.time()

    parquet_result = read_parquet(
        parquet_path
    )

    parquet_read = time.time() - start



    # -------------------------
    # 출력
    # -------------------------

    csv_size = (
        os.path.getsize(csv_path)
        /
        (1024 * 1024)
    )

    parquet_size = (
        os.path.getsize(parquet_path)
        /
        (1024 * 1024)
    )


    print(f"""
📄 CSV
- 쓰기 : {csv_write:.4f}초
- 읽기 : {csv_read:.4f}초
- 용량 : {csv_size:.2f} MB
- rows : {len(csv_result):,}


📦 Parquet
- 쓰기 : {parquet_write:.4f}초
- 읽기 : {parquet_read:.4f}초
- 용량 : {parquet_size:.2f} MB
- rows : {len(parquet_result):,}
""")


# ==========================================================
# Main Test
# ==========================================================

@pytest.mark.asyncio
async def test_pipeline():

    print("🚀 API 호출")

    weather_raw, country_raw, ip_raw = await main()

    print("✓ API 완료")


    try:

        weather = (
            WeatherSchema
            .model_validate(weather_raw)
            .model_dump()
        )


        country_target = (
            country_raw[0]
            if isinstance(country_raw, list)
            else country_raw
        )

        country = (
            CountrySchema
            .model_validate(country_target)
            .model_dump()
        )


        ip = (
            IpSchema
            .model_validate(ip_raw)
            .model_dump()
        )


        print("✓ Pydantic 검증 완료")


        # ------------------------------------
        # 각각 50만 row 생성
        # ------------------------------------

        target_rows = 500_000


        weather_data = [
            weather
            for _ in range(target_rows)
        ]


        country_data = [
            country
            for _ in range(target_rows)
        ]


        ip_data = [
            ip
            for _ in range(target_rows)
        ]


        # ------------------------------------
        # 각각 테스트
        # ------------------------------------

        benchmark(
            "weather",
            weather_data
        )


        benchmark(
            "country",
            country_data
        )


        benchmark(
            "ip",
            ip_data
        )


    except ValidationError as e:

        print(e.json(indent=2))


if __name__ == "__main__":
    asyncio.run(test_pipeline())