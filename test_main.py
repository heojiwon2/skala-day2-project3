"""
Day1 종합실습 — 스키마 검증 + 저장 형식(CSV vs Parquet) 성능 비교 테스트
작성자 : 울산 4반 허지원

====================================================================
1) api.main()으로 3개 API(날씨/국가/IP)를 동시 수집
2) Pydantic v2 모델로 각 응답을 검증 (실패 시 pytest.fail로 테스트 실패 처리)
3) 검증된 레코드를 50만 행으로 복제해 CSV/Parquet 저장·재로딩 성능 비교
4) 저장 전후 행 수 일치, Parquet 용량이 CSV보다 작은지 assert로 검증
"""
import asyncio
import os
import csv
import time
import pyarrow as pa
import pyarrow.parquet as pq
from typing import List
from pydantic import BaseModel, Field, ValidationError
from typing_extensions import Annotated
import pytest

# 1. API 가져오는 기존 코드
from api import main  # api.py에서 main() 함수를 가져옵니다.

# =====================================================================
# 2. Pydantic v2 스키마 정의
# =====================================================================
Latitude = Annotated[float, Field(ge=-90.0, le=90.0)]
Longitude = Annotated[float, Field(ge=-180.0, le=180.0)]


class WeatherHourly(BaseModel):
    """Open-Meteo hourly 블록 — 시간별 타임스탬프 배열만 검증 대상으로 사용"""
    time: List[str]


class WeatherSchema(BaseModel):
    """Open-Meteo 응답 검증 스키마 (위경도 범위 + hourly 필드 존재 여부)"""
    latitude: Latitude
    longitude: Longitude
    timezone: str
    hourly: WeatherHourly


class CountrySchema(BaseModel):
    """RestCountries 응답 검증 스키마"""
    name: str
    alpha2Code: str = Field(alias="alpha2Code")
    population: Annotated[int, Field(ge=0)]  # 인구는 음수일 수 없음
    region: str


class IpSchema(BaseModel):
    """ip-api 응답 검증 스키마 (위경도는 Latitude/Longitude로 범위 재검증)"""
    status: str
    query: str
    countryCode: str
    city: str
    lat: Latitude
    lon: Longitude


# ==========================================================
# CSV 저장 / 읽기
# ==========================================================

def save_csv(data: list[dict], path: str) -> None:
    """dict 리스트를 CSV로 저장한다.

    예외 처리: 디스크 공간 부족/권한 오류(OSError)를 어떤 파일에서
    실패했는지 알 수 있는 메시지로 감싸서 다시 던진다.
    """
    fields = list(data[0].keys())
    try:
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(data)
    except OSError as e:
        raise OSError(f"CSV 저장 실패: {path} ({e})") from e


def read_csv(path: str) -> list[dict]:
    """CSV를 다시 읽어 dict 리스트로 반환한다."""
    result = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                result.append(row)
    except OSError as e:
        raise OSError(f"CSV 읽기 실패: {path} ({e})") from e
    return result


# ==========================================================
# Parquet 저장 / 읽기
# ==========================================================

def save_parquet(data: list[dict], path: str) -> None:
    """dict 리스트를 Parquet(SNAPPY 압축)으로 저장한다."""
    try:
        table = pa.Table.from_pylist(data)
        pq.write_table(table, path, compression="SNAPPY")
    except (pa.ArrowInvalid, OSError) as e:
        raise OSError(f"Parquet 저장 실패: {path} ({e})") from e


def read_parquet(path: str) -> list[dict]:
    """Parquet을 다시 읽어 dict 리스트로 반환한다."""
    try:
        table = pq.read_table(path)
        return table.to_pylist()
    except (pa.ArrowInvalid, OSError) as e:
        raise OSError(f"Parquet 읽기 실패: {path} ({e})") from e


# ==========================================================
# 성능 테스트
# ==========================================================

def benchmark(name: str, data: list[dict]) -> dict:
    """CSV/Parquet 저장·재로딩 시간과 파일 크기를 측정해 출력하고, 지표를 반환한다.

    반환값을 dict로 넘겨줘야 호출부(test_pipeline)에서 행 수 일치·
    용량 비교 같은 assert 검증을 할 수 있다.
    """
    print("\n" + "=" * 60)
    print(f"📂 {name.upper()} 데이터 테스트")

    csv_path = f"{name}.csv"
    parquet_path = f"{name}.parquet"

    # -------------------------
    # CSV
    # -------------------------
    start = time.time()
    save_csv(data, csv_path)
    csv_write = time.time() - start

    start = time.time()
    csv_result = read_csv(csv_path)
    csv_read = time.time() - start

    # -------------------------
    # Parquet
    # -------------------------
    start = time.time()
    save_parquet(data, parquet_path)
    parquet_write = time.time() - start

    start = time.time()
    parquet_result = read_parquet(parquet_path)
    parquet_read = time.time() - start

    # -------------------------
    # 출력
    # -------------------------
    csv_size = os.path.getsize(csv_path) / (1024 * 1024)
    parquet_size = os.path.getsize(parquet_path) / (1024 * 1024)

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

    return {
        "csv_rows": len(csv_result),
        "parquet_rows": len(parquet_result),
        "csv_size": csv_size,
        "parquet_size": parquet_size,
    }


# ==========================================================
# Main Test
# ==========================================================

@pytest.mark.asyncio
async def test_pipeline():
    """API 수집 → 스키마 검증 → 저장/재로딩 성능 비교까지 End2End로 검증한다."""
    print("API 호출")
    weather_raw, country_raw, ip_raw = await main()
    print("API 완료")

    # 스키마 검증 실패 시 pytest.fail로 테스트를 명시적으로 실패시킨다.
    # (기존에는 ValidationError를 print만 하고 넘어가서, 검증이 깨져도
    #  테스트가 계속 통과 처리되는 문제가 있었음)
    try:
        weather = WeatherSchema.model_validate(weather_raw).model_dump()

        country_target = (
            country_raw[0] if isinstance(country_raw, list) else country_raw
        )
        country = CountrySchema.model_validate(country_target).model_dump()

        ip = IpSchema.model_validate(ip_raw).model_dump()
    except ValidationError as e:
        pytest.fail(f"Pydantic 스키마 검증 실패:\n{e}")

    print("✓ Pydantic 검증 완료")

    # ------------------------------------
    # 각각 50만 row 생성 (CSV vs Parquet 성능 차이를 보기 위한 대량 데이터)
    # ------------------------------------
    target_rows = 500_000
    weather_data = [weather for _ in range(target_rows)]
    country_data = [country for _ in range(target_rows)]
    ip_data = [ip for _ in range(target_rows)]

    # ------------------------------------
    # 각각 벤치마크 실행 + 결과 검증
    # ------------------------------------
    for name, data in (
        ("weather", weather_data),
        ("country", country_data),
        ("ip", ip_data),
    ):
        result = benchmark(name, data)

        # 저장 전후 행 수가 그대로 보존되는지 확인
        assert result["csv_rows"] == target_rows, (
            f"{name}: CSV 재로딩 행 수 불일치 ({result['csv_rows']} != {target_rows})"
        )
        assert result["parquet_rows"] == target_rows, (
            f"{name}: Parquet 재로딩 행 수 불일치 ({result['parquet_rows']} != {target_rows})"
        )
        # 반복 데이터는 컬럼형 압축(Parquet+SNAPPY)이 훨씬 유리해야 정상
        assert result["parquet_size"] < result["csv_size"], (
            f"{name}: Parquet({result['parquet_size']:.2f}MB)이 "
            f"CSV({result['csv_size']:.2f}MB)보다 작지 않음"
        )


if __name__ == "__main__":
    asyncio.run(test_pipeline())
