# 프로젝트 정리 계획

## 삭제할 항목

### 1. Scripts (필수 스크립트만 유지)
**유지:**
- `01_create_tables.py`
- `01_import_ontology.py`
- `04_fetch_dart.py`
- `reclassify_all_companies.py`
- `refetch_all_missing_revenue.py`
- `sync_krx_stocks.py`
- `check_state_consistency.py`
- `__init__.py`
- `ESSENTIAL_SCRIPTS.md`

**삭제:** 나머지 모든 스크립트 (약 200개)

### 2. Data 디렉토리 (임시 파일 삭제)
**유지:**
- `kosdaq_data.csv`
- `kospi_data.csv`
- `krx_sector_industry.csv`
- `krx_sectors_list.txt`
- `Economic_Ontology_V5.xlsx`
- `CAPEX_ADDITIONS.csv` (확인 필요)

**삭제:**
- `ab_test_results/`
- `gemini_test_results/`
- `gemini_test_monitoring/`
- `quantization_test_results/`
- `pdf_cache/`
- `*.txt` (로그/임시 파일)
- `*.json` (테스트 결과)
- `*.log` (로그 파일)
- `export_*.xlsx` (임시 export 파일)
- `KF-DeBERTa_모델학습_샘플링_데이터.xlsx` (테스트 데이터)

### 3. 임시 스크립트
**삭제:**
- `check_docker.ps1`
- `check_server.py`
- `check_versions.py`
- `start_backend.ps1`
- `start_server.ps1`
- `restart_docker.ps1`

### 4. 중복 문서
**유지:**
- `README.md`
- `LICENSE`
- `.gitignore`
- `requirements.txt`
- `Dockerfile`
- `docker-compose.yml`
- `Makefile`
- `run.sh`

**삭제:**
- `GITHUB_PREP_FINAL.md`
- `QUICKSTART.md`
- `QUICK_START.md`
- `README_DOCKER.md`
- `README_SERVER.md`
- `DOCKER_COMMANDS.md`
- `DOCKER_SETUP.md`
- `LOCAL_SETUP.md`
- `MONITORING_GUIDE.md`
- `PGVECTOR_SETUP.md`
- `PROJECT_STRUCTURE.md`
- `PROJECT_STRUCTURE_CLEANED.md`
- `STOCK_DATA_GUIDE.md`
- `STOCK_DATA_SETUP.md`

### 5. Docs 디렉토리
**유지:**
- `SECTOR_CLASSIFICATION_ARCHITECTURE.md`
- `IR_DECK_ARCHITECTURE.md`
- `DATABASE_START_GUIDE.md`
- `28_sectors_structure.md`
- `SECTOR_CLASSIFICATION_SUMMARY.md`

**삭제:**
- `battery_segment_fix_briefing.md` (임시 브리핑)
- `final_all_improvements_briefing.md` (임시 브리핑)
- `year_parameter_fix_briefing.md` (임시 브리핑)
- `GITHUB_UPLOAD_COMPLETE.md` (임시 브리핑)

### 6. 기타 디렉토리
**삭제:**
- `utils/` (app/utils와 중복)
- `docs_cache/`
- `reports/` (이미 .gitignore에 있음)
- `status/` (이미 .gitignore에 있음)
- `enrichment_debug.log`

### 7. 루트 파일
**삭제:**
- `dart_analysis_result.json` (임시 분석 결과)
- `test_file.py` (테스트 파일)

## 유지할 핵심 구조

```
news-insight-backend/
├── app/                    # 핵심 애플리케이션
├── extractors/             # 데이터 추출기
├── parsers/                # 파서
├── scripts/                # 필수 스크립트만 (8개)
├── sql/                    # 데이터베이스 스키마
├── tests/                  # 테스트
├── data/                   # 필수 데이터만 (6개 파일)
├── docs/                   # 핵심 문서만 (5개)
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── .gitignore
├── README.md
└── LICENSE
```

