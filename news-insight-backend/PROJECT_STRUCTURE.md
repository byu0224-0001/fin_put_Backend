# News Insight Backend 프로젝트 구조

## 📁 전체 디렉토리 구조

```
news-insight-backend/
├── app/                          # 메인 애플리케이션 코드
│   ├── __init__.py
│   ├── main.py                   # FastAPI 애플리케이션 진입점
│   ├── config.py                 # 설정 관리
│   ├── db.py                     # 데이터베이스 연결
│   ├── celery_worker.py          # Celery 워커 설정
│   │
│   ├── models/                   # SQLAlchemy 모델
│   │   ├── article.py            # 뉴스 기사 모델
│   │   ├── stock.py              # 주식 정보 모델
│   │   ├── company_detail.py     # 기업 상세 정보 모델
│   │   ├── company_detail_raw.py # 원본 데이터 모델
│   │   ├── company_detail_version.py # 버전 관리 모델
│   │   ├── investor_sector.py    # 섹터 분류 모델
│   │   ├── edge.py               # KG Edge 모델
│   │   ├── sector_reference.py   # 섹터 참조 모델
│   │   ├── value_chain_reference.py # 밸류체인 참조 모델
│   │   ├── economic_variable.py  # 경제 변수 모델
│   │   ├── processing_log.py     # 처리 로그 모델
│   │   └── ...
│   │
│   ├── routes/                   # API 라우트
│   │   ├── article.py            # 기사 관련 API
│   │   ├── feed.py               # 피드 관련 API
│   │   └── insight.py            # 인사이트 관련 API
│   │
│   ├── services/                 # 비즈니스 로직 서비스
│   │   ├── dart_parser.py        # DART API 파서
│   │   ├── llm_handler.py        # LLM 처리 핸들러
│   │   ├── sector_classifier.py  # 섹터 분류 (Rule-based)
│   │   ├── sector_classifier_ensemble.py # 섹터 분류 (Ensemble)
│   │   ├── sector_classifier_embedding.py # 섹터 분류 (임베딩)
│   │   ├── sector_classifier_reranker.py # 섹터 분류 (Re-ranking)
│   │   ├── sector_classifier_validator.py # 섹터 분류 (GPT 검증)
│   │   ├── value_chain_classifier.py # 밸류체인 분류 (하이브리드)
│   │   ├── value_chain_classifier_embedding.py # 밸류체인 분류 (임베딩)
│   │   ├── value_chain_classifier_reranker.py # 밸류체인 분류 (Re-ranking)
│   │   ├── value_chain_classifier_validator.py # 밸류체인 분류 (GPT 검증)
│   │   ├── entity_resolver.py    # 엔티티 해결 (기업명 매칭)
│   │   ├── rss_collector.py      # RSS 피드 수집
│   │   ├── parser.py             # 텍스트 파서
│   │   ├── deduplicator.py       # 중복 제거
│   │   ├── embedding_filter.py   # 임베딩 필터링
│   │   ├── retry_handler.py      # 재시도 핸들러
│   │   └── pipelines/            # NLP 파이프라인
│   │       ├── entities.py       # 개체명 인식
│   │       ├── keywords.py       # 키워드 추출
│   │       ├── sentiment.py      # 감성 분석
│   │       └── ...
│   │
│   └── utils/                    # 유틸리티 함수
│       ├── stock_query.py        # 주식 조회 유틸
│       ├── text_chunking.py      # 텍스트 청킹
│       ├── semantic_sentence_extractor.py # 의미 기반 문장 추출
│       ├── company_complexity_detector.py # 복합기업 감지
│       └── ...
│
├── scripts/                      # 실행 스크립트
│   ├── 04_fetch_dart.py         # DART 데이터 수집
│   ├── 05_check_company_details.py # 기업 정보 확인
│   ├── 05_extract_relations.py  # KG 관계 추출
│   ├── 45_auto_classify_sectors.py # 섹터 자동 분류
│   ├── reclassify_all_sectors_ensemble_optimized.py # 전체 섹터 재분류
│   ├── reclassify_all_value_chains.py # 전체 밸류체인 재분류
│   ├── 99_check_db_status.py    # DB 상태 확인
│   └── ...
│
├── sql/                          # SQL 스크립트
│   ├── schema.sql                # 기본 스키마
│   ├── schema_v2.sql             # 버전 2 스키마
│   └── migrate_*.sql             # 마이그레이션 스크립트
│
├── data/                         # 데이터 파일
│   ├── *.csv                     # CSV 데이터
│   ├── *.xlsx                    # Excel 데이터
│   └── *.txt                     # 텍스트 데이터 (로그 제외)
│
├── docs/                         # 문서
│   └── *.md                      # 마크다운 문서
│
├── docs_cache/                   # 문서 캐시
│   └── opendartreader_corp_codes_*.pkl # DART 기업 코드 캐시
│
├── logs/                         # 로그 파일 (gitignore)
│   └── *.log                     # 실행 로그
│
├── status/                       # 상태 파일
│   ├── *_status.json             # 작업 상태 JSON
│   └── *_completed.flag          # 완료 플래그
│
├── docker-compose.yml            # Docker Compose 설정
├── Dockerfile                    # Docker 이미지 설정
├── requirements.txt              # Python 의존성
├── Makefile                     # Make 명령어
├── README.md                     # 프로젝트 메인 문서
└── *.md                          # 기타 문서 파일
```

## 🔧 주요 컴포넌트 설명

### 1. 데이터 수집 (Data Collection)

#### DART API 수집
- **스크립트**: `scripts/04_fetch_dart.py`
- **서비스**: `app/services/dart_parser.py`
- **프로세스**:
  1. DART API로 사업보고서 핵심 섹션 추출
  2. 임베딩 필터링으로 관련 청크만 선택
  3. LLM으로 구조화된 데이터 추출
  4. `CompanyDetailRaw` 및 `CompanyDetail` 테이블에 저장

### 2. 섹터 분류 (Sector Classification)

#### Ensemble 섹터 분류 파이프라인
- **서비스**: `app/services/sector_classifier_ensemble.py`
- **4단계 앙상블**:
  1. **Rule-based** (가중치 40%): 키워드 매칭
  2. **임베딩 모델** (가중치 30%): KF-DeBERTa 후보 생성
  3. **BGE-M3 Re-ranking** (가중치 20%): Top-5 → Top-2
  4. **GPT 검증** (가중치 10%): 최종 1~3개 섹터 결정

#### 전체 재분류
- **스크립트**: `scripts/reclassify_all_sectors_ensemble_optimized.py`
- **최적화**: 배치 처리, 병렬화, 조건부 GPT 호출

### 3. 밸류체인 분류 (Value Chain Classification)

#### 하이브리드 밸류체인 분류
- **서비스**: `app/services/value_chain_classifier.py`
- **프로세스**:
  1. Rule-based 분류 (Confidence > 0.85면 즉시 반환)
  2. Ensemble 분류 (임베딩 + BGE-M3 + GPT)

#### 전체 재분류
- **스크립트**: `scripts/reclassify_all_value_chains.py`
- **주의**: 섹터 분류가 먼저 완료되어야 함

### 4. KG 관계 추출 (Knowledge Graph Edge Creation)

#### 관계 추출
- **스크립트**: `scripts/05_extract_relations.py`
- **4가지 Edge 타입**:
  1. **SUPPLIES_TO**: 공급망 관계 (supply_chain → Edge)
  2. **SELLS_TO**: 판매 관계 (clients → Edge)
  3. **POTENTIAL_SUPPLIES_TO**: 역방향 추론
  4. **VALUE_CHAIN_RELATED**: 밸류체인 기반 관계

## 📊 데이터베이스 스키마

### 주요 테이블

1. **stocks**: 주식 기본 정보
2. **company_details**: 구조화된 기업 정보
3. **company_details_raw**: 원본 데이터 (Markdown, LLM JSON)
4. **investor_sectors**: 섹터 분류 결과
5. **edges**: KG 관계 (Edge)
6. **articles**: 뉴스 기사
7. **economic_variables**: 경제 변수

## 🚀 실행 워크플로우

### 1. 초기 설정
```bash
# 환경 변수 설정
cp .env.example .env

# 의존성 설치
pip install -r requirements.txt

# 데이터베이스 초기화
python scripts/01_create_tables.py
```

### 2. 데이터 수집
```bash
# DART 데이터 수집
python scripts/04_fetch_dart.py --year 2024
```

### 3. 섹터 분류
```bash
# 전체 기업 섹터 재분류
python scripts/reclassify_all_sectors_ensemble_optimized.py
```

### 4. 밸류체인 분류
```bash
# 전체 기업 밸류체인 재분류
python scripts/reclassify_all_value_chains.py
```

### 5. KG 관계 추출
```bash
# 관계 추출
python scripts/05_extract_relations.py
```

### 6. 확인
```bash
# 기업 정보 확인
python scripts/05_check_company_details.py

# Edge 확인
python scripts/08_check_edges.py

# DB 상태 확인
python scripts/99_check_db_status.py
```

## 📝 주요 설정 파일

- **`.env`**: 환경 변수 (DART_API_KEY, OPENAI_API_KEY 등)
- **`requirements.txt`**: Python 패키지 의존성
- **`docker-compose.yml`**: Docker Compose 설정
- **`config.py`**: 애플리케이션 설정

## 🔍 모니터링 및 로그

- **로그 파일**: `logs/` 폴더 (gitignore)
- **상태 파일**: `status/` 폴더 (JSON + 플래그)
- **데이터 로그**: `data/*.txt` (일부)

## 🧹 정리된 파일

다음 파일들은 정리되었습니다:
- ✅ `__pycache__/` 폴더 삭제
- ✅ `*.pyc` 파일 삭제
- ✅ pip 설치 로그 파일 삭제 (`1.6.0`, `2.4.18`)
- ✅ 루트 로그 파일 삭제 (`sector_reclassify.log`)
- ✅ data 폴더 로그 파일 삭제 (`*.log`)

## 📚 추가 문서

- `README.md`: 프로젝트 개요
- `QUICK_START.md`: 빠른 시작 가이드
- `DOCKER_SETUP.md`: Docker 설정 가이드
- `docs/`: 상세 문서

