# 프로젝트 구조 (정리 완료)

## 🎯 프로젝트 정체성

**"기업·산업을 '섹터 × 밸류체인 × 인과 구조'로 해석하고, 투자 판단이 데이터로 축적·재사용되는 학습형 투자 시스템"**

### 핵심 차별점
- **정답을 주지 않는다** - 경계·복합·비중을 드러낸다
- **판단을 기록 → 복기 → 재사용 가능하게 만든다**

---

## 📁 프로젝트 구조

```
news-insight-backend/
├── app/                          # 메인 애플리케이션 코드
│   ├── main.py                   # FastAPI 진입점
│   ├── config.py                 # 설정 관리
│   ├── db.py                     # 데이터베이스 연결
│   ├── celery_worker.py          # Celery 워커 설정
│   │
│   ├── models/                   # SQLAlchemy 모델
│   │   ├── stock.py              # 주식 기본 정보
│   │   ├── company_detail.py     # 기업 상세 정보
│   │   ├── company_detail_raw.py # 원본 데이터
│   │   ├── investor_sector.py    # 섹터 분류 결과
│   │   ├── edge.py               # KG Edge 모델
│   │   ├── economic_variable.py  # 경제 변수
│   │   ├── sector_reference.py   # 섹터 참조
│   │   └── value_chain_reference.py # 밸류체인 참조
│   │
│   ├── routes/                   # API 라우트
│   │   ├── feed.py               # RSS 피드
│   │   ├── article.py            # 기사 관련
│   │   ├── insight.py            # 인사이트
│   │   └── scenario.py           # 시나리오 분석 (KG V1.5)
│   │
│   ├── services/                 # 비즈니스 로직
│   │   ├── dart_parser.py        # DART API 파서
│   │   ├── llm_handler.py        # LLM 처리
│   │   ├── sector_classifier.py  # 섹터 분류 (Rule-based)
│   │   ├── sector_classifier_ensemble.py # 섹터 분류 (Ensemble)
│   │   ├── value_chain_classifier_embedding.py # 밸류체인 분류
│   │   ├── kg_edge_builder.py    # KG 엣지 빌더
│   │   ├── kg_explanation_layer.py # KG 설명 레이어
│   │   └── ...
│   │
│   └── utils/                    # 유틸리티
│       ├── text_cleaner.py
│       ├── semantic_sentence_extractor.py
│       └── ...
│
├── scripts/                      # 실행 스크립트
│   ├── 04_fetch_dart.py          # DART 데이터 수집
│   ├── 05_extract_relations.py   # KG 관계 추출
│   ├── build_driven_by_edges.py  # DRIVEN_BY 엣지 생성
│   ├── build_macro_graph.py      # Macro Graph 생성
│   ├── classify_value_chain_final.py # 밸류체인 분류
│   ├── reclassify_all_sectors_ensemble_optimized.py # 섹터 재분류
│   ├── reclassify_all_value_chains.py # 밸류체인 재분류
│   ├── verify_direct_indirect_classification.py # 검증
│   └── ...
│
├── sql/                          # SQL 스크립트
│   ├── schema.sql                # 기본 스키마
│   ├── schema_v2.sql             # 버전 2 스키마
│   └── migrations/               # 마이그레이션
│
├── docs/                         # 문서
│   ├── IR_DECK_ARCHITECTURE.md   # IR 아키텍처
│   ├── IR_DECK_ARCHITECTURE_DETAILED.md # 상세 아키텍처
│   ├── phase1_freeze_checklist.md # Phase 1 체크리스트
│   └── ...
│
├── docs_cache/                   # 문서 캐시
│   └── opendartreader_corp_codes_*.pkl
│
├── logs/                         # 로그 파일 (gitignore)
│
├── status/                       # 상태 파일
│
├── data/                         # 데이터 파일
│
├── docker-compose.yml            # Docker Compose 설정
├── Dockerfile                    # Docker 이미지
├── requirements.txt              # Python 의존성
└── README.md                     # 프로젝트 메인 문서
```

---

## 🔧 핵심 기능

### 1. 섹터 분류 (Rule + AI 하이브리드)
- **서비스**: `app/services/sector_classifier_ensemble.py`
- **스크립트**: `scripts/reclassify_all_sectors_ensemble_optimized.py`
- **4단계 앙상블**: Rule-based → 임베딩 → Re-ranking → GPT 검증

### 2. 밸류체인 분류 (AI 기반)
- **서비스**: `app/services/value_chain_classifier_embedding.py`
- **스크립트**: `scripts/classify_value_chain_final.py`
- **하이브리드**: Centroid (60%) + Text Anchor (40%)

### 3. KG 구축 (Knowledge Graph)
- **DRIVEN_BY 엣지**: `scripts/build_driven_by_edges.py`
- **Macro Graph**: `scripts/build_macro_graph.py`
- **관계 추출**: `scripts/05_extract_relations.py`

### 4. 인과 추론
- **Explanation Layer**: `app/services/kg_explanation_layer.py`
- **Scenario API**: `app/routes/scenario.py`
- **2-Hop 추론**: 변수 → 변수 → 기업

---

## 🗑️ 정리된 파일

### 삭제된 항목
1. **테스트 파일** (11개)
   - `test_*.py` (루트 및 scripts/)
   - `test_all_models.py`
   - `test_embedding_direct.py`

2. **디버그 스크립트** (3개)
   - `debug_*.py`

3. **중복 브리핑 문서** (17개)
   - 루트의 임시 브리핑 문서들

4. **중복 check 스크립트** (4개)
   - `check_embeddings_count.py`
   - `check_progress.py`
   - `check_final_results.py`
   - `check_status_now.py`

5. **__pycache__ 폴더** (모두 삭제)

### 유지된 핵심 스크립트
- `build_driven_by_edges.py` - KG 엣지 생성
- `build_macro_graph.py` - Macro Graph 생성
- `classify_value_chain_final.py` - 밸류체인 분류
- `reclassify_all_sectors_ensemble_optimized.py` - 섹터 재분류
- `reclassify_all_value_chains.py` - 밸류체인 재분류
- `verify_direct_indirect_classification.py` - 검증

---

## 📊 데이터베이스 스키마

### 주요 테이블
1. **stocks** - 주식 기본 정보
2. **company_details** - 구조화된 기업 정보
3. **company_details_raw** - 원본 데이터
4. **investor_sector** - 섹터 분류 결과
5. **edges** - KG 관계 (DRIVEN_BY, MACRO_LINK, SUPPLIES_TO 등)
6. **economic_variables** - 경제 변수 온톨로지
7. **sector_reference** - 섹터 참조
8. **value_chain_reference** - 밸류체인 참조

---

## 🚀 실행 워크플로우

### 1. 데이터 수집
```bash
python scripts/04_fetch_dart.py --year 2024
```

### 2. 섹터 분류
```bash
python scripts/reclassify_all_sectors_ensemble_optimized.py
```

### 3. 밸류체인 분류
```bash
python scripts/classify_value_chain_final.py
```

### 4. KG 엣지 생성
```bash
# DRIVEN_BY 엣지
python scripts/build_driven_by_edges.py

# Macro Graph
python scripts/build_macro_graph.py

# 관계 추출
python scripts/05_extract_relations.py
```

### 5. 검증
```bash
python scripts/verify_direct_indirect_classification.py
```

---

## 📝 주요 문서

- `README.md` - 프로젝트 개요
- `PROJECT_STRUCTURE.md` - 상세 구조
- `docs/IR_DECK_ARCHITECTURE.md` - IR 아키텍처
- `docs/phase1_freeze_checklist.md` - Phase 1 체크리스트
- `docs/KG_ARCHITECTURE_VISUALIZATION.md` - KG 구조 시각화

---

## 🔄 다음 단계 (Phase 2)

1. 증권사 리포트 수집
2. 어휘 업그레이드 사전 생성
3. Evidence Pool 확장

---

**정리 완료일**: 2024-12-19
**프로젝트 버전**: V1.5.4

