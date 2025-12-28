# 섹터 분류 시스템 요약

## 🎯 핵심 개요

### 메인 함수
- **`classify_sector_ensemble_won()`**: 전체 파이프라인 진입점
- **`classify_sector_ensemble()`**: Step 0-3.5 기존 Ensemble 방식

### 전체 프로세스
```
Step 0-3.5: 기존 Ensemble (Rule + Embedding + Boosting + Sub-sector)
  ↓
Step 0.5-0.8: 특수 케이스 처리 (지주사, 리츠, Primary, L2)
  ↓
Step 4A-4.5: 고급 분석 (Driver Signal + Gemini Reasoning + Exposure Drivers)
  ↓
최종 결과 반환
```

---

## 📊 단계별 요약

### Step 0-3.5: 기존 Ensemble 방식

| Step | 이름 | 가중치 | 설명 |
|------|------|--------|------|
| 0 | KRX 업종 Prior | - | KRX 공시 업종 정보 활용 |
| 1 | Rule-based | 40% | 키워드 기반 분류, HIGH면 즉시 반환 |
| 2 | Solar Embedding | 30% | 임베딩 모델로 Top-5 후보 생성 |
| 2.5 | Anchor Boosting | - | Anchor 기업 기반 가산점 |
| 2.6 | KG Edge Boosting | - | Knowledge Graph 기반 가산점 |
| 3 | BGE-M3 | 제거됨 | Solar Embedding으로 통합 |
| 3.5 | Sub-sector 분류 | - | 규칙 기반 하위 섹터 분류 |
| 4 | GPT 검증 | 10% | 선택적 최종 검증 |

### Step 0.5-0.8: 특수 케이스 처리

| Step | 이름 | 설명 |
|------|------|------|
| 0.5 | 지주사 분류 | 지주사 키워드 감지 시 `SEC_HOLDING` 추가 |
| 0.6 | 리츠 분류 | 리츠 키워드 감지 시 `SEC_REIT` 추가 |
| 0.7 | Primary 결정 | Multi-sector 케이스에서 Primary 섹터 결정 |
| 0.8 | L2 분리 | 규칙 기반으로 L2 섹터 분리 |

### Step 4A-4.5: 고급 분석

| Step | 이름 | 설명 |
|------|------|------|
| 4A | Driver Signal | KF-DeBERTa로 가격/수량/비용 드라이버 추출 |
| 4B | Gemini Reasoning | 인과 구조 생성 (로컬 LLM 대체) |
| 4.5 | Exposure Drivers | 노출 드라이버 추출 |

---

## 🔄 Fallback 메커니즘

### Fallback 순서
1. **Rule-based Fallback** (`fallback_type: 'RULE'`)
   - 후보 생성 실패 시
   - Confidence: `MEDIUM`

2. **Top-1 Candidate Fallback** (`fallback_type: 'TOP1'`)
   - Rule-based 실패, 후보 있음
   - Confidence: `LOW`

3. **KRX Fallback** (`fallback_type: 'KRX'`)
   - KRX 업종 정보 있음
   - Confidence: `VERY_LOW`

4. **UNKNOWN Fallback** (`fallback_type: 'UNKNOWN'`)
   - 모든 방법 실패
   - 섹터: `SEC_UNKNOWN`
   - Confidence: `VERY_LOW`

### NULL 섹터 강제 처리
- 조건: `sector_l1` 또는 `major_sector`가 NULL
- 처리: `SEC_UNKNOWN` 할당
- 설정: `fallback_used='TRUE'`, `fallback_type='UNKNOWN'`

---

## 🚀 Boosting 시스템

### Anchor Boosting
- **목적**: Anchor 기업(고객사) 기반 가산점 부여
- **Gate 조건**:
  - 범용 섹터 완전 금지
  - Role Gate 감쇠 (80%)
  - Top-2 gap < 0.03
  - Budget 체크 (`MAX_TOTAL_BOOST = 0.05`)

### KG Edge Boosting
- **목적**: Knowledge Graph Edge 기반 가산점 부여
- **Gate 조건**: Anchor Boosting과 동일
- **추가**: Edge 타입별 가중치 차등

### Boosting 로그
```python
{
    'anchor_applied': bool,
    'kg_applied': bool,
    'multiplier': float,
    'final_boost': float,
    'reason': str
}
```

---

## 📈 Confidence 레벨

| 레벨 | 조건 | 설명 |
|------|------|------|
| HIGH | `ensemble_score >= 0.7` 또는 `rule_score >= 0.9` | 높은 신뢰도 |
| MEDIUM | `0.5 <= ensemble_score < 0.7` | 중간 신뢰도 |
| LOW | `0.3 <= ensemble_score < 0.5` | 낮은 신뢰도 |
| VERY_LOW | `ensemble_score < 0.3` 또는 Fallback | 매우 낮은 신뢰도 |

---

## 📊 가중치 시스템

### 기본 가중치
- **Rule-based**: 40%
- **Solar Embedding**: 50% (KF-DeBERTa + BGE-M3 통합)
- **GPT 검증**: 10% (선택적)

### 동적 조정
- GPT 비활성화 시: Rule 50%, Embedding 50%
- Rule HIGH: Rule 가중치 증가
- 임베딩 신뢰도 높음: Embedding 가중치 증가

---

## 📁 주요 파일

```
app/services/
├── sector_classifier_ensemble.py      # Step 0-3.5 기존 Ensemble
├── sector_classifier_ensemble_won.py  # Step 4A-4.5 Gemini-Reasoning
├── sector_classifier.py              # Rule-based 분류기
├── sector_classifier_embedding.py    # 임베딩 모델 후보 생성
├── sector_classifier_validator.py    # GPT 검증
├── gemini_handler.py                 # Gemini-Reasoning Handler
└── sentence_signal_extractor.py      # KF-DeBERTa Driver Signal
```

---

## 📊 현재 상태

### 성능 지표
- **NULL 섹터**: 0개 ✅
- **임베딩 커버리지**: 100% ✅
- **LOW confidence 비율**: 38.81%
- **Fallback 사용률**: 0.1% (3개)
- **Boosting 적용률**: 1,865개 기업 (로그 저장)

### 데이터 구조
```python
{
    'major_sector': str,        # 주요 섹터
    'sub_sector': str,          # 하위 섹터
    'sector_l1': str,           # L1 섹터 코드
    'sector_l2': str,           # L2 섹터 코드
    'value_chain': str,         # 밸류체인
    'weight': float,            # 섹터 가중치
    'is_primary': bool,         # Primary 섹터 여부
    'confidence': str,          # Confidence 레벨
    'method': str,              # 분류 방법
    'rule_score': float,        # Rule-based 점수
    'embedding_score': float,   # 임베딩 점수
    'ensemble_score': float,   # 앙상블 점수
    'reasoning': str,           # 분류 근거
    'fallback_used': str,       # Fallback 사용 여부
    'fallback_type': str,       # Fallback 타입
    'boosting_log': dict        # Boosting 로그
}
```

---

## 🔗 관련 문서

1. **[SECTOR_CLASSIFICATION_ARCHITECTURE.md](./SECTOR_CLASSIFICATION_ARCHITECTURE.md)**: 상세 아키텍처 문서
2. **[SECTOR_CLASSIFICATION_FLOWCHART.md](./SECTOR_CLASSIFICATION_FLOWCHART.md)**: 플로우차트
3. **[SECTOR_CLASSIFICATION_VISUALIZATION.md](./SECTOR_CLASSIFICATION_VISUALIZATION.md)**: 시각화 다이어그램

---

**작성일**: 2025-12-16
**버전**: v1.0

