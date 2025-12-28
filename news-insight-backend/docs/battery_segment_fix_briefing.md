# 배터리사업 세그먼트 추출 문제 해결 브리핑

**작성일**: 2025-12-28  
**상태**: ✅ `--ticker` 옵션 추가 완료, 데이터 재수집 필요

---

## 📋 문제 확인

### 현재 상태

SK이노베이션 (096770) revenue_by_segment:
- PX: 18.95%
- B-C: 11.20%
- 경유: 23.36%
- 등유: 0.36%
- 제품: 8.82%
- 기 타: 0.12%
- 나프타: 16.81%
- 항공유: 20.38%

**문제점:**
- ❌ 배터리사업(15.41%) 세그먼트가 추출되지 않음
- 총 8개 세그먼트만 존재

---

## ✅ 완료된 개선

### 1. `--ticker` 옵션 추가

**변경 사항:**
- `refetch_all_missing_revenue.py`에 `--ticker` 옵션 추가
- 특정 티커만 강제 재수집 가능
- 기존 데이터가 있어도 재수집 가능

**사용법:**
```powershell
python scripts/refetch_all_missing_revenue.py --ticker 096770 --apply
```

**변경된 함수:**
- `get_missing_revenue_companies(db, limit=None, ticker=None)`: ticker 파라미터 추가
- `refetch_all_missing_revenue(..., ticker=None)`: ticker 파라미터 추가

---

## 🔍 원인 분석

### 가능한 원인 후보

#### 1. 데이터 재수집 미실행 (해결됨 ✅)
- 기존 revenue_by_segment가 존재하여 재수집 대상에서 제외됨
- **해결**: `--ticker` 옵션으로 강제 재수집 가능

#### 2. DART 보고서 구조 문제 (확인 필요)
- 배터리사업이 별도 표에 있거나 다른 섹션에 있을 수 있음
- 연결 재무제표에서 배터리사업이 별도 자회사로 분리되어 있을 수 있음

#### 3. 파서 로직 문제 (확인 필요)
- 배터리사업이 "배터리사업"이 아닌 다른 이름으로 표시될 수 있음
  - 예: "SK온", "2차전지사업", "전지사업", "배터리 부문"
- 계층 구조에서 배터리사업이 하위 품목으로만 표시될 수 있음

#### 4. 테이블 선택 문제 (확인 필요)
- 배터리사업이 포함된 표가 사업부문 표가 아닌 다른 표에 있을 수 있음
- 테이블 선택 우선순위에서 배터리사업 표가 누락될 수 있음

---

## 🚀 다음 스텝

### Step 1: SK이노베이션 데이터 재수집 (즉시 실행)

```powershell
# 1. 데이터 재수집
python scripts/refetch_all_missing_revenue.py --ticker 096770 --apply

# 2. 재수집 후 분류
python scripts/reclassify_all_companies.py --ticker 096770 --apply

# 3. 결과 확인
python scripts/check_sk_innovation_revenue.py
```

**확인 사항:**
- 배터리사업(15.41%) 추출 여부
- 총 세그먼트 수 증가 여부
- 파서 로그에서 배터리사업 추출 과정 확인

### Step 2: 파서 로직 디버깅 (재수집 후에도 배터리 미추출 시)

**확인 사항:**
1. DART 보고서에서 배터리사업 데이터 존재 여부
2. 파서가 배터리사업 표를 선택하는지
3. 배터리사업 세그먼트가 추출되는지

**디버깅 방법:**
- `revenue_table_parser.py`에 로깅 추가
- DART 보고서 HTML 구조 확인
- 파서가 선택한 테이블 확인

### Step 3: 파서 로직 개선 (필요 시)

**개선 방향:**

#### A. 배터리사업 키워드 확장
```python
# SEGMENT_TO_SECTOR_MAP에 추가
'배터리사업': 'SEC_BATTERY',
'2차전지사업': 'SEC_BATTERY',
'전지사업': 'SEC_BATTERY',
'배터리 부문': 'SEC_BATTERY',
```

#### B. 테이블 선택 로직 개선
```python
# 배터리 관련 키워드가 있는 표도 우선순위 상향
if any(kw in table_text for kw in ['배터리', '전지', '2차전지', 'SK온']):
    score += 5  # 배터리 관련 표 우선순위 상향
```

#### C. 계층 구조 처리 개선
```python
# "배터리사업" 상위 부문 인식
if any(kw in segment_name for kw in ['배터리사업', '2차전지사업', '전지사업']):
    current_business_segment = segment_name
```

#### D. 자회사명 매핑 개선
```python
# consolidate_by_business_segment에서 SK온 → 배터리사업 매핑
company_to_segment = {
    'SK온': '배터리사업',  # '배터리' → '배터리사업'으로 변경
    'SK에너지': '석유사업',
    'SK지오센트릭': '화학사업',
}
```

---

## 📊 예상 결과

### 재수집 후 예상 revenue_by_segment

```
- 배터리사업: 15.41% ✅ (신규 추출)
- 석유사업: 58.46% (Crude + 무연 + 경유 + B-C + 기타)
- 화학사업: 26.16% (PX + B-C + 나프타)
- 기타사업: 2.09%
```

### 듀얼 섹터 정보

- Primary: SEC_CHEM (46.96%)
- Secondary: SEC_ENERGY (44.10%)
- Margin: 2.86%
- Enabled: True

### 배터리 누락 설명

- 텍스트 신호: biz_summary/keywords/products에서 배터리 키워드 감지
- revenue_by_segment에 배터리 있음 → battery_missing 저장 안 함 ✅

---

## 📝 개선 사항 요약

### 완료된 개선

1. ✅ `--ticker` 옵션 추가
   - 특정 티커만 재수집 가능
   - 기존 데이터가 있어도 강제 재수집 가능

### 확인 필요 (재수집 후)

1. 🔍 배터리사업 추출 여부
2. 🔍 파서 로직 정상 작동 여부
3. 🔍 DART 보고서 구조 확인

### 추가 개선 필요 (확인 후)

1. 배터리사업 키워드 확장
2. 테이블 선택 로직 개선
3. 계층 구조 처리 개선
4. 자회사명 매핑 개선

---

## 🎯 최종 결론

**현재 상태:**
- ✅ `--ticker` 옵션 추가 완료
- 🔍 데이터 재수집 필요
- 🔍 파서 로직 테스트 필요

**다음 액션:**
1. SK이노베이션 데이터 재수집 실행
2. 재수집 후 배터리사업 추출 확인
3. 미추출 시 파서 로직 디버깅 및 개선

---

**작성자**: AI Assistant  
**다음 액션**: 데이터 재수집 후 배터리사업 추출 확인

