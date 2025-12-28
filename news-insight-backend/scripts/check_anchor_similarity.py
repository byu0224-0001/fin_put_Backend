#!/usr/bin/env python3
"""
Anchor 간 유사도 검증 스크립트

5단계 밸류체인 Anchor 간 cosine similarity를 계산하여
Anchor가 서로 구분되는지 확인합니다.
"""
import sys
import os
from pathlib import Path
from typing import Dict
import numpy as np

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# UTF-8 인코딩 설정 (Windows 환경)
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

from dotenv import load_dotenv
from app.services.value_chain_classifier_embedding import (
    get_value_chain_reference_embeddings,
    compute_cosine_similarity,
    VALUE_CHAIN_ANCHORS
)

load_dotenv()

# 5단계 밸류체인
PRIORITY_VC_CODES = ['UPSTREAM', 'MID_HARD', 'MID_SOFT', 'DOWN_BIZ', 'DOWN_SERVICE']


def compute_anchor_similarity_matrix() -> Dict[str, Dict[str, float]]:
    """
    Anchor 간 유사도 매트릭스 계산
    
    Returns:
        {vc1: {vc2: similarity, ...}, ...} 딕셔너리
    """
    print("=" * 80)
    print("Anchor 임베딩 생성 중...")
    print("=" * 80)
    
    # Anchor 임베딩 로드
    anchor_embeddings = get_value_chain_reference_embeddings()
    
    # 5단계만 필터링
    filtered_embeddings = {
        vc: anchor_embeddings[vc]
        for vc in PRIORITY_VC_CODES
        if vc in anchor_embeddings
    }
    
    print(f"✅ {len(filtered_embeddings)}개 Anchor 임베딩 준비 완료\n")
    
    # 유사도 매트릭스 계산
    similarity_matrix = {}
    
    for vc1 in PRIORITY_VC_CODES:
        if vc1 not in filtered_embeddings:
            continue
        
        similarity_matrix[vc1] = {}
        
        for vc2 in PRIORITY_VC_CODES:
            if vc2 not in filtered_embeddings:
                continue
            
            if vc1 == vc2:
                similarity_matrix[vc1][vc2] = 1.0
            else:
                similarity = compute_cosine_similarity(
                    filtered_embeddings[vc1],
                    filtered_embeddings[vc2]
                )
                similarity_matrix[vc1][vc2] = similarity
    
    return similarity_matrix


def print_similarity_matrix(matrix: Dict[str, Dict[str, float]]):
    """
    유사도 매트릭스를 표로 출력
    """
    print("=" * 80)
    print("Anchor 간 Cosine Similarity 매트릭스")
    print("=" * 80)
    print("\n기준: similarity < 0.5 → 구분 잘 됨 ✅, similarity > 0.7 → 사실상 동일 ⚠️\n")
    
    # 헤더
    header = f"{'':<15}"
    for vc in PRIORITY_VC_CODES:
        if vc in matrix:
            vc_short = vc.replace('_', '\n')  # 줄바꿈으로 표시
            header += f"{vc_short:<15}"
    print(header)
    print("-" * 90)
    
    # 각 행 출력
    for vc1 in PRIORITY_VC_CODES:
        if vc1 not in matrix:
            continue
        
        row = f"{vc1:<15}"
        for vc2 in PRIORITY_VC_CODES:
            if vc2 not in matrix:
                continue
            
            sim = matrix[vc1][vc2]
            
            # 상태 표시
            if vc1 == vc2:
                status = "  (self)"
            elif sim < 0.5:
                status = "  ✅"
            elif sim < 0.7:
                status = "  ⚠️"
            else:
                status = "  ❌"
            
            row += f"{sim:>6.3f}{status:<9}"
        
        print(row)
    
    print("-" * 90)


def analyze_similarity_issues(matrix: Dict[str, Dict[str, float]]):
    """
    유사도 매트릭스를 분석하여 문제점 도출
    """
    print("\n" + "=" * 80)
    print("📊 Anchor 간 유사도 분석")
    print("=" * 80)
    
    issues = []
    warnings = []
    
    for vc1 in PRIORITY_VC_CODES:
        if vc1 not in matrix:
            continue
        
        for vc2 in PRIORITY_VC_CODES:
            if vc2 not in matrix or vc1 >= vc2:  # 중복 방지
                continue
            
            sim = matrix[vc1][vc2]
            
            if sim >= 0.7:
                issues.append({
                    'vc1': vc1,
                    'vc2': vc2,
                    'similarity': sim,
                    'severity': 'HIGH',
                    'message': f"{vc1}와 {vc2}가 사실상 동일 (similarity={sim:.3f})"
                })
            elif sim >= 0.5:
                warnings.append({
                    'vc1': vc1,
                    'vc2': vc2,
                    'similarity': sim,
                    'severity': 'MEDIUM',
                    'message': f"{vc1}와 {vc2}가 유사함 (similarity={sim:.3f})"
                })
    
    # 심각한 문제 출력
    if issues:
        print("\n❌ 심각한 문제 (similarity ≥ 0.7):")
        for issue in issues:
            print(f"  - {issue['message']}")
    
    # 경고 출력
    if warnings:
        print("\n⚠️  경고 (0.5 ≤ similarity < 0.7):")
        for warning in warnings:
            print(f"  - {warning['message']}")
    
    # 정상 케이스
    if not issues and not warnings:
        print("\n✅ 모든 Anchor가 잘 구분됩니다 (similarity < 0.5)")
    else:
        print("\n💡 개선 제안:")
        print("  1. 유사도가 높은 Anchor 쌍의 텍스트를 더 차별화하세요")
        print("  2. 비즈니스 지표(KPI, 원가 구조)를 Anchor에 더 명확히 포함하세요")
        print("  3. 구조적 차이(물리적 생산 여부, 사용자 기반 등)를 강조하세요")
    
    return issues, warnings


def print_anchor_texts():
    """
    현재 Anchor 텍스트 출력 (참고용)
    """
    print("\n" + "=" * 80)
    print("📝 현재 Anchor 텍스트")
    print("=" * 80)
    
    for vc_code in PRIORITY_VC_CODES:
        if vc_code not in VALUE_CHAIN_ANCHORS:
            continue
        
        anchor = VALUE_CHAIN_ANCHORS[vc_code]
        name_ko = anchor.get('name_ko', '')
        description = anchor.get('description', '')
        
        print(f"\n{vc_code} ({name_ko}):")
        print(f"  {description[:200]}..." if len(description) > 200 else f"  {description}")


def main():
    """메인 함수"""
    print("=" * 80)
    print("Anchor 간 유사도 검증")
    print("=" * 80)
    
    try:
        # 1. 유사도 매트릭스 계산
        matrix = compute_anchor_similarity_matrix()
        
        # 2. 매트릭스 출력
        print_similarity_matrix(matrix)
        
        # 3. 문제 분석
        issues, warnings = analyze_similarity_issues(matrix)
        
        # 4. Anchor 텍스트 출력 (참고용)
        print_anchor_texts()
        
        # 5. 최종 요약
        print("\n" + "=" * 80)
        print("📋 최종 요약")
        print("=" * 80)
        print(f"✅ 검증 완료 Anchor 수: {len(matrix)}개")
        print(f"❌ 심각한 문제: {len(issues)}개")
        print(f"⚠️  경고: {len(warnings)}개")
        
        if issues:
            print("\n⚠️  Anchor 텍스트 개선이 필요합니다!")
            return 1
        elif warnings:
            print("\n💡 Anchor 텍스트 개선을 권장합니다.")
            return 0
        else:
            print("\n✅ Anchor가 잘 구분됩니다!")
            return 0
        
    except Exception as e:
        print(f"\n❌ [ERROR] 유사도 검증 중 오류: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
