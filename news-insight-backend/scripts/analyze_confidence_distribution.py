"""
route_confidence 분포 분석 스크립트

테스트 후 confidence 분포를 분석하여 임계값 캘리브레이션
"""
import sys
import os
from pathlib import Path
from dotenv import load_dotenv
from collections import defaultdict
import json
from datetime import datetime

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

if sys.platform == 'win32':
    import codecs
    if sys.stdout.encoding != 'utf-8':
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

load_dotenv(project_root / '.env')

from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# DB 연결
user = quote_plus(os.getenv('POSTGRES_USER', 'postgres'))
pwd = quote_plus(os.getenv('POSTGRES_PASSWORD', 'postgres'))
host = os.getenv('POSTGRES_HOST', 'localhost')
port = os.getenv('POSTGRES_PORT', '5432')
db_name = os.getenv('POSTGRES_DB', 'newsdb')
url = f'postgresql://{user}:{pwd}@{host}:{port}/{db_name}'

engine = create_engine(url)

def analyze_confidence_distribution():
    """route_confidence 분포 분석"""
    logger.info("=" * 80)
    logger.info("route_confidence 분포 분석")
    logger.info("=" * 80)
    
    # KIRS 리포트의 route_confidence 분포 조회
    query = text("""
        SELECT 
            report_id,
            key_points->>'route_confidence' as route_confidence,
            key_points->>'route_evidence' as route_evidence,
            processing_status,
            key_points->>'hold_reason' as hold_reason
        FROM broker_reports
        WHERE source = '한국IR협의회'
            AND key_points->>'route_confidence' IS NOT NULL
        ORDER BY created_at DESC
        LIMIT 100;
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query)
        rows = result.fetchall()
    
    if not rows:
        logger.warning("route_confidence 데이터가 없습니다. KIRS 리포트를 먼저 처리하세요.")
        return
    
    # 분포 계산
    distribution = defaultdict(int)
    bucket_samples = defaultdict(list)
    hold_by_bucket = defaultdict(int)
    pass_by_bucket = defaultdict(int)
    
    for row in rows:
        try:
            confidence = float(row[1]) if row[1] else 0.0
        except (ValueError, TypeError):
            continue
        
        # 버킷 분류
        if confidence < 0.5:
            bucket = "0.0-0.5"
        elif confidence < 0.6:
            bucket = "0.5-0.6"
        elif confidence < 0.7:
            bucket = "0.6-0.7"
        elif confidence < 0.8:
            bucket = "0.7-0.8"
        else:
            bucket = "0.8-1.0"
        
        distribution[bucket] += 1
        
        # 샘플 수집 (각 버킷당 최대 5개)
        if len(bucket_samples[bucket]) < 5:
            bucket_samples[bucket].append({
                "report_id": row[0],
                "confidence": confidence,
                "status": row[3],
                "hold_reason": row[4]
            })
        
        # HOLD/PASS 분류
        if row[3] == "HOLD":
            hold_by_bucket[bucket] += 1
        elif row[3] == "ENRICHED":
            pass_by_bucket[bucket] += 1
    
    # 리포트 출력
    logger.info(f"\n총 {len(rows)}개 리포트 분석")
    logger.info("\n" + "=" * 80)
    logger.info("Confidence 분포")
    logger.info("=" * 80)
    
    total = len(rows)
    for bucket in ["0.0-0.5", "0.5-0.6", "0.6-0.7", "0.7-0.8", "0.8-1.0"]:
        count = distribution[bucket]
        hold_count = hold_by_bucket[bucket]
        pass_count = pass_by_bucket[bucket]
        percentage = (count / total * 100) if total > 0 else 0
        
        logger.info(f"\n{bucket}: {count}개 ({percentage:.1f}%)")
        logger.info(f"  - HOLD: {hold_count}개")
        logger.info(f"  - PASS: {pass_count}개")
        
        if bucket_samples[bucket]:
            logger.info(f"  - 샘플:")
            for sample in bucket_samples[bucket]:
                logger.info(f"    * {sample['report_id']}: confidence={sample['confidence']:.2f}, status={sample['status']}")
    
    # 추천 임계값 계산
    logger.info("\n" + "=" * 80)
    logger.info("임계값 추천")
    logger.info("=" * 80)
    
    # 0.7 이상에서 PASS 비율이 80% 이상이면 0.7을 추천
    pass_ratio_07 = (pass_by_bucket["0.7-0.8"] + pass_by_bucket["0.8-1.0"]) / max(distribution["0.7-0.8"] + distribution["0.8-1.0"], 1) * 100
    if pass_ratio_07 >= 80:
        logger.info(f"✅ 0.7 이상 PASS 비율: {pass_ratio_07:.1f}% → CONFIDENCE_PASS_THRESHOLD = 0.7 추천")
    else:
        logger.info(f"⚠️ 0.7 이상 PASS 비율: {pass_ratio_07:.1f}% → 더 높은 임계값 고려 필요")
    
    # 0.6 미만에서 HOLD 비율이 90% 이상이면 0.6을 추천
    hold_ratio_06 = (hold_by_bucket["0.0-0.5"] + hold_by_bucket["0.5-0.6"]) / max(distribution["0.0-0.5"] + distribution["0.5-0.6"], 1) * 100
    if hold_ratio_06 >= 90:
        logger.info(f"✅ 0.6 미만 HOLD 비율: {hold_ratio_06:.1f}% → CONFIDENCE_THRESHOLD = 0.6 유지 추천")
    else:
        logger.info(f"⚠️ 0.6 미만 HOLD 비율: {hold_ratio_06:.1f}% → 더 낮은 임계값 고려 필요")
    
    # 결과 저장
    report = {
        "timestamp": datetime.now().isoformat(),
        "total_reports": total,
        "distribution": dict(distribution),
        "hold_by_bucket": dict(hold_by_bucket),
        "pass_by_bucket": dict(pass_by_bucket),
        "bucket_samples": {k: v[:5] for k, v in bucket_samples.items()},
        "recommendations": {
            "confidence_threshold": 0.6 if hold_ratio_06 >= 90 else None,
            "confidence_pass_threshold": 0.7 if pass_ratio_07 >= 80 else None
        }
    }
    
    output_file = project_root / "reports" / f"confidence_distribution_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    logger.info(f"\n분포 분석 리포트 저장: {output_file}")
    
    return report

def main():
    """메인 실행 함수"""
    try:
        analyze_confidence_distribution()
    except Exception as e:
        logger.error(f"분포 분석 중 오류: {e}", exc_info=True)

if __name__ == "__main__":
    main()

