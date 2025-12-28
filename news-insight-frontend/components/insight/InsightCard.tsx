/**
 * 인사이트 카드 컴포넌트
 */

'use client';

import { Card, CardHeader, CardContent } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Insight } from '@/lib/types';
import { TimeAgo } from '@/components/common/TimeAgo';
import Link from 'next/link';

interface InsightCardProps {
  insight: Insight;
}

export function InsightCard({ insight }: InsightCardProps) {
  return (
    <Card className="hover:shadow-md transition">
      <CardHeader className="space-y-2">
        <div className="flex items-center gap-2 flex-wrap">
          {insight.title && (
            <h3 className="text-lg font-semibold flex-1">{insight.title}</h3>
          )}
          <Badge variant="secondary">
            <TimeAgo iso={insight.created_at} />
          </Badge>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* 요약 */}
        {insight.summary && (
          <p className="text-sm text-muted-foreground leading-relaxed">
            {insight.summary}
          </p>
        )}

        {/* 핵심 포인트 */}
        {insight.bullet_points && insight.bullet_points.length > 0 && (
          <ul className="list-disc pl-5 space-y-1">
            {insight.bullet_points.slice(0, 3).map((point, idx) => (
              <li key={idx} className="text-sm text-foreground">
                {point}
              </li>
            ))}
          </ul>
        )}

        {/* 키워드 */}
        {insight.keywords && insight.keywords.length > 0 && (
          <div className="flex flex-wrap gap-2">
            {insight.keywords.slice(0, 8).map((keyword, idx) => (
              <Badge key={idx} variant="outline" className="text-xs">
                {keyword}
              </Badge>
            ))}
          </div>
        )}

        {/* 감정 */}
        {insight.sentiment && (
          <div>
            <Badge
              variant={insight.sentiment === 'positive' ? 'default' : 'secondary'}
            >
              {insight.sentiment === 'positive'
                ? '긍정'
                : insight.sentiment === 'negative'
                ? '부정'
                : '중립'}
            </Badge>
          </div>
        )}

        {/* 상세 보기 링크 */}
        <Link
          href={`/article/${insight.article_id}`}
          className="text-sm text-primary hover:underline inline-block"
        >
          기사 보기 →
        </Link>
      </CardContent>
    </Card>
  );
}

