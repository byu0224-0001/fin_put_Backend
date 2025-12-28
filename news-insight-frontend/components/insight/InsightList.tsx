/**
 * 인사이트 리스트 컴포넌트
 */

'use client';

import { InsightCard } from './InsightCard';
import { Insight } from '@/lib/types';
import { Separator } from '@/components/ui/separator';

interface InsightListProps {
  insights: Insight[];
}

export function InsightList({ insights }: InsightListProps) {
  if (insights.length === 0) {
    return (
      <div className="text-center py-12">
        <p className="text-muted-foreground">인사이트가 없습니다.</p>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {insights.map((insight, index) => (
        <div key={insight.article_id}>
          <InsightCard insight={insight} />
          {index < insights.length - 1 && <Separator className="my-4" />}
        </div>
      ))}
    </div>
  );
}

