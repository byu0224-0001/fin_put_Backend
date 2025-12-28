/**
 * 뉴스 피드 리스트 컴포넌트
 */

'use client';

import { useFeed } from '@/lib/hooks';
import { ArticleCard } from './ArticleCard';
import { Loader2 } from 'lucide-react';

interface FeedListProps {
  limit?: number;
  offset?: number;
  deduplicate?: boolean;
}

export function FeedList({ limit = 20, offset = 0, deduplicate = true }: FeedListProps) {
  const { data, isLoading, error } = useFeed({ limit, offset, deduplicate });

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-12">
        <Loader2 className="w-8 h-8 animate-spin text-gray-400" />
        <span className="ml-3 text-gray-500">기사를 불러오는 중...</span>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex flex-col items-center justify-center py-12 text-center">
        <p className="text-red-500 mb-2">기사를 불러오는 중 오류가 발생했습니다.</p>
        <p className="text-sm text-gray-500">{error.message}</p>
      </div>
    );
  }

  if (!data || data.articles.length === 0) {
    return (
      <div className="flex items-center justify-center py-12">
        <p className="text-gray-500">표시할 기사가 없습니다.</p>
      </div>
    );
  }

  return (
    <div className="w-full">
      {/* 통계 정보 (선택적) */}
      {data.original_count > 0 && (
        <div className="px-4 py-2 bg-gray-50 border-b border-gray-200 text-xs text-gray-600">
          {data.deduplication_enabled && (
            <span>
              {data.original_count}개 기사 중 {data.deduplicated_count}개 표시
              {data.cached && ' (캐시됨)'}
            </span>
          )}
        </div>
      )}

      {/* 기사 리스트 */}
      <div className="divide-y divide-gray-100">
        {data.articles.map((article) => (
          <ArticleCard key={article.id} article={article} />
        ))}
      </div>
    </div>
  );
}

