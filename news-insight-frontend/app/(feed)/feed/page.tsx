/**
 * 홈 피드 페이지
 */

'use client';

import { FeedList } from '@/components/feed/FeedList';
import { useState } from 'react';
import { Button } from '@/components/ui/button';
import { useFeed } from '@/lib/hooks';

export default function FeedPage() {
  const [offset, setOffset] = useState(0);
  const limit = 20;

  const { data, isLoading } = useFeed({ limit, offset, deduplicate: true });
  const hasMore = data && data.total > offset + limit;

  return (
    <main className="min-h-screen bg-white">
      <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* 페이지 헤더 */}
        <div className="mb-8">
          <h1 className="text-2xl font-bold text-gray-900 mb-2">Today</h1>
          <p className="text-sm text-gray-500">
            최신 뉴스를 중복 없이, 다양한 관점으로 제공합니다
          </p>
        </div>

        {/* 피드 리스트 */}
        <FeedList limit={limit} offset={offset} deduplicate={true} />

        {/* 더 보기 버튼 */}
        {hasMore && (
          <div className="flex justify-center mt-8">
            <Button
              variant="secondary"
              onClick={() => setOffset(offset + limit)}
              disabled={isLoading}
            >
              {isLoading ? '로딩 중...' : '더 보기'}
            </Button>
          </div>
        )}
      </div>
    </main>
  );
}

