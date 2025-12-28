/**
 * 내 인사이트 페이지
 */

'use client';

import { useInsights } from '@/lib/hooks';
import { InsightList } from '@/components/insight/InsightList';
import { Loader2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { useState } from 'react';

export default function InsightsPage() {
  const [offset, setOffset] = useState(0);
  const limit = 20;

  const { data, isLoading, error } = useInsights({ limit, offset });

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-12">
        <Loader2 className="w-8 h-8 animate-spin text-gray-400" />
        <span className="ml-3 text-gray-500">인사이트를 불러오는 중...</span>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex flex-col items-center justify-center py-12 text-center">
        <p className="text-red-500 mb-2">인사이트를 불러오는 중 오류가 발생했습니다.</p>
        <p className="text-sm text-gray-500">{error.message}</p>
      </div>
    );
  }

  const insights = data?.insights || [];

  return (
    <main className="min-h-screen bg-white">
      <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* 페이지 헤더 */}
        <div className="mb-8">
          <h1 className="text-2xl font-bold text-gray-900 mb-2">내 인사이트</h1>
          <p className="text-sm text-gray-500">
            AI가 분석한 뉴스 인사이트를 확인하세요
          </p>
        </div>

        {/* 인사이트 리스트 */}
        <InsightList insights={insights} />

        {/* 더 보기 버튼 */}
        {data && data.total > offset + limit && (
          <div className="flex justify-center mt-8">
            <Button
              variant="secondary"
              onClick={() => setOffset(offset + limit)}
            >
              더 보기
            </Button>
          </div>
        )}
      </div>
    </main>
  );
}

