/**
 * 기사 상세 페이지
 */

'use client';

import { use, useEffect } from 'react';
import { useArticle, useArticleInsight } from '@/lib/hooks';
import { Loader2 } from 'lucide-react';
import Link from 'next/link';
import { ReaderActions } from '@/components/reader/ReaderActions';

export default function ArticlePage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = use(params);
  const articleId = parseInt(id);

  const { data, isLoading, error } = useArticle(articleId);
  const { data: insightData } = useArticleInsight(articleId);

  // 디버깅: 데이터 확인
  useEffect(() => {
    if (data && process.env.NODE_ENV === 'development') {
      console.log('=== Article Data ===');
      console.log('AI Summary:', data.ai_summary);
      console.log('Keywords:', data.ai_summary?.keywords);
      console.log('Keywords type:', typeof data.ai_summary?.keywords);
      console.log('Keywords is array:', Array.isArray(data.ai_summary?.keywords));
      console.log('Keywords length:', data.ai_summary?.keywords?.length);
    }
    if (insightData && process.env.NODE_ENV === 'development') {
      console.log('=== Insight Data ===');
      console.log('Keywords:', insightData.keywords);
      console.log('Keywords type:', typeof insightData.keywords);
      console.log('Keywords is array:', Array.isArray(insightData.keywords));
      console.log('Keywords length:', insightData.keywords?.length);
    }
  }, [data, insightData]);

  if (isLoading) {
    return (
      <div className="min-h-screen bg-white flex items-center justify-center">
        <Loader2 className="w-8 h-8 animate-spin text-gray-400" />
      </div>
    );
  }

  if (error || !data) {
    return (
      <div className="min-h-screen bg-white flex items-center justify-center">
        <div className="text-center">
          <p className="text-red-500 mb-4">기사를 불러올 수 없습니다.</p>
          <Link href="/feed" className="text-blue-500 hover:underline">
            피드로 돌아가기
          </Link>
        </div>
      </div>
    );
  }

  const article = data.article;

  return (
    <main className="min-h-screen bg-white">
      <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* 뒤로가기 */}
        <Link
          href="/feed"
          className="inline-flex items-center text-sm text-gray-500 hover:text-gray-700 mb-6"
        >
          ← 피드로
        </Link>

        {/* 기사 제목 */}
        <h1 className="text-2xl sm:text-3xl font-bold text-gray-900 mb-4">
          {article.title}
        </h1>

        {/* 메타데이터 */}
        <div className="flex items-center gap-4 mb-6 text-sm text-gray-500">
          <span>{article.source}</span>
          <span>•</span>
          <span>{article.published_at ? new Date(article.published_at).toLocaleString('ko-KR') : '날짜 없음'}</span>
        </div>

        {/* AI 분석 요청 버튼 */}
        <div className="mb-6">
          <ReaderActions articleId={articleId} url={article.link} />
        </div>

        {/* AI 요약 - AI 분석 완료 후에만 표시 */}
        {(data.ai_summary || insightData) && (
          <div className="border-t border-gray-200 pt-6 mb-6">
            <h2 className="text-xl font-bold text-gray-900 mb-4">AI 요약</h2>
            <div className="space-y-4">
              <div>
                <h3 className="font-semibold text-gray-800 mb-2">요약</h3>
                <p className="text-gray-700 leading-relaxed">
                  {data.ai_summary?.summary || insightData?.summary}
                </p>
              </div>

              {/* 키워드 섹션 - 키워드가 있으면 표시 */}
              {(() => {
                const keywords = data.ai_summary?.keywords || insightData?.keywords || [];
                const hasKeywords = Array.isArray(keywords) && keywords.length > 0;
                
                if (hasKeywords) {
                  return (
                    <div>
                      <h3 className="font-semibold text-gray-800 mb-2">키워드</h3>
                      <div className="flex flex-wrap gap-2">
                        {keywords.map((keyword: string, idx: number) => (
                          <span
                            key={idx}
                            className="px-3 py-1 bg-blue-100 text-blue-800 rounded-full text-sm"
                          >
                            {keyword}
                          </span>
                        ))}
                      </div>
                    </div>
                  );
                }
                return null;
              })()}

              {((data.ai_summary?.bullet_points &&
                data.ai_summary.bullet_points.length > 0) ||
                (insightData?.bullet_points &&
                  insightData.bullet_points.length > 0)) && (
                <div>
                  <h3 className="font-semibold text-gray-800 mb-2">
                    핵심 포인트
                  </h3>
                  <ul className="list-disc list-inside space-y-1 text-gray-700">
                    {(data.ai_summary?.bullet_points ||
                      insightData?.bullet_points ||
                      []).map((point: string, idx: number) => (
                      <li key={idx}>{point}</li>
                    ))}
                  </ul>
                </div>
              )}
            </div>
          </div>
        )}

        {/* 원문 WebView (iframe) */}
        <div className="mt-8 pt-6 border-t border-gray-200">
          <div className="mb-4">
            <a
              href={article.link}
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center text-blue-600 hover:text-blue-700 font-medium"
            >
              원문 보기 (새 창) →
            </a>
          </div>
          <iframe
            src={article.link}
            className="w-full h-[70vh] border rounded-lg"
            title={article.title}
            sandbox="allow-same-origin allow-scripts allow-popups allow-forms"
          />
        </div>
      </div>
    </main>
  );
}

