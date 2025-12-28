/**
 * 뉴스 기사 카드 컴포넌트
 * 이미지 + 헤드라인 + 메타데이터 + 요약 형식
 */

import Link from 'next/link';
import { Article } from '@/lib/types';
import { formatRelativeTime, truncateText, stripHtml } from '@/lib/utils';
import { Badge } from '@/components/ui/badge';

interface ArticleCardProps {
  article: Article;
}

export function ArticleCard({ article }: ArticleCardProps) {
  // 요약 텍스트 정리 (HTML 제거)
  const cleanSummary = stripHtml(article.summary || '요약 생성 중입니다...');
  const displaySummary = truncateText(cleanSummary, 120);

  const relatedCount = article.related_articles?.length ?? 0;
  const relatedSourceCount =
    relatedCount > 0
      ? new Set(article.related_articles.map((item) => item.source)).size
      : 0;

  // 이미지 URL 처리
  const imageUrl = article.image_url || '/placeholder-news.jpg';

  return (
    <Link 
      href={`/article/${article.id}`}
      className="flex gap-4 p-4 hover:bg-gray-50 transition-colors border-b border-gray-100"
    >
      {/* 이미지 영역 (왼쪽) */}
      <div className="flex-shrink-0 w-24 h-24 sm:w-28 sm:h-28 rounded-lg overflow-hidden bg-gray-200">
        {article.image_url ? (
          <img
            src={article.image_url}
            alt={article.title}
            className="w-full h-full object-cover"
            onError={(e) => {
              e.currentTarget.src = '/placeholder-news.jpg';
            }}
          />
        ) : (
          <div className="w-full h-full flex items-center justify-center bg-gray-100">
            <div className="text-gray-400 text-xs text-center px-2">
              이미지 없음
            </div>
          </div>
        )}
      </div>

      {/* 텍스트 영역 (오른쪽) */}
      <div className="flex-1 min-w-0">
        {/* 헤드라인 */}
        <h2 className="text-base sm:text-lg font-bold text-gray-900 mb-1.5 line-clamp-2 leading-snug">
          {article.title}
        </h2>

        {/* 메타데이터 (언론사 / 시간) */}
        <div className="flex items-center gap-2 mb-2 flex-wrap">
          <Badge variant="secondary" className="text-xs">
            {article.source}
          </Badge>
          {article.representative && (
            <Badge variant="outline" className="text-xs">
              대표
            </Badge>
          )}
          <span className="text-xs sm:text-sm text-gray-500">
            {formatRelativeTime(article.published_at)}
          </span>
        </div>

        {/* 요약 */}
        <p className="text-sm text-gray-700 line-clamp-2 leading-relaxed">
          {displaySummary}
        </p>

        {relatedCount > 0 && (
          <p className="text-xs text-gray-500 mt-2">
            외 {relatedCount}개 매체 보도
            {relatedSourceCount > 0 && ` · ${relatedSourceCount}개 출처`}
          </p>
        )}
      </div>
    </Link>
  );
}

