/**
 * React Query 커스텀 훅
 */

import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getFeed, requestParse, getParseStatus, getInsights, getArticleInsight, getArticle } from './api';
import { FeedResponse, ParseRequest, ParseStatus, InsightsResponse } from './types';

/**
 * 피드 조회 훅
 */
export function useFeed(params: {
  limit?: number;
  offset?: number;
  source?: string;
  deduplicate?: boolean;
  similarity_threshold?: number;
  enable_bert?: boolean;
} = {}) {
  return useQuery<FeedResponse>({
    queryKey: ['feed', params],
    queryFn: () => getFeed(params),
    staleTime: 60_000, // 1분
    refetchOnWindowFocus: false,
  });
}

/**
 * 기사 상세 조회 훅
 */
export function useArticle(articleId: number) {
  return useQuery({
    queryKey: ['article', articleId],
    queryFn: () => getArticle(articleId),
    enabled: !!articleId,
  });
}

/**
 * 기사 파싱 요청 훅
 */
export function useRequestParse() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (payload: ParseRequest) => requestParse(payload),
    onSuccess: () => {
      // 인사이트 목록 무효화
      queryClient.invalidateQueries({ queryKey: ['insights'] });
    },
  });
}

/**
 * 파싱 상태 확인 훅 (폴링)
 */
export function useParseStatus(taskId?: string, enabled = !!taskId) {
  return useQuery<ParseStatus>({
    queryKey: ['parseStatus', taskId],
    queryFn: () => getParseStatus(taskId!),
    enabled,
    refetchInterval: (query) => {
      const data = query.state.data;
      if (!data) return 3000; // 3초마다 폴링
      // 완료되면 폴링 중단
      return data.status === 'done' || data.status === 'failed' ? false : 3000;
    },
  });
}

/**
 * 인사이트 목록 조회 훅
 */
export function useInsights(params: { limit?: number; offset?: number } = {}) {
  return useQuery<InsightsResponse>({
    queryKey: ['insights', params],
    queryFn: () => getInsights(params),
  });
}

/**
 * 기사 인사이트 조회 훅
 */
export function useArticleInsight(articleId: number, enabled = true) {
  return useQuery({
    queryKey: ['articleInsight', articleId],
    queryFn: () => getArticleInsight(articleId),
    enabled: enabled && !!articleId,
    retry: false,
    retryOnMount: false,
    refetchOnWindowFocus: false,
  });
}

