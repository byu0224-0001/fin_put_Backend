/**
 * 백엔드 API 클라이언트 (axios 기반)
 */

import axios from 'axios';
import { FeedResponse, InsightsResponse, ParseRequest, ParseStatus, ArticleResponse, ArticleInsightResponse } from './types';

const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL || process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

export const api = axios.create({
  baseURL: API_BASE_URL,
  timeout: 90000, // BERT 처리 시간 고려하여 90초로 증가 (기존 15초)
  headers: {
    'Content-Type': 'application/json',
  },
});

// 에러 인터셉터 추가
api.interceptors.response.use(
  (response) => response,
  (error) => {
    // 네트워크 오류 처리
    if (error.code === 'ECONNREFUSED' || error.code === 'ERR_NETWORK') {
      console.error('백엔드 서버에 연결할 수 없습니다:', API_BASE_URL);
      throw new Error(`백엔드 서버에 연결할 수 없습니다. 서버가 실행 중인지 확인해주세요. (${API_BASE_URL})`);
    }
    // 타임아웃 오류 처리
    if (error.code === 'ECONNABORTED') {
      console.error('요청이 타임아웃되었습니다.');
      throw new Error('요청 시간이 초과되었습니다. 잠시 후 다시 시도해주세요.');
    }
    // 기타 HTTP 오류
    if (error.response) {
      console.error('API 오류:', error.response.status, error.response.data);
      throw new Error(error.response.data?.detail || `서버 오류가 발생했습니다. (${error.response.status})`);
    }
    // 기타 오류
    console.error('알 수 없는 오류:', error.message);
    throw error;
  }
);

/**
 * 피드 가져오기
 */
export async function getFeed(params: {
  limit?: number;
  offset?: number;
  source?: string;
  deduplicate?: boolean;
  similarity_threshold?: number;
  enable_bert?: boolean;
} = {}): Promise<FeedResponse> {
  const res = await api.get('/api/feed', { params });
  return res.data;
}

/**
 * 기사 상세 정보 가져오기
 */
export async function getArticle(articleId: number): Promise<ArticleResponse> {
  const res = await api.get(`/api/article/${articleId}`);
  return res.data;
}

/**
 * 기사 파싱 요청 (WebView에서 호출)
 */
export async function requestParse(body: ParseRequest): Promise<{
  task_id: string;
  article_id?: number;
  status: string;
}> {
  const res = await api.post('/api/article/parse', body);
  return res.data;
}

/**
 * 작업 상태 확인 (article_id 기반으로 task_id 조회 필요 시)
 */
export async function getParseStatus(taskId: string): Promise<ParseStatus> {
  const res = await api.get(`/api/article/status/${taskId}`);
  return res.data;
}

/**
 * 기사별 파싱 상태 확인 (article_id로 직접 조회)
 */
export async function getArticleParseStatus(articleId: number) {
  // 백엔드에 해당 엔드포인트가 없으면, 인사이트가 있는지 확인하는 방식으로 대체
  const res = await api.get(`/api/article/${articleId}/insight`);
  return res.data;
}

/**
 * 인사이트 목록 가져오기
 */
export async function getInsights(params: {
  limit?: number;
  offset?: number;
} = {}): Promise<InsightsResponse> {
  const res = await api.get('/api/insight', { params });
  return res.data;
}

/**
 * 특정 기사 인사이트 가져오기
 */
export async function getArticleInsight(articleId: number): Promise<ArticleInsightResponse | null> {
  try {
    const res = await api.get(`/api/article/${articleId}/insight`);
    return res.data;
  } catch (error: any) {
    if (error.response?.status === 404) {
      return null;
    }
    throw error;
  }
}
