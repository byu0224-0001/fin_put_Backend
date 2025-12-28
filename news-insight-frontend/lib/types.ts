/**
 * API 응답 타입 정의
 */

export interface RelatedArticle {
  title: string;
  source: string;
  link: string;
  url: string;
  published_at: string | null;
  image_url: string | null;
  cluster_id: string | null;
  representative: boolean;
}

export interface Article {
  id: number;
  title: string;
  summary: string;
  source: string;
  url: string;
  link: string;
  published_at: string;
  image_url: string | null;
  cluster_id: string | null;
  representative: boolean;
  related_articles: RelatedArticle[];
}

export interface FeedResponse {
  articles: Article[];
  total: number;
  original_count: number;
  deduplicated_count: number;
  limit: number;
  offset: number;
  deduplication_enabled: boolean;
  cached: boolean;
}

export interface Insight {
  article_id: number;
  title: string | null;
  summary: string;
  keywords: string[];
  entities: Record<string, any>;
  bullet_points: string[];
  sentiment: string;
  created_at: string;
}

export interface ParseRequest {
  article_id?: number;
  url?: string;
  text?: string;
}

export interface ParseStatus {
  status: 'pending' | 'processing' | 'done' | 'failed';
  message?: string;
  task_id?: string;
  article_id?: number;
  progress?: number;
  result?: any;
}

export interface InsightsResponse {
  insights: Insight[];
  total: number;
  limit: number;
  offset: number;
}

export interface ArticleResponse {
  article: {
    id: number;
    title: string;
    source: string;
    link: string;
    summary: string | null;
    published_at: string | null;
  };
  ai_summary: {
    summary: string | null;
    keywords: string[];
    entities: Record<string, any>;
    bullet_points: string[];
    sentiment: string | null;
    created_at: string | null;
  } | null;
}

export interface ArticleInsightResponse {
  article_id: number;
  title: string | null;
  link: string | null;
  summary: string;
  keywords: string[];
  entities: Record<string, any>;
  bullet_points: string[];
  sentiment: string;
  created_at: string;
}

