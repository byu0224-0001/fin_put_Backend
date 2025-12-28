/**
 * 기사 읽기 페이지에서 AI 분석 요청 액션 컴포넌트
 */

'use client';

import { Button } from '@/components/ui/button';
import { useRequestParse, useParseStatus } from '@/lib/hooks';
import { useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';
import { useEffect, useState } from 'react';
import { Loader2 } from 'lucide-react';

interface ReaderActionsProps {
  articleId: number;
  url: string;
}

export function ReaderActions({ articleId, url }: ReaderActionsProps) {
  const queryClient = useQueryClient();
  const { mutateAsync, isPending } = useRequestParse();
  const [taskId, setTaskId] = useState<string | undefined>(undefined);

  const { data: status } = useParseStatus(taskId, !!taskId);

  const handleAnalyze = async () => {
    try {
      const res = await mutateAsync({ url });
      setTaskId(res.task_id);
      toast.success('AI 분석 요청됨', {
        description: '백그라운드에서 처리합니다.',
      });
    } catch (error: any) {
      toast.error('요청 실패', {
        description: error.message || '다시 시도해 주세요.',
      });
    }
  };

  useEffect(() => {
    if (!status) return;

    if (status.status === 'done') {
      queryClient.invalidateQueries({ queryKey: ['articleInsight', articleId] });
      queryClient.invalidateQueries({ queryKey: ['article', articleId] });
      toast.success('분석 완료', {
        description: '요약과 키워드가 업데이트되었습니다.',
      });
    }

    if (status.status === 'failed') {
      toast.error('분석 실패', {
        description: status.message || '다시 시도해 주세요.',
      });
    }
  }, [status, articleId, queryClient]);

  const isAnalyzing = Boolean(isPending || (taskId && status?.status !== 'done' && status?.status !== 'failed'));

  return (
    <div className="flex gap-2">
      <Button onClick={handleAnalyze} disabled={isAnalyzing}>
        {isAnalyzing ? (
          <>
            <Loader2 className="w-4 h-4 mr-2 animate-spin" />
            {status?.status === 'processing' ? '분석 중...' : '요청 중...'}
          </>
        ) : (
          'AI 분석 요청'
        )}
      </Button>
    </div>
  );
}

