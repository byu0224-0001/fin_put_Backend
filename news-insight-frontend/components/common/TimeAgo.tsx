/**
 * 상대 시간 표시 컴포넌트
 */

import { formatRelativeTime } from '@/lib/utils';

interface TimeAgoProps {
  iso: string;
  className?: string;
}

export function TimeAgo({ iso, className }: TimeAgoProps) {
  return <span className={className}>{formatRelativeTime(iso)}</span>;
}

