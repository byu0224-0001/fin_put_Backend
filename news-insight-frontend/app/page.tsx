/**
 * 홈페이지 - 피드로 리다이렉트
 */

import { redirect } from 'next/navigation';

export default function HomePage() {
  redirect('/feed');
}
