import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import "./globals.css";
import { QueryProvider } from "@/components/QueryProvider";
import Link from "next/link";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "News Insight - 뉴스 인사이트 플랫폼",
  description: "AI 기반 뉴스 분석 및 인사이트 플랫폼",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="ko">
      <body
        className={`${geistSans.variable} ${geistMono.variable} antialiased`}
      >
        <QueryProvider>
          <header className="border-b sticky top-0 bg-white z-10">
            <nav className="max-w-5xl mx-auto px-4 sm:px-6 lg:px-8 py-4 flex gap-6">
              <Link href="/feed" className="font-semibold hover:text-gray-600">
                피드
              </Link>
              <Link href="/insights" className="font-semibold hover:text-gray-600">
                내 인사이트
              </Link>
            </nav>
          </header>
          <main>{children}</main>
        </QueryProvider>
      </body>
    </html>
  );
}
