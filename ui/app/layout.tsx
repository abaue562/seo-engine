import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "SEO Engine — Command Center",
  description: "Autonomous SEO operating system",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
