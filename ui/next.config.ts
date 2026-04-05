import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  basePath: "/seo",
  async rewrites() {
    return [
      {
        source: "/api/:path*",
        destination: (process.env.SEO_API_INTERNAL || "http://seo-engine-api:8900") + "/:path*",
      },
    ];
  },
};

export default nextConfig;
