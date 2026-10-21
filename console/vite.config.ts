import type { ProxyOptions } from "vite";
import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";

// `npm run dev` proxies the API to a coordinator on :8080. The coordinator rejects cross-origin API
// calls, so the proxy presents the coordinator's own origin rather than the dev server's.
const target = process.env.CONDUCTOR_URL ?? "http://127.0.0.1:8080";
const proxy: ProxyOptions = { target, changeOrigin: true, headers: { Origin: target } };

export default defineConfig({
  plugins: [react()],
  build: { outDir: "dist", emptyOutDir: true, sourcemap: false, target: "es2022" },
  server: { proxy: { "/api": proxy, "/healthz": proxy } },
  test: { environment: "jsdom", globals: true, setupFiles: ["src/test-setup.ts"] },
});
