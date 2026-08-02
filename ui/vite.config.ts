/// <reference types="vitest" />
import { defineConfig, loadEnv, UserConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "node:path";

// Trendx UI Vite config — aligne sur Dockerfile.ui VITE_API_BASE_URL
export default defineConfig(({ mode }): UserConfig => {
  const env = loadEnv(mode, process.cwd(), "");
  const API_BASE_URL = env.VITE_API_BASE_URL ?? "http://localhost:8000";

  return {
    plugins: [react()],
    base: "/",
    resolve: {
      alias: {
        "@": path.resolve(__dirname, "./src"),
      },
    },
    server: {
      port: 5173,
      host: "0.0.0.0",
      strictPort: true,
      proxy: {
        "/api": {
          target: API_BASE_URL,
          changeOrigin: true,
          timeout: 60_000,
          proxyTimeout: 300_000,
        },
        "/docs": API_BASE_URL,
        "/redoc": API_BASE_URL,
        "/openapi.json": API_BASE_URL,
        "/health": API_BASE_URL,
      },
    },
    build: {
      outDir: "dist",
      sourcemap: mode !== "production",
      target: "es2022",
      cssCodeSplit: true,
      reportCompressedSize: false,
      chunkSizeWarningLimit: 2000,
      rollupOptions: {
        output: {
          manualChunks: {
            react: ["react", "react-dom", "react-router-dom"],
            echarts: ["echarts", "echarts-for-react", "recharts"],
            state: ["zustand", "jotai", "@tanstack/react-query", "@tanstack/react-table"],
            utils: ["date-fns", "date-fns-tz", "axios", "yup", "formik", "apache-arrow"],
          },
        },
      },
    },
    test: {
      globals: true,
      environment: "jsdom",
      setupFiles: ["./src/test-setup.ts"],
    },
  };
});
