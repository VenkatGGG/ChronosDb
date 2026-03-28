import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, ".", "");
  const consoleTarget = env.CHRONOS_CONSOLE_API ?? "http://127.0.0.1:8080";

  return {
    plugins: [react()],
    server: {
      port: 5173,
      proxy: {
        "/api": {
          target: consoleTarget,
          changeOrigin: true,
        },
      },
    },
  };
});
