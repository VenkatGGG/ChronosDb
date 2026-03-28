import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";
export default defineConfig(function (_a) {
    var _b;
    var mode = _a.mode;
    var env = loadEnv(mode, ".", "");
    var consoleTarget = (_b = env.CHRONOS_CONSOLE_API) !== null && _b !== void 0 ? _b : "http://127.0.0.1:8080";
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
