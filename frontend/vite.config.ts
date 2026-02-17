import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    port: 3000,

    // ✅ ОВА ГО ДОДАВАШ
    allowedHosts: true,

    proxy: {
      '/api': {
        target: 'http://127.0.0.1:5000',
        changeOrigin: true,
        secure: false
      }
    }
  },
  define: {
    // Allow environment variable for backend URL
    'import.meta.env.VITE_BACKEND_URL': JSON.stringify(
      process.env.VITE_BACKEND_URL || 'http://127.0.0.1:5000'
    )
  }
})
