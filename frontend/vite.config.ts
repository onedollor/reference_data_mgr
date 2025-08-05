import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    host: '0.0.0.0',
    port: 5173,
    open: false
  },
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  css: {
    preprocessorOptions: {
      less: {
        javascriptEnabled: true,
        modifyVars: {
          '@primary-color': '#2563eb',
          '@success-color': '#059669',
          '@warning-color': '#d97706',
          '@error-color': '#dc2626',
          '@border-radius-base': '4px',
          '@font-size-base': '14px',
        },
      },
    },
  },
  optimizeDeps: {
    include: [
      'react',
      'react-dom',
      'react-router-dom',
      'antd',
      '@ant-design/icons',
      'axios',
      'zustand',
      '@tanstack/react-query',
      'dayjs',
      'recharts',
      'react-dropzone',
      'classnames'
    ]
  }
})