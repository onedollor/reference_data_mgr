import React from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ConfigProvider, App as AntApp } from 'antd';
import { useTranslation } from 'react-i18next';
import zhCN from 'antd/locale/zh_CN';
import enUS from 'antd/locale/en_US';
import frFR from 'antd/locale/fr_FR';
import dayjs from 'dayjs';
import 'dayjs/locale/zh-cn';
import 'dayjs/locale/en';
import 'dayjs/locale/fr';

import Layout from './components/Layout/Layout';
import Login from './pages/Login/Login';
import ConfigManagement from './pages/ConfigManagement/ConfigManagement';
import FileUpload from './pages/FileUpload/FileUpload';
import UserManagement from './pages/UserManagement/UserManagement';
import LogViewer from './pages/LogViewer/LogViewer';
import Dashboard from './pages/Dashboard/Dashboard';
import { useAuthStore } from './stores/authStore';

import './i18n';
import './App.css';

// 获取Ant Design locale
const getAntdLocale = (lang: string) => {
  switch (lang) {
    case 'zh':
      return zhCN;
    case 'fr':
      return frFR;
    default:
      return enUS;
  }
};

// 设置dayjs locale
const setDayjsLocale = (lang: string) => {
  switch (lang) {
    case 'zh':
      dayjs.locale('zh-cn');
      break;
    case 'fr':
      dayjs.locale('fr');
      break;
    default:
      dayjs.locale('en');
      break;
  }
};

// 创建QueryClient
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      refetchOnWindowFocus: false,
    },
  },
});

// Ant Design主题配置
const themeConfig = {
  token: {
    colorPrimary: '#2563eb',
    colorSuccess: '#059669',
    colorWarning: '#d97706',
    colorError: '#dc2626',
    borderRadius: 4,
    fontSize: 14,
  },
  components: {
    Layout: {
      siderBg: '#f8fafc',
      headerBg: '#ffffff',
    },
    Menu: {
      itemBg: 'transparent',
      itemSelectedBg: '#e0f2fe',
      itemSelectedColor: '#2563eb',
    },
  },
};

const ProtectedRoute: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const { isAuthenticated } = useAuthStore();
  
  if (!isAuthenticated) {
    return <Navigate to="/login" replace />;
  }
  
  return <>{children}</>;
};

const App: React.FC = () => {
  const { i18n } = useTranslation();
  
  // 根据当前语言设置dayjs locale
  React.useEffect(() => {
    setDayjsLocale(i18n.language);
  }, [i18n.language]);

  return (
    <QueryClientProvider client={queryClient}>
      <ConfigProvider 
        locale={getAntdLocale(i18n.language)}
        theme={themeConfig}
      >
        <AntApp>
          <Router>
            <div className="app">
              <Routes>
                <Route path="/login" element={<Login />} />
                <Route path="/" element={
                  <ProtectedRoute>
                    <Layout />
                  </ProtectedRoute>
                }>
                  <Route index element={<Navigate to="/dashboard" replace />} />
                  <Route path="dashboard" element={<Dashboard />} />
                  <Route path="configs" element={<ConfigManagement />} />
                  <Route path="upload" element={<FileUpload />} />
                  <Route path="users" element={<UserManagement />} />
                  <Route path="logs" element={<LogViewer />} />
                </Route>
                <Route path="*" element={<Navigate to="/dashboard" replace />} />
              </Routes>
            </div>
          </Router>
        </AntApp>
      </ConfigProvider>
    </QueryClientProvider>
  );
};

export default App;