import React, { useState } from 'react';
import { Outlet, useNavigate, useLocation } from 'react-router-dom';
import {
  Layout as AntLayout,
  Menu,
  Button,
  Dropdown,
  Avatar,
  Space,
  Typography,
  theme,
} from 'antd';
import {
  DashboardOutlined,
  SettingOutlined,
  CloudUploadOutlined,
  TeamOutlined,
  FileTextOutlined,
  MenuFoldOutlined,
  MenuUnfoldOutlined,
  UserOutlined,
  LogoutOutlined,
} from '@ant-design/icons';
import { useTranslation } from 'react-i18next';
import type { MenuProps } from 'antd';

import { useAuthStore } from '../../stores/authStore';
import LanguageSelector from '../LanguageSelector';
import './Layout.less';

const { Header, Sider, Content } = AntLayout;
const { Text } = Typography;

// 菜单项配置
const getMenuItems = (t: any) => [
  {
    key: '/dashboard',
    icon: <DashboardOutlined />,
    label: t('navigation.home'),
  },
  {
    key: '/configs',
    icon: <SettingOutlined />,
    label: t('navigation.settings'),
  },
  {
    key: '/upload',
    icon: <CloudUploadOutlined />,
    label: t('navigation.fileUpload'),
  },
  {
    key: '/users',
    icon: <TeamOutlined />,
    label: t('navigation.dataManagement'),
  },
  {
    key: '/logs',
    icon: <FileTextOutlined />,
    label: 'Logs',
  },
];

const Layout: React.FC = () => {
  const { t } = useTranslation();
  const [collapsed, setCollapsed] = useState(false);
  const navigate = useNavigate();
  const location = useLocation();
  const { user, logout } = useAuthStore();
  const {
    token: { colorBgContainer },
  } = theme.useToken();

  // 处理菜单点击
  const handleMenuClick = (key: string) => {
    navigate(key);
  };

  // 用户下拉菜单
  const userMenuItems: MenuProps['items'] = [
    {
      key: 'profile',
      icon: <UserOutlined />,
      label: 'Profile',
    },
    {
      type: 'divider',
    },
    {
      key: 'logout',
      icon: <LogoutOutlined />,
      label: t('auth.logout'),
      onClick: () => {
        logout();
        navigate('/login');
      },
    },
  ];

  return (
    <AntLayout className="layout-container">
      {/* 侧边栏 */}
      <Sider
        trigger={null}
        collapsible
        collapsed={collapsed}
        className="layout-sider"
        theme="light"
        width={240}
        collapsedWidth={64}
      >
        <div className="layout-logo">
          <div className="logo-icon">
            <DashboardOutlined />
          </div>
          {!collapsed && (
            <div className="logo-text">
              <Text strong>CSV Manager</Text>
            </div>
          )}
        </div>
        
        <Menu
          theme="light"
          mode="inline"
          selectedKeys={[location.pathname]}
          items={getMenuItems(t)}
          onClick={({ key }) => handleMenuClick(key)}
          className="layout-menu"
        />
      </Sider>

      {/* 主内容区 */}
      <AntLayout className="layout-main">
        {/* 顶部导航 */}
        <Header className="layout-header" style={{ background: colorBgContainer }}>
          <div className="header-left">
            <Button
              type="text"
              icon={collapsed ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />}
              onClick={() => setCollapsed(!collapsed)}
              className="collapse-button"
            />
          </div>
          
          <div className="header-right">
            <Space size="middle">
              <LanguageSelector />
              <Dropdown
                menu={{ items: userMenuItems }}
                placement="bottomRight"
                arrow
              >
                <Space className="user-info">
                  <Avatar size="small" icon={<UserOutlined />} />
                  <Text>{user?.full_name || user?.username}</Text>
                </Space>
              </Dropdown>
            </Space>
          </div>
        </Header>

        {/* 内容区域 */}
        <Content className="layout-content">
          <div className="content-wrapper">
            <Outlet />
          </div>
        </Content>
      </AntLayout>
    </AntLayout>
  );
};

export default Layout;