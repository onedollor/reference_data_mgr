import React from 'react'
import { Card, Row, Col, Statistic, Typography, Divider } from 'antd'
import { 
  FileTextOutlined, 
  CheckCircleOutlined, 
  ExclamationCircleOutlined,
  UserOutlined 
} from '@ant-design/icons'
import { useTranslation } from 'react-i18next'

const { Title, Paragraph } = Typography

const Dashboard: React.FC = () => {
  const { t } = useTranslation()
  
  return (
    <div style={{ padding: '24px' }}>
      <Title level={2}>{t('dashboard.title')}</Title>
      <Paragraph type="secondary">
        {t('dashboard.description')}
      </Paragraph>
      
      <Row gutter={[16, 16]} style={{ marginBottom: '24px' }}>
        <Col xs={24} sm={12} md={6}>
          <Card>
            <Statistic
              title={t('dashboard.totalFiles')}
              value={1128}
              prefix={<FileTextOutlined />}
              valueStyle={{ color: '#3f8600' }}
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} md={6}>
          <Card>
            <Statistic
              title={t('dashboard.validationRate')}
              value={98.5}
              precision={1}
              suffix="%"
              prefix={<CheckCircleOutlined />}
              valueStyle={{ color: '#1890ff' }}
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} md={6}>
          <Card>
            <Statistic
              title={t('dashboard.pendingFiles')}
              value={5}
              prefix={<ExclamationCircleOutlined />}
              valueStyle={{ color: '#faad14' }}
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} md={6}>
          <Card>
            <Statistic
              title={t('dashboard.activeUsers')}
              value={23}
              prefix={<UserOutlined />}
              valueStyle={{ color: '#722ed1' }}
            />
          </Card>
        </Col>
      </Row>

      <Row gutter={[16, 16]}>
        <Col xs={24} lg={12}>
          <Card title={t('dashboard.recentActivity')} extra={<a href="#">{t('dashboard.viewAll')}</a>}>
            <div style={{ minHeight: '200px', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
              <div style={{ textAlign: 'center', color: '#999' }}>
                <FileTextOutlined style={{ fontSize: '48px', marginBottom: '16px' }} />
                <p>{t('dashboard.chartPlaceholder')}</p>
                <p style={{ fontSize: '12px' }}>{t('dashboard.apiConnectionNeeded')}</p>
              </div>
            </div>
          </Card>
        </Col>
        <Col xs={24} lg={12}>
          <Card title={t('dashboard.validationResults')} extra={<a href="#">{t('dashboard.detailedReport')}</a>}>
            <div style={{ minHeight: '200px', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
              <div style={{ textAlign: 'center', color: '#999' }}>
                <CheckCircleOutlined style={{ fontSize: '48px', marginBottom: '16px' }} />
                <p>{t('dashboard.validationChartPlaceholder')}</p>
                <p style={{ fontSize: '12px' }}>{t('dashboard.apiConnectionNeeded')}</p>
              </div>
            </div>
          </Card>
        </Col>
      </Row>

      <Divider />

      <Card title={t('dashboard.systemInfo')}>
        <Row gutter={[16, 16]}>
          <Col span={8}>
            <Statistic title={t('dashboard.systemVersion')} value="1.0.0" />
          </Col>
          <Col span={8}>
            <Statistic title={t('dashboard.databaseStatus')} value={t('dashboard.normal')} valueStyle={{ color: '#3f8600' }} />
          </Col>
          <Col span={8}>
            <Statistic title={t('dashboard.lastBackup')} value={t('dashboard.hoursAgo', { hours: 2 })} />
          </Col>
        </Row>
      </Card>
    </div>
  )
}

export default Dashboard