import React, { useState } from 'react'
import { Form, Input, Button, Card, Typography, message } from 'antd'
import { UserOutlined, LockOutlined } from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import { useAuthStore } from '../../stores/authStore'
import { useNavigate } from 'react-router-dom'

const { Title } = Typography

const Login: React.FC = () => {
  const { t } = useTranslation()
  const [loading, setLoading] = useState(false)
  const { login } = useAuthStore()
  const navigate = useNavigate()

  const onFinish = async (values: { username: string; password: string }) => {
    setLoading(true)
    try {
      const success = await login(values.username, values.password)
      if (success) {
        message.success(t('auth.loginSuccess'))
        navigate('/dashboard')
      } else {
        message.error(t('auth.invalidCredentials'))
      }
    } catch (error) {
      message.error(t('auth.loginError'))
    } finally {
      setLoading(false)
    }
  }

  return (
    <div style={{ 
      height: '100vh', 
      display: 'flex', 
      alignItems: 'center', 
      justifyContent: 'center',
      background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)'
    }}>
      <Card style={{ width: 400, boxShadow: '0 4px 12px rgba(0,0,0,0.15)' }}>
        <div style={{ textAlign: 'center', marginBottom: 24 }}>
          <Title level={2} style={{ color: '#1890ff', marginBottom: 8 }}>
            {t('auth.systemTitle')}
          </Title>
          <p style={{ color: '#666', margin: 0 }}>{t('auth.systemDescription')}</p>
        </div>
        
        <Form
          name="login"
          onFinish={onFinish}
          autoComplete="off"
          size="large"
        >
          <Form.Item
            name="username"
            rules={[{ required: true, message: t('auth.usernameRequired') }]}
          >
            <Input 
              prefix={<UserOutlined />} 
              placeholder={t('auth.username')} 
              defaultValue="admin"
            />
          </Form.Item>

          <Form.Item
            name="password"
            rules={[{ required: true, message: t('auth.passwordRequired') }]}
          >
            <Input.Password 
              prefix={<LockOutlined />} 
              placeholder={t('auth.password')}
              defaultValue="admin123"
            />
          </Form.Item>

          <Form.Item>
            <Button 
              type="primary" 
              htmlType="submit" 
              loading={loading}
              block
            >
              {t('auth.login')}
            </Button>
          </Form.Item>
        </Form>
        
        <div style={{ textAlign: 'center', color: '#999', fontSize: '12px' }}>
          <p>{t('auth.defaultAccount')}</p>
        </div>
      </Card>
    </div>
  )
}

export default Login