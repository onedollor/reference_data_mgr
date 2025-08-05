import React, { useState } from 'react'
import { 
  Card, 
  Table, 
  Button, 
  Space, 
  Typography, 
  Tag, 
  Modal, 
  Form, 
  Input, 
  Select,
  message,
  Avatar 
} from 'antd'
import { PlusOutlined, EditOutlined, DeleteOutlined, UserOutlined } from '@ant-design/icons'
import { useTranslation } from 'react-i18next'

const { Title } = Typography
const { Option } = Select

interface User {
  id: string
  username: string
  email: string
  role: 'admin' | 'operator' | 'viewer'
  status: 'active' | 'inactive'
  lastLogin: string
  createdAt: string
}

const UserManagement: React.FC = () => {
  const { t } = useTranslation()
  const [users, setUsers] = useState<User[]>([
    {
      id: '1',
      username: 'admin',
      email: 'admin@company.com',
      role: 'admin',
      status: 'active',
      lastLogin: '2025-08-05 10:30:00',
      createdAt: '2025-01-01 00:00:00'
    },
    {
      id: '2',
      username: 'operator1',
      email: 'operator1@company.com',
      role: 'operator',
      status: 'active',
      lastLogin: '2025-08-05 09:15:00',
      createdAt: '2025-01-15 10:30:00'
    },
    {
      id: '3',
      username: 'viewer1',
      email: 'viewer1@company.com',
      role: 'viewer',
      status: 'inactive',
      lastLogin: '2025-08-03 16:45:00',
      createdAt: '2025-02-01 14:20:00'
    }
  ])

  const [modalVisible, setModalVisible] = useState(false)
  const [editingUser, setEditingUser] = useState<User | null>(null)
  const [form] = Form.useForm()

  const roleColors = {
    admin: 'red',
    operator: 'blue',
    viewer: 'green'
  }

  const roleLabels = {
    admin: t('userManagement.roles.admin'),
    operator: t('userManagement.roles.operator'),
    viewer: t('userManagement.roles.viewer')
  }

  const columns = [
    {
      title: t('userManagement.table.user'),
      dataIndex: 'username',
      key: 'username',
      render: (username: string, record: User) => (
        <Space>
          <Avatar icon={<UserOutlined />} />
          <div>
            <div>{username}</div>
            <div style={{ fontSize: '12px', color: '#999' }}>{record.email}</div>
          </div>
        </Space>
      )
    },
    {
      title: t('userManagement.table.role'),
      dataIndex: 'role',
      key: 'role',
      render: (role: keyof typeof roleColors) => (
        <Tag color={roleColors[role]}>
          {roleLabels[role]}
        </Tag>
      )
    },
    {
      title: t('userManagement.table.status'),
      dataIndex: 'status',
      key: 'status',
      render: (status: string) => (
        <Tag color={status === 'active' ? 'success' : 'default'}>
          {status === 'active' ? t('userManagement.status.active') : t('userManagement.status.inactive')}
        </Tag>
      )
    },
    {
      title: t('userManagement.table.lastLogin'),
      dataIndex: 'lastLogin',
      key: 'lastLogin',
    },
    {
      title: t('userManagement.table.createdAt'),
      dataIndex: 'createdAt',
      key: 'createdAt',
    },
    {
      title: t('common.actions'),
      key: 'action',
      render: (_: any, record: User) => (
        <Space size="middle">
          <Button 
            type="link" 
            icon={<EditOutlined />}
            onClick={() => handleEdit(record)}
          >
            {t('common.edit')}
          </Button>
          <Button 
            type="link" 
            danger 
            icon={<DeleteOutlined />}
            onClick={() => handleDelete(record.id)}
            disabled={record.username === 'admin'}
          >
            {t('common.delete')}
          </Button>
        </Space>
      ),
    },
  ]

  const handleAdd = () => {
    setEditingUser(null)
    form.resetFields()
    setModalVisible(true)
  }

  const handleEdit = (user: User) => {
    setEditingUser(user)
    form.setFieldsValue(user)
    setModalVisible(true)
  }

  const handleDelete = (id: string) => {
    Modal.confirm({
      title: t('userManagement.deleteConfirm.title'),
      content: t('userManagement.deleteConfirm.content'),
      onOk: () => {
        setUsers(users.filter(u => u.id !== id))
        message.success(t('common.deleteSuccess'))
      }
    })
  }

  const handleSave = async () => {
    try {
      const values = await form.validateFields()
      
      if (editingUser) {
        // 编辑
        setUsers(users.map(u => 
          u.id === editingUser.id ? { ...editingUser, ...values } : u
        ))
        message.success(t('common.updateSuccess'))
      } else {
        // 新增
        const newUser: User = {
          id: Date.now().toString(),
          ...values,
          lastLogin: '-',
          createdAt: new Date().toLocaleString('zh-CN')
        }
        setUsers([...users, newUser])
        message.success(t('common.addSuccess'))
      }
      
      setModalVisible(false)
    } catch (error) {
      console.error('Form validation failed:', error)
    }
  }

  return (
    <div style={{ padding: '24px' }}>
      <div style={{ marginBottom: '24px', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <div>
          <Title level={2}>{t('userManagement.title')}</Title>
          <p style={{ color: '#666', margin: 0 }}>
            {t('userManagement.description')}
          </p>
        </div>
        <Button 
          type="primary" 
          icon={<PlusOutlined />}
          onClick={handleAdd}
        >
          {t('userManagement.addUser')}
        </Button>
      </div>

      <Card>
        <Table 
          columns={columns} 
          dataSource={users}
          rowKey="id"
          pagination={{
            pageSize: 10,
            showSizeChanger: true,
            showQuickJumper: true,
            showTotal: (total, range) => t('common.pagination.total', { start: range[0], end: range[1], total })
          }}
        />
      </Card>

      <Modal
        title={editingUser ? t('userManagement.editUser') : t('userManagement.addUser')}
        open={modalVisible}
        onOk={handleSave}
        onCancel={() => setModalVisible(false)}
        width={500}
      >
        <Form
          form={form}
          layout="vertical"
          initialValues={{ status: 'active', role: 'viewer' }}
        >
          <Form.Item
            name="username"
            label={t('userManagement.form.username')}
            rules={[
              { required: true, message: t('userManagement.form.usernameRequired') },
              { pattern: /^[a-zA-Z0-9_]{3,20}$/, message: t('userManagement.form.usernamePattern') }
            ]}
          >
            <Input placeholder={t('userManagement.form.usernamePlaceholder')} />
          </Form.Item>

          <Form.Item
            name="email"
            label={t('userManagement.form.email')}
            rules={[
              { required: true, message: t('userManagement.form.emailRequired') },
              { type: 'email', message: t('userManagement.form.emailInvalid') }
            ]}
          >
            <Input placeholder={t('userManagement.form.emailPlaceholder')} />
          </Form.Item>

          {!editingUser && (
            <Form.Item
              name="password"
              label={t('userManagement.form.password')}
              rules={[
                { required: true, message: t('userManagement.form.passwordRequired') },
                { min: 6, message: t('userManagement.form.passwordMinLength') }
              ]}
            >
              <Input.Password placeholder={t('userManagement.form.passwordPlaceholder')} />
            </Form.Item>
          )}

          <Form.Item
            name="role"
            label={t('userManagement.form.role')}
            rules={[{ required: true, message: t('userManagement.form.roleRequired') }]}
          >
            <Select placeholder={t('userManagement.form.rolePlaceholder')}>
              <Option value="admin">{t('userManagement.roles.admin')}</Option>
              <Option value="operator">{t('userManagement.roles.operator')}</Option>
              <Option value="viewer">{t('userManagement.roles.viewer')}</Option>
            </Select>
          </Form.Item>

          <Form.Item
            name="status"
            label={t('userManagement.form.status')}
            rules={[{ required: true, message: t('userManagement.form.statusRequired') }]}
          >
            <Select placeholder={t('userManagement.form.statusPlaceholder')}>
              <Option value="active">{t('userManagement.status.active')}</Option>
              <Option value="inactive">{t('userManagement.status.inactive')}</Option>
            </Select>
          </Form.Item>
        </Form>
      </Modal>
    </div>
  )
}

export default UserManagement