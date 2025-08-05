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
  Switch,
  message 
} from 'antd'
import { PlusOutlined, EditOutlined, DeleteOutlined, SettingOutlined } from '@ant-design/icons'
import { useTranslation } from 'react-i18next'

const { Title } = Typography
const { Option } = Select

interface ConfigItem {
  id: string
  name: string
  pattern: string
  tableName: string
  validationType: 'stored_procedure' | 'python_class'
  validator: string
  enabled: boolean
  description: string
}

const ConfigManagement: React.FC = () => {
  const { t } = useTranslation()
  const [configs, setConfigs] = useState<ConfigItem[]>([
    {
      id: '1',
      name: 'Customer Data Import',
      pattern: 'customer_*.yyyyMMddHHmmss.csv',
      tableName: 'customers',
      validationType: 'stored_procedure',
      validator: 'sp_validate_customer_data',
      enabled: true,
      description: 'Customer basic information data import configuration'
    },
    {
      id: '2',
      name: 'Order Data Import',
      pattern: 'order_*.yyyyMMddHHmmss.csv',
      tableName: 'orders',
      validationType: 'python_class',
      validator: 'OrderDataValidator',
      enabled: true,
      description: 'Order transaction data import configuration'
    }
  ])

  const [modalVisible, setModalVisible] = useState(false)
  const [editingConfig, setEditingConfig] = useState<ConfigItem | null>(null)
  const [form] = Form.useForm()

  const columns = [
    {
      title: t('configManagement.configName'),
      dataIndex: 'name',
      key: 'name',
    },
    {
      title: t('configManagement.filePattern'),
      dataIndex: 'pattern',
      key: 'pattern',
      render: (pattern: string) => <code>{pattern}</code>
    },
    {
      title: 'Table Name',
      dataIndex: 'tableName',
      key: 'tableName',
      render: (tableName: string) => <code>{tableName}</code>
    },
    {
      title: 'Validation Type',
      dataIndex: 'validationType',
      key: 'validationType',
      render: (type: string) => (
        <Tag color={type === 'stored_procedure' ? 'blue' : 'green'}>
          {type === 'stored_procedure' ? 'Stored Procedure' : 'Python Class'}
        </Tag>
      )
    },
    {
      title: 'Validator',
      dataIndex: 'validator',
      key: 'validator',
      render: (validator: string) => <code>{validator}</code>
    },
    {
      title: t('common.status'),
      dataIndex: 'enabled',
      key: 'enabled',
      render: (enabled: boolean) => (
        <Tag color={enabled ? 'success' : 'default'}>
          {enabled ? t('userManagement.active') : t('userManagement.inactive')}
        </Tag>
      )
    },
    {
      title: t('configManagement.actions'),
      key: 'action',
      render: (_: any, record: ConfigItem) => (
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
          >
            {t('common.delete')}
          </Button>
        </Space>
      ),
    },
  ]

  const handleAdd = () => {
    setEditingConfig(null)
    form.resetFields()
    setModalVisible(true)
  }

  const handleEdit = (config: ConfigItem) => {
    setEditingConfig(config)
    form.setFieldsValue(config)
    setModalVisible(true)
  }

  const handleDelete = (id: string) => {
    Modal.confirm({
      title: t('fileUpload.deleteConfirmTitle'),
      content: 'Are you sure you want to delete this configuration? This action cannot be undone.',
      onOk: () => {
        setConfigs(configs.filter(c => c.id !== id))
        message.success(t('fileUpload.deleteSuccess'))
      }
    })
  }

  const handleSave = async () => {
    try {
      const values = await form.validateFields()
      
      if (editingConfig) {
        // 编辑
        setConfigs(configs.map(c => 
          c.id === editingConfig.id ? { ...editingConfig, ...values } : c
        ))
        message.success('Updated successfully')
      } else {
        // 新增
        const newConfig: ConfigItem = {
          id: Date.now().toString(),
          ...values
        }
        setConfigs([...configs, newConfig])
        message.success('Added successfully')
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
          <Title level={2}>{t('configManagement.title')}</Title>
          <p style={{ color: '#666', margin: 0 }}>
            {t('configManagement.description')}
          </p>
        </div>
        <Button 
          type="primary" 
          icon={<PlusOutlined />}
          onClick={handleAdd}
        >
          {t('configManagement.addConfig')}
        </Button>
      </div>

      <Card>
        <Table 
          columns={columns} 
          dataSource={configs}
          rowKey="id"
          pagination={{
            pageSize: 10,
            showSizeChanger: true,
            showQuickJumper: true,
            showTotal: (total, range) => `${range[0]}-${range[1]} of ${total} items`
          }}
        />
      </Card>

      <Modal
        title={editingConfig ? t('configManagement.editConfig') : t('configManagement.addConfig')}
        open={modalVisible}
        onOk={handleSave}
        onCancel={() => setModalVisible(false)}
        width={600}
      >
        <Form
          form={form}
          layout="vertical"
          initialValues={{ enabled: true }}
        >
          <Form.Item
            name="name"
            label={t('configManagement.configName')}
            rules={[{ required: true, message: 'Please enter configuration name' }]}
          >
            <Input placeholder="e.g.: Customer Data Import" />
          </Form.Item>

          <Form.Item
            name="pattern"
            label={t('configManagement.filePattern')}
            rules={[{ required: true, message: 'Please enter file pattern' }]}
          >
            <Input placeholder="e.g.: customer_*.yyyyMMddHHmmss.csv" />
          </Form.Item>

          <Form.Item
            name="tableName"
            label="Target Table Name"
            rules={[{ required: true, message: 'Please enter target table name' }]}
          >
            <Input placeholder="e.g.: customers" />
          </Form.Item>

          <Form.Item
            name="validationType"
            label="Validation Type"
            rules={[{ required: true, message: 'Please select validation type' }]}
          >
            <Select placeholder="Select validation type">
              <Option value="stored_procedure">Stored Procedure</Option>
              <Option value="python_class">Python Class</Option>
            </Select>
          </Form.Item>

          <Form.Item
            name="validator"
            label="Validator"
            rules={[{ required: true, message: 'Please enter validator name' }]}
          >
            <Input placeholder="e.g.: sp_validate_customer_data or CustomerDataValidator" />
          </Form.Item>

          <Form.Item
            name="description"
            label={t('configManagement.description')}
          >
            <Input.TextArea rows={3} placeholder="Configuration description and usage" />
          </Form.Item>

          <Form.Item
            name="enabled"
            label="Enable Status"
            valuePropName="checked"
          >
            <Switch checkedChildren={t('userManagement.active')} unCheckedChildren={t('userManagement.inactive')} />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  )
}

export default ConfigManagement