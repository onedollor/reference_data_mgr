import React, { useState } from 'react'
import { 
  Card, 
  Table, 
  Button, 
  Space, 
  Typography, 
  Tag, 
  Input,
  Select,
  DatePicker,
  Row,
  Col 
} from 'antd'
import { SearchOutlined, ReloadOutlined, DownloadOutlined } from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import dayjs from 'dayjs'

const { Title } = Typography
const { RangePicker } = DatePicker
const { Option } = Select

interface LogEntry {
  id: string
  timestamp: string
  level: 'INFO' | 'WARN' | 'ERROR' | 'DEBUG'
  category: 'UPLOAD' | 'VALIDATION' | 'SYSTEM' | 'USER'
  message: string
  details?: string
  user: string
  filename?: string
}

const LogViewer: React.FC = () => {
  const { t } = useTranslation()
  const [logs, setLogs] = useState<LogEntry[]>([
    {
      id: '1',
      timestamp: '2025-08-05 10:30:15',
      level: 'INFO',
      category: 'UPLOAD',
      message: 'CSV文件上传成功',
      details: '文件: customer_data.20250805103015.csv, 大小: 2.5MB, 记录数: 1250',
      user: 'admin',
      filename: 'customer_data.20250805103015.csv'
    },
    {
      id: '2',
      timestamp: '2025-08-05 10:30:18',
      level: 'INFO',
      category: 'VALIDATION',
      message: '数据验证完成',
      details: '验证规则: sp_validate_customer_data, 成功: 1248, 失败: 2',
      user: 'admin',
      filename: 'customer_data.20250805103015.csv'
    },
    {
      id: '3',
      timestamp: '2025-08-05 10:29:45',
      level: 'ERROR',
      category: 'UPLOAD',
      message: '文件格式验证失败',
      details: '错误: 文件名格式不符合要求, 期望: *.yyyyMMddHHmmss.csv',
      user: 'operator1',
      filename: 'invalid_file.csv'
    },
    {
      id: '4',
      timestamp: '2025-08-05 10:25:30',
      level: 'WARN',
      category: 'SYSTEM',
      message: '磁盘空间不足警告',
      details: '可用空间: 1.2GB, 建议清理临时文件',
      user: 'system'
    },
    {
      id: '5',
      timestamp: '2025-08-05 10:20:12',
      level: 'INFO',
      category: 'USER',
      message: '用户登录',
      details: 'IP: 192.168.1.100, 浏览器: Chrome/122.0.0.0',
      user: 'operator1'
    }
  ])

  const [loading, setLoading] = useState(false)
  const [filters, setFilters] = useState({
    level: '',
    category: '',
    user: '',
    dateRange: null as any
  })

  const levelColors = {
    INFO: 'blue',
    WARN: 'orange',
    ERROR: 'red',
    DEBUG: 'gray'
  }

  const categoryColors = {
    UPLOAD: 'green',
    VALIDATION: 'purple',
    SYSTEM: 'cyan',
    USER: 'gold'
  }

  const columns = [
    {
      title: t('logs.table.timestamp'),
      dataIndex: 'timestamp',
      key: 'timestamp',
      width: 160,
      sorter: (a: LogEntry, b: LogEntry) => 
        dayjs(a.timestamp).unix() - dayjs(b.timestamp).unix(),
      defaultSortOrder: 'descend' as const
    },
    {
      title: t('logs.table.level'),
      dataIndex: 'level',
      key: 'level',
      width: 80,
      render: (level: keyof typeof levelColors) => (
        <Tag color={levelColors[level]}>{level}</Tag>
      ),
      filters: [
        { text: 'INFO', value: 'INFO' },
        { text: 'WARN', value: 'WARN' },
        { text: 'ERROR', value: 'ERROR' },
        { text: 'DEBUG', value: 'DEBUG' }
      ],
      onFilter: (value: any, record: LogEntry) => record.level === value
    },
    {
      title: t('logs.table.category'),
      dataIndex: 'category',
      key: 'category',
      width: 100,
      render: (category: keyof typeof categoryColors) => (
        <Tag color={categoryColors[category]}>{category}</Tag>
      ),
      filters: [
        { text: 'UPLOAD', value: 'UPLOAD' },
        { text: 'VALIDATION', value: 'VALIDATION' },
        { text: 'SYSTEM', value: 'SYSTEM' },
        { text: 'USER', value: 'USER' }
      ],
      onFilter: (value: any, record: LogEntry) => record.category === value
    },
    {
      title: t('logs.table.user'),
      dataIndex: 'user',
      key: 'user',
      width: 100
    },
    {
      title: t('logs.table.message'),
      dataIndex: 'message',
      key: 'message',
      ellipsis: true
    },
    {
      title: t('logs.table.filename'),
      dataIndex: 'filename',
      key: 'filename',
      width: 200,
      render: (filename: string) => filename ? <code>{filename}</code> : '-'
    }
  ]

  const expandedRowRender = (record: LogEntry) => (
    <div style={{ margin: 0 }}>
      <p><strong>{t('logs.details')}:</strong></p>
      <pre style={{ 
        background: '#f5f5f5', 
        padding: '12px', 
        borderRadius: '4px',
        fontSize: '12px',
        margin: 0,
        whiteSpace: 'pre-wrap'
      }}>
        {record.details || t('logs.noDetails')}
      </pre>
    </div>
  )

  const handleRefresh = () => {
    setLoading(true)
    // 模拟刷新数据
    setTimeout(() => {
      setLoading(false)
    }, 1000)
  }

  const handleExport = () => {
    // 模拟导出功能
    console.log('导出日志...')
  }

  return (
    <div style={{ padding: '24px' }}>
      <div style={{ marginBottom: '24px', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <div>
          <Title level={2}>{t('logs.title')}</Title>
          <p style={{ color: '#666', margin: 0 }}>
            {t('logs.description')}
          </p>
        </div>
        <Space>
          <Button 
            icon={<ReloadOutlined />}
            onClick={handleRefresh}
            loading={loading}
          >
            {t('common.refresh')}
          </Button>
          <Button 
            icon={<DownloadOutlined />}
            onClick={handleExport}
          >
            {t('common.export')}
          </Button>
        </Space>
      </div>

      <Card style={{ marginBottom: '16px' }}>
        <Row gutter={[16, 16]}>
          <Col xs={24} sm={12} md={6}>
            <Input
              placeholder={t('logs.searchPlaceholder')}
              prefix={<SearchOutlined />}
              allowClear
            />
          </Col>
          <Col xs={24} sm={12} md={4}>
            <Select
              placeholder={t('logs.levelPlaceholder')}
              allowClear
              style={{ width: '100%' }}
              value={filters.level}
              onChange={(value) => setFilters({ ...filters, level: value })}
            >
              <Option value="INFO">INFO</Option>
              <Option value="WARN">WARN</Option>
              <Option value="ERROR">ERROR</Option>
              <Option value="DEBUG">DEBUG</Option>
            </Select>
          </Col>
          <Col xs={24} sm={12} md={4}>
            <Select
              placeholder={t('logs.categoryPlaceholder')}
              allowClear
              style={{ width: '100%' }}
              value={filters.category}
              onChange={(value) => setFilters({ ...filters, category: value })}
            >
              <Option value="UPLOAD">UPLOAD</Option>
              <Option value="VALIDATION">VALIDATION</Option>
              <Option value="SYSTEM">SYSTEM</Option>
              <Option value="USER">USER</Option>
            </Select>
          </Col>
          <Col xs={24} sm={12} md={4}>
            <Input
              placeholder={t('logs.userPlaceholder')}
              allowClear
              value={filters.user}
              onChange={(e) => setFilters({ ...filters, user: e.target.value })}
            />
          </Col>
          <Col xs={24} sm={12} md={6}>
            <RangePicker
              style={{ width: '100%' }}
              showTime
              format="YYYY-MM-DD HH:mm:ss"
              value={filters.dateRange}
              onChange={(dates) => setFilters({ ...filters, dateRange: dates })}
            />
          </Col>
        </Row>
      </Card>

      <Card>
        <Table 
          columns={columns} 
          dataSource={logs}
          rowKey="id"
          loading={loading}
          expandable={{ expandedRowRender }}
          scroll={{ x: 1000 }}
          pagination={{
            pageSize: 20,
            showSizeChanger: true,
            showQuickJumper: true,
            showTotal: (total, range) => t('common.pagination.total', { start: range[0], end: range[1], total })
          }}
        />
      </Card>
    </div>
  )
}

export default LogViewer