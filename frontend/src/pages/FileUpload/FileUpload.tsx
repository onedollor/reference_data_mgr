import React, { useState } from 'react';
import {
  Card,
  Upload,
  Button,
  Table,
  Space,
  Progress,
  Alert,
  Typography,
  Divider,
  Tag,
  Modal,
  Select,
  Form,
  message,
} from 'antd';
import {
  CloudUploadOutlined,
  FileOutlined,
  DeleteOutlined,
  PlayCircleOutlined,
  CheckCircleOutlined,
  ExclamationCircleOutlined,
} from '@ant-design/icons';
import { useDropzone } from 'react-dropzone';
import { useTranslation } from 'react-i18next';
import type { UploadFile, TableColumnsType } from 'antd';

import { useConfigStore } from '../../stores/configStore';
import { useFileStore } from '../../stores/fileStore';
import './FileUpload.less';

const { Title, Text } = Typography;
const { Option } = Select;

// 文件状态类型
type FileStatus = 'uploading' | 'uploaded' | 'processing' | 'completed' | 'failed';

// 文件信息接口
interface FileInfo {
  id: string;
  name: string;
  size: number;
  status: FileStatus;
  progress: number;
  errorMessage?: string;
  configId?: string;
  uploadTime: string;
}

const FileUpload: React.FC = () => {
  const { t } = useTranslation();
  const [files, setFiles] = useState<FileInfo[]>([]);
  const [selectedFiles, setSelectedFiles] = useState<string[]>([]);
  const [importModalVisible, setImportModalVisible] = useState(false);
  const [form] = Form.useForm();
  
  const { configs, fetchConfigs } = useConfigStore();
  const { uploadFile, importFiles } = useFileStore();

  // 初始化配置列表
  React.useEffect(() => {
    fetchConfigs();
  }, [fetchConfigs]);

  // 文件拖拽处理
  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    accept: {
      'text/csv': ['.csv'],
    },
    maxSize: 100 * 1024 * 1024, // 100MB
    onDrop: handleFileDrop,
  });

  // 处理文件拖拽
  function handleFileDrop(acceptedFiles: File[]) {
    const newFiles: FileInfo[] = acceptedFiles.map(file => ({
      id: `${Date.now()}-${Math.random()}`,
      name: file.name,
      size: file.size,
      status: 'uploaded' as FileStatus,
      progress: 100,
      uploadTime: new Date().toLocaleString(),
    }));
    
    setFiles(prev => [...prev, ...newFiles]);
    message.success(t('fileUpload.uploadSuccess', { count: acceptedFiles.length }));
  }

  // 验证文件名格式
  const validateFileName = (filename: string): boolean => {
    const pattern = /^.+\.\d{14}\.csv$/;
    return pattern.test(filename);
  };

  // 删除文件
  const handleDeleteFile = (fileId: string) => {
    setFiles(prev => prev.filter(f => f.id !== fileId));
    setSelectedFiles(prev => prev.filter(id => id !== fileId));
  };

  // 批量删除文件
  const handleBatchDelete = () => {
    if (selectedFiles.length === 0) {
      message.warning(t('fileUpload.selectFilesToDelete'));
      return;
    }

    Modal.confirm({
      title: t('fileUpload.deleteConfirmTitle'),
      content: t('fileUpload.deleteConfirmContent', { count: selectedFiles.length }),
      onOk: () => {
        setFiles(prev => prev.filter(f => !selectedFiles.includes(f.id)));
        setSelectedFiles([]);
        message.success(t('fileUpload.deleteSuccess'));
      },
    });
  };

  // 开始导入
  const handleStartImport = () => {
    if (selectedFiles.length === 0) {
      message.warning(t('fileUpload.selectFilesToImport'));
      return;
    }

    // 验证文件名格式
    const selectedFileInfos = files.filter(f => selectedFiles.includes(f.id));
    const invalidFiles = selectedFileInfos.filter(f => !validateFileName(f.name));
    
    if (invalidFiles.length > 0) {
      message.error(t('fileUpload.invalidFileNames', { files: invalidFiles.map(f => f.name).join(', ') }));
      return;
    }

    setImportModalVisible(true);
  };

  // 执行导入
  const handleExecuteImport = async (values: any) => {
    try {
      const { configId } = values;
      
      // 更新文件状态为处理中
      setFiles(prev => prev.map(f => 
        selectedFiles.includes(f.id) 
          ? { ...f, status: 'processing' as FileStatus, configId, progress: 0 }
          : f
      ));

      // 模拟导入过程
      for (const fileId of selectedFiles) {
        // 这里应该调用实际的API
        await simulateImport(fileId);
      }

      setImportModalVisible(false);
      setSelectedFiles([]);
      message.success(t('fileUpload.importTaskStarted'));
      
    } catch (error) {
      message.error(t('fileUpload.importFailed'));
    }
  };

  // 模拟导入过程
  const simulateImport = async (fileId: string) => {
    return new Promise<void>((resolve) => {
      let progress = 0;
      const timer = setInterval(() => {
        progress += Math.random() * 20;
        if (progress >= 100) {
          progress = 100;
          clearInterval(timer);
          
          // 随机设置成功或失败
          const isSuccess = Math.random() > 0.2;
          setFiles(prev => prev.map(f => 
            f.id === fileId 
              ? { 
                  ...f, 
                  status: isSuccess ? 'completed' : 'failed',
                  progress: 100,
                  errorMessage: isSuccess ? undefined : t('fileUpload.dataValidationFailed')
                }
              : f
          ));
          resolve();
        } else {
          setFiles(prev => prev.map(f => 
            f.id === fileId ? { ...f, progress } : f
          ));
        }
      }, 500);
    });
  };

  // 状态渲染
  const renderStatus = (status: FileStatus, progress: number, errorMessage?: string) => {
    switch (status) {
      case 'uploaded':
        return <Tag color="blue">{t('fileUpload.uploaded')}</Tag>;
      case 'processing':
        return (
          <Space>
            <Progress 
              type="circle" 
              size={20} 
              percent={Math.round(progress)} 
              showInfo={false}
            />
            <Text>{t('fileUpload.processing')}</Text>
          </Space>
        );
      case 'completed':
        return <Tag color="success" icon={<CheckCircleOutlined />}>{t('fileUpload.completed')}</Tag>;
      case 'failed':
        return (
          <Tag 
            color="error" 
            icon={<ExclamationCircleOutlined />}
            title={errorMessage}
          >
            {t('fileUpload.failed')}
          </Tag>
        );
      default:
        return <Tag>{t('fileUpload.unknown')}</Tag>;
    }
  };

  // 表格列配置
  const columns: TableColumnsType<FileInfo> = [
    {
      title: t('fileUpload.fileName'),
      dataIndex: 'name',
      key: 'name',
      render: (name: string) => (
        <Space>
          <FileOutlined />
          <Text>{name}</Text>
          {!validateFileName(name) && (
            <Tag color="warning" size="small">{t('fileUpload.formatError')}</Tag>
          )}
        </Space>
      ),
    },
    {
      title: t('fileUpload.fileSize'),
      dataIndex: 'size',
      key: 'size',
      width: 120,
      render: (size: number) => {
        const sizeInMB = (size / 1024 / 1024).toFixed(2);
        return `${sizeInMB} MB`;
      },
    },
    {
      title: t('fileUpload.uploadTime'),
      dataIndex: 'uploadTime',
      key: 'uploadTime',
      width: 180,
    },
    {
      title: t('fileUpload.status'),
      dataIndex: 'status',
      key: 'status',
      width: 120,
      render: (status: FileStatus, record: FileInfo) => 
        renderStatus(status, record.progress, record.errorMessage),
    },
    {
      title: t('fileUpload.actions'),
      key: 'action',
      width: 100,
      render: (_, record: FileInfo) => (
        <Button
          type="text"
          danger
          icon={<DeleteOutlined />}
          onClick={() => handleDeleteFile(record.id)}
          disabled={record.status === 'processing'}
        />
      ),
    },
  ];

  // 行选择配置
  const rowSelection = {
    selectedRowKeys: selectedFiles,
    onChange: (keys: React.Key[]) => {
      setSelectedFiles(keys as string[]);
    },
    getCheckboxProps: (record: FileInfo) => ({
      disabled: record.status === 'processing',
    }),
  };

  return (
    <div className="file-upload-page">
      <div className="page-header">
        <Title level={2}>{t('fileUpload.title')}</Title>
        <Text type="secondary">
          {t('fileUpload.description')}
        </Text>
      </div>

      {/* 文件上传区域 */}
      <Card className="upload-card">
        <div {...getRootProps()} className={`upload-area ${isDragActive ? 'drag-active' : ''}`}>
          <input {...getInputProps()} />
          <div className="upload-content">
            <CloudUploadOutlined className="upload-icon" />
            <Title level={4}>
              {isDragActive ? t('fileUpload.dragActiveText') : t('fileUpload.dragText')}
            </Title>
            <Text type="secondary">
              {t('fileUpload.supportText')}
            </Text>
          </div>
        </div>
      </Card>

      {/* 文件列表 */}
      <Card 
        title={t('fileUpload.fileList')}
        extra={
          <Space>
            <Button 
              danger 
              onClick={handleBatchDelete}
              disabled={selectedFiles.length === 0}
            >
              {t('fileUpload.batchDelete')}
            </Button>
            <Button 
              type="primary"
              icon={<PlayCircleOutlined />}
              onClick={handleStartImport}
              disabled={selectedFiles.length === 0}
            >
              {t('fileUpload.startImport')}
            </Button>
          </Space>
        }
      >
        {files.length === 0 ? (
          <div className="empty-state">
            <FileOutlined className="empty-icon" />
            <Text type="secondary">{t('fileUpload.noFiles')}</Text>
          </div>
        ) : (
          <Table
            columns={columns}
            dataSource={files}
            rowKey="id"
            rowSelection={rowSelection}
            pagination={{ pageSize: 10 }}
          />
        )}
      </Card>

      {/* 导入配置模态框 */}
      <Modal
        title={t('fileUpload.configImportTitle')}
        open={importModalVisible}
        onCancel={() => setImportModalVisible(false)}
        onOk={() => form.submit()}
        okText={t('fileUpload.startImport')}
        cancelText={t('common.cancel')}
      >
        <Form
          form={form}
          layout="vertical"
          onFinish={handleExecuteImport}
        >
          <Alert
            message={t('fileUpload.selectedFilesInfo', { count: selectedFiles.length })}
            type="info"
            showIcon
            style={{ marginBottom: 16 }}
          />
          
          <Form.Item
            name="configId"
            label={t('fileUpload.selectConfig')}
            rules={[{ required: true, message: t('fileUpload.selectConfigRequired') }]}
          >
            <Select placeholder={t('fileUpload.selectConfigPlaceholder')}>
              {configs.map(config => (
                <Option key={config.id} value={config.id}>
                  {config.csv_filename_pattern} - {config.validation_rule}
                </Option>
              ))}
            </Select>
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
};

export default FileUpload;