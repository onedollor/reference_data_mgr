import { create } from 'zustand'

interface Config {
  id: string
  csv_filename_pattern: string
  validation_rule: string
  description?: string
  created_at: string
  updated_at: string
}

interface ConfigStore {
  configs: Config[]
  loading: boolean
  error: string | null
  fetchConfigs: () => Promise<void>
  addConfig: (config: Omit<Config, 'id' | 'created_at' | 'updated_at'>) => Promise<void>
  updateConfig: (id: string, config: Partial<Config>) => Promise<void>
  deleteConfig: (id: string) => Promise<void>
}

export const useConfigStore = create<ConfigStore>((set, get) => ({
  configs: [
    {
      id: '1',
      csv_filename_pattern: 'data.*.csv',
      validation_rule: '标准数据验证',
      description: '默认配置，适用于标准数据文件',
      created_at: '2024-01-01T00:00:00Z',
      updated_at: '2024-01-01T00:00:00Z'
    },
    {
      id: '2', 
      csv_filename_pattern: 'report.*.csv',
      validation_rule: '报表数据验证',
      description: '适用于报表类数据文件',
      created_at: '2024-01-01T00:00:00Z',
      updated_at: '2024-01-01T00:00:00Z'
    }
  ],
  loading: false,
  error: null,

  fetchConfigs: async () => {
    set({ loading: true, error: null })
    try {
      // 模拟API调用
      await new Promise(resolve => setTimeout(resolve, 1000))
      // 在实际项目中，这里会调用API获取配置列表
      set({ loading: false })
    } catch (error) {
      set({ 
        loading: false, 
        error: error instanceof Error ? error.message : '获取配置失败' 
      })
    }
  },

  addConfig: async (configData) => {
    set({ loading: true, error: null })
    try {
      await new Promise(resolve => setTimeout(resolve, 500))
      const newConfig: Config = {
        ...configData,
        id: Date.now().toString(),
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString()
      }
      set(state => ({
        configs: [...state.configs, newConfig],
        loading: false
      }))
    } catch (error) {
      set({ 
        loading: false, 
        error: error instanceof Error ? error.message : '添加配置失败' 
      })
    }
  },

  updateConfig: async (id, configData) => {
    set({ loading: true, error: null })
    try {
      await new Promise(resolve => setTimeout(resolve, 500))
      set(state => ({
        configs: state.configs.map(config => 
          config.id === id 
            ? { ...config, ...configData, updated_at: new Date().toISOString() }
            : config
        ),
        loading: false
      }))
    } catch (error) {
      set({ 
        loading: false, 
        error: error instanceof Error ? error.message : '更新配置失败' 
      })
    }
  },

  deleteConfig: async (id) => {
    set({ loading: true, error: null })
    try {
      await new Promise(resolve => setTimeout(resolve, 500))
      set(state => ({
        configs: state.configs.filter(config => config.id !== id),
        loading: false
      }))
    } catch (error) {
      set({ 
        loading: false, 
        error: error instanceof Error ? error.message : '删除配置失败' 
      })
    }
  }
}))