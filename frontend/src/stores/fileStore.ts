import { create } from 'zustand'

interface FileUploadResult {
  id: string
  filename: string
  status: 'success' | 'error'
  message?: string
  uploadedAt: string
}

interface ImportJob {
  id: string
  fileIds: string[]
  configId: string
  status: 'pending' | 'processing' | 'completed' | 'failed'
  progress: number
  startedAt: string
  completedAt?: string
  errorMessage?: string
}

interface FileStore {
  uploadResults: FileUploadResult[]
  importJobs: ImportJob[]
  loading: boolean
  error: string | null
  uploadFile: (file: File) => Promise<FileUploadResult>
  importFiles: (fileIds: string[], configId: string) => Promise<ImportJob>
  getImportJob: (jobId: string) => ImportJob | null
  clearUploadResults: () => void
}

export const useFileStore = create<FileStore>((set, get) => ({
  uploadResults: [],
  importJobs: [],
  loading: false,
  error: null,

  uploadFile: async (file: File) => {
    set({ loading: true, error: null })
    try {
      // 模拟文件上传
      await new Promise(resolve => setTimeout(resolve, 1000))
      
      const result: FileUploadResult = {
        id: `upload_${Date.now()}_${Math.random()}`,
        filename: file.name,
        status: 'success',
        message: '文件上传成功',
        uploadedAt: new Date().toISOString()
      }

      set(state => ({
        uploadResults: [...state.uploadResults, result],
        loading: false
      }))

      return result
    } catch (error) {
      const result: FileUploadResult = {
        id: `upload_${Date.now()}_${Math.random()}`,
        filename: file.name,
        status: 'error',
        message: error instanceof Error ? error.message : '文件上传失败',
        uploadedAt: new Date().toISOString()
      }

      set(state => ({
        uploadResults: [...state.uploadResults, result],
        loading: false,
        error: error instanceof Error ? error.message : '文件上传失败'
      }))

      return result
    }
  },

  importFiles: async (fileIds: string[], configId: string) => {
    set({ loading: true, error: null })
    try {
      const job: ImportJob = {
        id: `import_${Date.now()}_${Math.random()}`,
        fileIds,
        configId,
        status: 'pending',
        progress: 0,
        startedAt: new Date().toISOString()
      }

      set(state => ({
        importJobs: [...state.importJobs, job],
        loading: false
      }))

      // 模拟导入过程
      setTimeout(() => {
        set(state => ({
          importJobs: state.importJobs.map(j => 
            j.id === job.id 
              ? { ...j, status: 'processing' as const }
              : j
          )
        }))

        // 模拟进度更新
        let progress = 0
        const progressInterval = setInterval(() => {
          progress += Math.random() * 20
          if (progress >= 100) {
            progress = 100
            clearInterval(progressInterval)
            
            // 随机成功或失败
            const isSuccess = Math.random() > 0.2
            set(state => ({
              importJobs: state.importJobs.map(j => 
                j.id === job.id 
                  ? { 
                      ...j, 
                      status: isSuccess ? 'completed' : 'failed',
                      progress: 100,
                      completedAt: new Date().toISOString(),
                      errorMessage: isSuccess ? undefined : '数据验证失败'
                    }
                  : j
              )
            }))
          } else {
            set(state => ({
              importJobs: state.importJobs.map(j => 
                j.id === job.id 
                  ? { ...j, progress }
                  : j
              )
            }))
          }
        }, 500)
      }, 1000)

      return job
    } catch (error) {
      set({ 
        loading: false, 
        error: error instanceof Error ? error.message : '启动导入任务失败' 
      })
      throw error
    }
  },

  getImportJob: (jobId: string) => {
    const state = get()
    return state.importJobs.find(job => job.id === jobId) || null
  },

  clearUploadResults: () => {
    set({ uploadResults: [] })
  }
}))