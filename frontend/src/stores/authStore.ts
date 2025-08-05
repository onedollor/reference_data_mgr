import { create } from 'zustand'

interface User {
  id: string
  username: string
  role: string
}

interface AuthStore {
  isAuthenticated: boolean
  user: User | null
  login: (username: string, password: string) => Promise<boolean>
  logout: () => void
}

export const useAuthStore = create<AuthStore>((set) => ({
  isAuthenticated: true, // 开发模式默认已登录
  user: {
    id: '1',
    username: 'admin',
    role: 'admin'
  },
  login: async (username: string, password: string) => {
    // 简单的登录逻辑
    if (username === 'admin' && password === 'admin123') {
      set({
        isAuthenticated: true,
        user: {
          id: '1',
          username: 'admin',
          role: 'admin'
        }
      })
      return true
    }
    return false
  },
  logout: () => {
    set({
      isAuthenticated: false,
      user: null
    })
  }
}))