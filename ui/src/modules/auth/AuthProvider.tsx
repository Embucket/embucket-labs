import { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react';

import type { AuthResponse } from '@/orval/models';

export interface AuthContext {
  isAuthenticated: boolean;
  setAuthenticated: (data: AuthResponse) => void;
  resetAuthenticated: () => void;
  user: string | null;
}

const AuthContext = createContext<AuthContext | undefined>(undefined);

const ACCESS_TOKEN_KEY = 'accessToken';

export const AuthProvider = ({ children }: { children: React.ReactNode }) => {
  const [user, setUser] = useState<string | null>(() => {
    const token = localStorage.getItem(ACCESS_TOKEN_KEY);
    if (token) {
      return 'username';
    }
    return null;
  });

  const setAuthenticated = useCallback((data: AuthResponse) => {
    localStorage.setItem(ACCESS_TOKEN_KEY, data.accessToken);
    setUser('username');
  }, []);

  const resetAuthenticated = useCallback(() => {
    localStorage.removeItem(ACCESS_TOKEN_KEY);
    setUser(null);
  }, []);

  // TODO: This is a hack to reset the authenticated state when the refresh token fails in the interceptor.
  useEffect(() => {
    const handleRefreshTokenFailed = resetAuthenticated;
    window.addEventListener('auth:refreshTokenFailed', handleRefreshTokenFailed);
    return () => {
      window.removeEventListener('auth:refreshTokenFailed', handleRefreshTokenFailed);
    };
  }, [resetAuthenticated]);

  const value = useMemo(
    () => ({
      user,
      isAuthenticated: !!user,
      setAuthenticated,
      resetAuthenticated,
    }),
    [user, setAuthenticated, resetAuthenticated],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
};

// eslint-disable-next-line react-refresh/only-export-components
export const useAuth = () => {
  const context = useContext(AuthContext);

  if (context === undefined) throw new Error('useAuth must be used within a AuthProvider');

  return context;
};
