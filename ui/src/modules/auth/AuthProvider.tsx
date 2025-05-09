import { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react';

import { flushSync } from 'react-dom';

import { useRefreshAuthToken } from '@/orval/auth';
import type { AuthResponse } from '@/orval/models';

import { AxiosInterceptors } from './AxiosInterceptors';

export interface AuthContextType {
  isAuthenticated: boolean;
  setAuthenticated: (data: AuthResponse) => void;
  resetAuthenticated: () => void;
  getAccessToken: () => string | null;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

export const AuthProvider = ({ children }: { children: React.ReactNode }) => {
  const [accessToken, setAccessToken] = useState<string | null>(null);

  const {
    mutate: refresh,
    isPending,
    isIdle,
  } = useRefreshAuthToken({
    mutation: {
      onSuccess: (data) => {
        setAuthenticated(data);
      },
    },
  });

  const getAccessToken = useCallback(() => {
    return accessToken;
  }, [accessToken]);

  const setAuthenticated = useCallback((data: AuthResponse) => {
    // Important to use flushSync, so TanStack Router beforeLoad context is updated
    flushSync(() => {
      setAccessToken(data.accessToken);
    });
  }, []);

  const resetAuthenticated = useCallback(() => {
    setAccessToken(null);
  }, []);

  useEffect(refresh, [refresh]);

  const isAuthenticated = !!accessToken;

  const value = useMemo(
    () => ({
      isAuthenticated,
      setAuthenticated,
      resetAuthenticated,
      getAccessToken,
    }),
    [isAuthenticated, setAuthenticated, resetAuthenticated, getAccessToken],
  );

  // Important to return null here, so TanStack Router context is correctly initialized
  if (isIdle || isPending) {
    return null;
  }

  return (
    <AuthContext.Provider value={value}>
      {children}
      <AxiosInterceptors
        onSetAuthenticated={setAuthenticated}
        onResetAuthenticated={resetAuthenticated}
        getAccessToken={getAccessToken}
      />
    </AuthContext.Provider>
  );
};

// eslint-disable-next-line react-refresh/only-export-components
export const useAuth = () => {
  const context = useContext(AuthContext);

  if (context === undefined) throw new Error('useAuth must be used within a AuthProvider');

  return context;
};
