import { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react';

import { flushSync } from 'react-dom';

export interface AuthContext {
  isAuthenticated: boolean;
  isLoading: boolean;
  login: (username: string) => Promise<void>;
  logout: () => void;
  user: string | null;
}

const AuthContext = createContext<AuthContext | undefined>(undefined);

const key = 'auth.user';

const getUserFromLocalStorage = () => {
  return localStorage.getItem(key) ?? null;
};

const setStoredUser = (user: string | null) => {
  if (user) {
    localStorage.setItem(key, user);
  } else {
    localStorage.removeItem(key);
  }
};

export const AuthProvider = ({ children }: { children: React.ReactNode }) => {
  const [user, setUser] = useState<string | null>(getUserFromLocalStorage());
  const [isLoading, setIsLoading] = useState<boolean>(false);
  const isAuthenticated = !!user;

  const logout = useCallback(() => {
    setStoredUser(null);
    setUser(null);
  }, []);

  const login = useCallback(async (username: string) => {
    setIsLoading(true);
    // Delay 1 sec
    await new Promise((resolve) => setTimeout(resolve, 1000));
    // https://github.com/TanStack/router/discussions/1668#discussioncomment-9726727
    flushSync(() => {
      setStoredUser(username);
      setUser(username);
      setIsLoading(false);
    });
  }, []);

  useEffect(() => {
    setUser(getUserFromLocalStorage());
  }, []);

  const value = useMemo(
    () => ({ isAuthenticated, isLoading, user, login, logout }),
    [isAuthenticated, user, login, logout, isLoading],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
};

// eslint-disable-next-line react-refresh/only-export-components
export const useAuth = () => {
  const context = useContext(AuthContext);

  if (context === undefined) throw new Error('useAuth must be used within a AuthProvider');

  return context;
};
