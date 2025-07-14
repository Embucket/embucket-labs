import React from 'react';

import { ReactQueryDevtools } from '@tanstack/react-query-devtools';
import ReactDOM from 'react-dom/client';
import { Toaster } from 'sonner';

import { ReactQueryProvider } from './providers/react-query-provider';
import { ReactRouterProvider } from './providers/react-router-provider';
import { ThemeProvider } from './providers/theme-provider';

import './i18n';
import './globals.css';

import { AuthProvider } from '../modules/auth/AuthProvider';

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <ReactQueryProvider>
      <ThemeProvider>
        <AuthProvider>
          <ReactRouterProvider />
        </AuthProvider>
        <Toaster richColors />
      </ThemeProvider>
      <ReactQueryDevtools initialIsOpen={false} />
    </ReactQueryProvider>
  </React.StrictMode>,
);
