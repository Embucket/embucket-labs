import { useEffect } from 'react';

import type { AxiosError, AxiosRequestConfig, InternalAxiosRequestConfig } from 'axios';

import { UNAUTHORIZED_STATUS_CODE } from '@/constants';
import axiosInstance from '@/lib/axios';
import { refreshAuthToken } from '@/orval/auth';
import { type AuthErrorResponse, type AuthResponse } from '@/orval/models';

const AUTHORIZATION_HEADER_PREFIX = 'Bearer';

interface AxiosInterceptorsProps {
  onSetAuthenticated: (data: AuthResponse) => void;
  onResetAuthenticated: () => void;
  getAccessToken: () => string | null;
}

type OriginalRequest = AxiosRequestConfig & { _retry?: boolean };
type InterceptorError = AxiosError<AuthErrorResponse>;

const isTokenExpiredError = (error: InterceptorError, originalRequest: OriginalRequest) => {
  return (
    error.response?.status === UNAUTHORIZED_STATUS_CODE &&
    error.response.data.errorKind === 'expiredSignature' &&
    !originalRequest._retry
  );
};

export const AxiosInterceptors = ({
  onSetAuthenticated,
  onResetAuthenticated,
  getAccessToken,
}: AxiosInterceptorsProps) => {
  useEffect(() => {
    const requestInterceptor = axiosInstance.interceptors.request.use(
      (config: InternalAxiosRequestConfig) => {
        const token = getAccessToken();
        if (token) {
          config.headers.Authorization = `${AUTHORIZATION_HEADER_PREFIX} ${token}`;
        }
        return config;
      },
      (error: AxiosError) => {
        return Promise.reject(error);
      },
    );

    const responseInterceptor = axiosInstance.interceptors.response.use(
      (response) => response,
      async (error: InterceptorError) => {
        const originalRequest = error.config as OriginalRequest | undefined;

        if (originalRequest && isTokenExpiredError(error, originalRequest)) {
          originalRequest._retry = true;

          try {
            const authResponse = await refreshAuthToken();
            onSetAuthenticated(authResponse);

            if (originalRequest.headers) {
              originalRequest.headers.Authorization = `${AUTHORIZATION_HEADER_PREFIX} ${authResponse.accessToken}`;
            }
            return await axiosInstance(originalRequest);
          } catch (refreshError) {
            onResetAuthenticated();
            return Promise.reject(refreshError as Error);
          }
        }
        return Promise.reject(error);
      },
    );

    return () => {
      axiosInstance.interceptors.request.eject(requestInterceptor);
      axiosInstance.interceptors.response.eject(responseInterceptor);
    };
  }, [onSetAuthenticated, onResetAuthenticated, getAccessToken]);

  return null;
};
