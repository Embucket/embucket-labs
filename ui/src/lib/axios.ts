import type { AxiosError, AxiosRequestConfig } from 'axios';
import axios from 'axios';

import { refresh as refreshToken } from '@/orval/auth';

const axiosInstance = axios.create({
  baseURL: import.meta.env.VITE_API_URL,
  withCredentials: true,
});

/** Interceptor for requests sent from the application:
retrieve the Access Token from localStorage and
add it to every API request made using the axios instance.
*/
axiosInstance.interceptors.request.use(
  (config) => {
    const token = localStorage.getItem('accessToken');
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
  },
  (error: AxiosError) => {
    return Promise.reject(error);
  },
);

/** Interceptor for responses received by the application:
check if the response indicates an expired access token, and
if so, send a refresh token request to obtain a new access token and
retry the original request using the updated token.
Here the refresh token is stored as cookies.
*/
axiosInstance.interceptors.response.use(
  (response) => {
    return response;
  },
  async (error: AxiosError<{ message: string }>) => {
    const originalRequest = error.config as (AxiosRequestConfig & { _retry?: boolean }) | undefined;

    if (
      error.response &&
      error.response.status === 401 &&
      // TODO: Handle type
      error.response.data.message === 'Token expired' &&
      originalRequest &&
      !originalRequest._retry
    ) {
      originalRequest._retry = true;

      try {
        const { accessToken } = await refreshToken();

        localStorage.setItem('accessToken', accessToken);

        if (originalRequest.headers) {
          originalRequest.headers.Authorization = `Bearer ${accessToken}`;
        }

        return await axiosInstance(originalRequest);
      } catch (refreshError) {
        localStorage.removeItem('accessToken');
        window.dispatchEvent(new CustomEvent('auth:refreshTokenFailed'));
        return Promise.reject(refreshError as Error);
      }
    }

    return Promise.reject(error);
  },
);

export default axiosInstance;
