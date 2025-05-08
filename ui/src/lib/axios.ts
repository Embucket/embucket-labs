import type { AxiosError, AxiosRequestConfig } from 'axios';
import axios from 'axios';

const axiosInstance = axios.create({
  // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
  baseURL: import.meta.env.VITE_API_URL,
  withCredentials: true,
});

/** Interceptor for requests sent from the application:
retrieve the Access Token from localStorage and
add it to every API request made using the axios instance.
*/
axiosInstance.interceptors.request.use(
  function (config) {
    const token = localStorage.getItem('accessToken');
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
  },
  function (error: AxiosError) {
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
  function (response) {
    return response;
  },
  async function (error: AxiosError) {
    const originalRequest = error.config as (AxiosRequestConfig & { _retry?: boolean }) | undefined;

    if (
      error.response &&
      error.response.status === 403 &&
      originalRequest &&
      !originalRequest._retry
    ) {
      originalRequest._retry = true;

      try {
        // Define a type for the expected response data
        interface RefreshTokenResponse {
          accessToken: string;
        }

        const response = await axios.get<RefreshTokenResponse>(
          'http://localhost:3000/auth/refresh-token',
          {
            withCredentials: true, // This attaches cookies (e.g., refresh token) to the request
          },
        );
        // Update the access token
        localStorage.setItem('accessToken', response.data.accessToken);

        if (originalRequest.headers) {
          originalRequest.headers.Authorization = `Bearer ${response.data.accessToken}`;
        }

        return await axiosInstance(originalRequest);
      } catch (refreshError) {
        // It's important to handle the refresh token error, e.g., by redirecting to login
        // eslint-disable-next-line no-console
        console.error('Error fetching data:', refreshError);
      }
    }

    return Promise.reject(new Error(error.message));
  },
);

export default axiosInstance;
