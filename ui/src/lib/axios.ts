import axios from 'axios';

// Define a type for window.env to avoid type errors
interface WindowWithEnv extends Window {
  env?: {
    API_URL?: string;
  };
}
const typedWindow = window as WindowWithEnv;

// Check if the placeholder in index.html was replaced.
const isDeployed =
  typeof typedWindow.env?.API_URL === 'string' && !typedWindow.env.API_URL.startsWith('__');

// Set the base URL:
// - In a deployed container, use the absolute URL from window.env.
// - For local dev, use a relative path for the Vite proxy.
const API_BASE_URL = isDeployed ? typedWindow.env?.API_URL : '/';

const axiosInstance = axios.create({
  baseURL: API_BASE_URL,
  withCredentials: true,
});

// axiosInstance.interceptors.response.use(async (response) => {
//   // ⏳ add 3s delay
//   await new Promise((resolve) => setTimeout(resolve, 3000));
//   return response;
// });

// See request interceptor in AxiosInterceptors.tsx
// See response interceptor in AxiosInterceptors.tsx

export default axiosInstance;
