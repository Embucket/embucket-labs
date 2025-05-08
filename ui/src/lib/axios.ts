import axios from 'axios';

const axiosInstance = axios.create({
  baseURL: import.meta.env.VITE_API_URL,
  withCredentials: true,
});

// See request interceptor in AxiosInterceptors.tsx
// See response interceptor in AxiosInterceptors.tsx

export default axiosInstance;
