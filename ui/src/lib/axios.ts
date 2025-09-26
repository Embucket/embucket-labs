import axios from 'axios';

const axiosInstance = axios.create({
  baseURL: '/',
  withCredentials: true,
});

// See request interceptor in AxiosInterceptors.tsx
// See response interceptor in AxiosInterceptors.tsx

export default axiosInstance;
