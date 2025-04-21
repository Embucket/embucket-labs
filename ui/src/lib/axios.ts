import axios from 'axios';

const axiosInstance = axios.create({
  // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
  baseURL: import.meta.env.VITE_API_URL,
  withCredentials: true,
});

export default axiosInstance;
