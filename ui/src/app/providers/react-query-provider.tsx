import { MutationCache, QueryCache, QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { toast } from 'sonner';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: Infinity,
      refetchOnReconnect: false,
      retry: false,
    },
  },
  queryCache: new QueryCache({
    onError: (error) => {
      toast.error(error.message);
    },
  }),
  mutationCache: new MutationCache({
    onError: (error) => {
      toast.error(error.message);
    },
  }),
});

interface ReactQueryProviderProps {
  children: React.ReactNode;
}

export const ReactQueryProvider = ({ children }: ReactQueryProviderProps) => {
  return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>;
};
