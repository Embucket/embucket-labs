import { Info } from 'lucide-react';
import type { ToasterProps } from 'sonner';
import { Toaster as Sonner } from 'sonner';

import { useTheme } from '@/app/providers/theme-provider';

const Toaster = ({ ...props }: ToasterProps) => {
  const { theme = 'system' } = useTheme();

  return (
    <Sonner
      theme={theme as ToasterProps['theme']}
      className="toaster group"
      icons={{
        error: <Info className="size-5" />,
      }}
      toastOptions={{
        classNames: {
          error: 'text-destructive!',
        },
      }}
      {...props}
    />
  );
};

export { Toaster };
