import { Info } from 'lucide-react';
import { useTheme } from 'next-themes';
import type { ToasterProps } from 'sonner';
import { Toaster as Sonner } from 'sonner';

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
