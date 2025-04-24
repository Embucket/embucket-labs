import type { ReactNode } from 'react';

import { SidebarInset } from '@/components/ui/sidebar';
import { cn } from '@/lib/utils';

interface LayoutProps {
  children: ReactNode;
}

// TODO: Use layout.css
export function Layout({ children }: LayoutProps) {
  return (
    <SidebarInset
      className={cn(
        'my-4 max-h-[calc(100vh-16px-16px)]',
        'mr-4 ml-2 w-[calc(100vw-var(--sidebar-width)-16px-8px)]',
      )}
    >
      {
        // TODO: Use css variable
      }
      <div className="relative size-full rounded-lg border bg-[#1F1F1F]">{children}</div>
    </SidebarInset>
  );
}
