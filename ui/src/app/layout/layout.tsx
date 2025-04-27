import type { ReactNode } from 'react';

import { SidebarInset } from '@/components/ui/sidebar';
import { cn } from '@/lib/utils';

interface LayoutProps {
  children: ReactNode;
}

export function Layout({ children }: LayoutProps) {
  // TODO: Use layout.css and css variables
  // TODO: Hardcode
  return (
    <SidebarInset
      className={cn(
        'my-4 max-h-[calc(100vh-16px-16px)]',
        'mr-4 w-[calc(100vw-(var(--sidebar-width))-16px)]',
      )}
    >
      <div className="relative size-full rounded-lg border bg-[#1F1F1F]">{children}</div>
    </SidebarInset>
  );
}
