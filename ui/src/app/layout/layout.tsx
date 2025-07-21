import type { ReactNode } from 'react';

import { SidebarInset } from '@/components/ui/sidebar';
import { cn } from '@/lib/utils';

interface LayoutProps {
  children: ReactNode;
}

export function Layout({ children }: LayoutProps) {
  return (
    <SidebarInset
      className={cn(
        'mt-[var(--content-mt)] mb-[var(--content-mb)] max-h-[calc(100vh-var(--content-mt)-var(--content-mb))]',
        'mr-[var(--content-mr)] w-[calc(100vw-(var(--sidebar-width))-var(--content-mr))]',
      )}
    >
      <div className="bg-background-secondary relative size-full rounded-lg border">{children}</div>
    </SidebarInset>
  );
}
