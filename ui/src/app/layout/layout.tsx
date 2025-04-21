import type { ReactNode } from 'react';

import { cn } from '@/lib/utils';

interface LayoutProps {
  children: ReactNode;
}

export const Layout = ({ children }: LayoutProps) => {
  return (
    <div className="flex min-h-screen w-full">
      <div className="flex w-full flex-col">
        <main
          className={cn(
            'flex min-h-screen flex-col',
            'pt-(--layout-content-pt) pr-(--layout-content-pr) pb-(--layout-content-pb) pl-(--layout-content-pl)',
            'md:pl-[calc(var(--sidebar-w)+var(--sidebar-ml)+var(--layout-content-pl))]',
          )}
        >
          {children}
        </main>
      </div>
    </div>
  );
};
