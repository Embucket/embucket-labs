import type { ReactNode } from 'react';

import { ScrollArea, ScrollBar } from '../ui/scroll-area';

interface DataTableScrollAreaProps {
  children: ReactNode;
}

export const DataTableScrollArea = ({ children }: DataTableScrollAreaProps) => {
  return (
    <ScrollArea className="size-full">
      {/* <ScrollArea className="size-full md:max-w-[calc(100vw-var(--sidebar-w)-var(--sidebar-ml)-var(--layout-content-pl)-var(layout-content-pr))]"> */}
      {children}
      <ScrollBar orientation="horizontal" />
    </ScrollArea>
  );
};
