import type { ReactNode } from 'react';

import type { LucideIcon } from 'lucide-react';

import { EmptyContainer } from '@/components/empty-container';
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import { cn } from '@/lib/utils';

interface DataPageContentProps {
  isEmpty: boolean;
  hasTabs?: boolean;
  emptyStateIcon: LucideIcon;
  children: ReactNode;
  emptyStateTitle: string;
  emptyStateDescription: string;
}

export function DataPageContent({
  children,
  isEmpty,
  hasTabs,
  emptyStateIcon: Icon,
  emptyStateTitle,
  emptyStateDescription,
}: DataPageContentProps) {
  return !isEmpty ? (
    <ScrollArea
      className={cn('h-[calc(100vh-72px-31px)]', hasTabs && 'h-[calc(100vh-72px-31px-53px)]')}
    >
      <div className="flex size-full flex-col p-4">
        <ScrollArea tableViewport>
          {children}
          <ScrollBar orientation="horizontal" />
        </ScrollArea>
      </div>
      <ScrollBar orientation="vertical" />
    </ScrollArea>
  ) : (
    <EmptyContainer
      className="h-[calc(100vh-72px-31px)]"
      Icon={Icon}
      title={emptyStateTitle}
      description={emptyStateDescription}
    />
  );
}
