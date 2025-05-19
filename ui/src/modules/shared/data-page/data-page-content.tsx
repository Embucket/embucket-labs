import type { ReactNode } from 'react';

import type { LucideIcon } from 'lucide-react';

import { EmptyContainer } from '@/components/empty-container';
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import { cn } from '@/lib/utils';

interface DataPageContentProps {
  isEmpty: boolean;
  hasTabs?: boolean;
  Table: ReactNode;
  emptyStateIcon: LucideIcon;
  emptyStateTitle: string;
  emptyStateDescription: string;
}

export function DataPageContent({
  isEmpty,
  Table,
  hasTabs,
  emptyStateIcon: Icon,
  emptyStateTitle,
  emptyStateDescription,
}: DataPageContentProps) {
  return !isEmpty ? (
    <ScrollArea
      className={cn(
        'h-[calc(100vh-117px-32px-2px)]',
        hasTabs && 'h-[calc(100vh-117px-32px-2px-53px)]',
      )}
    >
      <div className="flex size-full flex-col p-4">
        <ScrollArea tableViewport>
          {Table}
          <ScrollBar orientation="horizontal" />
        </ScrollArea>
      </div>
      <ScrollBar orientation="vertical" />
    </ScrollArea>
  ) : (
    <EmptyContainer
      className="h-[calc(100vh-117px-32px-2px)]"
      Icon={Icon}
      title={emptyStateTitle}
      description={emptyStateDescription}
    />
  );
}
