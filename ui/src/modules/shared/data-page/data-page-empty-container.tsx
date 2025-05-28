import type { LucideIcon } from 'lucide-react';

import { EmptyContainer } from '@/components/empty-container';
import { cn } from '@/lib/utils';

interface DataPageEmptyContainerProps {
  Icon: LucideIcon;
  title: string;
  description: string;
  hasTabs?: boolean;
}

export function DataPageEmptyContainer({
  Icon,
  title,
  description,
  hasTabs,
}: DataPageEmptyContainerProps) {
  return (
    <EmptyContainer
      className={cn(
        'h-[calc(100vh-72px-28px-3px)]',
        hasTabs && 'h-[calc(100vh-72px-28px-3px-52px-1px)]',
      )}
      Icon={Icon}
      title={title}
      description={description}
    />
  );
}
