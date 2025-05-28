import type { LucideIcon } from 'lucide-react';

import { EmptyContainer } from '@/components/empty-container';
import { cn } from '@/lib/utils';

interface DataPageEmptyContainerProps {
  Icon: LucideIcon;
  title: string;
  description: string;
  tabs?: boolean;
}

export function DataPageEmptyContainer({
  Icon,
  title,
  description,
  tabs,
}: DataPageEmptyContainerProps) {
  return (
    <EmptyContainer
      className={cn('h-[calc(100vh-65px-32px-2px)]', tabs && 'h-[calc(100vh-65px-32px-2px-53px)]')}
      Icon={Icon}
      title={title}
      description={description}
    />
  );
}
