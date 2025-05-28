import type { LucideIcon } from 'lucide-react';

import { EmptyContainer } from '@/components/empty-container';
import { cn } from '@/lib/utils';

interface PageEmptyContainerProps {
  Icon: LucideIcon;
  title: string;
  description: string;
  tabs?: boolean;
}

export function PageEmptyContainer({ Icon, title, description, tabs }: PageEmptyContainerProps) {
  return (
    <EmptyContainer
      className={cn('h-[calc(100vh-65px-32px-2px)]', tabs && 'h-[calc(100vh-65px-32px-2px-53px)]')}
      Icon={Icon}
      title={title}
      description={description}
    />
  );
}
