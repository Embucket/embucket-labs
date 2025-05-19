import { Plus, type LucideIcon } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';

interface EmptyContainerProps {
  description?: string;
  title: string;
  onCtaClick?: () => void;
  ctaText?: string;
  Icon: LucideIcon;
  className?: string;
}

export function EmptyContainer({
  title,
  description,
  onCtaClick,
  ctaText,
  Icon,
  className,
}: EmptyContainerProps) {
  return (
    <div
      className={cn(
        'flex h-full w-full min-w-[256px] flex-1 flex-col items-center justify-center p-4 text-center',
        className,
      )}
    >
      <Icon strokeWidth={1.5} className="text-muted-foreground mb-4 size-12 font-thin" />
      <h3 className="mb-2 text-sm font-medium tracking-tight">{title}</h3>
      {description && <p className="text-muted-foreground text-sm">{description}</p>}
      {onCtaClick && ctaText && (
        <Button onClick={onCtaClick} size="sm" className="mt-4">
          <Plus className="mr-2 size-4" />
          {ctaText}
        </Button>
      )}
    </div>
  );
}
