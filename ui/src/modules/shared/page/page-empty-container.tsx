import { cva, type VariantProps } from 'class-variance-authority';
import type { LucideIcon } from 'lucide-react';

import { EmptyContainer } from '@/components/empty-container';

const containerVariants = cva('', {
  variants: {
    variant: {
      basic: 'h-[calc(100vh-var(--content-mb)-var(--content-mt)-65px-2px)]',
      toolbar: 'h-[calc(100vh-var(--content-mb)-var(--content-mt)-65px-2px-64px)]',
      tabs: 'h-[calc(100vh-var(--content-mb)-var(--content-mt)-65px-2px-64px-53px)]',
    },
  },
  defaultVariants: {
    variant: 'basic',
  },
});

type ContainerVariants = VariantProps<typeof containerVariants>;

interface PageEmptyContainerProps extends ContainerVariants {
  Icon: LucideIcon;
  title: string;
  description: string;
}

export function PageEmptyContainer({
  Icon,
  title,
  description,
  variant = 'basic',
}: PageEmptyContainerProps) {
  return (
    <EmptyContainer
      className={containerVariants({ variant })}
      Icon={Icon}
      title={title}
      description={description}
    />
  );
}
