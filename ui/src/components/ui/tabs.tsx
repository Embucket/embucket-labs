import { createContext, useContext } from 'react';

import * as TabsPrimitive from '@radix-ui/react-tabs';
import { cva, type VariantProps } from 'class-variance-authority';

import { cn } from '@/lib/utils';

const tabsListVariants = cva('inline-flex items-center justify-center', {
  variants: {
    variant: {
      default: 'bg-muted text-muted-foreground h-9 w-fit rounded-lg p-[3px]',
      underline: 'w-full justify-start border-b',
    },
  },
  defaultVariants: {
    variant: 'default',
  },
});

const tabsTriggerVariants = cva(
  'cursor-pointer text-sm font-medium transition-colors focus-visible:outline-none disabled:pointer-events-none disabled:opacity-50',
  {
    variants: {
      variant: {
        default: [
          'inline-flex h-[calc(100%-1px)] flex-1 items-center justify-center gap-1.5 rounded-md border-transparent px-2 py-1',
          'text-foreground dark:text-muted-foreground',
          'data-[state=active]:bg-background data-[state=active]:shadow-sm',
          'hover:bg-hover',
          'dark:data-[state=active]:text-foreground dark:data-[state=active]:border-input',
          'focus-visible:border-ring focus-visible:ring-ring/50 focus-visible:ring-[3px] focus-visible:outline-1',
          'whitespace-nowrap transition-[color,box-shadow]',
          "[&_svg]:pointer-events-none [&_svg]:shrink-0 [&_svg:not([class*='size-'])]:size-4",
        ],
        underline: [
          'relative px-3 py-4 text-gray-400 hover:text-gray-200',
          'data-[state=active]:text-white',
          'after:absolute after:right-0 after:bottom-0 after:left-0 after:h-[2px] after:scale-x-0 after:transform after:bg-white after:transition-transform',
          'data-[state=active]:after:scale-x-100',
        ],
      },
    },
    defaultVariants: {
      variant: 'default',
    },
  },
);

type TabsVariant = VariantProps<typeof tabsListVariants>['variant'];

const TabsContext = createContext<TabsVariant>('default');

function Tabs({
  className,
  variant = 'default',
  ...props
}: React.ComponentProps<typeof TabsPrimitive.Root> & { variant?: TabsVariant }) {
  return (
    <TabsContext.Provider value={variant}>
      <TabsPrimitive.Root
        data-slot="tabs"
        className={cn('flex flex-col gap-2', className)}
        {...props}
      />
    </TabsContext.Provider>
  );
}

function TabsList({ className, ...props }: React.ComponentProps<typeof TabsPrimitive.List>) {
  const variant = useContext(TabsContext);

  return (
    <TabsPrimitive.List
      data-slot="tabs-list"
      className={cn(tabsListVariants({ variant }), className)}
      {...props}
    />
  );
}

function TabsTrigger({ className, ...props }: React.ComponentProps<typeof TabsPrimitive.Trigger>) {
  const variant = useContext(TabsContext);

  return (
    <TabsPrimitive.Trigger
      data-slot="tabs-trigger"
      className={cn(tabsTriggerVariants({ variant }), className)}
      {...props}
    />
  );
}

function TabsContent({ className, ...props }: React.ComponentProps<typeof TabsPrimitive.Content>) {
  return (
    <TabsPrimitive.Content
      data-slot="tabs-content"
      className={cn('flex-1 outline-none', className)}
      {...props}
    />
  );
}

export { Tabs, TabsList, TabsTrigger, TabsContent };
