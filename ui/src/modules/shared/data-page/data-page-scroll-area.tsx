import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import { cn } from '@/lib/utils';

interface DataPageScrollAreaProps {
  hasTabs?: boolean;
  children: React.ReactNode;
}

export function DataPageScrollArea({ children, hasTabs }: DataPageScrollAreaProps) {
  return (
    <ScrollArea
      className={cn(
        'h-[calc(100vh-72px-64px-31px)]',
        hasTabs && 'h-[calc(100vh-72px-64px-31px-53px)]',
      )}
    >
      <div className="flex size-full flex-col p-4 pt-0">
        <ScrollArea tableViewport>
          {children}
          <ScrollBar orientation="horizontal" />
        </ScrollArea>
      </div>
      <ScrollBar orientation="vertical" />
    </ScrollArea>
  );
}
