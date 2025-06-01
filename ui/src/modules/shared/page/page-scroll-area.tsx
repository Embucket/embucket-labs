import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import { cn } from '@/lib/utils';

interface PageScrollAreaProps {
  tabs?: boolean;
  children: React.ReactNode;
}

export function PageScrollArea({ children, tabs }: PageScrollAreaProps) {
  return (
    <ScrollArea
      className={cn(
        'h-[calc(100vh-65px-64px-32px-2px)]',
        tabs && 'h-[calc(100vh-65px-64px-32px-2px-53px)]',
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
