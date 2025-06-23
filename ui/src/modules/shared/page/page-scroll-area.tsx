import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import { cn } from '@/lib/utils';

interface PageScrollAreaProps {
  tabs?: boolean;
  children: React.ReactNode;
}

export function PageScrollArea({ children, tabs }: PageScrollAreaProps) {
  return (
    <ScrollArea
      tableViewport
      className={cn(
        'px-4 pb-4',
        'h-[calc(100vh-32px-65px-64px-2px)]',
        tabs && 'h-[calc(100vh-32px-65px-53px-64px-10px)]',
      )}
    >
      {children}
      <ScrollBar orientation="horizontal" />
      <ScrollBar orientation="vertical" />
    </ScrollArea>
  );
}
