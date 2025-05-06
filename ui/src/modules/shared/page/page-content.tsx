import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';

interface PageContentProps {
  children: React.ReactNode;
}

export const PageContent = ({ children }: PageContentProps) => {
  // TODO: Hardcode
  return (
    <ScrollArea className="h-[calc(100vh-65px-32px)] p-4">
      {children}
      <ScrollBar orientation="vertical" />
    </ScrollArea>
  );
};
