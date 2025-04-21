import { useEffect } from 'react';

import { ResizableHandle, ResizablePanel } from '@/components/ui/resizable';
import { cn } from '@/lib/utils';

import { useSqlEditorPanelsState } from './sql-editor-panels-state-provider';

export const SqlEditorResizablePanel = ({
  children,
  className,
  onCollapse,
  onExpand,
  // minSize,
  ...props
}: React.ComponentProps<typeof ResizablePanel>) => {
  const { setIsAnyPanelCollapsing, isDragging, isAnyPanelCollapsing } = useSqlEditorPanelsState();

  useEffect(() => {
    setIsAnyPanelCollapsing(false);
  }, [setIsAnyPanelCollapsing]);

  return (
    <ResizablePanel
      {...props}
      className={cn(
        isDragging && isAnyPanelCollapsing && 'transition-all duration-300 ease-in-out',
        !isDragging && 'transition-all duration-300 ease-in-out',
        className,
      )}
      onCollapse={() => {
        onCollapse?.();
        setIsAnyPanelCollapsing(true);
      }}
      onExpand={() => {
        onExpand?.();
        setIsAnyPanelCollapsing(false);
      }}
      // minSize={isDragging ? undefined : minSize}
    >
      {children}
    </ResizablePanel>
  );
};

export const SqlEditorResizableHandle = ({
  ...props
}: React.ComponentProps<typeof ResizableHandle>) => {
  const { setIsResizing } = useSqlEditorPanelsState();

  return (
    <ResizableHandle withHandle onDragging={(dragging) => setIsResizing(dragging)} {...props} />
  );
};
