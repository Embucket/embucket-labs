import { SidebarMenuButton, SidebarMenuItem } from '@/components/ui/sidebar';
import { Skeleton } from '@/components/ui/skeleton';

const WORKSHEETS = 7;

const SkeletonRow = () => {
  return (
    <div className="flex w-full items-center gap-2">
      <Skeleton className="h-5 w-full" />
    </div>
  );
};

export const EditorLeftPanelWorksheetsSkeleton = () => {
  return (
    <>
      {Array.from({ length: WORKSHEETS }).map((_, index) => (
        <SidebarMenuItem key={index}>
          <SidebarMenuButton disabled>
            <SkeletonRow />
          </SidebarMenuButton>
        </SidebarMenuItem>
      ))}
    </>
  );
};
