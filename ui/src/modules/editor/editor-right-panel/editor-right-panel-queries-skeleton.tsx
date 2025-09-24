import { SidebarMenu, SidebarMenuButton, SidebarMenuItem } from '@/components/ui/sidebar';
import { Skeleton } from '@/components/ui/skeleton';

const QUERIES = 12;

const SkeletonRow = () => {
  return (
    <div className="flex w-full items-center gap-2">
      <Skeleton className="h-5 w-full" />
    </div>
  );
};

export const EditorRightPanelQueriesSkeleton = () => {
  return (
    <SidebarMenu>
      {Array.from({ length: QUERIES }).map((_, index) => (
        <SidebarMenuItem key={index}>
          <SidebarMenuButton disabled>
            <SkeletonRow />
          </SidebarMenuButton>
        </SidebarMenuItem>
      ))}
    </SidebarMenu>
  );
};
