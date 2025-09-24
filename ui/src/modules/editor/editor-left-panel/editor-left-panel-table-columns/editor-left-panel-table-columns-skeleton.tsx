import { Skeleton } from '@/components/ui/skeleton';

const COLUMNS = 8;

const SkeletonColumn = () => {
  return (
    <div className="flex h-5 w-full items-center justify-between gap-2">
      <Skeleton className="h-4 w-full" />
      <Skeleton className="h-4 w-14" />
    </div>
  );
};

export const EditorLeftPanelTableColumnsSkeleton = () => {
  return (
    <div className="mt-2 flex flex-col gap-2">
      {Array.from({ length: COLUMNS }).map((_, index) => (
        <SkeletonColumn key={index} />
      ))}
    </div>
  );
};
