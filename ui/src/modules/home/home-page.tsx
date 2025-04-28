import { FileText, Search } from 'lucide-react';

import { EmptyContainer } from '@/components/empty-container';
import { Input, InputIcon, InputRoot } from '@/components/ui/input';
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import { useGetDashboard } from '@/orval/dashboard';
import { useGetWorksheets } from '@/orval/worksheets';

import HomeActionButtons from './home-action-buttons';
import { HomeDashboardMetrics } from './home-dashboard-metrics';
import { HomeWorksheetsTable } from './home-worksheets-table';

export function HomePage() {
  const { data: { items: worksheets } = {}, isLoading } = useGetWorksheets();
  const { data: dashboardData } = useGetDashboard();

  if (!dashboardData) {
    return null;
  }

  return (
    <>
      <div className="flex items-center justify-between border-b p-4">
        <h1 className="text-xl font-semibold">Home</h1>
        <InputRoot>
          <InputIcon>
            <Search />
          </InputIcon>
          <Input className="min-w-80" disabled placeholder="Search" />
        </InputRoot>
      </div>
      {/* TODO: Hardcode */}
      <ScrollArea className="h-[calc(100vh-65px-32px)]">
        <div className="p-4">
          <p className="mb-2 text-3xl font-semibold">Welcome!</p>
          <p className="text-muted-foreground font-light">Nice seeing you here 😎</p>
        </div>
        <HomeActionButtons />
        <div className="flex size-full flex-col p-4">
          <p className="mb-4 font-semibold">Overview</p>
          <HomeDashboardMetrics dashboardData={dashboardData} />

          <div className="mt-4 flex size-full flex-col">
            <p className="mb-4 font-semibold">Worksheets</p>
            {worksheets?.length ? (
              <ScrollArea tableViewport>
                <HomeWorksheetsTable worksheets={worksheets} isLoading={isLoading} />
                <ScrollBar orientation="horizontal" />
              </ScrollArea>
            ) : (
              <EmptyContainer
                Icon={FileText}
                title="No SQL Worksheets Created Yet"
                description="Create your first worksheet to start querying data"
                // onCtaClick={() => {}}
                // ctaText="Create Worksheet"
              />
            )}
          </div>
        </div>
        <ScrollBar orientation="vertical" />
      </ScrollArea>
    </>
  );
}
