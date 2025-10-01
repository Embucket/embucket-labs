import { FileText, Search } from 'lucide-react';

import { EmptyContainer } from '@/components/empty-container';
import { Input, InputIcon, InputRoot } from '@/components/ui/input';
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import { useGetDashboard } from '@/orval/dashboard';
import { useGetWorksheets } from '@/orval/worksheets';

import { PageHeader } from '../shared/page/page-header';
import HomeActionButtons from './home-action-buttons';
import { HomeDashboardMetrics } from './home-dashboard-metrics';
import { HomeWorksheetsTable } from './home-worksheets-table';

export function HomePage() {
  const { data: { items: worksheets } = {}, isLoading: isWorksheetsLoading } = useGetWorksheets();
  const { data: dashboardData, isLoading: isDashboardLoading } = useGetDashboard();

  return (
    <>
      <PageHeader
        title="Home"
        Action={
          <InputRoot className="min-w-[300px]">
            <InputIcon>
              <Search />
            </InputIcon>
            <Input disabled placeholder="Search" />
          </InputRoot>
        }
      />

      <div className="p-4">
        <p className="mb-2 text-3xl font-semibold">Welcome!</p>
        <p className="text-muted-foreground font-light">Nice seeing you here 😎</p>
      </div>
      <HomeActionButtons isLoading={isWorksheetsLoading} />
      <div className="flex size-full flex-col p-4">
        <p className="mb-4 font-semibold">Overview</p>
        <HomeDashboardMetrics isLoading={isDashboardLoading} dashboardData={dashboardData} />

        <div className="mt-4 flex size-full flex-col">
          <p className="mb-4 font-semibold">Worksheets</p>
          {!worksheets?.length && !isWorksheetsLoading ? (
            <EmptyContainer
              // TODO: Hardcode
              className="max-h-[calc(100vh-200px-322px)]"
              Icon={FileText}
              title="No SQL Worksheets Created Yet"
              description="Create your first worksheet to start querying data"
              // onCtaClick={() => {}}
              // ctaText="Create Worksheet"
            />
          ) : (
            <ScrollArea tableViewport className="h-[calc(100vh-200px-322px)]">
              <HomeWorksheetsTable worksheets={worksheets ?? []} isLoading={isWorksheetsLoading} />
              <ScrollBar orientation="horizontal" />
            </ScrollArea>
          )}
        </div>
      </div>
    </>
  );
}
