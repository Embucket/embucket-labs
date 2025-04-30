import { FileText } from 'lucide-react';

import { EmptyContainer } from '@/components/empty-container';
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import { useGetDashboard } from '@/orval/dashboard';
import { useGetWorksheets } from '@/orval/worksheets';

import { PageContent } from '../shared/page/page-content';
import { PageHeader } from '../shared/page/page-header';
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
      <PageHeader title="Home" />
      <PageContent>
        <div className="mb-4">
          <p className="mb-2 text-3xl font-semibold">Welcome!</p>
          <p className="text-muted-foreground font-light">Nice seeing you here 😎</p>
        </div>
        <HomeActionButtons />
        <div className="flex size-full flex-col">
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
                // TODO: Hardcode
                className="min-h-[calc(100vh-200px-344px)]"
                Icon={FileText}
                title="No SQL Worksheets Created Yet"
                description="Create your first worksheet to start querying data"
                // onCtaClick={() => {}}
                // ctaText="Create Worksheet"
              />
            )}
          </div>
        </div>
      </PageContent>
    </>
  );
}
