import { Search } from 'lucide-react';

import { Input, InputIcon, InputRoot } from '@/components/ui/input';
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import { useGetDashboard } from '@/orval/dashboard';
import { useGetWorksheets } from '@/orval/worksheets';

import HomeActionButtons from './home-action-buttons';
import { HomeDashboardMetrics } from './home-dashboard-metrics';
import { HomeWorksheetsTable } from './home-worksheets-table';
import { HomeWorksheetsTableEmpty } from './home-worksheets-table-empty';

export function HomePage() {
  const { data: { items: worksheets } = {}, isLoading } = useGetWorksheets();
  const { data: dashboardData } = useGetDashboard();

  if (!dashboardData) {
    return null;
  }

  return (
    <div className="flex size-full flex-col">
      <div className="flex items-center justify-between border-b p-4">
        <h1 className="text-xl font-semibold">Home</h1>
        <InputRoot>
          <InputIcon>
            <Search />
          </InputIcon>
          <Input className="min-w-80" disabled placeholder="Search" />
        </InputRoot>
      </div>
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
            <ScrollArea>
              <HomeWorksheetsTable worksheets={worksheets} isLoading={isLoading} />
              <ScrollBar orientation="horizontal" />
            </ScrollArea>
          ) : (
            <HomeWorksheetsTableEmpty />
          )}
        </div>
      </div>
    </div>
  );
}
