import { Search } from 'lucide-react';

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
    <div>
      <div className="flex items-center justify-between border-b p-4">
        <h1 className="text-xl font-semibold">Home</h1>
        <InputRoot>
          <InputIcon>
            <Search />
          </InputIcon>
          <Input disabled placeholder="Search" />
        </InputRoot>
      </div>
      <div className="p-4">
        <p className="mb-2 text-3xl font-semibold">Welcome!</p>
        <p className="text-muted-foreground font-light">Fancy seeing you here 😎</p>
      </div>
      <HomeActionButtons />
      <div className="p-4">
        <div>
          <p className="mb-4 font-semibold">Overview</p>
          <HomeDashboardMetrics dashboardData={dashboardData} />
          {/* <div className="grid grid-cols-3 gap-4">
            <Card className="gap-2 bg-transparent p-6">
              <CardTitle className="font-sm">Databases</CardTitle>
              <CardContent className="p-0 text-2xl font-bold">
                {dashboardData.totalDatabases}
              </CardContent>
            </Card>
            <Card className="gap-2 bg-transparent p-6">
              <CardTitle className="font-sm">Schemas</CardTitle>
              <CardContent className="p-0 text-2xl font-bold">
                {dashboardData.totalSchemas}
              </CardContent>
            </Card>
            <Card className="gap-2 bg-transparent p-6">
              <CardTitle className="font-sm">Tables</CardTitle>
              <CardContent className="p-0 text-2xl font-bold">
                {dashboardData.totalTables}
              </CardContent>
            </Card>
          </div> */}
        </div>
        <div className="mt-4">
          <p className="mb-4 font-semibold">Worksheets</p>
          {worksheets?.length && (
            <ScrollArea className="size-full">
              <HomeWorksheetsTable worksheets={worksheets} isLoading={isLoading} />
              <ScrollBar orientation="horizontal" />
            </ScrollArea>
          )}
        </div>
      </div>
    </div>
  );
}
