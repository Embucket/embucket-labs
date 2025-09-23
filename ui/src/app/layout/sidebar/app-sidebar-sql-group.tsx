import { useQueryClient } from '@tanstack/react-query';
import { useLocation, useNavigate } from '@tanstack/react-router';
import { Activity, SquareTerminal } from 'lucide-react';

import { useSidebar } from '@/components/ui/sidebar';
import { useEditorSettingsStore } from '@/modules/editor/editor-settings-store';
import { getGetWorksheetsQueryKey, useCreateWorksheet, useGetWorksheets } from '@/orval/worksheets';

import { AppSidebarGroup } from './app-sidebar-group';

export function AppSidebarSqlGroup() {
  const { open } = useSidebar();
  const { data: { items: worksheets } = {}, isFetching: isFetchingWorksheets } = useGetWorksheets();

  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const { pathname } = useLocation();

  const addTab = useEditorSettingsStore((state) => state.addTab);

  const { mutate, isPending } = useCreateWorksheet({
    mutation: {
      onSuccess: (worksheet) => {
        queryClient.invalidateQueries({
          queryKey: getGetWorksheetsQueryKey(),
        });
        addTab(worksheet);
        navigate({
          to: '/sql-editor/$worksheetId',
          params: {
            worksheetId: worksheet.id.toString(),
          },
        });
      },
    },
  });

  const handleCreateWorksheet = () => {
    if (!worksheets?.length) {
      mutate({
        data: {
          name: '',
          content: '',
        },
      });
      return;
    }
    addTab(worksheets[0]);
    navigate({
      to: '/sql-editor/$worksheetId',
      params: {
        worksheetId: worksheets[0]?.id.toString(),
      },
    });
  };

  return (
    <AppSidebarGroup
      items={[
        {
          name: 'SQL',
          linkProps: {
            to: '/sql-editor/$worksheetId',
            params: {
              worksheetId: worksheets?.[0]?.id.toString(),
            },
          },
          Icon: SquareTerminal,
          disabled: isFetchingWorksheets || isPending,
          onClick: handleCreateWorksheet,
          isActive: pathname.includes('/sql-editor'),
        },
        {
          name: 'Queries',
          linkProps: {
            to: '/queries',
          },
          Icon: Activity,
          isActive: pathname.includes('/queries'),
        },
      ]}
      open={open}
    />
  );
}
