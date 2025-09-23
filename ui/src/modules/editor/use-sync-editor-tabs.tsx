import { useCallback, useEffect, useRef } from 'react';

import { useNavigate, useParams } from '@tanstack/react-router';

import { useEditorSettingsStore } from '@/modules/editor/editor-settings-store';
import { useGetWorksheets } from '@/orval/worksheets';

export const useSyncEditorTabs = () => {
  const { worksheetId } = useParams({ from: '/sql-editor/$worksheetId/' });

  const { data: { items: worksheets } = {}, isFetching: isFetchingWorksheets } = useGetWorksheets();

  const navigate = useNavigate();
  const tabs = useEditorSettingsStore((state) => state.tabs);
  const setTabs = useEditorSettingsStore((state) => state.setTabs);

  const isInitialRender = useRef(true);

  const syncEditorTabs = useCallback(() => {
    if (worksheets?.length) {
      let updatedTabs = [...tabs];

      // Remove old tabs that are not in the current worksheets
      updatedTabs = updatedTabs.filter((tab) => worksheets.some((w) => w.id === tab.id));

      // Add the first worksheet to tabs if there are no items in it
      if (!updatedTabs.length) {
        navigate({
          to: '/sql-editor/$worksheetId',
          params: { worksheetId: worksheets[0].id.toString() },
        });
        updatedTabs.push(worksheets[0]);
      }

      // Ensure the active tab matches the current worksheetId
      if (!updatedTabs.find((t) => t.id.toString() === worksheetId)) {
        const firstWorksheetId = worksheets[0]?.id;
        if (firstWorksheetId) {
          navigate({
            to: '/sql-editor/$worksheetId',
            params: { worksheetId: firstWorksheetId.toString() },
          });
        }
      }

      // Update the tabs state in a single operation
      setTabs(updatedTabs);
    }
    isInitialRender.current = false;
  }, [navigate, setTabs, tabs, worksheetId, worksheets]);

  useEffect(() => {
    if (!isFetchingWorksheets && isInitialRender.current) {
      syncEditorTabs();
    }
  }, [syncEditorTabs, isFetchingWorksheets]);

  return { syncEditorTabs };
};
