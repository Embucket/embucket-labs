import { useEffect, useRef } from 'react';

import { useNavigate, useParams } from '@tanstack/react-router';

import { useSqlEditorSettingsStore } from '@/modules/sql-editor/sql-editor-settings-store';
import type { Worksheet } from '@/orval/models';

interface UseSqlEditorTabsSyncProps {
  isFetchingWorksheets: boolean;
  worksheets?: Worksheet[];
}

export const useSqlEditorTabsSync = ({
  isFetchingWorksheets,
  worksheets,
}: UseSqlEditorTabsSyncProps) => {
  const { worksheetId } = useParams({ from: '/sql-editor/$worksheetId/' });
  const navigate = useNavigate();
  const tabs = useSqlEditorSettingsStore((state) => state.tabs);
  const setTabs = useSqlEditorSettingsStore((state) => state.setTabs); // Assuming a `setTabs` method exists

  const isInitialRender = useRef(true);

  useEffect(() => {
    if (!isFetchingWorksheets && worksheets?.length && isInitialRender.current) {
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

      isInitialRender.current = false;
    }
  }, [worksheetId, tabs, worksheets, isFetchingWorksheets, navigate, setTabs]);
};
