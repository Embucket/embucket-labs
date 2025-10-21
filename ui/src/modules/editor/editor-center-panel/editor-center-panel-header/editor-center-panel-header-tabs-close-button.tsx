import { useNavigate, useParams } from '@tanstack/react-router';
import { X } from 'lucide-react';

import { Button } from '@/components/ui/button';
import type { Worksheet } from '@/orval/models';

import { useEditorSettingsStore } from '../../editor-settings-store';

interface EditorCenterPanelHeaderTabsCloseButtonProps {
  tab: Worksheet;
  disabled: boolean;
}

export function EditorCenterPanelHeaderTabsCloseButton({
  disabled,
  tab,
}: EditorCenterPanelHeaderTabsCloseButtonProps) {
  const { worksheetId } = useParams({ from: '/sql-editor/$worksheetId/' });

  const navigate = useNavigate();
  const tabs = useEditorSettingsStore((state) => state.tabs);
  const removeTab = useEditorSettingsStore((state) => state.removeTab);

  const handleTabClose = (e: React.MouseEvent, tab: Worksheet) => {
    e.stopPropagation();
    e.preventDefault();
    const tabIndex = tabs.findIndex((t) => t.id === tab.id);
    removeTab(tab.id);

    if (tabs.length === 1) {
      navigate({ to: '/home' });
      return;
    }
    if (tabIndex === 0 && tabs.length > 1) {
      // If the first tab is closed, navigate to the next tab
      navigate({
        to: '/sql-editor/$worksheetId',
        params: { worksheetId: tabs[1]?.id.toString() },
      });
    } else if (tabs.length > 1 && tab.id.toString() === worksheetId) {
      // Otherwise, navigate to the first tab
      navigate({
        to: '/sql-editor/$worksheetId',
        params: { worksheetId: tabs[0]?.id.toString() },
      });
    }
  };

  return (
    <Button
      disabled={disabled}
      variant="ghost"
      onClick={(e) => handleTabClose(e, tab)}
      className="ml-auto size-6 transition-none"
    >
      <X />
    </Button>
  );
}
