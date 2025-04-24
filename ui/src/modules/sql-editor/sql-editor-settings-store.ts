import { create } from 'zustand';
import { persist } from 'zustand/middleware';

import type { Worksheet } from '@/orval/models';

interface SqlEditorSettingsStore {
  tabs: Worksheet[];
  addTab: (tab: Worksheet) => void;
  removeTab: (tab: Worksheet) => void;
}

const initialState = {
  tabs: [],
};

export const useSqlEditorSettingsStore = create<SqlEditorSettingsStore>()(
  persist(
    (set, get) => ({
      ...initialState,
      addTab: (tab: Worksheet) => {
        const { tabs } = get();
        const existingTab = tabs.find((t) => t.id === tab.id);
        if (!existingTab) {
          set({ tabs: [...tabs, tab] });
        }
      },
      removeTab: (tab: Worksheet) => {
        const { tabs } = get();
        const updatedTabs = tabs.filter((t) => t.id !== tab.id);
        set({ tabs: updatedTabs });
      },
    }),
    {
      name: 'sql-editor-settings',
    },
  ),
);
