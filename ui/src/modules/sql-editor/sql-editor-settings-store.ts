import { create } from 'zustand';
import { persist } from 'zustand/middleware';

import type { QueryRecord, Worksheet } from '@/orval/models';

import type { SelectedTree } from './sql-editor-left-panel/sql-editor-left-panel-trees/sql-editor-left-panel-trees-items';

interface SqlEditorSettingsStore {
  tabs: Worksheet[];
  addTab: (tab: Worksheet) => void;
  removeTab: (tabId: Worksheet['id']) => void;
  setTabs: (tabs: Worksheet[]) => void;

  queryRecord?: QueryRecord;
  setQueryRecord: (queryRecord: QueryRecord) => void;

  selectedTree?: SelectedTree;
  setSelectedTree: (selectedTree: SelectedTree) => void;
}

const initialState = {
  queryRecord: undefined,
  selectedTree: undefined,
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
      removeTab: (tabId: Worksheet['id']) => {
        const { tabs } = get();
        const updatedTabs = tabs.filter((t) => t.id !== tabId);
        set({ tabs: updatedTabs });
      },
      setTabs: (tabs: Worksheet[]) => {
        set({ tabs });
      },
      setQueryRecord: (queryRecord: QueryRecord) => {
        set({ queryRecord });
      },
      setSelectedTree: (selectedTree: SelectedTree) => {
        set({ selectedTree });
      },
    }),
    {
      name: 'sql-editor-settings',
    },
  ),
);
