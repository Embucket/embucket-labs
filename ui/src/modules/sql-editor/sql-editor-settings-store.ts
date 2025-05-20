import { create } from 'zustand';
import { persist } from 'zustand/middleware';

import type { QueryRecord, Worksheet } from '@/orval/models';

import type { SelectedTree } from '../shared/trees/trees-items';

export type LeftPanelTab = 'databases' | 'worksheets';

export interface SqlEditorContext {
  databaseName: string;
  schema: string;
}

interface SqlEditorSettingsStore {
  selectedContext: SqlEditorContext;
  setSelectedContext: (context: SqlEditorContext) => void;

  selectedLeftPanelTab: LeftPanelTab;
  setSelectedLeftPanelTab: (tab: LeftPanelTab) => void;

  tabs: Worksheet[];
  addTab: (tab: Worksheet) => void;
  removeTab: (tabId: Worksheet['id']) => void;
  setTabs: (tabs: Worksheet[]) => void;

  selectedQueryRecords: Record<Worksheet['id'], QueryRecord>;
  setSelectedQueryRecord: (worksheetId: Worksheet['id'], queryRecord: QueryRecord) => void;
  getSelectedQueryRecord: (worksheetId: Worksheet['id']) => QueryRecord | undefined;

  selectedTree?: SelectedTree;
  setSelectedTree: (selectedTree: SelectedTree) => void;
}

const initialState = {
  selectedContext: {
    databaseName: '',
    schema: '',
  },
  selectedLeftPanelTab: 'databases' as LeftPanelTab,
  selectedQueryRecords: {},
  selectedTree: undefined,
  tabs: [],
};

export const useSqlEditorSettingsStore = create<SqlEditorSettingsStore>()(
  persist(
    (set, get) => ({
      ...initialState,
      setSelectedContext: (context: SqlEditorContext) => {
        set({ selectedContext: context });
      },
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
      setSelectedQueryRecord: (worksheetId: Worksheet['id'], queryRecord: QueryRecord) => {
        const { selectedQueryRecords } = get();
        set({
          selectedQueryRecords: {
            ...selectedQueryRecords,
            [worksheetId]: queryRecord,
          },
        });
      },
      getSelectedQueryRecord: (worksheetId: Worksheet['id']) => {
        const { selectedQueryRecords } = get();
        return selectedQueryRecords[worksheetId];
      },
      setSelectedTree: (selectedTree: SelectedTree) => {
        set({ selectedTree });
      },

      setSelectedLeftPanelTab: (selectedLeftPanelTab: LeftPanelTab) => {
        set({ selectedLeftPanelTab });
      },
    }),
    {
      name: 'sql-editor-settings',
    },
  ),
);
