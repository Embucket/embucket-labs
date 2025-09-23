import { useEffect, useRef } from 'react';

import { useGetNavigationTrees } from '@/orval/navigation-trees';

import { useEditorSettingsStore } from './editor-settings-store';

export const useSyncEditorSelectedTree = () => {
  const selectedTree = useEditorSettingsStore((state) => state.selectedTree);
  const setSelectedTree = useEditorSettingsStore((state) => state.setSelectedTree);

  const { data: { items: navigationTrees } = {}, isFetching: isFetchingNavigationTrees } =
    useGetNavigationTrees();

  const firstTime = useRef(true);

  useEffect(() => {
    if (isFetchingNavigationTrees || !navigationTrees?.length || !firstTime.current) {
      return;
    }

    const navigationTreeDatabase = navigationTrees.find(
      (database) => database.name === selectedTree?.databaseName,
    );

    const navigationTreeSchema = navigationTreeDatabase?.schemas.find(
      (schema) => schema.name === selectedTree?.schemaName,
    );

    const tablesOrViews = [
      ...(navigationTreeSchema?.tables ?? []),
      ...(navigationTreeSchema?.views ?? []),
    ];
    const navigationTreeTable = tablesOrViews.find(
      (table) => table.name === selectedTree?.tableName,
    );

    if (!navigationTreeTable) {
      setSelectedTree({
        databaseName: '',
        schemaName: '',
        tableName: '',
      });
      firstTime.current = false;
    }
  }, [navigationTrees, selectedTree, isFetchingNavigationTrees, setSelectedTree]);
};
