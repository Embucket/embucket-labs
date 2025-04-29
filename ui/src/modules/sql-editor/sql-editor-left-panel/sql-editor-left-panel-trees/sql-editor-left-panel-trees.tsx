import { useState } from 'react';

import type { SelectedTree } from '@/modules/shared/trees/trees-items';
import {
  TreesDatabases,
  TreesLayout,
  TreesSchemas,
  TreesTables,
} from '@/modules/shared/trees/trees-items';
import { useGetNavigationTrees } from '@/orval/navigation-trees';

import { useSqlEditorPanelsState } from '../../sql-editor-panels-state-provider';
import { useSqlEditorSettingsStore } from '../../sql-editor-settings-store';
import { SqlEditorUploadDialog } from '../../sql-editor-upload-dropzone/sql-editor-upload-dialog';
import { SqlEditorLeftPanelTreesTableDropdown } from './sql-editor-left-panel-trees-table-dropdown';

export function SqlEditorLeftPanelTrees() {
  const [isLoadDataDialogOpened, setIsLoadDataDialogOpened] = useState(false);

  const { data: { items: navigationTrees } = {}, isFetching: isFetchingNavigationTrees } =
    useGetNavigationTrees();

  const selectedTree = useSqlEditorSettingsStore((state) => state.selectedTree);
  const setSelectedTree = useSqlEditorSettingsStore((state) => state.setSelectedTree);
  const { isLeftBottomPanelExpanded, leftBottomRef } = useSqlEditorPanelsState();

  const handleTableClick = (tree: SelectedTree) => {
    if (!isLeftBottomPanelExpanded) {
      leftBottomRef.current?.resize(20);
    }
    setSelectedTree(tree);
  };

  return (
    <>
      <TreesLayout>
        <TreesDatabases
          databases={navigationTrees}
          isFetchingDatabases={isFetchingNavigationTrees}
          defaultOpen={(db) =>
            db.schemas.some((schema) =>
              schema.tables.some((table) => table.name === selectedTree?.tableName),
            )
          }
        >
          {(database) => (
            <TreesSchemas
              schemas={database.schemas}
              defaultOpen={(schema) =>
                schema.tables.some((table) => table.name === selectedTree?.tableName)
              }
            >
              {(schema) => (
                <TreesTables
                  tables={schema.tables}
                  database={database}
                  schema={schema}
                  onClick={(table) =>
                    handleTableClick({
                      databaseName: database.name,
                      schemaName: schema.name,
                      tableName: table.name,
                    })
                  }
                  isActive={(table) =>
                    selectedTree?.tableName === table.name &&
                    selectedTree.schemaName === schema.name &&
                    selectedTree.databaseName === database.name
                  }
                  defaultOpen={schema.tables.some(
                    (table) => table.name === selectedTree?.tableName,
                  )}
                  renderDropdownMenu={(tree, hovered) => (
                    <SqlEditorLeftPanelTreesTableDropdown
                      onLoadDataClick={() => {
                        setIsLoadDataDialogOpened(true);
                        setSelectedTree(tree);
                      }}
                      hovered={hovered}
                    />
                  )}
                />
              )}
            </TreesSchemas>
          )}
        </TreesDatabases>
      </TreesLayout>
      {selectedTree && (
        <SqlEditorUploadDialog
          opened={isLoadDataDialogOpened}
          onSetOpened={setIsLoadDataDialogOpened}
          selectedTree={selectedTree}
        />
      )}
    </>
  );
}
