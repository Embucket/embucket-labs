import { useState } from 'react';

import { TableDataUploadDialog } from '@/modules/shared/table-data-upload-dialog/table-data-upload-dialog';
import type { SelectedTree } from '@/modules/shared/trees/trees-items';
import {
  TreesDatabases,
  TreesLayout,
  TreesSchemas,
  TreesTables,
} from '@/modules/shared/trees/trees-items';
import { useGetNavigationTrees } from '@/orval/navigation-trees';

import { useEditorPanelsState } from '../../editor-panels-state-provider';
import { useEditorSettingsStore } from '../../editor-settings-store';
import { EditorLeftPanelTreesTableDropdown } from './editor-left-panel-trees-table-dropdown';

export function EditorLeftPanelTrees() {
  const [isLoadDataDialogOpened, setIsLoadDataDialogOpened] = useState(false);

  const {
    data: { items: navigationTrees } = {},
    isFetching: isFetchingNavigationTrees,
    isLoading: isLoadingNavigationTrees,
  } = useGetNavigationTrees();

  const selectedTree = useEditorSettingsStore((state) => state.selectedTree);
  const setSelectedTree = useEditorSettingsStore((state) => state.setSelectedTree);
  const { isLeftBottomPanelExpanded, leftBottomRef } = useEditorPanelsState();

  const handleTableClick = (tree: SelectedTree) => {
    if (!isLeftBottomPanelExpanded) {
      leftBottomRef.current?.resize(20);
    }
    setSelectedTree(tree);
  };

  return (
    <>
      <TreesLayout scrollAreaClassName="h-[calc(100%-56px-2px)]">
        <TreesDatabases
          databases={navigationTrees}
          isFetchingDatabases={isFetchingNavigationTrees}
          isLoadingDatabases={isLoadingNavigationTrees}
          defaultOpen={(db) =>
            db.schemas.some((schema) =>
              [...schema.tables, ...schema.views].some(
                (table) => table.name === selectedTree?.tableName,
              ),
            ) && db.name === selectedTree?.databaseName
          }
        >
          {(database) => (
            <TreesSchemas
              database={database}
              schemas={database.schemas}
              defaultOpen={(schema) =>
                [...schema.tables, ...schema.views].some(
                  (table) => table.name === selectedTree?.tableName,
                ) && schema.name === selectedTree?.schemaName
              }
            >
              {(schema) => (
                <>
                  {!!schema.tables.length && (
                    <TreesTables
                      label="Tables"
                      tables={schema.tables}
                      database={database}
                      schema={schema}
                      onClick={(tree) =>
                        handleTableClick({
                          databaseName: tree.databaseName,
                          schemaName: tree.schemaName,
                          tableName: tree.tableName,
                        })
                      }
                      isActive={(table) =>
                        selectedTree?.tableName === table.name &&
                        selectedTree.schemaName === schema.name &&
                        selectedTree.databaseName === database.name
                      }
                      defaultOpen={true}
                      renderDropdownMenu={(tree, hovered) => (
                        <EditorLeftPanelTreesTableDropdown
                          onLoadDataClick={() => {
                            setIsLoadDataDialogOpened(true);
                            setSelectedTree(tree);
                          }}
                          hovered={hovered}
                        />
                      )}
                    />
                  )}
                  {/* TODO: DRY */}
                  {!!schema.views.length && (
                    <TreesTables
                      label="Views"
                      tables={schema.views}
                      database={database}
                      schema={schema}
                      onClick={(tree) =>
                        handleTableClick({
                          databaseName: tree.databaseName,
                          schemaName: tree.schemaName,
                          tableName: tree.tableName,
                        })
                      }
                      isActive={(table) =>
                        selectedTree?.tableName === table.name &&
                        selectedTree.schemaName === schema.name &&
                        selectedTree.databaseName === database.name
                      }
                      defaultOpen={true}
                      renderDropdownMenu={(tree, hovered) => (
                        <EditorLeftPanelTreesTableDropdown
                          onLoadDataClick={() => {
                            setIsLoadDataDialogOpened(true);
                            setSelectedTree(tree);
                          }}
                          hovered={hovered}
                        />
                      )}
                    />
                  )}
                </>
              )}
            </TreesSchemas>
          )}
        </TreesDatabases>
      </TreesLayout>
      {selectedTree && (
        <TableDataUploadDialog
          opened={isLoadDataDialogOpened}
          onSetOpened={setIsLoadDataDialogOpened}
          databaseName={selectedTree.databaseName}
          schemaName={selectedTree.schemaName}
          tableName={selectedTree.tableName}
        />
      )}
    </>
  );
}
