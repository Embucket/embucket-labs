import { useEffect } from 'react';

import { useEditorSettingsStore } from '../../editor-settings-store';

// TODO: DRY
interface SelectOption {
  value: string;
  label: string;
}

interface UseSyncEditorContextProps {
  databasesOptions: SelectOption[];
  schemasOptions: SelectOption[];
}

export const useSyncEditorContext = ({
  databasesOptions,
  schemasOptions,
}: UseSyncEditorContextProps) => {
  const { selectedContext, setSelectedContext } = useEditorSettingsStore();
  const { database: selectedDatabase, schema: selectedSchema } = selectedContext;

  useEffect(() => {
    // 1. Do nothing until all data is loaded.
    // If we don't have options yet, just wait.
    if (!databasesOptions.length || !schemasOptions.length) {
      return;
    }

    // 2. Check if the current context (from localStorage) is still valid
    const isDatabaseValid = databasesOptions.some((opt) => opt.value === selectedDatabase);
    const isSchemaValid = schemasOptions.some((opt) => opt.value === selectedSchema);

    // 3. If everything is already valid, we don't need to do anything.
    if (isDatabaseValid && isSchemaValid) {
      return;
    }

    // 4. If one or both are invalid (or empty), set a new valid context.
    // This will use the existing value if it's valid, or fall back to the default.
    const newContext = {
      database: isDatabaseValid ? selectedDatabase : databasesOptions[0].value,
      schema: isSchemaValid ? selectedSchema : schemasOptions[0].value,
    };

    // 5. Only call setSelectedContext if the context actually needs to change.
    if (newContext.database !== selectedDatabase || newContext.schema !== selectedSchema) {
      setSelectedContext(newContext);
    }
  }, [selectedDatabase, selectedSchema, setSelectedContext, databasesOptions, schemasOptions]);
};
