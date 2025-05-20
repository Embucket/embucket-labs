import { Check } from 'lucide-react';

interface Option {
  value: string;
  label: string;
}

interface SqlEditorContextDropdownDatabasesProps {
  databases: Option[];
  selectedDatabase: string | null;
  onSelectDatabase: (value: string) => void;
}

export const SqlEditorContextDropdownDatabases = ({
  databases,
  selectedDatabase,
  onSelectDatabase,
}: SqlEditorContextDropdownDatabasesProps) => {
  return (
    <div className="flex flex-col border-r">
      <div className="max-h-60 overflow-y-auto p-1">
        {databases.map((db) => (
          <button
            key={db.value}
            onClick={() => onSelectDatabase(db.value)}
            className="hover:bg-accent flex w-full items-center rounded-md px-2 py-1.5 text-sm"
          >
            {db.label}
            {selectedDatabase === db.value && <Check className="text-primary ml-auto size-4" />}
          </button>
        ))}
      </div>
    </div>
  );
};
