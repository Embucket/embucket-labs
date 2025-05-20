import { Check } from 'lucide-react';

interface Option {
  value: string;
  label: string;
}

interface SqlEditorContextDropdownSchemasProps {
  schemas: Option[];
  selectedSchema: string | null;
  onSelectSchema: (value: string) => void;
  isDisabled: boolean;
}

export const SqlEditorContextDropdownSchemas = ({
  schemas,
  selectedSchema,
  onSelectSchema,
  isDisabled,
}: SqlEditorContextDropdownSchemasProps) => {
  return (
    <div className="flex flex-col">
      <div className="max-h-60 overflow-y-auto p-1">
        {isDisabled && (
          <p className="text-muted-foreground p-2 text-sm">Select a database to see its schemas.</p>
        )}
        {!isDisabled &&
          schemas.map((schema) => (
            <button
              key={schema.value}
              onClick={() => onSelectSchema(schema.value)}
              className="hover:bg-accent flex w-full items-center rounded-md px-2 py-1.5 text-sm"
            >
              {schema.label}
              {selectedSchema === schema.value && <Check className="text-primary ml-auto size-4" />}
            </button>
          ))}
      </div>
    </div>
  );
};
