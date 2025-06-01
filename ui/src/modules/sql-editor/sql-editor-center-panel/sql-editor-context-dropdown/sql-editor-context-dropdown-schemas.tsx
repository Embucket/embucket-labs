import { ScrollArea } from '@/components/ui/scroll-area';
import { SidebarMenu, SidebarMenuButton } from '@/components/ui/sidebar';

// TODO: DRY
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
}: SqlEditorContextDropdownSchemasProps) => {
  return (
    <ScrollArea className="max-h-60 pl-2">
      <SidebarMenu>
        {schemas.map((db) => (
          <SidebarMenuButton
            className="hover:bg-sidebar-secondary-accent data-[active=true]:bg-sidebar-secondary-accent!"
            key={db.value}
            onClick={() => onSelectSchema(db.value)}
            isActive={selectedSchema === db.value}
          >
            {db.label}
          </SidebarMenuButton>
        ))}
      </SidebarMenu>
    </ScrollArea>
  );
};
