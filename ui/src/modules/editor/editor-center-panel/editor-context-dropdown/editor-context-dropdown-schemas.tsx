import { ScrollArea } from '@/components/ui/scroll-area';
import { SidebarMenu, SidebarMenuButton } from '@/components/ui/sidebar';

// TODO: DRY
interface Option {
  value: string;
  label: string;
}

interface EditorContextDropdownSchemasProps {
  schemas: Option[];
  selectedSchema: string | null;
  onSelectSchema: (value: string) => void;
  isDisabled: boolean;
}

export const EditorContextDropdownSchemas = ({
  schemas,
  selectedSchema,
  onSelectSchema,
}: EditorContextDropdownSchemasProps) => {
  return (
    <ScrollArea className="max-h-60 pl-2">
      <SidebarMenu>
        {schemas.map((db) => (
          <SidebarMenuButton
            className="hover:bg-hover data-[active=true]:bg-hover!"
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
