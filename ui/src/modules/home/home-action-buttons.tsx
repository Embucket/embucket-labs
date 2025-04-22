import { Database, Plus, Upload } from 'lucide-react';

export default function HomeActionButtons() {
  return (
    <div className="w-full p-4 pb-0">
      <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
        <button className="hover:bg-sidebar-secondary-accent bg-muted flex cursor-pointer items-center gap-3 rounded-md border p-6 text-white transition-colors">
          <Plus className="text-muted-foreground size-5" />
          <span className="text-sm font-medium">Create SQL Worksheet</span>
        </button>

        <button className="hover:bg-sidebar-secondary-accent bg-muted flex cursor-pointer items-center gap-3 rounded-md border p-6 text-white transition-colors">
          <Database className="text-muted-foreground size-5" />
          <span className="text-sm font-medium">Create Database</span>
        </button>

        <button className="hover:bg-sidebar-secondary-accent bg-muted flex cursor-pointer items-center gap-3 rounded-md border p-6 text-white transition-colors">
          <Upload className="text-muted-foreground size-5" />
          <span className="text-sm font-medium">Upload Local Files</span>
        </button>
      </div>
    </div>
  );
}
