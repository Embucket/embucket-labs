import { UploadCloud } from 'lucide-react';

import { Button } from '@/components/ui/button';

export const MAX_FILE_SIZE = 100 * 1024 * 1024; // 100 MB

interface TableDataUploadDialogDropzoneContentProps {
  isDragActive: boolean;
}

export function TableDataUploadDialogDropzoneContent({
  isDragActive,
}: TableDataUploadDialogDropzoneContentProps) {
  return (
    <div className="flex flex-col items-center justify-center">
      <UploadCloud strokeWidth={1.5} className="text-muted-foreground mb-4 size-12 font-thin" />
      {isDragActive ? (
        <p className="font-medium">{1}</p>
      ) : (
        <>
          <p className="font-medium">Drag and drop files or</p>
          <Button className="mt-2 mb-4">Browse</Button>
          <p className="text-muted-foreground mb-px text-sm font-light">File size limit: 250MB </p>
          <p className="text-muted-foreground text-sm font-light">Supported formats: CSV</p>
        </>
      )}
    </div>
  );
}
