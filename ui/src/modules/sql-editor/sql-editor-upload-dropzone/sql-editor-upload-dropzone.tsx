import { useState } from 'react';

import { Check, LoaderCircle } from 'lucide-react';
import type { DropzoneProps, FileRejection } from 'react-dropzone';
import { useDropzone } from 'react-dropzone';
import { toast } from 'sonner';

import { cn } from '@/lib/utils';

import { DropzoneContent, MAX_FILE_SIZE } from './sql-editor-upload-dropzone-content.tsx';

const MAX_FILES = 1;
const ACCEPT: DropzoneProps['accept'] = {
  // 'application/vnd.ms-excel': ['.csv'],
  'text/csv': ['.csv'],
};

interface DropzoneWrapperProps {
  onUpload: (file: File) => void;
  isDisabled?: boolean;
}

export function TableDataUploadDropzone({ onUpload, isDisabled }: DropzoneWrapperProps) {
  const [isAnimating] = useState(false);
  const [isCompleted] = useState(false);

  const handleDrop = (acceptedFiles: File[], rejectedFiles: FileRejection[]) => {
    if (rejectedFiles.length) {
      rejectedFiles.forEach((file) => {
        toast.error(`Rejected file: ${file.file.name}`, {
          description: file.errors.map((error) => error.message).join('. ') + '.',
        });
      });
    }

    if (acceptedFiles.length) {
      onUpload(acceptedFiles[0]);
      // setIsAnimating(true);
      // animate();
    }
  };

  // This function is used to simulate the animation of the dropzone
  // function animate() {
  //   setIsAnimating(true);
  //   setIsCompleted(false);

  //   return new Promise((resolve) => {
  //     setTimeout(() => {
  //       setIsAnimating(false);
  //       setIsCompleted(true);
  //       setTimeout(() => {
  //         setIsCompleted(false);
  //         resolve(undefined);
  //       }, 2000);
  //     }, 2000);
  //   });
  // }

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    maxFiles: MAX_FILES,
    accept: ACCEPT,
    multiple: false,
    maxSize: MAX_FILE_SIZE,
    disabled: isDisabled ?? isAnimating,
    onDrop: handleDrop,
  });

  return (
    <div
      {...getRootProps()}
      className={cn(
        'group hover:bg-muted/75 relative w-full cursor-pointer rounded-md border-1 border-dashed px-6 py-8 text-center transition',
        'ring-offset-background focus-visible:ring-ring focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:outline-none',
        isDragActive && 'border-muted-foreground',
        isDisabled && 'pointer-events-none opacity-60',
        (isAnimating || isCompleted) && 'cursor-auto hover:bg-inherit',
      )}
    >
      <input {...getInputProps()} />
      {isAnimating ? (
        <div className="flex flex-col items-center justify-center gap-4 sm:px-5">
          <div className="rounded-full border border-dashed p-3">
            <LoaderCircle className="size-7 animate-spin" aria-hidden="true" />
          </div>
        </div>
      ) : isCompleted ? (
        <div className="flex flex-col items-center justify-center gap-4 sm:px-5">
          <div className="rounded-full border border-dashed p-3">
            <Check className="size-7 text-green-500" aria-hidden="true" />
          </div>
        </div>
      ) : (
        <DropzoneContent isDragActive={isDragActive} />
      )}
    </div>
  );
}
