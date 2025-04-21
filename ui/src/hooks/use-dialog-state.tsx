import { useState } from 'react';

export const useDialogState = () => {
  const [dialogOpened, setDialogOpened] = useState<boolean>(false);

  const openDialog = () => {
    setDialogOpened(true);
  };

  const closeDialog = () => {
    setDialogOpened(false);
  };

  return {
    dialogOpened,
    setDialogOpened,
    openDialog,
    closeDialog,
  };
};
