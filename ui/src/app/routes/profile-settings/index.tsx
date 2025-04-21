// TODO: Remove this line once submit function is implemented
/* eslint-disable @typescript-eslint/no-empty-function */
import { createFileRoute } from '@tanstack/react-router';

import { ProfileSettingsForm } from '@/modules/profile-settings/profile-settings-form';

export const Route = createFileRoute('/profile-settings/')({
  component: ProfileSettings,
});

function ProfileSettings() {
  return <ProfileSettingsForm isLoading={false} onSubmit={() => {}} />;
}
