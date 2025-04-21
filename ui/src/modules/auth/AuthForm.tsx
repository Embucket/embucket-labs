import { EmbucketLogo, EmbucketLogoText } from '@/app/layout/sidebar/logo';
import { Card, CardContent, CardHeader } from '@/components/ui/card';

import { AuthFormGoogleButton } from './AuthFormGoogleButton';

export const AuthForm = () => {
  return (
    <Card>
      <CardHeader>
        <div className="mx-auto pt-6">
          <div className="flex items-center gap-1">
            <EmbucketLogo />
            <EmbucketLogoText />
          </div>
        </div>
      </CardHeader>
      <CardContent>
        <AuthFormGoogleButton />
      </CardContent>
    </Card>
  );
};
