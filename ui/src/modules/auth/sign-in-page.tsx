import { EmbucketLogo } from '@/app/layout/sidebar/logo';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';

import { SignInForm } from './sign-in-form';

export function SignInPage() {
  return (
    <div className="flex min-h-screen flex-col items-center justify-center">
      <div className="flex min-w-[400px] flex-col gap-6">
        <div className="m-auto flex items-center gap-1">
          <EmbucketLogo />
        </div>
        <Card>
          <CardHeader>
            <CardTitle className="text-2xl font-semibold">Login</CardTitle>
            <CardDescription className="text-sm font-light">
              Enter your username below to login to your account
            </CardDescription>
          </CardHeader>
          <CardContent>
            <SignInForm />
          </CardContent>
        </Card>
        <p className="text-muted-foreground text-center text-xs font-light">
          2025 Embucket. All rights reserved. Privacy and Terms.
        </p>
      </div>
    </div>
  );
}
