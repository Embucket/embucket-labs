import { createFileRoute } from '@tanstack/react-router';

import { AuthForm } from '@/modules/auth/AuthForm';

export const Route = createFileRoute('/')({
  component: Auth,
});

function Auth() {
  return (
    <div className="flex h-screen w-full items-center justify-center">
      <AuthForm />
    </div>
  );
}
