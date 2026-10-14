import type { JSX } from 'solid-js';
import { createContext, useContext, createSignal, Show, onCleanup } from 'solid-js';
import * as jose from 'jose';
import type { schema } from '../schema.gen';
import { type GetTable, type TableModel, useQuery, useDb } from '@spooky-sync/client-solid';

type User = TableModel<GetTable<typeof schema, 'user'>>;

interface AuthContextType {
  userId: () => string | null;
  user: () => User | null;
  isLoading: () => boolean;
  signIn: (username: string, password: string) => Promise<void>;
  signUp: (username: string, password: string) => Promise<void>;
  signOut: () => Promise<void>;
}

const AuthContext = createContext<AuthContextType>();

export function AuthProvider(props: { children: JSX.Element }) {
  const db = useDb<typeof schema>();
  const [userId, setUserId] = createSignal<string | null>(null);
  const [isInitializing, setIsInitializing] = createSignal(true);
  onCleanup(
    db.auth.subscribe((uid) => {
      setUserId(uid);
      setIsInitializing(false);
    })
  );

  // The signed-in user's row, live: a rename or new avatar shows everywhere.
  // No query while signed out.
  const userQuery = useQuery(() => {
    const id = userId();
    return id ? db.query('user').where({ id }).one().build() : null;
  });
  const user = () => userQuery.data() ?? null;

  const signIn = async (username: string, password: string) => {
    await db.auth.signIn('account', { username, password });
  };

  const signUp = async (username: string, password: string) => {
    // Generate the user's Ed25519 share-link keypair before signup so both
    // halves land on the user row atomically. WebCrypto Ed25519 isn't
    // universally supported yet; if generation fails we still want signup
    // to succeed (the SIGNUP query treats the keypair fields as optional)
    // and let the user backfill a key later.
    let share_pubkey: string | null = null;
    let share_privkey: string | null = null;
    try {
      const { publicKey, privateKey } = await jose.generateKeyPair('Ed25519', { extractable: true });
      share_pubkey  = await jose.exportSPKI(publicKey);
      share_privkey = await jose.exportPKCS8(privateKey);
    } catch (e) {
      console.warn('[auth] Ed25519 keypair generation failed; signing up without one.', e);
    }
    // The codegen types the keypair fields as required strings; null is
    // accepted by the SIGNUP block at runtime (the field is option<string>),
    // but TypeScript doesn't know that.
    await db.auth.signUp('account', { username, password, share_pubkey, share_privkey } as any);
  };

  const signOut = async () => {
    await db.auth.signOut();
  };

  const authValue: AuthContextType = {
    userId,
    user,
    // Only until the session is known. The user's own row is not waited for:
    // ids come from `userId()`, and the name and avatar fill in when the row
    // arrives, so a slow server never blanks the whole app.
    isLoading: isInitializing,
    signIn,
    signUp,
    signOut,
  };

  return (
    <AuthContext.Provider value={authValue}>
      <Show
        when={!authValue.isLoading()}
        fallback={
          <div class="min-h-screen flex items-center justify-center">
            <div class="text-lg">Loading...</div>
          </div>
        }
      >
        {props.children}
      </Show>
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
}
