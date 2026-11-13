import type { ReactElement } from 'react';
import {
  Activity,
  BookOpen,
  Bot,
  Braces,
  Clock,
  Cloud,
  Crown,
  DatabaseBackup,
  FolderSync,
  Github,
  LayoutDashboard,
  Lock,
  Rocket,
  Server,
  SquarePlay,
  Users,
  Wrench,
  Zap,
} from 'lucide-react';

const props = { size: 16, strokeWidth: 1.75, 'aria-hidden': true } as const;

const ICONS: Record<string, ReactElement> = {
  zap: <Zap {...props} />,
  activity: <Activity {...props} />,
  clock: <Clock {...props} />,
  folder: <FolderSync {...props} />,
  braces: <Braces {...props} />,
  wrench: <Wrench {...props} />,
  cloud: <Cloud {...props} />,
  server: <Server {...props} />,
  rocket: <Rocket {...props} />,
  backup: <DatabaseBackup {...props} />,
  lock: <Lock {...props} />,
  console: <LayoutDashboard {...props} />,
  book: <BookOpen {...props} />,
  play: <SquarePlay {...props} />,
  bot: <Bot {...props} />,
  crown: <Crown {...props} />,
  users: <Users {...props} />,
  github: <Github {...props} />,
};

export function NavIcon({ name }: { name: string }) {
  return ICONS[name] ?? null;
}
