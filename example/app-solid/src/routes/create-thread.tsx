import { useNavigate } from '@solidjs/router';
import { CreateThreadDialog } from '../components/CreateThreadDialog';

export default function CreateThreadPage() {
  const navigate = useNavigate();
  const close = () => (window.history.length > 1 ? window.history.back() : navigate('/'));
  return <CreateThreadDialog onClose={close} />;
}
