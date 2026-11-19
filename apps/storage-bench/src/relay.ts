/**
 * The shared-tabs test's post office: a SharedWorker both tabs connect to, so
 * the benchmark tab can hand the leader tab a MessagePort (as the client's
 * tabs broker does for a follower) and the two can exchange a few commands.
 * A message goes to whoever said hello under the name in `to`, ports and all.
 */
const peers = new Map<string, MessagePort>();

(self as unknown as { onconnect: (ev: MessageEvent) => void }).onconnect = (ev) => {
  const port = ev.ports[0];
  port.onmessage = (msg: MessageEvent) => {
    const data = msg.data as { type: string; role?: string; to?: string };
    if (data.type === 'hello' && data.role) {
      peers.set(data.role, port);
      // Whoever was waiting for this peer hears of it.
      for (const [role, other] of peers) if (role !== data.role) other.postMessage({ type: 'peer', role: data.role });
      for (const role of peers.keys()) if (role !== data.role) port.postMessage({ type: 'peer', role });
      return;
    }
    if (data.to) peers.get(data.to)?.postMessage(data, [...msg.ports]);
  };
  port.start();
};
