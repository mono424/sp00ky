// TCP proxy adding a fixed one-way delay per direction (order preserved).
import net from 'net';
const [listen, target, oneWay] = [Number(process.argv[2]), Number(process.argv[3]), Number(process.argv[4])];
net.createServer((client) => {
  const upstream = net.connect(target, '127.0.0.1');
  const pipe = (from, to) => {
    let last = 0;
    from.on('data', (chunk) => {
      const at = Math.max(Date.now() + oneWay, last);
      last = at;
      setTimeout(() => to.writable && to.write(chunk), at - Date.now());
    });
    from.on('close', () => setTimeout(() => to.destroy(), oneWay + 5));
    from.on('error', () => to.destroy());
  };
  pipe(client, upstream);
  pipe(upstream, client);
}).listen(listen, () => console.log(`proxy :${listen} -> :${target} one-way ${oneWay}ms`));
