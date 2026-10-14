const url = process.argv[2];
const ws = new WebSocket(url, 'json');
const times = [];
let id = 0, t0 = 0;
ws.onopen = () => { t0 = performance.now(); ws.send(JSON.stringify({ id: ++id, method: 'ping' })); };
ws.onmessage = () => {
  times.push(performance.now() - t0);
  if (times.length >= 20) { times.sort((a, b) => a - b); console.log(url, 'ping RTT ms min', times[0].toFixed(0), 'median', times[10].toFixed(0), 'max', times[19].toFixed(0)); ws.close(); return; }
  setTimeout(() => { t0 = performance.now(); ws.send(JSON.stringify({ id: ++id, method: 'ping' })); }, 100);
};
ws.onerror = (e) => { console.log('error', e.message ?? e); process.exit(1); };
