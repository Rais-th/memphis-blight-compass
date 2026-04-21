(function () {
  const nav = `
    <nav class="site">
      <a class="brand" href="/">BLIGHT COMPASS</a>
      <a class="link" href="/map" data-nav="map">Map</a>
      <a class="link" href="/top" data-nav="top">Top 50</a>
      <a class="link" href="/equity" data-nav="equity">Equity</a>
      <a class="link" href="/about" data-nav="about">About</a>
      <a class="link" href="/subscribe" data-nav="subscribe">Subscribe</a>
    </nav>`;
  document.addEventListener('DOMContentLoaded', function () {
    const host = document.getElementById('nav');
    if (host) host.outerHTML = nav;
    const active = document.body.getAttribute('data-page');
    if (active) {
      const el = document.querySelector(`[data-nav="${active}"]`);
      if (el) el.classList.add('active');
    }
  });
})();

window.fmtNum = (n) => n == null ? '—' : Number(n).toLocaleString();
window.fmtCurrency = (n) => n == null ? '—' : '$' + Number(n).toLocaleString();
window.fmtDate = (iso) => {
  if (!iso) return '—';
  try { return new Date(iso).toLocaleDateString(); } catch { return iso; }
};
window.scorePill = (score) => {
  const s = Number(score || 0);
  const cls = s >= 6 ? '' : (s >= 3 ? 'mid' : 'low');
  return `<span class="score-pill ${cls}">${s.toFixed(1)}</span>`;
};
