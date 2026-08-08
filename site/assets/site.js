// Copy buttons on code blocks, and active-section tracking in the docs nav.
document.addEventListener('DOMContentLoaded', () => {
  document.querySelectorAll('.code').forEach((block) => {
    const pre = block.querySelector('pre');
    const btn = block.querySelector('.copy');
    if (!pre || !btn) return;
    btn.addEventListener('click', async () => {
      try {
        await navigator.clipboard.writeText(pre.innerText.trim());
        const was = btn.textContent;
        btn.textContent = 'Copied';
        setTimeout(() => { btn.textContent = was; }, 1400);
      } catch {
        btn.textContent = 'Press Ctrl+C';
      }
    });
  });

  const links = [...document.querySelectorAll('.docs-nav a')];
  const sections = links
    .map((a) => document.querySelector(a.getAttribute('href')))
    .filter(Boolean);
  if (!sections.length) return;

  // Highlight the section nearest the top of the viewport.
  const spy = new IntersectionObserver(
    (entries) => {
      entries.forEach((e) => {
        if (!e.isIntersecting) return;
        links.forEach((a) => a.classList.remove('active'));
        const link = links.find((a) => a.getAttribute('href') === '#' + e.target.id);
        if (link) link.classList.add('active');
      });
    },
    { rootMargin: '-90px 0px -70% 0px', threshold: 0 }
  );
  sections.forEach((s) => spy.observe(s));
});
