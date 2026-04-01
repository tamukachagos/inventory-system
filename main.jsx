import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import Root from './Root.jsx'

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <Root />
  </StrictMode>,
)

const splash = document.getElementById('app-splash');
if (splash) {
  requestAnimationFrame(() => {
    splash.classList.add('fade-out');
    window.setTimeout(() => splash.remove(), 180);
  });
}

if ('serviceWorker' in navigator) {
  window.addEventListener('load', () => {
    navigator.serviceWorker.register('/sw.js').catch(() => {});
  });
}
