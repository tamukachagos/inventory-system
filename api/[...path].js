const app = require('../server');

module.exports = (req, res) => {
  if (req.url === '/api' || req.url === '/api/') {
    req.url = '/';
  } else if (req.url.startsWith('/api/')) {
    req.url = req.url.slice(4);
  }
  return app(req, res);
};
