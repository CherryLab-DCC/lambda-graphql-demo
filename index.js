const serverless = require('serverless-http');
const options = {
  request(req) {
    req.body = undefined;
  }
};
let handler;
module.exports.handler = async (event, context) => {
  context.callbackWaitsForEmptyEventLoop = false; // !important to reuse pool
  if (!handler) {
    handler = await import('./app.mjs').then(mod => serverless(mod.default, options));
  }
  const result = await handler(event, context);
  return result;
};
