(function redirectLegacyDocsHost() {
  if (typeof window === 'undefined') {
    return;
  }

  if (window.location.hostname !== 'docs.letsfg.co') {
    return;
  }

  var pathname = window.location.pathname === '/' ? '' : window.location.pathname;
  var target = 'https://letsfg.co/developers/docs' + pathname + window.location.search + window.location.hash;
  window.location.replace(target);
})();