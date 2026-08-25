// Live log tail over Server-Sent Events.
//
// SSE rather than a websocket because logs only travel one way, EventSource
// reconnects on its own, and it survives proxies that mangle upgrades. About
// thirty lines, no dependencies, and nothing to build.
(function () {
  const box = document.getElementById("logs");
  if (!box || !box.dataset.stream) return;

  const status = document.getElementById("stream-status");
  const source = new EventSource(box.dataset.stream + "?after=" + (box.dataset.after || 0));

  source.addEventListener("line", function (event) {
    const entry = JSON.parse(event.data);
    // Stay pinned to the bottom only if the reader already was; yanking the
    // view while somebody is reading back through a traceback is maddening.
    const pinned = box.scrollHeight - box.scrollTop - box.clientHeight < 40;
    const row = document.createElement("div");
    row.className = entry.stream;
    row.innerHTML =
      '<span class="t">' + entry.at + "</span> " +
      entry.line.replace(/&/g, "&amp;").replace(/</g, "&lt;");
    box.appendChild(row);
    if (pinned) box.scrollTop = box.scrollHeight;
  });

  source.addEventListener("done", function (event) {
    if (status) status.textContent = "job " + JSON.parse(event.data).state.toLowerCase();
    source.close();
    setTimeout(function () { window.location.reload(); }, 1500);
  });

  source.onerror = function () {
    if (status) status.textContent = "reconnecting…";
  };
})();
