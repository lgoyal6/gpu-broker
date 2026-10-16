// Run one bounded CPU simulation away from the page thread. Terminating this
// worker is the cancellation path because the Go policy loop is synchronous.
importScripts("./wasm_exec.js");

const ready = (async () => {
  const go = new Go();
  const response = await fetch("./gpubroker.wasm");
  if (!response.ok) throw new Error("Go simulator binary unavailable");
  const {instance} = await WebAssembly.instantiateStreaming(response, go.importObject);
  void go.run(instance);
  await new Promise(resolve => setTimeout(resolve, 0));
  if (typeof globalThis.gpuBrokerRun !== "function") throw new Error("Go simulator did not initialize");
})();

self.onmessage = async event => {
  self.postMessage({type:"started"});
  try {
    await ready;
    const response = await fetch("./carbon-fixture.json");
    if (!response.ok) throw new Error("Historical carbon fixture unavailable");
    const fixture = await response.text();
    if (fixture.length > 96 * 1024) throw new Error("Historical carbon fixture exceeds the demo size limit");
    const request = {...event.data, carbon_fixture:JSON.parse(fixture)};
    const payload = JSON.parse(globalThis.gpuBrokerRun(JSON.stringify(request)));
    self.postMessage({type:"result",payload});
  } catch (error) {
    self.postMessage({type:"error",message:error instanceof Error ? error.message : "Browser simulation failed"});
  }
};
