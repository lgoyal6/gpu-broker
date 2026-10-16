import {showSources, options, number, text, table, bars} from "./shared.js";
let data;
const metrics = {
  carbon_kg_estimate:{label:"Estimated carbon",unit:"kg CO₂",lower:true},
  cost_usd:{label:"Estimated cost",unit:"USD",lower:true},
  wait_hours_p99:{label:"p99 queue wait",unit:"hours",lower:true},
  jain_fairness:{label:"Jain fairness",unit:"/ 1",lower:false},
  gpu_utilization:{label:"GPU utilization",unit:"fraction",lower:false}
};
function update() {
  const workload = document.querySelector("#workload").value;
  const policy = document.querySelector("#policy").value, reference = document.querySelector("#reference").value;
  const metric = document.querySelector("#metric").value, spec = metrics[metric];
  const rows = data.results.filter(row => row.workload === workload);
  const selected = rows.find(row => row.policy === policy), base = rows.find(row => row.policy === reference);
  text("#chart-title",spec.label + " (" + spec.unit + ")");
  bars("#policy-bars",rows.map(row => ({label:row.policy,value:row[metric]})),spec.unit,metric === "jain_fairness" ? 4 : 2);
  const delta = selected[metric]-base[metric], percent = base[metric] ? Math.abs(delta/base[metric]*100) : null;
  text("#insight", policy === reference ? "Comparing the same recorded policy" : policy + ": " + (percent === null ? number(delta)+" difference" : number(percent,1)+"% "+(delta >= 0 ? "higher" : "lower")) + " " + spec.label.toLowerCase());
  text("#explanation","Selected: " + number(selected[metric],metric === "jain_fairness" ? 4 : 2) + " " + spec.unit + ". Reference " + reference + ": " + number(base[metric],metric === "jain_fairness" ? 4 : 2) + " " + spec.unit + ". " + (spec.lower ? "Lower" : "Higher") + " is preferable for this metric.");
  text("#outcome",selected.completed + " of " + selected.jobs + " jobs completed; " + selected.unfinished_at_horizon + " unfinished and " + selected.expired + " expired. Deadline misses: " + selected.deadline_misses + " / " + selected.deadline_jobs + ".");
  table("#results-table",rows.map(row => [row.policy,row.completed+" / "+row.jobs,row.unfinished_at_horizon,row.expired,row.deadline_misses+" / "+row.deadline_jobs,number(row.wait_hours_p99),number(row.cost_usd),number(row.carbon_kg_estimate),number(row.jain_fairness,4)]),policy);
  const list = document.querySelector("#metrics"); list.replaceChildren();
  for (const key of ["cost_usd","carbon_kg_estimate","wait_hours_p99","jain_fairness"]) {
    const setting=metrics[key], div=document.createElement("div"); div.className="metric";
    const label=document.createElement("span"); label.textContent=setting.label+" ("+setting.unit+")";
    const value=document.createElement("strong"); value.textContent=number(selected[key],key==="jain_fairness"?4:2);
    const difference=document.createElement("small");
    const raw=selected[key]-base[key]; difference.textContent=(raw>=0?"+":"")+number(raw,key==="jain_fairness"?4:2)+" vs "+reference;
    difference.className=(setting.lower ? raw<=0 : raw>=0)?"good":"bad";
    div.append(label,value,difference); list.append(div);
  }
  text("#version",selected.policy_version);
  text("#events",selected.delay_decisions+" delay decisions, "+selected.preemptions+" preemptions, "+selected.spot_interruptions+" simulated spot interruptions, and "+selected.fallback_decisions+" explicit fallback decisions.");
  text("#calls",selected.decide_calls+" native scheduling calls, "+selected.decisions+" recorded decisions. No runtime-latency benchmark is presented here.");
  bars("#tenant-bars",Object.entries(selected.tenant_mean_wait_hours).map(([label,value])=>({label,value})),"hours",2);
}
function reset() {
  for (const [id,value] of [["#workload","carbon-flex"],["#policy","lowest-carbon"],["#reference","lowest-cost"],["#metric","carbon_kg_estimate"]]) document.querySelector(id).value=value;
  update();
}
try {
  const response=await fetch("./data.json"); if (!response.ok) throw new Error("recording unavailable");
  data=await response.json();
  const unique = key => [...new Set(data.results.map(row=>row[key]))].sort();
  options("#workload",unique("workload"),"carbon-flex");
  options("#policy",unique("policy"),"lowest-carbon"); options("#reference",unique("policy"),"lowest-cost");
  options("#metric",Object.keys(metrics),"carbon_kg_estimate",key=>metrics[key].label);
  for (const id of ["#workload","#policy","#reference","#metric"]) document.querySelector(id).addEventListener("change",update);
  document.querySelector("#reset").addEventListener("click",reset);
  text("#dataset","Historical carbon dataset: "+data.carbon_dataset+". Dataset payload SHA-256: "+data.carbon_dataset_sha256+".");
  showSources(data,"lgoyal6/gpu-broker"); reset(); document.querySelector("#app").hidden=false;
} catch (error) {
  const box=document.querySelector("#error"); box.hidden=false;
  box.textContent="The recorded simulation could not load. Reload the page, or open data.json to inspect the retained results.";
}
