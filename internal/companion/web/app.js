"use strict";

const tabs = Array.from(document.querySelectorAll(".demo-tab"));
const panel = document.querySelector("#demo-panel");
const metrics = document.querySelector("#demo-metrics");
const title = document.querySelector("#demo-view-title");
const rows = document.querySelector("#demo-rows");
const symbols = { done: "✓", score: "≈", wait: "⌛", neutral: "▤" };

function textNode(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  node.textContent = text;
  return node;
}

function showView(data, activeTab) {
  const view = data[activeTab.dataset.view];
  for (const tab of tabs) {
    const active = tab === activeTab;
    tab.setAttribute("aria-selected", String(active));
    tab.tabIndex = active ? 0 : -1;
  }
  panel.setAttribute("aria-labelledby", activeTab.id);
  title.textContent = view.title;
  metrics.replaceChildren(...view.metrics.map(([label, value]) => {
    const metric = textNode("div", "metric", "");
    metric.append(textNode("dt", "", label), textNode("dd", "", value));
    return metric;
  }));
  rows.replaceChildren(...view.rows.map(({ title, detail, status, tone }) => {
    const row = document.createElement("li");
    const icon = textNode("span", `row-icon row-icon-${tone}`, symbols[tone]);
    icon.setAttribute("aria-hidden", "true");
    const copy = textNode("span", "row-copy", "");
    copy.append(textNode("strong", "", title), textNode("span", "", detail));
    const badge = textNode("span", "row-status", status);
    badge.dataset.tone = tone;
    row.append(icon, copy, badge);
    return row;
  }));
}

async function startDemo() {
  try {
    const response = await fetch("./demo.json");
    if (!response.ok) throw new Error("Demo data unavailable");
    const data = await response.json();
    tabs.forEach((tab, index) => {
      tab.disabled = false;
      tab.addEventListener("click", () => showView(data, tab));
      tab.addEventListener("keydown", (event) => {
        let next;
        switch (event.key) {
          case "ArrowRight": next = (index + 1) % tabs.length; break;
          case "ArrowLeft": next = (index + tabs.length - 1) % tabs.length; break;
          case "Home": next = 0; break;
          case "End": next = tabs.length - 1; break;
          default: return;
        }
        event.preventDefault();
        showView(data, tabs[next]);
        tabs[next].focus();
      });
    });
  } catch {
    document.querySelector("#demo-fallback").hidden = false;
  }
}

const clipboard = navigator.clipboard;
if (clipboard && window.isSecureContext) {
  const feedback = document.querySelector("#copy-status");
  for (const button of document.querySelectorAll("[data-copy]")) {
    button.hidden = false;
    button.addEventListener("click", async () => {
      const command = document.getElementById(button.dataset.copy);
      try {
        await clipboard.writeText(command.textContent);
        feedback.textContent = "Команда скопирована";
        button.textContent = "Скопировано";
      } catch {
        feedback.textContent = "Выдели и скопируй команду вручную";
      }
    });
  }
}

startDemo();
