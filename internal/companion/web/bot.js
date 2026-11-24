const botLink = document.querySelector("[data-bot-target]");
const destination = new URL(botLink.href);
if (new URLSearchParams(location.search).get("source") === "guide_connect") {
  destination.searchParams.set("start", "guide_connect");
}
botLink.href = destination.href;
location.replace(destination.href);
