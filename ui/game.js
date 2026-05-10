const eventsDiv = document.getElementById("events");

function addEvent(text) {
  const div = document.createElement("div");
  div.className = "event";

  div.textContent = text;

  eventsDiv.prepend(div);
}

const eventSource = new EventSource("http://localhost:8080/events");

eventSource.onmessage = (event) => {
  addEvent(event.data);
};

eventSource.onerror = () => {
  addEvent("SSE connection error");
};

async function sendOrder() {
  const playerId = document.getElementById("playerId").value;
  const unitId = document.getElementById("unitId").value;
  const orderType = document.getElementById("orderType").value;
  const payloadRaw = document.getElementById("payload").value;

  const body = {
    playerId,
    unitId,
    orderType,
    payload: payloadRaw,
    turn: 2,
    timestamp: Date.now()
  };

  const response = await fetch("http://localhost:8080/order", {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(body)
  });

  addEvent("ORDER RESPONSE: " + response.status);
}
