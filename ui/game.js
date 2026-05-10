const lightEventsDiv = document.getElementById("lightEvents");
const darkEventsDiv = document.getElementById("darkEvents");
const unitStates = {};

function addEvent(targetDiv, text) {
  const div = document.createElement("div");
  div.className = "event";
  div.textContent = text;
  targetDiv.prepend(div);
}

function handleIncomingEvent(eventData, targetDiv) {
  addEvent(targetDiv, eventData);

  try {
    const parsed = JSON.parse(eventData);

    if (parsed.topic === "game.events.unit") {
      updateUnitOnMap(parsed.value);
    }
  } catch (error) {
    console.error("Failed to parse event", error);
  }
}

const lightSource = new EventSource("http://localhost:8080/events?side=light");

lightSource.onmessage = (event) => {
  handleIncomingEvent(event.data, lightEventsDiv);
};

lightSource.onerror = () => {
  addEvent(lightEventsDiv, "Light SSE connection error");
};

const darkSource = new EventSource("http://localhost:8080/events?side=dark");

darkSource.onmessage = (event) => {
  handleIncomingEvent(event.data, darkEventsDiv);
};

darkSource.onerror = () => {
  addEvent(darkEventsDiv, "Dark SSE connection error");
};
eventSource.onerror = () => {
  addEvent(lightEventsDiv, "SSE connection error");
  addEvent(darkEventsDiv, "SSE connection error");
};

async function sendOrder(side) {
  const unitId = document.getElementById(`${side}UnitId`).value;
  const orderType = document.getElementById(`${side}OrderType`).value;
  const payloadRaw = document.getElementById(`${side}Payload`).value;

  const body = {
    playerId: side,
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

  const targetDiv = side === "light" ? lightEventsDiv : darkEventsDiv;
  addEvent(targetDiv, `ORDER RESPONSE: ${response.status}`);
}
function updateUnitOnMap(unit) {
  unitStates[unit.unitId] = unit;

  document.querySelectorAll(".units").forEach(container => {
    container.innerHTML = "";
  });

  Object.values(unitStates).forEach(u => {
    const regionEl = document.getElementById(`region-${u.region}`);

    if (!regionEl) {
      return;
    }

    const unitsDiv = regionEl.querySelector(".units");

    const unitEl = document.createElement("div");
    unitEl.className = "unit";
    unitEl.textContent = `${u.unitId} (${u.status})`;

    unitsDiv.appendChild(unitEl);
  });
}
