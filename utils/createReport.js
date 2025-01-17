   const { faker } = require("@faker-js/faker");
const { v4: uuidv4 } = require("uuid");

function createReport() {
  let reportData = [];
  let reportTypes = ["disposed", "missed", "autoFail", "autoDrop"];
  let disposeTypes = ["callback", "dnc", "etx"];
  let agentNames = ["Vicky", "Suman", "Lokesh", "Animesh", "Naman", "Mithul", "Saloni", "Parv", "Rebel", "Dhruv"];
  let disposeNames = ["External", "Transfer", "Do Not Call", "Follow Up"];

  for (let i = 0; i < 1000; i++) {
    let datetime = faker.date.between({ from: "2020-01-01T00:00:00Z", to: "2022-01-01T00:00:00Z" });
    let reportType = reportTypes[Math.floor(Math.random() * reportTypes.length)];
    let disposeType = disposeTypes[Math.floor(Math.random() * disposeTypes.length)];
    let disposeName = disposeNames[Math.floor(Math.random() * disposeNames.length)];
    let agentName = agentNames[Math.floor(Math.random() * agentNames.length)];
    let campaignName = `Campaign_${Math.floor(Math.random() * 10)}`;
    let processName = `Process_${Math.floor(Math.random() * 10)}`;
    let leadsetId = Math.floor(Math.random() * 199) + 1;
    let referenceUuid = uuidv4();
    let customerUuid = uuidv4();
    let hold = Math.floor(Math.random() * 479) + 1;
    let mute = Math.floor(Math.random() * 479) + 1;
    let ringing = Math.floor(Math.random() * 60) + 1;
    let transfer = Math.floor(Math.random() * 300) + 1;
    let conference = Math.floor(Math.random() * 3080) + 1;
    let callTime = Math.floor(Math.random() * 3000) + 1;
    let callDuration = hold + mute + ringing + transfer + conference + callTime;
    let disposeTime = Math.floor(Math.random() * 30) + 1;

    let calltype = reportType;

    if (calltype == "missed") {
      disposeName = "Agent not found";
      hold = 0;
      mute = 0;
      transfer = 0;
      conference = 0;
    } else if (calltype == "autoFail" || calltype == "autoDrop") {
      disposeName = faker.helpers.arrayElement([
        "busy",
        "decline",
        "does not exist",
        "not acceptable",
      ]);
      hold = 0;
      mute = 0;
      transfer = 0;
      conference = 0;
    } else {
      disposeName = faker.helpers.arrayElement([
        "follow up",
        "do not call",
        "external transfer",
      ]);
      if (disposeName == "follow up") {
        disposeType = "callback";
      } else if (disposeName == "do not call") {
        disposeType = "dnc";
      } else {
        disposeType = "etx";
      }
    }

    reportData.push({
      datetime,
      reportType,
      disposeType,
      disposeName,
      callDuration,
      agentName,
      campaignName,
      processName,
      leadsetId,
      referenceUuid,
      customerUuid,
      hold,
      mute,
      ringing,
      transfer,
      conference,
      callTime,
      disposeTime,
    });
  }

  return reportData;
}

module.exports = { createReport };
