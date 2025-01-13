const { faker } = require("@faker-js/faker");
const { v4: uuidv4 } = require("uuid");

function createReport() {
  let reportData = [];
  let reportType = ["disposed", "missed", "autoFail", "autoDrop"];
  let disposeType = ["callback", "dnc", "etx"];
  let agentName = [
    "Vicky",
    "Suman",
    "Lokesh",
    "Animesh",
    "Naman",
    "Mithul",
    "Saloni",
    "Parv",
    "Rebel",
    "Dhruv",
  ];
  let disposeName = ["External", "Transfer", "Do Not Call", "Follow Up"];

  for (let i = 0; i < 1000; i++) {
    let dateTime = faker.date.between({
      from: "2020-01-01T00:00:00Z",
      to: "2022-01-01T00:00:00Z",
    });
    let randomReportType =
      reportType[Math.floor(Math.random() * reportType.length)];
    let randomDisposeType =
      disposeType[Math.floor(Math.random() * disposeType.length)];
    let randomDisposeName =
      disposeName[Math.floor(Math.random() * disposeName.length)];
    let randomAgentName =
      agentName[Math.floor(Math.random() * agentName.length)];
    let campaignName = `Campaign_${Math.floor(Math.random() * 10)}`;
    let processName = `Process_${Math.floor(Math.random() * 10)}`;
    let leadSetId = Math.floor(Math.random() * 199) + 1;
    let referenceUuid = uuidv4();
    let customerUuid = uuidv4();
    let holdTime = Math.floor(Math.random() * 479) + 1;
    let muteTime = Math.floor(Math.random() * 479) + 1;
    let ringingTime = Math.floor(Math.random() * 60) + 1;
    let transferTime = Math.floor(Math.random() * 300) + 1;
    let conferenceTime = Math.floor(Math.random() * 3080) + 1;
    let callTime = Math.floor(Math.random() * 3000) + 1;
    let duration =
      holdTime +
      muteTime +
      ringingTime +
      transferTime +
      conferenceTime +
      callTime;
    let disposeTime = Math.floor(Math.random() * 30) + 1;

    let calltype = randomReportType;

    if (calltype == "missed") {
      randomDisposeName = "Agent not found";
      holdTime = 0;
      muteTime = 0;
      transferTime = 0;
      conferenceTime = 0;
    } else if (calltype == "autoFail" || calltype == "autoDrop") {
      randomDisposeName = faker.helpers.arrayElement([
        "busy",
        "decline",
        "does not exist",
        "not acceptable",
      ]);
      holdTime = 0;
      muteTime = 0;
      transferTime = 0;
      conferenceTime = 0;
    } else {
      randomDisposeName = faker.helpers.arrayElement([
        "follow up",
        "do not call",
        "external transfer",
      ]);
      if (randomDisposeName == "follow up") {
        randomDisposeType = "callback";
      } else if (randomDisposeName == "do not call") {
        randomDisposeType = "dnc";
      } else {
        randomDisposeType = "etx";
      }
    }

    reportData.push({
      dateTime,
      randomReportType,
      randomDisposeType,
      randomDisposeName,
      duration,
      randomAgentName,
      campaignName,
      processName,
      leadSetId,
      referenceUuid,
      customerUuid,
      holdTime,
      muteTime,
      ringingTime,
      transferTime,
      conferenceTime,
      callTime,
      disposeTime,
  });
  }

  return reportData;
}


module.exports = { createReport }