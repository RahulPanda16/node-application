// require("dotenv").config();
// let mysql = require("mysql2");
// let { faker } = require("@faker-js/faker");
// let { v4: uuidv4 } = require("uuid");

// let pool = mysql.createPool({
//   host: process.env.MYSQL_HOST,
//   user: process.env.MYSQL_USER,
//   password: process.env.MYSQL_PASSWORD,
//   database: process.env.MYSQL_DATABASE,
//   port: process.env.MYSQL_PORT,
// });

// let createData = async () => {
//   function getRandomEpochTime() {
//     let currentEpochTime = Math.floor(Date.now() / 1000);
//     let randomTime = Math.floor(Math.random() * 1e6);
//     return currentEpochTime - randomTime;
//   }

//   let reportData = [];
//   let reportType = ["disposed", "missed", "autoFail", "autoDrop"];
//   let disposeType = ["callback", "dnc", "etx"];
//   let agentName = [
//     "Vicky",
//     "Suman",
//     "Lokesh",
//     "Animesh",
//     "Naman",
//     "Mithul",
//     "Saloni",
//     "Parv",
//     "Rebel",
//     "Dhruv",
//   ];

//   let disposeName = ["External", "Transfer", "Do Not Call", "Follow Up"];

//   for (let i = 0; i < 10000; i++) {
//     let dateTime = faker.date.between({
//       from: "2020-01-01T00:00:00Z",
//       to: "2026-01-01T00:00:00Z",
//     });
//     let randomReportType =
//       reportType[Math.floor(Math.random() * reportType.length)];
//     let randomDisposeType =
//       disposeType[Math.floor(Math.random() * disposeType.length)];
//     let randomDisposeName =
//       disposeName[Math.floor(Math.random() * disposeName.length)];
//     let randomAgentName =
//       agentName[Math.floor(Math.random() * agentName.length)];
//     let campaignName = `Campaign_${Math.floor(Math.random() * 100)}`;
//     let processName = `Process_${Math.floor(Math.random() * 100)}`;
//     let leadSetId = Math.floor(Math.random() * 199) + 1;
//     let referenceUuid = uuidv4();
//     let customerUuid = uuidv4();
//     let holdTime = getRandomEpochTime();
//     let muteTime = getRandomEpochTime();
//     let ringingTime = getRandomEpochTime();
//     let transferTime = getRandomEpochTime();
//     let conferenceTime = getRandomEpochTime();
//     let callTime = getRandomEpochTime();
//     let duration =
//       holdTime +
//       muteTime +
//       ringingTime +
//       transferTime +
//       conferenceTime +
//       callTime;
//     let disposeTime = getRandomEpochTime();

//     let calltype = randomReportType;

//     if (calltype == "missed") {
//       randomDisposeName = "Agent not found";
//     } else if (calltype == "autoFailed" || calltype == "autoDrop") {
//       randomDisposeName = faker.helpers.arrayElement([
//         "busy",
//         "decline",
//         "does not exist",
//         "not acceptable",
//       ]);
//     } else {
//       randomDisposeName = faker.helpers.arrayElement([
//         "follow up",
//         "do not call",
//         "external transfer",
//       ]);
//       if (randomDisposeName == "follow up") {
//         randomDisposeType = "callback";
//       } else if (randomDisposeName == "do not call") {
//         randomDisposeType = "dnc";
//       } else {
//         randomDisposeType = "etx";
//       }
//     }

//     reportData.push([
//       dateTime,
//       randomReportType,
//       randomDisposeType,
//       randomDisposeName,
//       duration,
//       randomAgentName,
//       campaignName,
//       processName,
//       leadSetId,
//       referenceUuid,
//       customerUuid,
//       holdTime,
//       muteTime,
//       ringingTime,
//       transferTime,
//       conferenceTime,
//       callTime,
//       disposeTime,
//     ]);
//   }

//   try {
//     let query = `INSERT INTO reportdata (datetime, reportType, disposeType, disposeName, callDuration, agentName, campaignName, processName,leadsetid, referenceUuid, customerUuid, hold, mute, ringing, transfer, conference, callTime, disposeTime) VALUES ?`;
//     let connection = await pool.getConnection();
//     let [data] = await connection.query(query, [reportData]);
//     connection.release();

//     if (data.affectedRows !== bulkData.length) {
//       console.log({
//         message: "Failed to save some or all documents",
//         error: "Insertion count mismatch",
//       });
//     } else {
//       console.log({ message: "Successfully Added", data });
//     }
//   } catch (error) {
//     console.log({ message: "Failed to save", error: error.message });
//   }
// };

// createData();

require("dotenv").config();
let mysql = require('mysql2');
let { faker } = require("@faker-js/faker");
let { v4: uuidv4 } = require("uuid");

let pool = mysql.createPool({
  host: process.env.MYSQL_HOST,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD,
  database: process.env.MYSQL_DATABASE,
  port: process.env.MYSQL_PORT,
});

let createData = async () => {
  function getRandomEpochTime() {
    let currentEpochTime = Math.floor(Date.now() / 1000);
    let randomTime = Math.floor(Math.random() * 1e6);
    return currentEpochTime - randomTime;
  }

  let reportData = [];
  let reportType = ["disposed", "missed", "autoFail", "autoDrop"];
  let disposeType = ["callback", "dnc", "etx"];
  let agentName = ["Vicky", "Suman", "Lokesh", "Animesh", "Naman", "Mithul", "Saloni", "Parv", "Rebel", "Dhruv"];
  let disposeName = ['External', 'Transfer', 'Do Not Call', 'Follow Up'];

  for (let i = 0; i < 10000; i++) {
    let dateTime = faker.date.between({ from: '2020-01-01T00:00:00Z', to: '2026-01-01T00:00:00Z' });
    let randomReportType = reportType[Math.floor(Math.random() * reportType.length)];
    let randomDisposeType = disposeType[Math.floor(Math.random() * disposeType.length)];
    let randomDisposeName = disposeName[Math.floor(Math.random() * disposeName.length)];
    let randomAgentName = agentName[Math.floor(Math.random() * agentName.length)];
    let campaignName = `Campaign_${Math.floor(Math.random() * 100)}`;
    let processName = `Process_${Math.floor(Math.random() * 100)}`;
    let leadSetId = Math.floor(Math.random() * 199) + 1;
    let referenceUuid = uuidv4();
    let customerUuid = uuidv4();
    let holdTime = getRandomEpochTime();
    let muteTime = getRandomEpochTime();
    let ringingTime = getRandomEpochTime();
    let transferTime = getRandomEpochTime();
    let conferenceTime = getRandomEpochTime();
    let callTime = getRandomEpochTime();
    let duration = (holdTime + muteTime + ringingTime + transferTime + conferenceTime + callTime);
    let disposeTime = getRandomEpochTime();

    let calltype = randomReportType;

    if (calltype == 'missed') {
      randomDisposeName = 'Agent not found';
    } else if (calltype == "autoFail" || calltype == "autoDrop") {
      randomDisposeName = faker.helpers.arrayElement(["busy", "decline", "does not exist", "not acceptable"]);
    } else {
      randomDisposeName = faker.helpers.arrayElement(['follow up', 'do not call', 'external transfer']);
      if (randomDisposeName == 'follow up') {
        randomDisposeType = 'callback';
      } else if (randomDisposeName == 'do not call') {
        randomDisposeType = 'dnc';
      } else {
        randomDisposeType = 'etx';
      }
    }

    reportData.push([
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
      disposeTime
    ]);
  }

  try {
    let query = `INSERT INTO reportdata (datetime, reportType, disposeType, disposeName, callDuration, agentName, campaignName, processName, leadsetid, referenceUuid, customerUuid, hold, mute, ringing, transfer, conference, callTime, disposeTime) VALUES ?`;
    let connection = await pool.getConnection();
    let [data] = await connection.query(query, [reportData]);
    connection.release();

    if (data.affectedRows !== reportData.length) {
      console.log({
        message: "Failed to save some or all documents",
        error: "Insertion count mismatch",
      });
    } else {
      console.log({ message: "Successfully Added", data });
    }
  } catch (error) {
    console.log({ message: "Failed to save", error: error.message });
  }
};

createData();

