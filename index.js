require("dotenv").config();
const restify = require("restify");
const bodyParser = require("body-parser");
const connection = require("./config/db")
const { createReport } = require("./utils/createReport");
const { getMongoData, createMongoData, updateMongoData, deleteMongoData,createMongoBulk, db } = require("./Controller/mongodb");
const { getSqlData, createSqlData, updateSqlData, deleteSqlData, pool, } = require("./Controller/mysql");
const { createRedisData, getRedisData, deleteRedisData } = require("./Controller/redis");
const { deleteElasticData, createElasticData, readElasticDataAll, updateElasticData, deleteElasticIndex, createElasticBulkData, readElasticDataByIndex, createIndex, client } = require("./Controller/elasticsearch");

const server = restify.createServer();
server.use(bodyParser.json());

let mongoDb = db;
const mysqlDb = pool;
let elasticDb = client;

// Mongo Crud
server.get("/mongo/get", getMongoData);
server.post("/mongo/create", createMongoData);
server.put("/mongo/update/:id", updateMongoData);
server.del("/mongo/delete/:id", deleteMongoData);

// MYSQL CRUD
server.get("/sql/get", getSqlData);
server.post("/sql/create", createSqlData);
server.patch("/sql/update/:id", updateSqlData);
server.del("/sql/delete/:id", deleteSqlData);

// REDIS CRUD
server.post("/redis/create", createRedisData);
server.get("/redis/get", getRedisData);
server.del("/redis/delete/:name", deleteRedisData);

// Elastic CRUD
server.post("/elastic/create", createElasticData);
server.post("/elastic/createClient", createIndex);
server.get("/elastic/get", readElasticDataAll);
server.get("/elastic/get/:index", readElasticDataByIndex);
server.put("/elastic/update/:index/:id", updateElasticData);
server.del("/elastic/delete/:index/:id", deleteElasticData);
server.del("/elastic/deleteIndex/:index", deleteElasticIndex);
server.post("/elastic/createBulk", createElasticBulkData);

//Bulk Data Creation

server.post("/sql/createBulk", async (req, res) => {
  let bulkData = [];
  for (let i = 0; i < 10000; i++) {
    const firstname = faker.internet.username();
    const lastname = faker.internet.username();
    const email = faker.internet.email();
    const gender = faker.person.sex();
    const zodiacSign = faker.person.zodiacSign();
    const epocTime = new Date().valueOf();
    const date = new Date().toLocaleDateString();
    const time = new Date().toLocaleTimeString();
    const campaign = "campaign1";
    const login = Date.now();
    const logout = Date.now() + 80000000;

    bulkData.push({
      firstname,
      lastname,
      email,
      gender,
      zodiacSign,
      epocTime,
      date,
      time,
      campaign,
      login,
      logout,
    });
  }

  try {
    const query = `INSERT INTO bulkdata (firstname, lastname, email, gender, zodiacSign, epocTime, date, time) VALUES ?`;
    const connection = await mysqlDb.getConnection();
    const [data] = await connection.query(query, [bulkData.map(Object.values)]);
    connection.release();

    if (data.affectedRows !== bulkData.length) {
      res.json({
        message: "Failed to save some or all documents",
        error: "Insertion count mismatch",
      });
    } else {
      res.json({ message: "Successfully Added", data });
    }
  } catch (error) {
    res.json({ message: "Failed to save", error: error.message });
  }
});

server.post("/createreport", async (req, res) => {
  const reportData = createReport();
  try {
    let query = `INSERT INTO reportdata (datetime, reportType, disposeType, disposeName, callDuration, agentName, campaignName, processName, leadsetid, referenceUuid, customerUuid, hold, mute, ringing, transfer, conference, callTime, disposeTime) VALUES ?`;
    let connection = await mysqlDb.getConnection();
    let collection1 = await db.collection("reportdata");
    let [data] = await connection.query(query, [[reportData]]);
    let response = await collection1.insertMany(reportData, { ordered: true });
    connection.release();

    if (
      data.affectedRows !== reportData.length &&
      response.affectedRows !== reportData.length
    ) {
      res.json({
        message: "Failed to save some or all documents",
        error: "Insertion count mismatch",
      });
    } else {
      res.json({ message: "Successfully Added", data });
    }
  } catch (error) {
    res.json({ message: "Failed to save", error: error.message });
  }
});

server.post("/createreport/elastic", async (req, res) => {
  const reportData = createReport();
  const bulkData = [];

  // Push metadata and document pairs
  reportData.forEach((doc) => {
    bulkData.push({ index: { _index: "rahul-reportdata" } });
    bulkData.push(doc);
  });

  try {
    const response = await elasticDb .bulk({ body: bulkData });
    if (response.errors) {
      const erroredDocuments = response.items.filter(
        (item) => item.index && item.index.error
      );

      res.json({
        message: "Failed to save some or all documents",
        error: erroredDocuments,
      });
    } else {
      res.json({
        message: "Successfully Added",
        data: response,
      });
    }
  } catch (error) {
    res.json({ message: "Failed to save", error: error.message });
  }
});

server.post("/createreport/mongo", async (req, res) => {
  const reportData = createReport();
  try {
    let collection1 = await mongoDb.collection("reportdata");
    let response = await collection1.insertMany(reportData, { ordered: true });

    if (response.insertedCount !== reportData.length) {
      res.json({
        message: "Failed to save some or all documents",
        error: "Insertion count mismatch",
      });
    } else {
      res.json({ message: "Successfully Added", data: response.ops });
    }
  } catch (error) {
    res.json({ message: "Failed to save", error: error.message });
  }
});

// server.get("/mongo/getreport", async (req, res) => {
//   try {
//     let collection = await db.collection("reportdata");
//     // let response = await collection
//     //   .aggregate([
//     //     {
//     //       $group: {
//     //         _id: null,
//     //         totalCallDuration: { $sum: "$callDuration" },
//     //         lowestCallTime: { $min: "$callTime" },
//     //         highestCallTime: { $max: "$callTime" },
//     //         averageCallTime: {
//     //           $avg: {
//     //             $add: [
//     //               "$hold",
//     //               "$mute",
//     //               "$ringing",
//     //               "$transfer",
//     //               "$conference",
//     //             ],
//     //           },
//     //         },
//     //         averageDisposeTime: { $avg: "$disposeTime" },
//     //       },
//     //     },
//     //   ])
//     //   .toArray();
//           const pipeline = [
//             {
//               $group: {
//                 _id: {
//                   process_name: "$processName",
//                   call_hour: { $hour: "$datetime" },
//                   datetime: {
//                     $dateToString: {
//                       format: "%Y-%m-%d %H:00:00.000",
//                       date: "$datetime"
//                     }
//                   },
//                   total_calls: { $sum: "$callTime" },
//                   call_answered: { $sum: { $cond: [{ $eq: ["$type", "disposed"] }, 1, 0] } },
//                   missed_calls: { $sum: { $cond: [{ $eq: ["$type", "missed"] }, 1, 0] } },
//                   call_autodrop: { $sum: { $cond: [{ $eq: ["$type", "autoDrop"] }, 1, 0] } },
//                   call_autofail: { $sum: { $cond: [{ $eq: ["$type", "autoFailed"] }, 1, 0] } },
//                   talktime: { $sum: "$callDuration" }
//                 },
//               }
//             }
//           ];

//           const response = await collection.aggregate(pipeline).toArray();
//     res.json({
//       message: "Success",
//       data: response,
//     });
//   } catch (error) {
//     res.json({
//       message: "Failed",
//       data: response.error,
//     });
//   }
// });

let db1;
connection().then((database) => {
  db1 = database;
});

// server.get("/mongo/getreport", async (req, res) => {
//   try {
//     let collection1 = await db1.collection("reportdata");
//     const pipeline = [
//       {
//         $group: {
//           _id: {
//             process_name: "$processName",
//             call_hour: {
//               $hour: "$datetime",
//             },
//             datetime: {
//               $dateToString: {
//                 format: "%Y-%m-%d %H:00:00.000",
//                 date: "$datetime",
//               },
//             },
//           },
//           total_calls: { $sum: 1, },
//           call_answered: { $sum: { $cond: [ { $eq: ["$type", "disposed"], }, 1, 0, ], }, },
//           missed_calls: { $sum: { $cond: [ { $eq: ["$type", "missed"], }, 1, 0, ], }, },
//           call_autodrop: {
//             $sum: {
//               $cond: [
//                 {
//                   $eq: ["$type", "autoDrop"],
//                 },
//                 1,
//                 0,
//               ],
//             },
//           },
//           call_autofail: {
//             $sum: { $cond: [{ $eq: ["$type", "autoFailed"] }, 1, 0] },
//           },
//           talktime: { $sum: "$callDuration" },
//         },
//       },
//     ];
//     const response = await collection1.aggregate(pipeline).toArray();
//     res.json({ message: "Success", data: response });
//   } catch (error) {
//     console.error(error); // Log the error for debugging
//     res.json({ message: "Failed", data: error });
//   }
// });

server.get('/mongo/getreport', async (req, res) => {
  try {
    let collection1 = await db1.collection("reportdata");

    const pipeline = [
      {
        $group: {
          _id: "$campaignName",
          total_calls: { $sum: 1 },
          call_answered: { $sum: { $cond: [{ $eq: ["$type", "disposed"] }, 1, 0] } },
          missed_calls: { $sum: { $cond: [{ $eq: ["$type", "missed"] }, 1, 0] } },
          call_autodrop: { $sum: { $cond: [{ $eq: ["$type", "autoDrop"] }, 1, 0] } },
          call_autofail: { $sum: { $cond: [{ $eq: ["$type", "autoFailed"] }, 1, 0] } },
          talktime: { $sum: "$callDuration" }
        }
      },
      {
        $project: {
          process_name: "$processName",
          total_calls: 1,
          call_answered: 1,
          missed_calls: 1,
          call_autodrop: 1,
          call_autofail: 1,
          talktime: 1
        }
      }
    ];

    const result = await collection1.aggregate(pipeline).toArray();
    res.json({ message: "Success", response: result });
  } catch (error) {
    res.json({ message: "Error fetching summary", error: error.message });
  }
});

server.get("/sql/getreport", async (req, res) => {
  let connection = await mysqlDb.getConnection();

  const [data] = await connection.query(`
      SELECT 
        campaignName, 
        COUNT(*) AS Total_Calls,
        HOUR(datetime) AS Call_hours,
        DATE_FORMAT(datetime, '%Y-%m-%d %H:00:00.000') AS datetime, 
        SUM(CASE WHEN reportType = 'disposed' THEN 1 ELSE 0 END) AS Call_Answered,
        SUM(CASE WHEN reportType = 'missed' THEN 1 ELSE 0 END) AS Missed_Calls,
        SUM(CASE WHEN reportType = 'autoDrop' THEN 1 ELSE 0 END) AS Call_Autodrop, 
        SUM(CASE WHEN reportType = 'autoFail' THEN 1 ELSE 0 END) AS Call_Autofail, 
        SUM(callDuration) AS Talktime   
      FROM 
        reportdata 
      GROUP BY 
      campaignName
    `);

  connection.release();

  res.send(data);
});

server.get('/sql/getallreport', async(req,res) =>{
  let connection = await mysqlDb.getConnection();
  const [data] = await connection.query(`SELECT * FROM reportdata`);
  res.send(data);
})

server.get("/elastic/getreport", async (req, res) => {
  try {
    const response = await elasticDb .search({
      index: 'rahul-reportdata', 
      body: {
        size: 0,
        aggs: {
          campaigns: {
            terms: { field: 'campaignName.keyword' },
            aggs: {
              call_hours: {
                date_histogram: {
                  field: 'datetime',
                  calendar_interval: 'hour',
                  format: 'yyyy-MM-dd HH:00:00.000',
                },
                aggs: {
                  Total_Calls: { value_count: { field: 'campaignName.keyword' } },
                  Call_Answered: { sum: { script: "doc['reportType.keyword'].value == 'disposed' ? 1 : 0" } },
                  Missed_Calls: { sum: { script: "doc['reportType.keyword'].value == 'missed' ? 1 : 0" } },
                  Call_Autodrop: { sum: { script: "doc['reportType.keyword'].value == 'autoDrop' ? 1 : 0" } },
                  Call_Autofail: { sum: { script: "doc['reportType.keyword'].value == 'autoFail' ? 1 : 0" } },
                  Talktime: { sum: { field: 'callDuration' } }
                }
              }
            }
          }
        }
      }
    });

    res.json({
      message: "Success",
      data: response.aggregations.campaigns.buckets,
    });
  } catch (error) {
    console.error(error); // Log the error for debugging
    res.json({ message: "Failed", data: error });
  }
});


server.get("/sql/getbulkdata/:gender/:zsign", async (req, res) => {
  const gender = req.params.gender;
  const zsign = req.params.zsign;

  const connection = await pool.getConnection();
  const [data] = await connection.query(
    "SELECT * FROM bulkdata WHERE gender=? AND zodiacSign=? LIMIT 1000 ",
    [gender, zsign]
  );

  const [count] = await connection.query("SELECT COUNT(*) from ");
  connection.release();

  res.json({
    message: "success",
    response: data,
  });
});

server.get("/mongo/getbulkdata/:gender/:zsign", async (req, res) => {
  const zsign = req.params.zsign;
  const gender = req.params.gender;

  let collection = await mongoDb.collection("bulkdata");
  const resultdata = await collection
    .find({ gender: gender, zodiacSign: zsign })
    .toArray();
  res.json({ message: "success", response: resultdata });
});

server.get("/elastic/getbulkdata/:gender/:zsign", async (req, res) => {
  const gender = req.params.gender;
  const zsign = req.params.zsign;
  const response = await elasticDb .search({
    index: "rahul-detail",
    body: {
      query: {
        bool: {
          must: [{ term: { gender: gender } }, { term: { zsign: zsign } }],
        },
      },
    },
  });

  res.json({ message: "success", response });
});

server.post("/mongo/createBulk", createMongoBulk);

server.listen(process.env.PORT, process.env.IP_PORT, () => {
  console.log("%s listening at %s", server.name, server.url);
});
