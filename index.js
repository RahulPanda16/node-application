require("dotenv").config();
const restify = require("restify");
const bodyParser = require("body-parser");
const {
  getMongoData,
  createMongoData,
  updateMongoData,
  deleteMongoData,
  createMongoBulk,
  db,
  createMongoReport,
  getSummaryReport,
  getMongoBulkData,
  getMongoSummaryReport,
  getMongoAllReport,
} = require("./Controller/mongodb");
const {
  getSqlData,
  createSqlData,
  updateSqlData,
  deleteSqlData,
  pool,
  createSqlBulkData,
  createSqlReport,
  getSqlAllReport,
  getSqlSummaryReport,
  getSqlBulkData,
} = require("./Controller/mysql");
const {
  createRedisData,
  getRedisData,
  deleteRedisData,
} = require("./Controller/redis");
const {
  deleteElasticData,
  createElasticData,
  readElasticDataAll,
  updateElasticData,
  deleteElasticIndex,
  createElasticBulkData,
  readElasticDataByIndex,
  createIndex,
  client,
  getElasticBulkData,
  createElasticReport,
  getElasticAllReport,
} = require("./Controller/elasticsearch");
const { createReport } = require("./utils/createReport");

const server = restify.createServer();
server.use(bodyParser.json());

// Mongo Crud
server.get("/mongo/get", getMongoData);
server.post("/mongo/create", createMongoData);
server.put("/mongo/update/:id", updateMongoData);
server.del("/mongo/delete/:id", deleteMongoData);
server.post("/createreport/mongo", createMongoReport);
server.get("/mongo/getallreport", getMongoAllReport);
server.get("/mongo/getsummarizereport", getMongoSummaryReport);
server.post("/mongo/createBulk", createMongoBulk);
server.get("/mongo/getbulkdata/:gender/:zsign", getMongoBulkData);

// MYSQL CRUD
server.get("/sql/get", getSqlData);
server.post("/sql/create", createSqlData);
server.patch("/sql/update/:id", updateSqlData);
server.del("/sql/delete/:id", deleteSqlData);
server.post("/sql/createBulk", createSqlBulkData);
server.get("/sql/getbulkdata/:gender/:zsign", getSqlBulkData);
server.post("/createreport/sql", createSqlReport);
server.get("/sql/getallreport", getSqlAllReport);
server.get("/sql/getsummarizereport", getSqlSummaryReport);

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
server.get("/elastic/getbulkdata/:gender/:zsign", getElasticBulkData);
server.post("/createreport/elastic", createElasticReport);
server.get("/elastic/getallreport", getElasticAllReport);
server.get("/elastic/callreportsummary/get", async (req, res) => {
  try {
    const result = await client.search({
      index: "rahul-reportdata",
      body: {
        query: {
          range: {
            callDuration: {
              gt: 0,
            },
          },
        },
        aggs: {
          hours: {
            date_histogram: {
              field: "datetime",
              fixed_interval: "1h",
              format: "HH",
            },
            aggs: {
              Talktime: {
                sum: { field: "callDuration" },
              },
              Total_Calls: {
                sum: { field: "callTime" },
              },
              Call_Answered: {
                filter: { term: { "reportType.keyword": "disposed" } },
                aggs: {
                  sum: {
                    field: "datetime",
                  },
                },
              },
              Call_Autodrop: {
                filter: {
                  term: { "reportType.keyword": "autoDrop" },
                },
                aggs: {
                  sum: {
                    field: "datetime",
                  },
                },
              },
              Call_Autofail: {
                filter: {
                  term: { "reportType.keyword": "autoFail" },
                },
                aggs: {
                  sum: {
                    field: "datetime",
                  },
                },
              },
              Missed_Calls: {
                filter: {
                  term: { "reportType.keyword": "missed" },
                },
                aggs: {
                  sum: {
                    field: "datetime",
                  },
                },
              },
              Talktime: {
                sum: {
                  field: "callDuration",
                },
              },
            },
          },
        },
        size: 0,
      },
    });

    if (
      result &&
      result.aggregations &&
      result.aggregations.by_hour &&
      result.aggregations.by_hour.buckets
    ) {
      const resultArray = result.aggregations.by_hour.buckets.map((bucket) => ({
        key: bucket.key_as_string,
        Total_Calls: bucket.Total_Calls.value,
        Call_Answered: bucket.Call_Answered.count.value,
        Call_Autodrop: bucket.Call_Autodrop.count.value,
        Call_Autofail: bucket.Call_Autofail.count.value,
        Missed_Calls: bucket.Missed_Calls.count.value,
        Talktime: bucket.Talktime.value,
      }));
      res.send(resultArray);
    } else {
      throw new Error("Aggregation result is undefined or incomplete");
    }
  } catch (error) {
    console.error("Error fetching summary:", error);
    res.json({ message: "Error fetching summary", error: error.message });
  }
});

server.get("/elastic/fetchSummary", async (req, res) => {
  try {
    const response = await client.search({
      index: "rahul-reportdata",
      body: {
        size: 10000,
        query: {
          range: {
            callDuration: {
              gt: 0,
            },
          },
        },
        aggs: {
          hours: {
            date_histogram: {
              field: "datetime",
              fixed_interval: "1h",
              format: "HH",
              order: { _key: "asc" },
            },
            aggs: {
              totalHoldTime: {
                sum: {
                  field: "hold",
                },
              },
              totalMuteTime: {
                sum: {
                  field: "mute",
                },
              },
              totalRingingTime: {
                sum: {
                  field: "ringing",
                },
              },
              totalTransferTime: {
                sum: {
                  field: "transfer",
                },
              },
              totalConferenceTime: {
                sum: {
                  field: "conference",
                },
              },
              Total_Calls: {
                sum: {
                  field: "callTime",
                },
              },
              Call_Answered: {
                sum: {
                  field: "disposeTime",
                },
              },
              totalDuration: {
                sum: {
                  field: "callDuration",
                },
              },
              nonZeroDuration: {
                bucket_selector: {
                  buckets_path: {
                    totalDuration: "totalDuration",
                  },
                  script: "params.totalDuration > 0",
                },
              },
            },
          },
        },
      },
    });

    if (
      response.aggregations &&
      response.aggregations.hours &&
      response.aggregations.hours.buckets
    ) {
      res.json(response.aggregations.hours.buckets);
    } else {
      res.json([]);
    }
  } catch (error) {
    console.error("Error fetching summary data:", error);
    res.send("Error fetching summary data");
  }
});

// server.get("/elastic/callreportsummary/get1", async (req, res) => {
//   try {
//     const result = await client.search({
//         index: 'rahul-reportdata',
//         body: {
//             "size": 0,
//                 "aggs": {
//                 "group_by_hour": {
//                     "date_histogram": {
//                     "field": "datetime",
//                     "calendar_interval": "hour"
//                     },
//                     "aggs": {
//                     "total_duration": {
//                     "sum": {
//                         "field": "callDuration"
//                     }
//                     },
//                     "total_calltime": {
//                     "sum": {
//                         "field": "callTime"
//                     }
//                     },
//                     "total_hold": {
//                     "sum": {
//                         "field": "hold"
//                     }
//                     },
//                     "total_mute": {
//                     "sum": {
//                         "field": "mute"
//                     }
//                     },
//                     "total_ringing": {
//                     "sum": {
//                         "field": "ringing"
//                     }
//                     },
//                     "total_transfer": {
//                     "sum": {
//                         "field": "transfer"
//                     }
//                     },
//                     "total_conference": {
//                     "sum": {
//                         "field": "conference"
//                     }
//                     },
//                     "unique_calls": {
//                         "value_count": {
//                             "field": "referenceUUID.keyword"
//                         }
//                     }
//                     }
//                 }
//                 }
//                 }
//         });
//     if (
//       result &&
//       result.aggregations &&
//       result.aggregations.by_hour &&
//       result.aggregations.by_hour.buckets
//     ) {
//       const resultArray = result.aggregations.by_hour.buckets.map((bucket) => ({
//         key: bucket.key_as_string,
//         Total_Calls: bucket.Total_Calls.value,
//         Call_Answered: bucket.Call_Answered.count.value,
//         Call_Autodrop: bucket.Call_Autodrop.count.value,
//         Call_Autofail: bucket.Call_Autofail.count.value,
//         Missed_Calls: bucket.Missed_Calls.count.value,
//         Talktime: bucket.Talktime.value,
//       }));
//       res.send(resultArray);
//     } else {
//       throw new Error("Aggregation result is undefined or incomplete");
//     }
//   } catch (error) {
//     console.error("Error fetching summary:", error);
//     res.json({ message: "Error fetching summary", error: error.message });
//   }
// });
  
server.listen(process.env.PORT, process.env.IP_PORT, () => {
  console.log("%s listening at %s", server.name, server.url);
});

// console.log(result.hits)

// let data = result['aggregations']['group_by_hour']['buckets'];
// // console.log(data);
// let docs = [];
// data.forEach((doc) => {
//     docs.push({
//         'hour' : moment(doc.key).format('H'),
//         'call_count': doc.doc_count,
//         'total_ringing': doc.total_ringing.value,
//         'total_calltime': doc.total_calltime.value,
//         'total_hold': doc.total_hold.value,
//         'total_mute': doc.total_mute.value,
//         'total_transfer': doc.total_transfer.value,
//         'total_conference': doc.total_conference.value,
//         'total_duration': doc.total_duration.value,
//     });
// });
// // console.log(docs);
// res.send(docs);
