const connection = require("../config/db");
const { v4: uuidv4 } = require("uuid");
const id = uuidv4();
const { createReport } = require("../utils/createReport");

let db;
connection().then((database) => {
  db = database;
});

const getMongoData = async (req, res) => {
  try {
    let collection = await db.collection("users");
    var data = await collection.find().toArray();
    res.json({
      success: true,
      data: data,
    });
  } catch (error) {
    console.error(error);
    res.json({
      success: false,
      message: "Internal Server Error",
    });
  }
};

const createMongoData = async (req, res) => {
  const { firstname, lastname, email, password } = req.body;

  let collection = await db.collection("users");

  await collection.insertOne({
    id,
    firstname,
    lastname,
    email,
    password,
  });

  res.json({
    message: "Data Added Successfully",
  });
};

const updateMongoData = async (req, res) => {
  try {
    const id = req.params.id;
    const firstname = req.body.firstname;
    const lastname = req.body.lastname;
    const email = req.body.email;

    let collection = await db.collection("users");
    let data = await collection.updateOne(
      { id: id },
      {
        $set: {
          firstname: firstname,
          lastname: lastname,
          email: email,
        },
      }
    );

    res.json({
      message: "updated",
      data: data,
    });
  } catch (err) {
    res.json({
      message: err,
    });
  }
};

const deleteMongoData = async (req, res) => {
  try {
    const { id } = req.params;
    const collection = db.collection("users");
    await collection.deleteOne({ id: id });

    res.json({
      message: "deleted",
    });
  } catch (err) {
    res.json({
      message: err,
    });
  }
};

const createMongoBulk = async (req, res) => {
  let bulkData = [];
  for (let i = 0; i < 1000000; i++) {
    const firstname = faker.internet.username();
    const lastname = faker.internet.username();
    const email = faker.internet.email();
    const gender = faker.person.sex();
    const zodiacSign = faker.person.zodiacSign();
    const epocTime = new Date().valueOf();
    const date = new Date().toLocaleDateString();
    const time = new Date().toLocaleTimeString();
    // const password = faker.internet.password();
    bulkData.push({
      id,
      firstname,
      lastname,
      email,
      gender,
      zodiacSign,
      epocTime,
      date,
      time,
    });
  }

  try {
    let collection = await db.collection("bulkdata");
    const response = await collection.insertMany(bulkData, { ordered: true });

    if (response.insertedCount !== bulkData.length) {
      res.json({
        message: "Failed to save some or all documents",
        error: "Insertion count mismatch",
      });
    } else {
      res.json({ message: "Successfully Added", data: response });
    }
  } catch (error) {
    res.json({ message: "Failed to save", error: error });
  }
};

const createMongoReport = async (req, res) => {
  const reportData = createReport();
  try {
    let collection1 = await db.collection("reportdata");
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
};

const getMongoAllReport = async (req, res) => {
  let collection1 = await db.collection("reportdata");
  let response = await collection1.find().toArray();
  res.send(response);
};

const getMongoSummaryReport = async (req, res) => {
  try {
    let collection1 = await db.collection("reportdata");
    const result = await collection1
      .aggregate([
        {
          $addFields: {
            hour: {
              $hour: {
                $dateFromString: {
                  dateString: {
                    $dateToString: {
                      format: "%Y-%m-%dT%H:%M:%S.%L",
                      date: "$datetime",
                    },
                  },
                },
              },
            },
          },
        },
        {
          $group: {
            _id: "$hour",
            Talktime: { $sum: "$callDuration" },
            Total_Calls: { $sum: 1 },
            Call_Answered: {
              $sum: { $cond: [{ $eq: ["$reportType", "disposed"] }, 1, 0] },
            },
            Missed_Calls: {
              $sum: { $cond: [{ $eq: ["$reportType", "missed"] }, 1, 0] },
            },
            Call_Autodrop: {
              $sum: { $cond: [{ $eq: ["$reportType", "autoDrop"] }, 1, 0] },
            },
            Call_Autofail: {
              $sum: { $cond: [{ $eq: ["$reportType", "autoFail"] }, 1, 0] },
            },
          },
        },
        {
          $sort: { _id: 1 },
        },
      ])
      .toArray();

    res.send(result);
  } catch (error) {
    res.json({ message: "Error fetching summary", error: error.message });
  }
};

const getMongoBulkData = async (req, res) => {
  const zsign = req.params.zsign;
  const gender = req.params.gender;

  let collection = await db.collection("bulkdata");
  const resultdata = await collection
    .find({ gender: gender, zodiacSign: zsign })
    .toArray();
  res.json({ message: "success", response: resultdata });
};

module.exports = {
  db,
  getMongoData,
  createMongoData,
  updateMongoData,
  deleteMongoData,
  createMongoBulk,
  createMongoReport,
  getMongoSummaryReport,
  getMongoAllReport,
  getMongoBulkData,
};
