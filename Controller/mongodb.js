const connection = require("../config/db");
const { v4: uuidv4 } = require("uuid");
const id = uuidv4();

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
  }

 const createMongoData =  async (req, res) => {
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
  }

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
}

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
}

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
  }

module.exports = {
    db,
    getMongoData,
    createMongoData,
    updateMongoData,
    deleteMongoData,
    createMongoBulk
}