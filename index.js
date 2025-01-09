require("dotenv").config();
const restify = require("restify");
const bodyParser = require("body-parser");
const connection = require("./config/db");
const Redis = require("ioredis");
const mysql = require("mysql2/promise");
const port = process.env.PORT;
const { v4: uuidv4 } = require("uuid");
const id = uuidv4();
const { Client } = require("@elastic/elasticsearch");
const { faker, da } = require("@faker-js/faker");

const server = restify.createServer();
server.use(bodyParser.json());

let db;
connection().then((database) => {
  db = database;
});

server.get("/mongo/get", async (req, res) => {
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
});

server.post("/mongo/create", async (req, res) => {
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
});

server.put("/mongo/update/:id", async (req, res) => {
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
});

server.post("/mongo/delete/:id", async (req, res) => {
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
});

// MYSQL CRUD
const pool = mysql.createPool({
  host: process.env.MYSQL_HOST,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD,
  database: process.env.MYSQL_DATABASE,
  port: process.env.MYSQL_PORT,
});

server.get("/sql/get", async (req, res) => {
  try {
    const connection = await pool.getConnection();
    const [data] = await connection.query(`SELECT * FROM users;`);
    connection.release();

    res.json({ users: data });
  } catch (err) {
    res.json({ message: err.message });
  }
});

server.post("/sql/create", async (req, res) => {
  try {
    const firstname = req.body.firstname;
    const lastname = req.body.lastname;
    const email = req.body.email;
    const password = req.body.password;

    const connection = await pool.getConnection();

    const [data] = await connection.query(
      `INSERT INTO users (firstname, lastname, email, password) 
                VALUES (?,?,?,?)`,
      [firstname, lastname, email, password]
    );

    connection.release();

    res.json({
      message: "User Created",
      data: data,
    });
  } catch (error) {
    res.json({
      message: error.message,
    });
    console.error(error);
  }
});

server.patch("/sql/update/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const { firstname, lastname, email } = req.body;
    const connection = await pool.getConnection();
    const [update] = await connection.query(
      `UPDATE users SET firstname = ?, lastname = ?, email = ? WHERE id = ?`,
      [firstname, lastname, email, id]
    );
    connection.release();

    res.json({ message: "updated", data: update });
  } catch (err) {
    res.json({ message: err.message });
  }
});

server.del("/sql/delete/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const connection = await pool.getConnection();
    await connection.query(`DELETE FROM users WHERE id = ?`, [id]);
    connection.release();

    res.json({ message: "Data Deleted Successfully" });
  } catch (err) {
    res.json({ message: err.message });
  }
});

//   REDIS CRUD

const redis = new Redis({
  port: process.env.REDIS_PORT,
  host: process.env.REDIS_IP,
  connectTimeout: 10000,
});

server.post("/redis/create", async (req, res) => {
  const firstname = req.body.firstname;
  const lastname = req.body.lastname;
  const email = req.body.email;
  const password = req.body.password;

  const response = await redis.hset("userRahul", {
    firstname: firstname,
    lastname: lastname,
    email: email,
    password: password,
  });
  res.json({
    response,
    message: "User saved successfully",
  });
});

redis.on("connect", () => {
  console.log("Redis Connected Successfully");
});

server.get("/redis/get", async (req, res) => {
  const response = await redis.hgetall("userRahul");
  res.json({
    response,
  });
});

server.del("/redis/delete/:name", async (req, res) => {
  console.log(req.params.name);
  const result = await redis.del(req.params.name);
  console.log(result);
  res.json({ result });
});

// Elasticsearch crud

const client = new Client({
  node: process.env.ELASTIC_SEARCH,
});

server.get("/sql/getbulkdata/:gender/:zsign", async (req, res) => {
  const gender = req.params.gender;
  const zsign = req.params.zsign;

  const connection = await pool.getConnection();
  const [data] = await connection.query(
    "SELECT * FROM bulkdata WHERE gender=? AND zodiacSign=? LIMIT 1000 ",
    [gender, zsign]
  );
  connection.release();

  res.json({
    message: "success",
    response: data,
  });
});

server.get("/mongo/getbulkdata/:gender/:zsign", async (req, res) => {
  const zsign = req.params.zsign;
  const gender = req.params.gender;

  let collection = await db.collection("bulkdata");
  const resultdata = await collection
    .find({ gender: gender, zodiacSign: zsign })
    .toArray();
  res.json({ message: "success", response: resultdata });
});

server.get("/elastic/getbulkdata/:gender/:zsign", async (req, res) => {
  const gender = req.params.gender;
  const zsign = req.params.zsign;
  const response = await client.search({
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

server.post("/elastic/createClient", async (req, res) => {
  const index = req.body.username;
  await client.indices.create({ index: index });

  res.json({
    message: "Successfully created index",
  });
});

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
    // const password = faker.internet.password();
    bulkData.push({
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
    const query = `INSERT INTO bulkdata (firstname, lastname, email, gender, zodiacSign, epocTime, date, time) VALUES ?`;
    const connection = await pool.getConnection();
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

server.post("/mongo/createBulk", async (req, res) => {
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
});

server.post("/elastic/createBulk", async (req, res) => {
  let bulkData = [];
  for (let i = 0; i < 100000; i++) {
    const firstname = faker.internet.username();
    const lastname = faker.internet.username();
    const email = faker.internet.email();
    // const password = faker.internet.password();
    const gender = faker.person.sex();
    const zodiacSign = faker.person.zodiacSign();
    const epocTime = new Date().valueOf();
    const date = new Date().toLocaleDateString();
    const time = new Date().toLocaleTimeString();

    bulkData.push({ index: { _index: "rahul-detail" } });
    bulkData.push({
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
    const response = await client.bulk({ body: bulkData });
    if (response.errors) {
      res.json({
        message: "Failed to save some or all documents",
        error: response.errors,
      });
    } else {
      res.json({
        message: "Successfully Added",
        data: response,
      });
    }
  } catch (error) {
    res.json({ message: "Failed to save", error: error });
  }
});

server.post("/elastic/deleteIndex/:index", async (req, res) => {
  const index = req.params.index;
  await client.indices.delete({ index: index });

  res.json({
    message: "Successfully deleted",
  });
});

server.post("/elastic/create", async (req, res) => {
  const { firstname, lastname, email, password } = req.body;

  try {
    const response = await client.index({
      index: "userrahul",
      body: { firstname, lastname, email, password },
    });
    res.json({
      message: "Successfully Added",
      data: response,
    });
  } catch (error) {
    res.json({
      message: "Failed to save",
      error: error,
    });
  }
});

server.get("/elastic/get", async (req, res) => {
  try {
    const response = await client.search({ index: "rahul" });

    res.json({
      message: "Success",
      data: response,
    });
  } catch (error) {
    res.json({
      message: "Failed",
      error: error,
    });
  }
});

server.get("/elastic/get/:index", async (req, res) => {
  const index = req.params.index;

  try {
    const response = await client.search({ index: index });

    res.json({
      message: "Success",
      data: response,
    });
  } catch (error) {
    res.json({
      message: "Failed",
      error: error,
    });
  }
});

server.put("/elastic/update/:index/:id", async (req, res) => {
  const { firstname, lastname, email } = req.body;
  const index = req.params.index;
  const id = req.params.id;

  const data = { firstname, lastname, email };

  const response = await client.update({
    index: index,
    id: id,
    doc: data,
  });

  res.json({
    message: "Successfully Updated",
    data: response,
  });
});

server.del("/elastic/delete/:index/:id", async (req, res) => {
  const index = req.params.index;
  const id = req.params.id;

  await client.delete({
    index: index,
    id: id,
  });

  res.json({
    message: "Successfully Deleted",
  });
});

server.listen(port, process.env.IP_PORT, () => {
  console.log("%s listening at %s", server.name, server.url);
});
