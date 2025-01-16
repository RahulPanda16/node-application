const mysql = require("mysql2/promise");
const { createReport } = require("../utils/createReport");

const pool = mysql.createPool({
  host: process.env.MYSQL_HOST,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD,
  database: process.env.MYSQL_DATABASE,
  port: process.env.MYSQL_PORT,
});

const getSqlData = async (req, res) => {
  try {
    const connection = await pool.getConnection();
    const [data] = await connection.query(`SELECT * FROM users;`);
    connection.release();

    res.json({ users: data });
  } catch (err) {
    res.json({ message: err.message });
  }
};

const createSqlData = async (req, res) => {
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
};

const updateSqlData = async (req, res) => {
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
};

const deleteSqlData = async (req, res) => {
  try {
    const { id } = req.params;
    const connection = await pool.getConnection();
    await connection.query(`DELETE FROM users WHERE id = ?`, [id]);
    connection.release();

    res.json({ message: "Data Deleted Successfully" });
  } catch (err) {
    res.json({ message: err.message });
  }
};

const createSqlBulkData = async (req, res) => {
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
    const connection = await pool.getConnection();
    const query = `INSERT INTO bulkdata (firstname, lastname, email, gender, zodiacSign, epocTime, date, time) VALUES ?`;
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
};

const createSqlReport = async (req, res) => {
  const reportData = createReport();
  try {
    let query = `INSERT INTO reportdata (datetime, reportType, disposeType, disposeName, callDuration, agentName, campaignName, processName, leadsetid, referenceUuid, customerUuid, hold, mute, ringing, transfer, conference, callTime, disposeTime) VALUES ?`;
    let connection = await pool.getConnection();
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
};

const getSqlAllReport = async (req, res) => {
  try {
    let connection = await pool.getConnection();
    const [data] = await connection.query(`SELECT * FROM reportdata`);
    res.send(data);
  } catch (error) {
    res.json({ message: "Error", response: error });
  }
};

const getSqlSummaryReport = async (req, res) => {
  try {
    let connection = await pool.getConnection();

    const [data] = await connection.query(`
      SELECT 
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
        HOUR(datetime)
    `);

    connection.release();
    res.send(data);
  } catch (error) {
    console.error("Error:", error);
    res.send({ code: "Internal", message: error.message });
  }
};

const getSqlBulkData = async (req, res) => {
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
};

module.exports = {
  pool,
  getSqlData,
  createSqlData,
  updateSqlData,
  deleteSqlData,
  createSqlBulkData,
  createSqlReport,
  getSqlAllReport,
  getSqlSummaryReport,
  getSqlBulkData,
};
