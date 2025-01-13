const mysql = require("mysql2/promise");

const pool = mysql.createPool({
    host: process.env.MYSQL_HOST,
    user: process.env.MYSQL_USER,
    password: process.env.MYSQL_PASSWORD,
    database: process.env.MYSQL_DATABASE,
    port: process.env.MYSQL_PORT,
  });

 const getSqlData =  async (req, res) => {
    try {
      const connection = await pool.getConnection();
      const [data] = await connection.query(`SELECT * FROM users;`);
      connection.release();
  
      res.json({ users: data });
    } catch (err) {
      res.json({ message: err.message });
    }
  }

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
  }

const updateSqlData =  async (req, res) => {
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
  }

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
}

module.exports = {
    pool,
    getSqlData,
    createSqlData,
    updateSqlData,
    deleteSqlData
}