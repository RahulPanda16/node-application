require('dotenv').config();
const restify = require("restify");
const bodyParser = require("body-parser");
const connection = require('./config/db');
const Redis = require("ioredis");
const mysql = require("mysql2");
const port = process.env.PORT;
const { v4: uuidv4 } = require('uuid');
const id = uuidv4();


// const connectionSql = mysql.createConnection({
//     host: "localhost",
//     user: "root",
//     password: "",
//     database:'nodecrud',
//     port:process.env.MYSQL_PORT
//   });


const server = restify.createServer();
const redis = new Redis({port:process.env.REDIS_PORT, host:process.env.REDIS_IP,connectTimeout:10000});


let db;
connection().then((database) =>{
    db = database; 
})

server.use(bodyParser.json());

server.get('/mongo/get', async (req, res) => { 
    try { 
        let collection = await db.collection("users"); 
        var data = await collection.find().toArray(); 
        res.json({ 
            success: true, 
            data: data 
        }); 
    } 
    catch (error) { 
        console.error(error); 
        res.json({ 
            success: false, 
            message: "Internal Server Error"
         });
    }
})

server.post('/mongo/create', async (req,res) =>{
    const { firstname, lastname, email, password } = req.body;

    let collection = await db.collection("users");

    await collection.insertOne({
        id,
        firstname,
        lastname,
        email,
        password
    })

    res.json({
        message:"Data Added Successfully"
    })
})

server.put("/mongo/update/:id", async (req, res) => {
    try {
      const id = req.params.id;
      const firstname = req.body.firstname;
      const lastname = req.body.lastname;
      const email = req.body.email;

      let collection = await db.collection("users");
      let data = await collection.updateOne({id:id}, {
        $set: {
            firstname:firstname,
            lastname:lastname,
            email:email
        }
      })
      
      res.json({
        message: "updated",
        data:data
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
      await collection.deleteOne({id:id});

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

server.get("/sql/get", async(req, res) => {
    try {
        const data = await connectionSql.execute(
          `SELECT *  from users;`
        );
        res.json({
          users:data,
        });
      } 
      catch (err) {
        res.json({
          message: err,
        });
      }
});


server.post("/sql/create", async(req,res) => {
    try {
        const firstname = req.body.firstname;
        const lastname = req.body.lastname;
        const email = req.body.email;
        const password = req.body.password;

        const [data] = await connectionSql.execute(
            `INSERT INTO users (fisrtname, lastname, email, password) 
                VALUES (?,?,?,?)`,
            [firstname, lastname, email, password]
          );

          res.json({
            message: "User Created",
            data:data
          });

    } catch (error) {
        res.json({
            message:error,
        })
        console.error(error);
    }
})

server.patch("/sql/update/:id", async (req, res) => {
    try {
      const { id } = req.params;
      const { firstname, lastname, email } = req.body;
      const [update] = await connection
        .execute(
          `UPDATE users set firstname = ?, lastname = ?, email = ? where id = ?`,
          [ firstname, lastname, email ,id]
        );
      res.json({
        message: "updated",
        data:update
      });
    } catch (err) {
      res.json({
        message: err,
      });
    }
  });

server.post("/sql/delete/:id", async (req, res) => {
    try {
      const { id } = req.params;
      await connection
        .execute(
          `DELETE FROM  users where id = ?`,
          [id]
        );
      res.json({
        message: "deleted",
      });
    } catch (err) {
      res.json({
        message: err,
      });
    }
  });


//   REDIS CRUD 
// const userKey = `user:user@gmail.com`;

server.post('/redis/create' , async(req,res) =>{
    const firstname = req.body.firstname;
    const lastname = req.body.lastname;
    const email = req.body.email;
    const password = req.body.password;

    const response = await redis.hset(
            'userRahul',
         {
             "firstname":firstname,
             "lastname":lastname,
             "email":email,
             "password":password
         }
     )
     res.json({
        response,
        message:"User saved successfully"
     })

})

server.get("/redis/get", async(req,res)=>{
   const response = await redis.hgetall('userRahul');
    res.json({
        response
    })
})



server.listen(port,process.env.IP_PORT, ()=>{
    console.log('%s listening at %s', server.name, server.url)
})
